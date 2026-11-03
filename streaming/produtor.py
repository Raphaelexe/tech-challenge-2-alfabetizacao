"""Produtor do streaming simulado: publica no Pub/Sub atualizações de indicador.

Cada evento é uma "nova medição" de 2025 para um município real, partindo da taxa
municipal oficial de 2024 (gold.indicador_municipio) mais um ruído. Os números são
SIMULADOS: toda mensagem leva o atributo origem=simulado, e a Gold os separa do oficial
pela coluna origem.

Para exercitar o micro-lote, o produtor gera de propósito:
- duplicados (~5%): a mesma mensagem publicada de novo, com o mesmo id_evento, como faz
  um produtor que reenvia depois de um timeout (entrega "pelo menos uma vez");
- inválidos (~3%): JSON quebrado, campo obrigatório ausente, tipo desconhecido,
  município inexistente ou taxa fora de 0-100.

ts_evento é gravado no momento em que o evento é criado, pelo relógio do produtor. O
momento da ingestão é o publish_time, atribuído pelo servidor do Pub/Sub. São carimbos
diferentes, então a latência medida não é zero por construção.

Uso:  python -m streaming.produtor --eventos-por-segundo 5 --duracao-s 120
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from config.settings import Settings, carregar
from pipeline.bq import ClienteBQ
from pipeline.observabilidade import configurar_logs, rodar_com_alerta
from pipeline.ops import gravador_bigquery, novo_id_execucao, registrar_etapa

log = logging.getLogger("produtor")

ANO_SIMULADO = 2025
# Cada tipo de defeito corresponde a um motivo de quarentena em streaming/sql/lote_stg.sql.
DEFEITOS = {
    "json_invalido": "json_invalido",
    "sem_taxa": "campo_obrigatorio_ausente",
    "tipo_desconhecido": "tipo_desconhecido",
    "municipio_inexistente": "municipio_inexistente",
    "taxa_fora_0_100": "taxa_fora_0_100",
}


@dataclass(frozen=True)
class Mensagem:
    dados: bytes
    id_evento: str | None
    tipo_envio: str  # 'valido', 'duplicado' ou o nome do defeito


def agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def gerar_evento(rng: random.Random, base: dict[str, float], ts_evento: str) -> dict:
    id_municipio = rng.choice(sorted(base))
    taxa = min(100.0, max(0.0, base[id_municipio] + rng.gauss(3.0, 4.0)))
    return {
        "id_evento": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "tipo": "atualizacao_indicador",
        "id_municipio": id_municipio,
        "ano": ANO_SIMULADO,
        "rede": "municipal",
        "taxa_alfabetizacao": round(taxa, 2),
        "ts_evento": ts_evento,
        "versao_schema": 1,
    }


def corromper(evento: dict, defeito: str) -> bytes:
    e = dict(evento)
    if defeito == "json_invalido":
        return json.dumps(e)[:-7].encode()  # corta o fim: JSON que não fecha
    if defeito == "sem_taxa":
        e.pop("taxa_alfabetizacao")
    elif defeito == "tipo_desconhecido":
        e["tipo"] = "atualizacao_desconhecida"
    elif defeito == "municipio_inexistente":
        e["id_municipio"] = "9999999"
    elif defeito == "taxa_fora_0_100":
        e["taxa_alfabetizacao"] = 130.0
    else:
        raise ValueError(defeito)
    return json.dumps(e).encode()


def plano_de_envio(
    rng: random.Random,
    base: dict[str, float],
    n_eventos: int,
    p_duplicado: float = 0.05,
    p_invalido: float = 0.03,
    relogio=agora_iso,
) -> Iterator[Mensagem]:
    """Gera as mensagens na ordem de envio. Duplicados saem algumas posições depois do original."""
    pendentes: list[tuple[int, Mensagem]] = []  # (posição em que sai, mensagem)
    for i in range(n_eventos):
        evento = gerar_evento(rng, base, relogio())
        if rng.random() < p_invalido:
            defeito = rng.choice(sorted(DEFEITOS))
            yield Mensagem(corromper(evento, defeito), evento["id_evento"], defeito)
        else:
            msg = Mensagem(json.dumps(evento).encode(), evento["id_evento"], "valido")
            yield msg
            if rng.random() < p_duplicado:
                pendentes.append((i + rng.randint(1, 20), Mensagem(msg.dados, msg.id_evento, "duplicado")))
                pendentes.sort(key=lambda p: p[0])
        while pendentes and pendentes[0][0] <= i:
            yield pendentes.pop(0)[1]
    for _, msg in pendentes:
        yield msg


def taxas_base(cliente: ClienteBQ, s: Settings) -> dict[str, float]:
    r = cliente.executar(
        f"SELECT id_municipio, taxa_alfabetizacao FROM {s.tabela(s.ds_gold, 'indicador_municipio')} "
        "WHERE ano = 2024 AND rede_padrao = 'municipal' AND tem_resultado"
    )
    return {x["id_municipio"]: x["taxa_alfabetizacao"] for x in r.linhas}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--eventos-por-segundo", type=float, default=5.0)
    parser.add_argument("--duracao-s", type=float, default=120.0)
    parser.add_argument("--semente", type=int, default=None, help="torna a sequência reproduzível")
    args = parser.parse_args()

    from google.cloud import pubsub_v1

    s = carregar()
    configurar_logs(s, "produtor")
    cliente = ClienteBQ(s)
    gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")
    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(max_messages=50, max_latency=0.05)
    )
    topico = publisher.topic_path(s.gcp_project, s.pubsub_topic)
    n_eventos = int(args.eventos_por_segundo * args.duracao_s)
    intervalo = 1.0 / args.eventos_por_segundo

    with registrar_etapa(gravar, novo_id_execucao(), "streaming_produtor", "streaming") as reg:
        base = taxas_base(cliente, s)
        reg.linhas_entrada = n_eventos
        rng = random.Random(args.semente)
        enviados: Counter[str] = Counter()
        futuros = []
        for msg in plano_de_envio(rng, base, n_eventos):
            futuros.append(publisher.publish(
                topico, msg.dados, origem="simulado", tipo_envio=msg.tipo_envio
            ))
            enviados[msg.tipo_envio] += 1
            time.sleep(intervalo)
        for f in futuros:
            f.result(timeout=60)  # levanta se alguma publicação falhou
        reg.linhas_saida = len(futuros)

    log.info("publicadas %d mensagens em %s: %s", len(futuros), topico, dict(enviados))


if __name__ == "__main__":
    rodar_com_alerta("produtor", main)
