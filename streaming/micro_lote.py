"""Micro-lote do streaming: Bronze (Pub/Sub) -> Silver (válidos / quarentena) -> Gold.

A cada execução:
1. lê a marca d'água (último publish_time processado) em ops.marca_dagua_stream e monta
   a janela (desde, ate]. ate = agora - margem, para não pular mensagem que a assinatura
   BigQuery ainda está gravando;
2. grava a janela interpretada e validada em silver.eventos_lote_stg (streaming/sql/lote_stg.sql);
3. roda as regras de quality/regras.yaml da tabela eventos_lote (ops.qualidade);
4. MERGE dos válidos não duplicados em silver.eventos_validos (por id_evento) e dos
   inválidos em silver.eventos_quarentena (por message_id): reprocessar a mesma janela
   não duplica nada;
5. grava a nova marca d'água e a latência p50/p95 em ops.execucoes;
6. recria a view gold.indicador_municipio_atualizado (oficial + simulado, coluna origem).

Se uma regra bloqueante reprovar, a marca d'água não avança: a próxima execução
reprocessa a mesma janela, e o MERGE garante que nada entra duas vezes.

Uso:  python -m streaming.micro_lote                       (um lote)
      python -m streaming.micro_lote --lotes 10 --intervalo-s 60
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from config.settings import RAIZ, Settings, carregar
from pipeline.bq import ClienteBQ
from pipeline.observabilidade import configurar_logs, rodar_com_alerta
from pipeline.ops import gravador_bigquery, gravador_lote_bigquery, novo_id_execucao, registrar_etapa
from pipeline.orquestrador import marcadores_silver, renderizar_silver
from quality.runner import Regra, carregar_regras, validar_tabela

log = logging.getLogger("micro_lote")

PASTA_SQL = RAIZ / "streaming" / "sql"
TABELA_LOTE = "eventos_lote"  # nome das regras em regras.yaml
EPOCA = datetime(1970, 1, 1, tzinfo=timezone.utc)
MARGEM_PADRAO_S = 30.0


def janela(ultima: datetime | None, agora: datetime, margem_s: float) -> tuple[datetime, datetime]:
    """(desde, ate] da próxima execução. Vazia (ate <= desde) se não passou tempo suficiente."""
    return (ultima or EPOCA), agora - timedelta(seconds=margem_s)


def _sql(nome: str, marcadores: dict[str, str]) -> str:
    return renderizar_silver((PASTA_SQL / f"{nome}.sql").read_text(encoding="utf-8"), marcadores)


def _parametros(valores: dict[str, tuple[str, Any]]) -> list:
    from google.cloud import bigquery

    return [bigquery.ScalarQueryParameter(n, tipo, v) for n, (tipo, v) in valores.items()]


def ultima_marca(cliente: ClienteBQ, s: Settings) -> datetime | None:
    r = cliente.executar(f"SELECT MAX(ate) AS ate FROM {s.tabela(s.ds_ops, 'marca_dagua_stream')}")
    return r.linhas[0]["ate"] if r.linhas else None


def processar_lote(
    cliente: ClienteBQ,
    s: Settings,
    gravar: Callable[[dict[str, Any]], None],
    gravar_qualidade: Callable[[list[dict[str, Any]]], None],
    gravar_marca: Callable[[dict[str, Any]], None],
    regras: list[Regra],
    margem_s: float = MARGEM_PADRAO_S,
    agora: datetime | None = None,
) -> dict[str, Any]:
    id_execucao = novo_id_execucao()
    agora = agora or datetime.now(timezone.utc)
    destino = s.tabela(s.ds_silver, "eventos_lote_stg")
    marcadores = marcadores_silver(s, {}, destino)
    estatisticas: dict[str, Any] = {"lidos": 0}

    with registrar_etapa(gravar, id_execucao, "streaming_silver", "silver") as reg:
        desde, ate = janela(ultima_marca(cliente, s), agora, margem_s)
        if ate <= desde:
            log.info("janela vazia (desde %s, ate %s): nada a processar", desde, ate)
            reg.linhas_entrada = reg.linhas_saida = 0
            return estatisticas

        reg.acumular(cliente.executar(
            _sql("lote_stg", marcadores), retornar_linhas=False,
            params=_parametros({
                "desde": ("TIMESTAMP", desde), "ate": ("TIMESTAMP", ate),
                "ts_processamento": ("TIMESTAMP", agora),
            }),
        ))
        r = cliente.executar(_sql("estatisticas", marcadores))
        reg.acumular(r)
        estatisticas = r.linhas[0]
        reg.linhas_entrada = estatisticas["lidos"]

        if estatisticas["lidos"]:
            validar_tabela(
                cliente, [x for x in regras if x.tabela == TABELA_LOTE],
                {**marcadores, "tabela": destino}, id_execucao, "silver", gravar_qualidade, reg,
            )  # levanta QualidadeBloqueanteError: a marca d'água não avança
            id_param = _parametros({"id_execucao": ("STRING", id_execucao)})
            r = cliente.executar(_sql("merge_validos", marcadores), params=id_param, retornar_linhas=False)
            reg.acumular(r)
            reg.linhas_saida = r.linhas_afetadas
            reg.acumular(cliente.executar(
                _sql("merge_quarentena", marcadores), params=id_param, retornar_linhas=False
            ))
            reg.latencia_p50_s = estatisticas["latencia_p50_s"]
            reg.latencia_p95_s = estatisticas["latencia_p95_s"]
        else:
            reg.linhas_saida = 0

        gravar_marca({
            "id_execucao": id_execucao, "desde": desde.isoformat(), "ate": ate.isoformat(),
            "eventos_lidos": estatisticas["lidos"], "ts_registro": agora.isoformat(),
        })

    with registrar_etapa(gravar, id_execucao, "streaming_gold", "gold") as reg:
        view = s.tabela(s.ds_gold, "indicador_municipio_atualizado")
        reg.acumular(cliente.executar(
            _sql("gold_indicador_municipio_atualizado", {**marcadores, "destino": view}),
            retornar_linhas=False,
        ))

    log.info(
        "lote %s: %s lidos, %s válidos novos, %s duplicados, %s inválidos | latência de "
        "ingestão p50 %.3fs p95 %.3fs | ponta a ponta p95 %.1fs",
        id_execucao, estatisticas["lidos"], estatisticas.get("validos_novos"),
        estatisticas.get("duplicados"), estatisticas.get("invalidos"),
        estatisticas.get("latencia_p50_s") or 0, estatisticas.get("latencia_p95_s") or 0,
        estatisticas.get("latencia_ponta_a_ponta_p95_s") or 0,
    )
    return estatisticas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lotes", type=int, default=1, help="quantos micro-lotes rodar (0 = sem fim)")
    parser.add_argument("--intervalo-s", type=float, default=60.0, help="espera entre micro-lotes")
    parser.add_argument("--margem-s", type=float, default=MARGEM_PADRAO_S,
                        help="não processa mensagens mais novas que isto (gravação em andamento)")
    args = parser.parse_args()

    s = carregar()
    configurar_logs(s, "micro_lote")
    cliente = ClienteBQ(s)
    gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")
    gravar_qualidade = gravador_lote_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.qualidade")
    gravar_marca = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.marca_dagua_stream")
    regras = carregar_regras()

    n = 0
    while True:
        processar_lote(cliente, s, gravar, gravar_qualidade, gravar_marca, regras, args.margem_s)
        n += 1
        if args.lotes and n >= args.lotes:
            break
        time.sleep(args.intervalo_s)


if __name__ == "__main__":
    rodar_com_alerta("micro_lote", main)
