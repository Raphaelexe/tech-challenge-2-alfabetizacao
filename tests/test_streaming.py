"""Testes do streaming simulado. Rodam sem GCP e sem Pub/Sub."""
import json
import random
import re
import string
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from config.settings import RAIZ, carregar
from pipeline.bq import ResultadoQuery
from quality.runner import QualidadeBloqueanteError, carregar_regras
from streaming import micro_lote
from streaming.micro_lote import PASTA_SQL, TABELA_LOTE, janela, processar_lote
from streaming.produtor import DEFEITOS, corromper, gerar_evento, plano_de_envio

S = carregar({"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"})
BASE = {"3550308": 58.1, "2304400": 85.3, "4314902": 44.2}
REGRAS = carregar_regras()


def campos(texto: str) -> set[str]:
    return {c for _, c, _, _ in string.Formatter().parse(texto) if c}


# ---------------------------------------------------------------- produtor

def test_evento_tem_todos_os_campos_e_carimbo_do_produtor():
    e = gerar_evento(random.Random(1), BASE, "2026-11-03T12:00:00.123Z")
    assert set(e) == {"id_evento", "tipo", "id_municipio", "ano", "rede",
                      "taxa_alfabetizacao", "ts_evento", "versao_schema"}
    assert e["ts_evento"] == "2026-11-03T12:00:00.123Z"
    assert e["id_municipio"] in BASE and 0 <= e["taxa_alfabetizacao"] <= 100


def test_plano_injeta_duplicados_e_invalidos_nas_proporcoes():
    msgs = list(plano_de_envio(random.Random(42), BASE, 5000))
    tipos = Counter(m.tipo_envio for m in msgs)
    originais = 5000
    assert 0.02 < sum(tipos[d] for d in DEFEITOS) / originais < 0.04
    assert 0.03 < tipos["duplicado"] / originais < 0.07
    assert set(DEFEITOS) <= set(tipos)  # todo tipo de defeito aparece


def test_duplicado_repete_o_mesmo_id_evento_e_os_mesmos_bytes():
    msgs = list(plano_de_envio(random.Random(7), BASE, 2000))
    por_id = {m.id_evento: m for m in msgs if m.tipo_envio == "valido"}
    dups = [m for m in msgs if m.tipo_envio == "duplicado"]
    assert dups and all(por_id[d.id_evento].dados == d.dados for d in dups)
    # o duplicado sai depois do original
    pos = {id(m): i for i, m in enumerate(msgs)}
    for d in dups:
        original = next(m for m in msgs if m.tipo_envio == "valido" and m.id_evento == d.id_evento)
        assert pos[id(original)] < pos[id(d)]


def test_cada_defeito_produz_a_mensagem_esperada():
    e = gerar_evento(random.Random(3), BASE, "2026-11-03T12:00:00.000Z")
    with pytest.raises(json.JSONDecodeError):
        json.loads(corromper(e, "json_invalido"))
    assert "taxa_alfabetizacao" not in json.loads(corromper(e, "sem_taxa"))
    assert json.loads(corromper(e, "municipio_inexistente"))["id_municipio"] == "9999999"
    assert json.loads(corromper(e, "taxa_fora_0_100"))["taxa_alfabetizacao"] == 130.0


def test_ts_evento_vem_do_relogio_no_momento_da_criacao():
    relogio = iter(f"2026-11-03T12:00:0{i}.000Z" for i in range(10))
    msgs = list(plano_de_envio(random.Random(5), BASE, 5, p_duplicado=0, p_invalido=0,
                               relogio=lambda: next(relogio)))
    assert [json.loads(m.dados)["ts_evento"][-6:] for m in msgs] == [
        f"{i}.000Z" for i in range(5)
    ]


# ---------------------------------------------------------- SQL do micro-lote

def sql(nome):
    return (PASTA_SQL / f"{nome}.sql").read_text(encoding="utf-8")


def test_todo_defeito_do_produtor_tem_motivo_no_validador():
    lote = sql("lote_stg")
    for motivo in set(DEFEITOS.values()):
        assert f"'{motivo}'" in lote, motivo


def test_janela_e_dedup_no_lote_e_contra_a_silver():
    lote = sql("lote_stg")
    assert "publish_time > @desde AND publish_time <= @ate" in lote
    assert "ROW_NUMBER() OVER (PARTITION BY v.id_evento" in lote
    assert "{silver}.eventos_validos" in lote
    assert "TIMESTAMP_DIFF(v.publish_time, v.ts_evento" in lote  # dois relógios diferentes


def test_merges_sao_idempotentes():
    assert "ON t.id_evento = s.id_evento\nWHEN NOT MATCHED THEN INSERT" in sql("merge_validos")
    assert "ON t.message_id = s.message_id\nWHEN NOT MATCHED THEN INSERT" in sql("merge_quarentena")
    for nome in ("merge_validos", "merge_quarentena"):
        assert "WHEN MATCHED" not in sql(nome)


def test_gold_separa_oficial_de_simulado():
    gold = sql("gold_indicador_municipio_atualizado")
    assert "CREATE OR REPLACE VIEW {destino}" in gold
    assert "'simulado' AS origem" in gold and "FROM {gold}.indicador_municipio" in gold
    assert "WHERE origem = 'simulado'" in gold


@pytest.mark.parametrize("arquivo", sorted(PASTA_SQL.glob("*.sql")), ids=lambda a: a.stem)
def test_sql_do_streaming_so_usa_marcadores_conhecidos(arquivo):
    assert campos(arquivo.read_text(encoding="utf-8")) <= {"destino", "bronze", "silver", "gold"}


def test_infra_tem_esquema_da_assinatura_bigquery():
    ddl = (RAIZ / "infra" / "sql" / "streaming.sql").read_text(encoding="utf-8")
    bloco = ddl.split("bronze.eventos_stream` (")[1].split(")")[0]
    assert [l.split()[0] for l in bloco.strip().splitlines()] == [
        "subscription_name", "message_id", "publish_time", "data", "attributes"
    ]
    setup = (RAIZ / "infra" / "setup.sh").read_text(encoding="utf-8")
    assert "--write-metadata" in setup and "gcp-sa-pubsub" in setup


def test_regras_do_lote():
    nomes = {r.nome: r for r in REGRAS if r.tabela == TABELA_LOTE}
    for bloqueante in ("eventos_lote_origem_nao_simulada", "eventos_lote_defeito_nao_detectado"):
        assert nomes[bloqueante].severidade == "bloqueante"


# ------------------------------------------------------------ fluxo

AGORA = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


def test_janela_respeita_margem_e_comeca_na_marca():
    marca = AGORA - timedelta(minutes=5)
    assert janela(marca, AGORA, 30) == (marca, AGORA - timedelta(seconds=30))
    desde, ate = janela(AGORA - timedelta(seconds=10), AGORA, 30)
    assert ate <= desde  # nada novo o bastante para processar


class ClienteLote:
    def __init__(self, lidos=100, reprovar=None, marca=None):
        self.lidos, self.reprovar, self.marca = lidos, reprovar or {}, marca
        self.sqls = []

    def executar(self, sql, params=None, limite_bytes=None, retornar_linhas=True):
        self.sqls.append(sql)
        if "MAX(ate)" in sql:
            return ResultadoQuery("j", 1, 1, 1, None, 0.1, [{"ate": self.marca}])
        if sql.startswith("-- Micro-lote: números"):
            return ResultadoQuery("j", 1, 1, 1, None, 0.1, [{
                "lidos": self.lidos, "validos_novos": 90, "duplicados": 6, "invalidos": 4,
                "latencia_p50_s": 0.12, "latencia_p95_s": 0.41, "latencia_ponta_a_ponta_p95_s": 70.0,
            }])
        if sql.startswith("SELECT\n  ("):
            regras = {r.nome: r for r in REGRAS}
            nomes = [n for n in regras if re.search(rf"\) AS {n},?$", sql, re.M)]
            linha = {n: self.reprovar.get(n, regras[n].limiar) for n in nomes}
            return ResultadoQuery("j", 1, 1, 1, None, 0.1, [linha])
        if "\nMERGE " in sql:
            return ResultadoQuery("j", 1, 1, 1, 90, 0.1)
        return ResultadoQuery("j", 1, 1, 1, None, 0.1)


@pytest.fixture(autouse=True)
def sem_biblioteca_bigquery(monkeypatch):
    monkeypatch.setattr(micro_lote, "_parametros", lambda valores: list(valores.items()))


def rodar(cliente):
    execucoes, qualidade, marcas = [], [], []
    est = processar_lote(cliente, S, execucoes.append, qualidade.extend, marcas.append,
                         REGRAS, agora=AGORA)
    return est, execucoes, qualidade, marcas


def test_lote_completo_grava_latencia_marca_e_atualiza_a_gold():
    cliente = ClienteLote(marca=AGORA - timedelta(minutes=2))
    est, execucoes, qualidade, marcas = rodar(cliente)
    silver, gold = execucoes
    assert silver["etapa"] == "streaming_silver" and silver["status"] == "sucesso"
    assert silver["linhas_entrada"] == 100 and silver["linhas_saida"] == 90
    assert silver["latencia_p50_s"] == 0.12 and silver["latencia_p95_s"] == 0.41
    assert gold["etapa"] == "streaming_gold" and gold["status"] == "sucesso"
    assert marcas[0]["ate"] == (AGORA - timedelta(seconds=30)).isoformat()
    assert len(qualidade) == sum(1 for r in REGRAS if r.tabela == TABELA_LOTE)
    assert sum("\nMERGE " in q for q in cliente.sqls) == 2
    assert any("CREATE OR REPLACE VIEW `meu-projeto.gold.indicador_municipio_atualizado`" in q
               for q in cliente.sqls)


def test_bloqueante_reprovada_nao_faz_merge_nem_avanca_a_marca():
    cliente = ClienteLote(reprovar={"eventos_lote_origem_nao_simulada": 3})
    with pytest.raises(QualidadeBloqueanteError, match="origem_nao_simulada"):
        rodar(cliente)
    assert not any("\nMERGE " in q for q in cliente.sqls)


def test_janela_vazia_nao_consulta_a_bronze():
    cliente = ClienteLote(marca=AGORA - timedelta(seconds=5))
    est, execucoes, _, marcas = rodar(cliente)
    assert est["lidos"] == 0 and marcas == [] and len(execucoes) == 1
    assert not any("eventos_stream" in q for q in cliente.sqls)


def test_lote_sem_mensagens_avanca_a_marca_sem_merge():
    cliente = ClienteLote(lidos=0)
    _, _, qualidade, marcas = rodar(cliente)
    assert len(marcas) == 1 and qualidade == []
    assert not any("\nMERGE " in q for q in cliente.sqls)
