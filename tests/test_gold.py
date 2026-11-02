"""Testes da Gold. Rodam sem acesso ao GCP."""
import re
import string

import pytest

from config.settings import carregar
from pipeline import orquestrador
from pipeline.orquestrador import CAMADAS, executar, sql_exportacao
from quality.runner import QualidadeBloqueanteError, carregar_regras
from tests.test_silver import BRONZE_TABELAS, ClienteSilver

S = carregar({"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"})
GOLD = CAMADAS["gold"]
REGRAS = carregar_regras()
ENTIDADES = {"resultado_municipio", "resultado_uf", "meta_brasil", "meta_uf",
             "meta_municipio", "alunos_avaliados"}


def sql(tabela: str) -> str:
    return next(e for e in GOLD if e.tabela == tabela).arquivo.read_text(encoding="utf-8")


def campos(texto: str) -> set[str]:
    return {c for _, c, _, _ in string.Formatter().parse(texto) if c}


@pytest.fixture(autouse=True)
def sem_biblioteca_bigquery(monkeypatch):
    monkeypatch.setattr(orquestrador, "_parametros", lambda valores: list(valores.items()))


# ------------------------------------------------------------------ SQL

@pytest.mark.parametrize("etapa", GOLD, ids=lambda e: e.tabela)
def test_sql_gold_e_reconstruivel_e_nao_le_a_bronze(etapa):
    texto = etapa.arquivo.read_text(encoding="utf-8")
    assert "CREATE OR REPLACE TABLE {destino}" in texto
    assert campos(texto) <= {"destino", "silver", "gold"}


def test_gold_le_so_tabelas_gold_publicadas_antes():
    ordem = [e.tabela for e in GOLD]
    for i, etapa in enumerate(GOLD):
        lidas = set(re.findall(r"\{gold\}\.(\w+)", etapa.arquivo.read_text(encoding="utf-8")))
        lidas.discard("INFORMATION_SCHEMA")
        assert lidas <= set(ordem[:i]), (etapa.tabela, lidas)


def test_as_seis_entidades_chegam_a_gold():
    lidas = set()
    for e in GOLD:
        lidas |= set(re.findall(r"\{silver\}\.(\w+)", e.arquivo.read_text(encoding="utf-8")))
    assert ENTIDADES <= lidas


@pytest.mark.parametrize("tabela", ["indicador_municipio", "indicador_uf", "indicador_brasil"])
def test_status_tem_quatro_estados_e_sem_meta_nao_vira_nao_atingiu(tabela):
    texto = sql(tabela)
    bloco = texto[texto.index("CASE\n    WHEN"):texto.index("END AS status_meta")]
    estados = re.findall(r"THEN '(\w+)'|ELSE '(\w+)'", bloco)
    assert {a or b for a, b in estados} == {"sem_resultado", "sem_meta", "atingiu", "nao_atingiu"}
    # sem_resultado e sem_meta são decididos antes da comparação; o ELSE só vê os dois presentes
    assert bloco.index("sem_resultado") < bloco.index("sem_meta") < bloco.index("atingiu")


@pytest.mark.parametrize("tabela", ["indicador_municipio", "indicador_uf", "indicador_brasil"])
def test_meta_pactuada_e_cenario_aspiracional_ficam_separados(tabela):
    texto = sql(tabela)
    assert "distancia_meta_pactuada" in texto
    assert "ROUND(100 - j.taxa_alfabetizacao, 2) AS distancia_cenario_aspiracional" in texto or \
           "ROUND(100 - m.taxa_alfabetizacao, 2) AS distancia_cenario_aspiracional" in texto
    assert "'oficial' AS origem" in texto


def test_comparacao_com_meta_e_no_mesmo_recorte_de_rede():
    for tabela in ("indicador_municipio", "indicador_uf"):
        assert "m.rede_padrao = r.rede_padrao" in sql(tabela)


def test_brasil_nomeia_media_simples_como_tal():
    texto = sql("indicador_brasil")
    assert "AVG(u.taxa_alfabetizacao), 2) AS media_simples_ufs" in texto
    assert "media_ponderada_ufs" in texto


def test_validacao_usa_avaliados_com_peso():
    texto = sql("validacao_microdado")
    assert "SUM(a.peso_aluno * a.alfabetizado)" in texto and "SUM(a.peso_aluno)" in texto
    assert "{silver}.alunos_avaliados" in texto


def test_df_vem_do_unico_municipio_sem_codigo_fixo():
    texto = sql("indicador_uf")
    assert "HAVING COUNT(*) = 1" in texto
    assert "5300108" not in texto and "'DF'" not in texto


def test_features_sem_vazamento_de_alvo():
    texto = sql("features_ml")
    select = texto[texto.rindex("SELECT\n  r.ano AS ano_base"):]
    for proibido in ("status", "distancia", "faixa"):
        assert proibido not in select
    assert "proximo.taxa_alfabetizacao AS alvo_taxa_alfabetizacao_proximo_ano" in select
    assert "proximo.ano = r.ano + 1" in select


# ------------------------------------------------------------- regras

def test_joins_resultado_meta_tem_limiar_medido():
    r = {x.nome: x for x in REGRAS}
    assert r["indicador_municipio_match_meta_resultado"].limiar == 0.99
    assert r["indicador_municipio_match_meta_resultado"].severidade == "bloqueante"
    assert r["indicador_uf_cobertura_publica_2023"].limiar == 24
    assert r["indicador_uf_cobertura_publica_2024"].limiar == 26
    assert r["features_ml_coluna_com_vazamento"].severidade == "bloqueante"


# ------------------------------------------------------------- fluxo

def test_gold_publica_e_exporta_cada_tabela():
    cliente, execucoes = ClienteSilver(), []
    executar(cliente, S, execucoes.append, ["gold"], gravar_qualidade=lambda _: None)
    assert [e["etapa"] for e in execucoes] == [f"gold_{e.tabela}" for e in GOLD]
    assert cliente.copias[0] == ("meu-projeto.gold.indicador_municipio_stg",
                                 "meu-projeto.gold.indicador_municipio")
    exportacoes = [q for q in cliente.sqls if q.startswith("EXPORT DATA")]
    assert len(exportacoes) == len(GOLD)
    assert "gs://meu-bucket/gold/features_ml/id_execucao=" in exportacoes[-1]


def test_regra_bloqueante_na_gold_nao_publica_nem_exporta():
    cliente = ClienteSilver(reprovar={"indicador_municipio_match_meta_resultado": 0.5})
    with pytest.raises(QualidadeBloqueanteError, match="match_meta_resultado"):
        executar(cliente, S, lambda _: None, ["gold"], gravar_qualidade=lambda _: None)
    assert cliente.copias == []
    assert not any(q.startswith("EXPORT DATA") for q in cliente.sqls)


def test_exportacao_parquet_snappy_em_prefixo_por_execucao():
    etapa = next(e for e in GOLD if e.tabela == "features_ml")
    texto, uri = sql_exportacao(S, etapa, "20261102T200747Z_3176ea")
    assert uri == "gs://meu-bucket/gold/features_ml/id_execucao=20261102T200747Z_3176ea/*.parquet"
    assert "format = 'PARQUET'" in texto and "compression = 'SNAPPY'" in texto
    assert "overwrite = false" in texto
    assert texto.endswith("SELECT * FROM `meu-projeto.gold.features_ml`")
