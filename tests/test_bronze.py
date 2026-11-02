"""Testes da Bronze e do orquestrador. Rodam sem acesso ao GCP (BigQuery simulado)."""
import re

import pytest

from config.settings import RAIZ, carregar
from pipeline import orquestrador
from pipeline.bq import MetadadosTabela, ResultadoQuery
from pipeline.orquestrador import (
    CAMADAS,
    COLUNAS_CONTROLE,
    ContagemDivergenteError,
    EsquemaDivergenteError,
    com_retentativa,
    executar,
    renderizar,
    verificar_contagem,
    verificar_esquema,
)

S = carregar({"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"})
BRONZE = CAMADAS["bronze"]


def ddl_bronze() -> dict[str, list[tuple[str, str]]]:
    """infra/sql/bronze.sql -> {tabela: [(coluna, tipo), ...]}"""
    ddl = (RAIZ / "infra" / "sql" / "bronze.sql").read_text(encoding="utf-8")
    tabelas = {}
    for m in re.finditer(r"bronze\.(\w+)` \((.*?)\n\)", ddl, re.S):
        linhas = [l.split() for l in m.group(2).strip().splitlines() if l.strip()]
        tabelas[m.group(1)] = [(l[0], l[1].rstrip(",")) for l in linhas]
    return tabelas


def colunas_do_insert(sql: str) -> tuple[list[str], list[str]]:
    insert = re.search(r"INSERT INTO \{destino\} \((.*?)\)\s*SELECT", sql, re.S).group(1)
    select = re.search(r"SELECT\s+(.*?)\s+FROM \{fonte\}", sql, re.S).group(1)
    limpar = lambda bloco: [c.strip() for c in bloco.split(",") if c.strip()]  # noqa: E731
    return limpar(insert), limpar(select)


# --------------------------------------------------------- contrato e SQL

def test_bronze_cobre_as_sete_tabelas_e_o_diretorio_com_dicionario_primeiro():
    assert BRONZE[0].tabela == "dicionario"
    assert {(e.dataset_fonte, e.fonte) for e in BRONZE} == {
        ("inep", t) for t in (
            "dicionario", "alunos", "municipio", "uf", "meta_alfabetizacao_brasil",
            "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio",
        )
    } | {("diretorios", "municipio")}


def test_ddl_tem_uma_tabela_por_etapa_com_colunas_de_controle():
    ddl = ddl_bronze()
    assert set(ddl) == {e.tabela for e in BRONZE}
    for tabela, cols in ddl.items():
        assert COLUNAS_CONTROLE <= {c for c, _ in cols}, tabela


@pytest.mark.parametrize("etapa", BRONZE, ids=lambda e: e.tabela)
def test_sql_da_bronze_bate_com_o_ddl(etapa):
    sql = etapa.arquivo.read_text(encoding="utf-8")
    inseridas, selecionadas = colunas_do_insert(sql)
    esperadas = [c for c, _ in ddl_bronze()[etapa.tabela]]
    assert inseridas == esperadas
    controle = ["@dt_ingestao", "@id_execucao", "@fonte"]
    assert selecionadas == [c for c in esperadas if c not in COLUNAS_CONTROLE] + controle


@pytest.mark.parametrize("etapa", BRONZE, ids=lambda e: e.tabela)
def test_sql_da_bronze_e_append_e_idempotente(etapa):
    sql = etapa.arquivo.read_text(encoding="utf-8").upper()
    assert "SELECT *" not in sql
    assert "TRUNCATE" not in sql and "OR REPLACE" not in sql and "DELETE" not in sql
    assert "WHERE NOT EXISTS" in sql


def test_alunos_exige_filtro_de_particao():
    ddl = (RAIZ / "infra" / "sql" / "bronze.sql").read_text(encoding="utf-8")
    bloco_alunos = ddl.split("bronze.alunos")[1]
    assert "require_partition_filter = TRUE" in bloco_alunos


def test_renderizar():
    sql = renderizar("INSERT INTO {destino} SELECT a FROM {fonte}", "`p.bronze.uf`", "`f.d.uf`")
    assert sql == "INSERT INTO `p.bronze.uf` SELECT a FROM `f.d.uf`"


# ------------------------------------------------------------ travas

CONTRATO = [("ano", "INT64"), ("rede", "STRING"), ("taxa", "FLOAT64"),
            ("dt_ingestao", "DATE"), ("id_execucao", "STRING"), ("fonte", "STRING")]


def test_esquema_igual_passa_com_nomes_legados_de_tipo():
    assert verificar_esquema("uf", CONTRATO, [("ano", "INTEGER"), ("rede", "STRING"),
                                              ("taxa", "FLOAT")]) == []


def test_coluna_nova_na_fonte_e_so_aviso():
    fonte = [("ano", "INT64"), ("rede", "STRING"), ("taxa", "FLOAT64"), ("extra", "STRING")]
    assert verificar_esquema("uf", CONTRATO, fonte) == ["extra"]


def test_coluna_removida_bloqueia():
    with pytest.raises(EsquemaDivergenteError, match="taxa sumiu"):
        verificar_esquema("uf", CONTRATO, [("ano", "INT64"), ("rede", "STRING")])


def test_tipo_alterado_bloqueia():
    with pytest.raises(EsquemaDivergenteError, match="rede mudou de STRING para INT64"):
        verificar_esquema("uf", CONTRATO, [("ano", "INT64"), ("rede", "INT64"), ("taxa", "FLOAT64")])


def test_contagem():
    verificar_contagem("uf", 145, 145)
    with pytest.raises(ContagemDivergenteError):
        verificar_contagem("uf", 140, 145)
    with pytest.raises(ContagemDivergenteError):
        verificar_contagem("uf", None, 145)


# ---------------------------------------------------------- retentativa

class Transitorio(Exception):
    pass


def test_retentativa_repete_erro_transitorio():
    chamadas, esperas = [], []

    def fn():
        chamadas.append(1)
        if len(chamadas) < 3:
            raise Transitorio()
        return "ok"

    r = com_retentativa(fn, tentativas=3, espera_s=1,
                        transitorio=lambda e: isinstance(e, Transitorio), dormir=esperas.append)
    assert r == "ok" and len(chamadas) == 3 and esperas == [1, 2]


def test_retentativa_nao_repete_erro_definitivo():
    chamadas = []

    def fn():
        chamadas.append(1)
        raise ValueError("sql errado")

    with pytest.raises(ValueError):
        com_retentativa(fn, transitorio=lambda e: isinstance(e, Transitorio), dormir=lambda _: None)
    assert len(chamadas) == 1


def test_retentativa_desiste_e_propaga():
    def fn():
        raise Transitorio()

    with pytest.raises(Transitorio):
        com_retentativa(fn, tentativas=2, transitorio=lambda e: True, dormir=lambda _: None)


# ------------------------------------------------------ fluxo completo

class ClienteFalso:
    """Imita ClienteBQ: metadados da fonte/bronze e execução das cargas."""

    def __init__(self, linhas_fonte=100, gravadas=100, fonte_extra=None, contagem_existente=100):
        self.linhas_fonte, self.gravadas = linhas_fonte, gravadas
        self.fonte_extra = fonte_extra or []
        self.contagem_existente = contagem_existente
        self.sqls = []

    def metadados(self, tabela_id):
        _, dataset, tabela = tabela_id.split(".")
        if tabela_id.startswith("meu-projeto.bronze."):
            return MetadadosTabela(linhas=0, colunas=ddl_bronze()[tabela])
        etapa = next(e for e in BRONZE
                     if e.fonte == tabela and S.dataset_fonte(e.dataset_fonte) == dataset)
        cols = [(c, t) for c, t in ddl_bronze()[etapa.tabela] if c not in COLUNAS_CONTROLE]
        return MetadadosTabela(linhas=self.linhas_fonte, colunas=cols + self.fonte_extra)

    def executar(self, sql, params=None, limite_bytes=None, retornar_linhas=True):
        self.sqls.append(sql)
        if sql.startswith("SELECT COUNT(*)"):
            return ResultadoQuery("job-c", 1, 1, 1, None, 0.1, [{"n": self.contagem_existente}])
        return ResultadoQuery("job-i", 10, 10, 10, self.gravadas, 0.1)


@pytest.fixture(autouse=True)
def sem_biblioteca_bigquery(monkeypatch):
    monkeypatch.setattr(orquestrador, "_parametros", lambda valores: list(valores.items()))


def test_bronze_completa_grava_uma_linha_por_tabela():
    gravadas = []
    executar(ClienteFalso(), S, gravadas.append, ["bronze"])
    assert [g["etapa"] for g in gravadas] == [f"bronze_{e.tabela}" for e in BRONZE]
    assert all(g["status"] == "sucesso" and g["linhas_saida"] == 100 for g in gravadas)
    assert len({g["id_execucao"] for g in gravadas}) == 1


def test_diretorio_le_do_dataset_de_diretorios_sem_avisar_colunas_fora_do_subconjunto(caplog):
    cliente = ClienteFalso(fonte_extra=[("centroide", "GEOGRAPHY")])
    executar(cliente, S, lambda _: None, ["bronze"], tabelas=["diretorio_municipio", "uf"])
    assert "FROM `basedosdados.br_bd_diretorios_brasil.municipio`" in cliente.sqls[0]
    avisos = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(avisos) == 1 and avisos[0].startswith("uf:")  # só a tabela completa avisa


def test_sql_executado_aponta_para_bronze_e_fonte():
    cliente = ClienteFalso()
    executar(cliente, S, lambda _: None, ["bronze"], tabelas=["uf"])
    assert cliente.sqls[0].count("`meu-projeto.bronze.uf`") == 2
    assert "FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`" in cliente.sqls[0]


def test_contagem_divergente_para_a_pipeline():
    gravadas = []
    with pytest.raises(ContagemDivergenteError):
        executar(ClienteFalso(gravadas=90), S, gravadas.append, ["bronze"])
    assert len(gravadas) == 1  # parou no dicionario, nada depois
    assert gravadas[0]["status"] == "falhou"
    assert "ContagemDivergenteError" in gravadas[0]["mensagem_erro"]


def test_esquema_divergente_para_antes_do_insert():
    cliente = ClienteFalso()
    cliente.metadados = lambda tid: (
        MetadadosTabela(0, ddl_bronze()["dicionario"]) if ".bronze." in tid
        else MetadadosTabela(27, [("id_tabela", "STRING")])
    )
    gravadas = []
    with pytest.raises(EsquemaDivergenteError):
        executar(cliente, S, gravadas.append, ["bronze"])
    assert cliente.sqls == [] and gravadas[0]["status"] == "falhou"


def test_retentativa_que_ja_tinha_gravado_confere_pela_contagem():
    """INSERT devolve 0 (NOT EXISTS achou a carga anterior): confere com COUNT(*)."""
    gravadas = []
    executar(ClienteFalso(gravadas=0, contagem_existente=100), S, gravadas.append,
             ["bronze"], tabelas=["uf"])
    assert gravadas[0]["status"] == "sucesso" and gravadas[0]["linhas_saida"] == 100


def test_retentativa_sem_carga_anterior_falha():
    with pytest.raises(ContagemDivergenteError):
        executar(ClienteFalso(gravadas=0, contagem_existente=0), S, lambda _: None,
                 ["bronze"], tabelas=["uf"])
