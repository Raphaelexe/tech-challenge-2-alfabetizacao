"""Testes da fase 1. Rodam sem acesso ao GCP (o BigQuery é simulado)."""
from types import SimpleNamespace

import pytest

from config.settings import ConfiguracaoInvalidaError, carregar, validar_identificador
from pipeline.bq import ClienteBQ, CustoAcimaDoLimiteError, formatar_bytes, verificar_limite
from pipeline.ops import registrar_etapa
from scripts.perfil_fonte import (
    agrupar_colunas,
    comparar_valores,
    escolher_colunas_categoricas,
    montar_markdown,
)

ENV = {"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"}


# ------------------------------------------------------------------ settings

def test_carregar_exige_variaveis_obrigatorias():
    with pytest.raises(ConfiguracaoInvalidaError, match="GCP_PROJECT"):
        carregar({"GCS_BUCKET": "b"})


def test_carregar_aplica_padroes():
    s = carregar(ENV)
    assert s.bq_location == "US"
    assert s.fonte("alunos") == "`basedosdados.br_inep_avaliacao_alfabetizacao.alunos`"
    assert s.tabela("ops", "execucoes") == "`meu-projeto.ops.execucoes`"


@pytest.mark.parametrize("valor", ["a`; DROP", "x y", "", "tab.ela"])
def test_identificador_invalido_e_recusado(valor):
    with pytest.raises(ConfiguracaoInvalidaError):
        validar_identificador(valor, "teste")


# ------------------------------------------------------------- trava de custo

def test_verificar_limite_bloqueia_acima_do_teto():
    with pytest.raises(CustoAcimaDoLimiteError):
        verificar_limite(11, 10)
    verificar_limite(10, 10)  # igual ao teto passa


def test_formatar_bytes():
    assert formatar_bytes(512) == "512 B"
    assert formatar_bytes(5 * 1024**3) == "5.0 GiB"
    assert formatar_bytes(None) == "-"


class FakeClient:
    """Imita bigquery.Client.query para dry run e execução."""

    def __init__(self, bytes_scan):
        self.bytes_scan = bytes_scan
        self.executadas = 0

    def query(self, sql, job_config=None):
        if job_config.dry_run:
            return SimpleNamespace(total_bytes_processed=self.bytes_scan)
        self.executadas += 1
        assert job_config.maximum_bytes_billed is not None  # segunda trava sempre ligada
        return SimpleNamespace(
            job_id="job-1", total_bytes_processed=self.bytes_scan,
            total_bytes_billed=self.bytes_scan, num_dml_affected_rows=None,
            result=lambda: [],
        )


@pytest.fixture
def com_bigquery(monkeypatch):
    """Usa um QueryJobConfig mínimo, para os testes não dependerem da biblioteca."""
    import sys
    import types

    fake_bq = types.ModuleType("google.cloud.bigquery")
    fake_bq.QueryJobConfig = lambda **kw: SimpleNamespace(
        dry_run=kw.get("dry_run", False),
        maximum_bytes_billed=kw.get("maximum_bytes_billed"),
    )
    google = sys.modules.get("google") or types.ModuleType("google")
    cloud = sys.modules.get("google.cloud") or types.ModuleType("google.cloud")
    cloud.bigquery = fake_bq
    google.cloud = cloud
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake_bq)


def test_query_cara_nao_e_executada(com_bigquery):
    fake = FakeClient(bytes_scan=100)
    cliente = ClienteBQ(carregar({**ENV, "MAX_BYTES_BILLED": "50"}), client=fake)
    with pytest.raises(CustoAcimaDoLimiteError):
        cliente.executar("SELECT * FROM t")
    assert fake.executadas == 0


def test_query_barata_e_executada(com_bigquery):
    fake = FakeClient(bytes_scan=10)
    cliente = ClienteBQ(carregar({**ENV, "MAX_BYTES_BILLED": "50"}), client=fake)
    r = cliente.executar("SELECT 1")
    assert r.job_id == "job-1" and r.bytes_estimados == 10 and fake.executadas == 1


# ----------------------------------------------------------- ops.execucoes

def test_registro_de_sucesso():
    gravadas = []
    with registrar_etapa(gravadas.append, "exec-1", "etapa_x", "silver") as reg:
        reg.acumular(SimpleNamespace(job_id="j", bytes_estimados=5,
                                     bytes_processados=5, bytes_faturados=10))
    linha = gravadas[0]
    assert linha["status"] == "sucesso"
    assert linha["bytes_faturados"] == 10 and linha["job_ids"] == ["j"]
    assert linha["duracao_s"] >= 0


def test_falha_e_gravada_e_propagada():
    """A exceção não pode ser engolida: grava 'falhou' E levanta de novo."""
    gravadas = []
    with pytest.raises(ValueError, match="boom"):
        with registrar_etapa(gravadas.append, "exec-1", "etapa_x"):
            raise ValueError("boom")
    assert gravadas[0]["status"] == "falhou"
    assert "boom" in gravadas[0]["mensagem_erro"]


def test_falha_ao_gravar_nao_esconde_erro_original():
    def gravador_quebrado(_):
        raise RuntimeError("bq fora")

    with pytest.raises(ValueError):
        with registrar_etapa(gravador_quebrado, "exec-1", "etapa_x"):
            raise ValueError("erro original")


def test_falha_ao_gravar_sem_erro_original_aparece():
    def gravador_quebrado(_):
        raise RuntimeError("bq fora")

    with pytest.raises(RuntimeError, match="bq fora"):
        with registrar_etapa(gravador_quebrado, "exec-1", "etapa_x"):
            pass


# --------------------------------------------------------------- perfil

def test_comparar_valores_aponta_divergencia_de_rede():
    valores = {
        "municipio": {"rede": ["municipal", "publica"]},
        "meta_alfabetizacao_municipio": {"rede": ["Pública"]},
        "uf": {"ano": ["2023", "2024"]},
    }
    comp = comparar_valores(valores)
    assert comp["rede"]["diverge"] is True
    assert comp["rede"]["ausentes_por_tabela"]["meta_alfabetizacao_municipio"] == [
        "municipal", "publica"
    ]
    assert "ano" not in comp  # só uma tabela tem a coluna


def test_comparar_valores_sem_divergencia():
    comp = comparar_valores({"a": {"serie": ["2"]}, "b": {"serie": ["2"]}})
    assert comp["serie"]["diverge"] is False


def test_escolhe_so_colunas_que_existem():
    colunas = agrupar_colunas([
        {"table_name": "uf", "ordinal_position": 2, "column_name": "rede", "data_type": "STRING"},
        {"table_name": "uf", "ordinal_position": 1, "column_name": "ano", "data_type": "INT64"},
        {"table_name": "uf", "ordinal_position": 3, "column_name": "taxa", "data_type": "FLOAT64"},
        {"table_name": "dicionario", "ordinal_position": 1, "column_name": "valor", "data_type": "STRING"},
    ])
    assert [c["coluna"] for c in colunas["uf"]] == ["ano", "rede", "taxa"]
    assert escolher_colunas_categoricas(colunas, {}) == {"uf": ["ano", "rede"]}


def test_markdown_gerado():
    perfil = {
        "data": "2026-11-01", "id_execucao": "x", "fonte": "basedosdados.ds",
        "colunas": {"uf": [{"coluna": "rede", "tipo": "STRING"}]},
        "tamanhos": {"uf": {"linhas": 1234, "bytes": 2048}},
        "valores": {"uf": {"rede": ["publica"]}},
        "n_distintos": {"uf": {"rede": 1}},
        "comparacao": {"rede": {"tabelas": ["uf", "municipio"], "diverge": True,
                                "ausentes_por_tabela": {"uf": ["municipal"]}}},
        "dry_run_alunos": 3 * 1024**3,
    }
    md = montar_markdown(perfil)
    assert "1.234" in md and "3.0 GiB" in md and "`uf` não tem: `municipal`" in md


def test_fonte_do_diretorio_usa_dataset_configuravel():
    s = carregar(ENV)
    assert s.fonte("municipio", "diretorios") == "`basedosdados.br_bd_diretorios_brasil.municipio`"
    s2 = carregar({**ENV, "FONTE_DIRETORIOS": "outro_ds"})
    assert s2.fonte("municipio", "diretorios") == "`basedosdados.outro_ds.municipio`"
