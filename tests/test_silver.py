"""Testes da Silver e das regras de qualidade. Rodam sem acesso ao GCP."""
import re
import string

import pytest

from config.settings import carregar
from pipeline import orquestrador
from pipeline.bq import MetadadosTabela, ResultadoQuery
from pipeline.orquestrador import (
    CAMADAS,
    CargaAusenteError,
    cenarios_de_falha,
    data_da_execucao,
    executar,
    filtro_carga,
    marcadores_silver,
    renderizar_silver,
)
from quality.runner import (
    QualidadeBloqueanteError,
    Regra,
    ResultadoRegra,
    carregar_regras,
    montar_consulta,
    validar_tabela,
)

S = carregar({"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"})
SILVER = CAMADAS["silver"]
BRONZE_TABELAS = [e.tabela for e in CAMADAS["bronze"]]
CARGAS = {t: "20261102T193509Z_1b0fa9" for t in BRONZE_TABELAS}
REGRAS = carregar_regras()


def campos(sql: str) -> set[str]:
    return {c for _, c, _, _ in string.Formatter().parse(sql) if c}


# ------------------------------------------------------------- SQL da Silver

@pytest.mark.parametrize("etapa", SILVER, ids=lambda e: e.tabela)
def test_sql_silver_e_reconstruivel_e_so_usa_marcadores_conhecidos(etapa):
    sql = etapa.arquivo.read_text(encoding="utf-8")
    assert "CREATE OR REPLACE TABLE {destino}" in sql
    permitidos = {"destino", "bronze", "silver"} | {f"carga_{t}" for t in BRONZE_TABELAS}
    assert campos(sql) <= permitidos
    assert "SELECT *" not in sql.upper()


@pytest.mark.parametrize("etapa", SILVER, ids=lambda e: e.tabela)
def test_toda_leitura_da_bronze_filtra_a_carga(etapa):
    sql = etapa.arquivo.read_text(encoding="utf-8")
    for tabela in re.findall(r"\{bronze\}\.(\w+)", sql):
        assert f"{{carga_{tabela}}}" in sql, tabela


@pytest.mark.parametrize("etapa", SILVER, ids=lambda e: e.tabela)
def test_silver_nao_preenche_nulo_com_zero(etapa):
    sql = etapa.arquivo.read_text(encoding="utf-8").upper()
    assert not re.search(r"(COALESCE|IFNULL)\([^()]*,\s*0\s*\)", sql)


def test_silver_le_so_tabelas_silver_ja_publicadas_antes():
    ordem = [e.tabela for e in SILVER]
    for i, etapa in enumerate(SILVER):
        lidas = set(re.findall(r"\{silver\}\.(\w+)", etapa.arquivo.read_text(encoding="utf-8")))
        assert lidas <= set(ordem[:i]), (etapa.tabela, lidas)


def test_rede_mapeia_todos_os_codigos_do_dicionario_e_os_textos_das_metas():
    sql = (SILVER[0].arquivo.parent / "rede.sql").read_text(encoding="utf-8")
    assert set(re.findall(r"WHEN '(\d)' THEN", sql)) == {str(i) for i in range(7)}
    assert "'Pública' AS valor" in sql and "STRUCT('Municipal'" in sql
    assert "ELSE 'desconhecido'" in sql


def test_alunos_avaliados_filtra_presentes_com_caderno():
    sql = (SILVER[0].arquivo.parent / "alunos_avaliados.sql").read_text(encoding="utf-8")
    assert "a.presenca = '1'" in sql and "a.preenchimento_caderno = '1'" in sql
    assert "{carga_alunos}" in sql


# ---------------------------------------------------------- marcadores/cargas

def test_data_da_execucao_e_filtro_de_carga():
    assert data_da_execucao("20261102T200747Z_3176ea") == "2026-11-02"
    assert filtro_carga("20261102T200747Z_3176ea") == (
        "dt_ingestao = DATE '2026-11-02' AND id_execucao = '20261102T200747Z_3176ea'"
    )
    with pytest.raises(ValueError):
        filtro_carga("x' OR '1'='1")


def test_renderizar_silver_sem_carga_falha_com_mensagem_clara():
    m = marcadores_silver(S, {"uf": CARGAS["uf"]}, "`p.silver.x_stg`")
    with pytest.raises(CargaAusenteError, match="carga_municipio"):
        renderizar_silver("SELECT 1 FROM {bronze}.municipio WHERE {carga_municipio}", m)
    sql = renderizar_silver("SELECT 1 FROM {bronze}.uf WHERE {carga_uf}", m)
    assert sql == ("SELECT 1 FROM `meu-projeto.bronze`.uf WHERE dt_ingestao = DATE '2026-11-02' "
                   "AND id_execucao = '20261102T193509Z_1b0fa9'")


# --------------------------------------------------------------- regras.yaml

def test_regras_cobrem_toda_tabela_silver_e_so_elas():
    tabelas = {e.tabela for e in SILVER}
    assert {r.tabela for r in REGRAS} == tabelas


@pytest.mark.parametrize("regra", REGRAS, ids=lambda r: r.nome)
def test_regra_so_usa_marcadores_conhecidos(regra):
    permitidos = {"tabela", "bronze", "silver"} | {f"carga_{t}" for t in BRONZE_TABELAS}
    assert campos(regra.sql) <= permitidos


def test_toda_tabela_silver_tem_regra_bloqueante():
    for e in SILVER:
        assert any(r.tabela == e.tabela and r.severidade == "bloqueante" for r in REGRAS), e.tabela


def test_joins_com_diretorio_tem_taxa_de_correspondencia_bloqueante():
    match = {r.tabela for r in REGRAS if r.nome.endswith("match_diretorio") and r.severidade == "bloqueante"}
    assert match == {"resultado_uf", "resultado_municipio", "meta_uf", "meta_municipio", "alunos_avaliados"}


# -------------------------------------------------------------------- runner

R_IGUAL = Regra("r_igual", "t", "bloqueante", "=", 0, "SELECT COUNT(*) FROM {tabela}")
R_MIN = Regra("r_min", "t", "informativa", ">=", 0.8, "SELECT 0.5")


def test_avaliacao_por_operador_e_nulo_reprova():
    assert ResultadoRegra(R_IGUAL, 0).aprovada
    assert not ResultadoRegra(R_IGUAL, 1).aprovada
    assert ResultadoRegra(R_MIN, 0.9).aprovada
    assert not ResultadoRegra(R_MIN, None).aprovada


def test_regras_da_tabela_viram_uma_query_so():
    sql = montar_consulta([R_IGUAL, R_MIN], {"tabela": "`p.silver.t_stg`"})
    assert sql.count("SELECT") == 3
    assert "(SELECT COUNT(*) FROM `p.silver.t_stg`) AS r_igual" in sql


class ClienteRegras:
    def __init__(self, valores):
        self.valores = valores

    def executar(self, sql, **_):
        return ResultadoQuery("job-q", 1, 1, 1, None, 0.1, [self.valores])


def test_bloqueante_reprovada_grava_tudo_e_levanta():
    gravadas = []
    with pytest.raises(QualidadeBloqueanteError, match="r_igual"):
        validar_tabela(ClienteRegras({"r_igual": 3, "r_min": 0.9}), [R_IGUAL, R_MIN],
                       {"tabela": "t"}, "exec-1", "silver", gravadas.extend)
    assert [g["status"] for g in gravadas] == ["reprovada", "aprovada"]
    assert gravadas[0]["severidade"] == "bloqueante" and gravadas[0]["valor_medido"] == 3


def test_informativa_reprovada_nao_bloqueia():
    gravadas = []
    validar_tabela(ClienteRegras({"r_igual": 0, "r_min": 0.1}), [R_IGUAL, R_MIN],
                   {"tabela": "t"}, "exec-1", "silver", gravadas.extend)
    assert [g["status"] for g in gravadas] == ["aprovada", "reprovada"]


# ------------------------------------------------------ fluxo da Silver

def valor_que_aprova(r: Regra) -> float:
    return r.limiar


class ClienteSilver:
    """Imita ClienteBQ para a Silver: cargas aprovadas, staging, regras, cópia."""

    def __init__(self, reprovar=None):
        self.reprovar = reprovar or {}  # nome da regra -> valor medido
        self.copias, self.removidas, self.sqls = [], [], []

    def executar(self, sql, params=None, limite_bytes=None, retornar_linhas=True):
        self.sqls.append(sql)
        if "REGEXP_EXTRACT(etapa" in sql:
            linhas = [{"tabela": t, "id_execucao": i} for t, i in CARGAS.items()]
            return ResultadoQuery("job-c", 1, 1, 1, None, 0.1, linhas)
        if sql.startswith("SELECT\n  ("):
            nomes = re.findall(r"\) AS (\w+),?$", sql, re.M)
            regras = {r.nome: r for r in REGRAS}
            linha = {n: self.reprovar.get(n, valor_que_aprova(regras[n])) for n in nomes}
            return ResultadoQuery("job-r", 1, 1, 1, None, 0.1, [linha])
        return ResultadoQuery("job-s", 1, 1, 1, None, 0.1)

    def metadados(self, tabela_id):
        return MetadadosTabela(linhas=42, colunas=[])

    def copiar(self, origem, destino):
        self.copias.append((origem, destino))
        return "job-copia"

    def remover(self, tabela_id):
        self.removidas.append(tabela_id)


@pytest.fixture(autouse=True)
def sem_biblioteca_bigquery(monkeypatch):
    monkeypatch.setattr(orquestrador, "_parametros", lambda valores: list(valores.items()))


def test_silver_completa_publica_cada_tabela_depois_das_regras():
    cliente, execucoes, qualidade = ClienteSilver(), [], []
    executar(cliente, S, execucoes.append, ["silver"], gravar_qualidade=qualidade.extend)
    assert [e["etapa"] for e in execucoes] == [f"silver_{e.tabela}" for e in SILVER]
    assert all(e["status"] == "sucesso" and e["linhas_saida"] == 42 for e in execucoes)
    assert cliente.copias[0] == ("meu-projeto.silver.dicionario_stg", "meu-projeto.silver.dicionario")
    assert len(cliente.copias) == len(SILVER) == len(cliente.removidas)
    assert len(qualidade) == len(REGRAS) and all(q["status"] == "aprovada" for q in qualidade)
    assert sum("REGEXP_EXTRACT(etapa" in q for q in cliente.sqls) == 1  # cargas lidas uma vez


def test_regra_bloqueante_reprovada_nao_publica_e_para_a_pipeline():
    cliente = ClienteSilver(reprovar={"resultado_uf_chave_duplicada": 3})
    execucoes, qualidade = [], []
    with pytest.raises(QualidadeBloqueanteError, match="resultado_uf_chave_duplicada"):
        executar(cliente, S, execucoes.append, ["silver"], gravar_qualidade=qualidade.extend)

    publicadas = [d for _, d in cliente.copias]
    assert "meu-projeto.silver.resultado_uf" not in publicadas       # não publicou
    assert "meu-projeto.silver.resultado_uf_stg" not in cliente.removidas  # staging fica
    assert execucoes[-1]["etapa"] == "silver_resultado_uf" and execucoes[-1]["status"] == "falhou"
    assert "resultado_municipio" not in " ".join(e["etapa"] for e in execucoes)  # parou
    assert any(q["regra"] == "resultado_uf_chave_duplicada" and q["status"] == "reprovada"
               for q in qualidade)


def test_informativa_reprovada_publica_mesmo_assim():
    cliente = ClienteSilver(reprovar={"resultado_uf_taxa_nula": 5})
    executar(cliente, S, lambda _: None, ["silver"], tabelas=["resultado_uf"],
             gravar_qualidade=lambda _: None)
    assert ("meu-projeto.silver.resultado_uf_stg", "meu-projeto.silver.resultado_uf") in cliente.copias


def test_sql_da_silver_chega_renderizado_com_a_carga():
    cliente = ClienteSilver()
    executar(cliente, S, lambda _: None, ["silver"], tabelas=["alunos_avaliados"],
             gravar_qualidade=lambda _: None)
    criacao = next(q for q in cliente.sqls if q.lstrip().startswith("--") or "CREATE OR REPLACE" in q)
    assert "CREATE OR REPLACE TABLE `meu-projeto.silver.alunos_avaliados_stg`" in criacao
    assert "FROM `meu-projeto.bronze`.alunos AS a" in criacao
    assert "dt_ingestao = DATE '2026-11-02' AND id_execucao = '20261102T193509Z_1b0fa9'" in criacao
    assert "{" not in criacao


# ------------------------------------------------- demonstração de falha

class ClienteComInjecao(ClienteSilver):
    """A query de regras vê o efeito do SQL injetado (como a staging real veria)."""

    def __init__(self):
        super().__init__()
        self.injetou = False

    def executar(self, sql, **kw):
        if "FALHA_TESTE" in sql or "'9', NULL, 'desconhecido'" in sql:
            self.injetou = True
        if self.injetou and sql.startswith("SELECT\n  ("):
            self.reprovar = {"resultado_municipio_rede_desconhecida": 1}
        return super().executar(sql, **kw)


def test_cenarios_de_falha_apontam_para_tabelas_silver_e_regras_bloqueantes():
    cenarios = cenarios_de_falha()
    assert set(cenarios) == {
        "resultado_municipio__rede_desconhecida", "resultado_municipio__chave_duplicada",
    }
    bloqueantes = {r.nome for r in REGRAS if r.severidade == "bloqueante"}
    for nome, c in cenarios.items():
        assert c.tabela in {e.tabela for e in SILVER}
        assert f"{c.tabela}_{nome.split('__')[1]}" in bloqueantes
        sql = c.arquivo.read_text(encoding="utf-8")
        assert campos(sql) == {"destino"}  # só mexe na staging


def test_falha_injetada_reprova_nao_publica_e_deixa_a_staging():
    cliente, execucoes, qualidade = ClienteComInjecao(), [], []
    cenario = cenarios_de_falha()["resultado_municipio__rede_desconhecida"]
    with pytest.raises(QualidadeBloqueanteError, match="resultado_municipio_rede_desconhecida"):
        executar(cliente, S, execucoes.append, ["silver"], tabelas=["resultado_municipio"],
                 gravar_qualidade=qualidade.extend, injecao=cenario)
    assert cliente.injetou
    injetado = next(q for q in cliente.sqls if "'9', NULL, 'desconhecido'" in q)
    assert "INSERT INTO `meu-projeto.silver.resultado_municipio_stg`" in injetado
    assert cliente.copias == [] and cliente.removidas == []
    assert execucoes[0]["status"] == "falhou"
    assert "QualidadeBloqueanteError" in execucoes[0]["mensagem_erro"]


def test_injecao_exige_a_tabela_do_cenario():
    cenario = cenarios_de_falha()["resultado_municipio__rede_desconhecida"]
    with pytest.raises(ValueError, match="resultado_municipio"):
        executar(ClienteSilver(), S, lambda _: None, ["silver"], tabelas=["resultado_uf"],
                 injecao=cenario)
