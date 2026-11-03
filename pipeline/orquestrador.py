"""Orquestrador da pipeline: executa as camadas em ordem, uma etapa por tabela.

Cada etapa roda dentro de registrar_etapa (uma linha em ops.execucoes) e com
retentativa para erros transitórios do BigQuery (5xx, 429). Qualquer outro erro, ou
o último erro transitório, encerra a execução com código diferente de zero.

Bronze (append-only), para cada tabela:
1. compara o esquema da fonte com o da tabela bronze (contrato em infra/sql/bronze.sql):
   coluna removida ou com tipo diferente para a carga antes do INSERT;
2. executa pipeline/bronze/<tabela>.sql com dt_ingestao, id_execucao e fonte;
3. confere se as linhas gravadas batem com as linhas da fonte.

Silver e Gold (CREATE OR REPLACE, reconstruíveis a partir da Bronze), para cada tabela:
1. lê a carga Bronze mais recente que terminou com sucesso em ops.execucoes (só ela
   passou pelas travas de esquema e contagem);
2. grava pipeline/<camada>/<tabela>.sql em <camada>.<tabela>_stg;
3. roda as regras de quality/regras.yaml sobre a staging e grava em ops.qualidade;
4. só se nenhuma bloqueante reprovar: copia a staging para <camada>.<tabela> e a remove.
   Se reprovar, a tabela publicada continua a anterior e a staging fica para inspeção.
5. Gold com exportar=True: exporta a tabela publicada em Parquet (Snappy) para
   gs://<GCS_BUCKET>/gold/<tabela>/id_execucao=<id>/, insumo da fase 3. Cada execução
   grava num prefixo novo, então as exportações anteriores continuam disponíveis.

Demonstração de falha: --injetar-falha <cenário> roda scripts/injetar_falha/<cenário>.sql
na staging da tabela do cenário, depois de montada e antes das regras. A Bronze e a Silver
publicada não são tocadas; a regra bloqueante reprova e a execução termina com erro.

Nova camada = arquivos pipeline/<camada>/<tabela>.sql (com {destino} e {fonte}) mais
uma entrada em CAMADAS.

Uso:  python -m pipeline.orquestrador
      python -m pipeline.orquestrador --camadas bronze --tabelas uf municipio
      python -m pipeline.orquestrador --camadas silver --tabelas resultado_municipio \
          --injetar-falha resultado_municipio__rede_desconhecida
"""
from __future__ import annotations

import argparse
import logging
import re
import string
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar

from config.settings import RAIZ, Settings, carregar
from pipeline.bq import ClienteBQ, ResultadoQuery
from pipeline.observabilidade import configurar_logs, rodar_com_alerta
from pipeline.ops import (
    GravadorLote,
    RegistroEtapa,
    gravador_bigquery,
    gravador_lote_bigquery,
    novo_id_execucao,
    registrar_etapa,
)
from quality.runner import Regra, carregar_regras, validar_tabela

log = logging.getLogger("orquestrador")

T = TypeVar("T")

COLUNAS_CONTROLE = {"dt_ingestao", "id_execucao", "fonte"}

# A API de metadados devolve os nomes do SQL legado; o DDL usa os do GoogleSQL.
_TIPOS_EQUIVALENTES = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}


class EsquemaDivergenteError(RuntimeError):
    """A fonte perdeu uma coluna do contrato da Bronze ou mudou o tipo dela."""


class ContagemDivergenteError(RuntimeError):
    """A carga gravou um número de linhas diferente do que a fonte tem."""


class CargaAusenteError(RuntimeError):
    """A Silver precisa de uma tabela Bronze que não tem carga aprovada."""


@dataclass(frozen=True)
class Etapa:
    camada: str
    tabela: str      # tabela de destino, no dataset da camada
    fonte: str = ""  # Bronze: tabela de origem na Base dos Dados
    dataset_fonte: str = "inep"  # chave de Settings.dataset_fonte
    subconjunto: bool = False    # copia só parte das colunas: as demais não geram aviso
    exportar: bool = False       # Gold: exporta em Parquet para o bucket depois de publicar

    @property
    def arquivo(self) -> Path:
        return RAIZ / "pipeline" / self.camada / f"{self.tabela}.sql"


CAMADAS: dict[str, list[Etapa]] = {
    "bronze": [
        # dicionario primeiro: a Silver traduz os códigos das outras tabelas com ele.
        Etapa("bronze", "dicionario", "dicionario"),
        Etapa("bronze", "diretorio_municipio", "municipio", "diretorios", subconjunto=True),
        Etapa("bronze", "meta_brasil", "meta_alfabetizacao_brasil"),
        Etapa("bronze", "meta_uf", "meta_alfabetizacao_uf"),
        Etapa("bronze", "meta_municipio", "meta_alfabetizacao_municipio"),
        Etapa("bronze", "uf", "uf"),
        Etapa("bronze", "municipio", "municipio"),
        Etapa("bronze", "alunos", "alunos"),
    ],
    # Ordem de dependência: dicionario -> rede; diretorio antes de tudo que tem município/UF.
    "silver": [
        Etapa("silver", t) for t in (
            "dicionario", "diretorio_municipio", "rede", "resultado_uf", "resultado_municipio",
            "meta_brasil", "meta_uf", "meta_municipio", "alunos_avaliados",
        )
    ],
    # validacao_microdado lê os microdados uma vez; brasil e features reaproveitam o resultado.
    "gold": [
        Etapa("gold", "indicador_municipio", exportar=True),
        Etapa("gold", "indicador_uf", exportar=True),
        Etapa("gold", "validacao_microdado", exportar=True),
        Etapa("gold", "indicador_brasil", exportar=True),
        Etapa("gold", "evolucao_temporal", exportar=True),
        Etapa("gold", "features_ml", exportar=True),
    ],
}

@dataclass(frozen=True)
class Injecao:
    """Cenário de falha: scripts/injetar_falha/<tabela>__<descricao>.sql."""
    nome: str
    arquivo: Path

    @property
    def tabela(self) -> str:
        return self.nome.split("__", 1)[0]


def cenarios_de_falha() -> dict[str, Injecao]:
    pasta = RAIZ / "scripts" / "injetar_falha"
    return {a.stem: Injecao(a.stem, a) for a in sorted(pasta.glob("*__*.sql"))}


_ID_EXECUCAO = re.compile(r"^(\d{4})(\d{2})(\d{2})T\d{6}Z_[0-9a-f]{6}$")


@dataclass
class Contexto:
    id_execucao: str
    gravar_qualidade: GravadorLote
    regras: list[Regra]
    cargas: dict[str, str] | None = None  # tabela bronze -> id_execucao da carga aprovada
    publicadas: list[str] = field(default_factory=list)
    injecao: Injecao | None = None

    @property
    def dt_ingestao(self) -> str:
        return data_da_execucao(self.id_execucao)


# ---------------------------------------------------------------- funções puras

def verificar_esquema(
    tabela: str, contrato: Iterable[tuple[str, str]], fonte: Iterable[tuple[str, str]]
) -> list[str]:
    """Compara o esquema da fonte com o contrato. Devolve colunas novas (só aviso)."""
    def normalizar(cols):
        return {nome: _TIPOS_EQUIVALENTES.get(tipo, tipo) for nome, tipo in cols}

    esperado = {n: t for n, t in normalizar(contrato).items() if n not in COLUNAS_CONTROLE}
    real = normalizar(fonte)
    problemas = []
    for nome, tipo in esperado.items():
        if nome not in real:
            problemas.append(f"coluna {nome} sumiu da fonte")
        elif real[nome] != tipo:
            problemas.append(f"coluna {nome} mudou de {tipo} para {real[nome]}")
    if problemas:
        raise EsquemaDivergenteError(
            f"{tabela}: " + "; ".join(problemas)
            + ". Revise a mudança e atualize infra/sql/bronze.sql e pipeline/bronze/."
        )
    return sorted(set(real) - set(esperado))


def verificar_contagem(tabela: str, gravadas: int | None, na_fonte: int | None) -> None:
    if gravadas is None or na_fonte is None or gravadas != na_fonte:
        raise ContagemDivergenteError(
            f"{tabela}: a fonte tem {na_fonte} linhas e a carga gravou {gravadas}."
        )


def renderizar(sql: str, destino: str, fonte: str) -> str:
    return sql.format(destino=destino, fonte=fonte)


def data_da_execucao(id_execucao: str) -> str:
    """'20261102T200747Z_3176ea' -> '2026-11-02'. É a dt_ingestao da carga na Bronze."""
    m = _ID_EXECUCAO.match(id_execucao)
    if not m:
        raise ValueError(f"id_execucao fora do formato esperado: {id_execucao!r}")
    return "-".join(m.groups())


def filtro_carga(id_execucao: str) -> str:
    """Filtro SQL de uma carga Bronze: partição (obrigatória em alunos) + execução."""
    return f"dt_ingestao = DATE '{data_da_execucao(id_execucao)}' AND id_execucao = '{id_execucao}'"


def marcadores_silver(s: Settings, cargas: dict[str, str], destino: str) -> dict[str, str]:
    return {
        "destino": destino,
        "bronze": f"`{s.gcp_project}.{s.ds_bronze}`",
        "silver": f"`{s.gcp_project}.{s.ds_silver}`",
        "gold": f"`{s.gcp_project}.{s.ds_gold}`",
        **{f"carga_{t}": filtro_carga(i) for t, i in cargas.items()},
    }


def renderizar_silver(sql: str, marcadores: dict[str, str]) -> str:
    usados = {campo for _, campo, _, _ in string.Formatter().parse(sql) if campo}
    faltando = sorted(usados - set(marcadores))
    if faltando:
        raise CargaAusenteError(
            f"sem carga Bronze aprovada para: {', '.join(faltando)}. Rode a camada bronze."
        )
    return sql.format(**marcadores)


def e_transitorio(exc: BaseException) -> bool:
    try:
        from google.api_core import exceptions as gexc
    except ImportError:
        return False
    return isinstance(exc, (gexc.ServerError, gexc.TooManyRequests))


def com_retentativa(
    fn: Callable[[], T],
    tentativas: int = 3,
    espera_s: float = 5.0,
    transitorio: Callable[[BaseException], bool] = e_transitorio,
    dormir: Callable[[float], None] = time.sleep,
) -> T:
    """Repete fn em erro transitório, com espera exponencial. Outros erros sobem direto."""
    for n in range(1, tentativas + 1):
        try:
            return fn()
        except Exception as exc:
            if n == tentativas or not transitorio(exc):
                raise
            espera = espera_s * 2 ** (n - 1)
            log.warning("erro transitório (%s), tentativa %d/%d em %.0fs", exc, n + 1, tentativas, espera)
            dormir(espera)
    raise AssertionError("inalcançável")


# ----------------------------------------------------------------- execução

def _parametros(valores: dict[str, tuple[str, Any]]) -> list:
    from google.cloud import bigquery

    return [bigquery.ScalarQueryParameter(n, tipo, v) for n, (tipo, v) in valores.items()]


def executar_bronze(
    cliente: ClienteBQ, s: Settings, etapa: Etapa, reg: RegistroEtapa, ctx: Contexto
) -> None:
    fonte_id = f"{s.fonte_projeto}.{s.dataset_fonte(etapa.dataset_fonte)}.{etapa.fonte}"
    destino_id = f"{s.gcp_project}.{s.ds_bronze}.{etapa.tabela}"

    fonte = com_retentativa(lambda: cliente.metadados(fonte_id))
    contrato = com_retentativa(lambda: cliente.metadados(destino_id))
    novas = verificar_esquema(etapa.tabela, contrato.colunas, fonte.colunas)
    if novas and not etapa.subconjunto:
        log.warning("%s: colunas novas na fonte, fora do contrato: %s", etapa.tabela, novas)
    reg.linhas_entrada = fonte.linhas

    params = _parametros({
        "dt_ingestao": ("DATE", ctx.dt_ingestao),
        "id_execucao": ("STRING", reg.id_execucao),
        "fonte": ("STRING", fonte_id),
    })
    destino = s.tabela(s.ds_bronze, etapa.tabela)
    sql = renderizar(
        etapa.arquivo.read_text(encoding="utf-8"), destino, s.fonte(etapa.fonte, etapa.dataset_fonte)
    )
    r: ResultadoQuery = com_retentativa(
        lambda: cliente.executar(sql, params=params, retornar_linhas=False)
    )
    reg.acumular(r)
    gravadas = r.linhas_afetadas

    if not gravadas:
        # 0 linhas: o NOT EXISTS achou esta carga gravada por uma tentativa anterior
        # (o job terminou no servidor, mas a resposta não chegou). Conta o que existe.
        c = cliente.executar(
            f"SELECT COUNT(*) AS n FROM {destino} "
            "WHERE dt_ingestao = @dt_ingestao AND id_execucao = @id_execucao",
            params=params[:2],
        )
        reg.acumular(c)
        gravadas = c.linhas[0]["n"]

    verificar_contagem(etapa.tabela, gravadas, fonte.linhas)
    reg.linhas_saida = gravadas


def cargas_aprovadas(cliente: ClienteBQ, s: Settings) -> dict[str, str]:
    """Carga Bronze mais recente com sucesso, por tabela (o id começa pelo timestamp)."""
    r = cliente.executar(
        "SELECT REGEXP_EXTRACT(etapa, r'^bronze_(.+)$') AS tabela, MAX(id_execucao) AS id_execucao "
        f"FROM {s.tabela(s.ds_ops, 'execucoes')} "
        "WHERE camada = 'bronze' AND status = 'sucesso' GROUP BY 1"
    )
    return {x["tabela"]: x["id_execucao"] for x in r.linhas}


def sql_exportacao(s: Settings, etapa: Etapa, id_execucao: str) -> tuple[str, str]:
    """EXPORT DATA da tabela publicada para o bucket, num prefixo por execução."""
    uri = f"gs://{s.gcs_bucket}/{etapa.camada}/{etapa.tabela}/id_execucao={id_execucao}/*.parquet"
    sql = (
        f"EXPORT DATA OPTIONS (uri = '{uri}', format = 'PARQUET', compression = 'SNAPPY', "
        f"overwrite = false) AS\nSELECT * FROM {s.tabela(getattr(s, f'ds_{etapa.camada}'), etapa.tabela)}"
    )
    return sql, uri


def executar_transformacao(
    cliente: ClienteBQ, s: Settings, etapa: Etapa, reg: RegistroEtapa, ctx: Contexto
) -> None:
    if ctx.cargas is None:  # lido uma vez, depois de a Bronze desta execução (se houver) terminar
        ctx.cargas = cargas_aprovadas(cliente, s)
        log.info("cargas bronze usadas: %s", ctx.cargas)

    dataset = getattr(s, f"ds_{etapa.camada}")
    staging = f"{etapa.tabela}_stg"
    staging_id = f"{s.gcp_project}.{dataset}.{staging}"
    final_id = f"{s.gcp_project}.{dataset}.{etapa.tabela}"
    marcadores = marcadores_silver(s, ctx.cargas, s.tabela(dataset, staging))

    sql = renderizar_silver(etapa.arquivo.read_text(encoding="utf-8"), marcadores)
    reg.acumular(com_retentativa(lambda: cliente.executar(sql, retornar_linhas=False)))
    if ctx.injecao is not None and ctx.injecao.tabela == etapa.tabela:
        log.warning("FALHA INJETADA: %s na staging %s", ctx.injecao.nome, staging_id)
        sql_falha = renderizar_silver(ctx.injecao.arquivo.read_text(encoding="utf-8"), marcadores)
        reg.acumular(cliente.executar(sql_falha, retornar_linhas=False))
    reg.linhas_saida = cliente.metadados(staging_id).linhas

    regras = [r for r in ctx.regras if r.tabela == etapa.tabela]
    validar_tabela(
        cliente, regras, {**marcadores, "tabela": marcadores["destino"]},
        ctx.id_execucao, etapa.camada, ctx.gravar_qualidade, reg,
    )  # levanta QualidadeBloqueanteError: nada abaixo roda, a tabela anterior continua publicada

    reg.job_ids.append(com_retentativa(lambda: cliente.copiar(staging_id, final_id)))
    cliente.remover(staging_id)
    ctx.publicadas.append(final_id)

    if etapa.exportar:
        sql, uri = sql_exportacao(s, etapa, ctx.id_execucao)
        reg.acumular(com_retentativa(lambda: cliente.executar(sql, retornar_linhas=False)))
        log.info("%s exportada para %s", final_id, uri)


EXECUTORES = {
    "bronze": executar_bronze,
    "silver": executar_transformacao,
    "gold": executar_transformacao,
}


def executar(
    cliente: ClienteBQ,
    s: Settings,
    gravar: Callable[[dict[str, Any]], None],
    camadas: list[str],
    tabelas: list[str] | None = None,
    gravar_qualidade: GravadorLote | None = None,
    regras: list[Regra] | None = None,
    injecao: Injecao | None = None,
) -> str:
    if injecao is not None and (
        "silver" not in camadas or (tabelas and injecao.tabela not in tabelas)
    ):
        raise ValueError(f"o cenário {injecao.nome} precisa da camada silver e da tabela {injecao.tabela}")
    ctx = Contexto(
        id_execucao=novo_id_execucao(),
        gravar_qualidade=gravar_qualidade or (lambda _: None),
        regras=carregar_regras() if regras is None else regras,
        injecao=injecao,
    )
    id_execucao = ctx.id_execucao
    log.info("execução %s | camadas %s", id_execucao, ", ".join(camadas))
    for camada in camadas:
        for etapa in CAMADAS[camada]:
            if tabelas and etapa.tabela not in tabelas:
                continue
            with registrar_etapa(gravar, id_execucao, f"{camada}_{etapa.tabela}", camada) as reg:
                EXECUTORES[camada](cliente, s, etapa, reg, ctx)
            log.info(
                "%s.%s ok: %s linhas em %.1fs",
                camada, etapa.tabela, reg.linhas_saida, reg.duracao_s or 0,
            )
    return id_execucao


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--camadas", nargs="+", choices=list(CAMADAS), default=list(CAMADAS))
    parser.add_argument(
        "--tabelas", nargs="+",
        choices=sorted({e.tabela for etapas in CAMADAS.values() for e in etapas}),
        help="limita a execução a estas tabelas de destino",
    )
    cenarios = cenarios_de_falha()
    parser.add_argument(
        "--injetar-falha", choices=sorted(cenarios), metavar="CENARIO",
        help=f"demonstração: altera a staging antes das regras ({', '.join(sorted(cenarios))})",
    )
    args = parser.parse_args()

    s = carregar()
    configurar_logs(s, "orquestrador")
    cliente = ClienteBQ(s)
    gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")
    gravar_qualidade = gravador_lote_bigquery(
        cliente.client, f"{s.gcp_project}.{s.ds_ops}.qualidade"
    )
    id_execucao = executar(
        cliente, s, gravar, args.camadas, args.tabelas, gravar_qualidade=gravar_qualidade,
        injecao=cenarios[args.injetar_falha] if args.injetar_falha else None,
    )
    log.info("pipeline concluída: execução %s", id_execucao)


if __name__ == "__main__":
    rodar_com_alerta("orquestrador", main)
