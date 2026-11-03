"""Cliente BigQuery com as travas de custo da pipeline.

Toda query passa por aqui:
1. dry run para estimar bytes antes de executar;
2. aborta se a estimativa passar do teto (MAX_BYTES_BILLED);
3. executa com maximum_bytes_billed como segunda trava (no próprio BigQuery);
4. devolve bytes estimados, processados e faturados para o registro em ops.execucoes.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from config.settings import Settings

log = logging.getLogger(__name__)


class CustoAcimaDoLimiteError(RuntimeError):
    """A estimativa do dry run ultrapassou o teto de bytes configurado."""


def verificar_limite(bytes_estimados: int, limite: int) -> None:
    """Levanta erro se a query fosse escanear mais que o limite."""
    if bytes_estimados > limite:
        raise CustoAcimaDoLimiteError(
            f"Query escanearia {formatar_bytes(bytes_estimados)}, "
            f"acima do limite de {formatar_bytes(limite)}. "
            "Selecione menos colunas ou filtre a partição."
        )


def formatar_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    valor = float(n)
    for unidade in ("B", "KiB", "MiB", "GiB", "TiB"):
        if valor < 1024 or unidade == "TiB":
            return f"{valor:.1f} {unidade}" if unidade != "B" else f"{int(valor)} B"
        valor /= 1024
    return f"{valor:.1f} TiB"


@dataclass(frozen=True)
class MetadadosTabela:
    linhas: int | None
    colunas: list[tuple[str, str]]  # (nome, tipo) na ordem da tabela


@dataclass
class ResultadoQuery:
    job_id: str
    bytes_estimados: int
    bytes_processados: int | None
    bytes_faturados: int | None
    linhas_afetadas: int | None
    duracao_s: float
    linhas: list[dict[str, Any]] = field(default_factory=list)


class ClienteBQ:
    def __init__(self, settings: Settings, client: Any = None):
        self.settings = settings
        if client is None:
            from google.cloud import bigquery

            client = bigquery.Client(
                project=settings.gcp_project, location=settings.bq_location
            )
        self.client = client

    def _config(self, params: list | None, **kwargs: Any):
        from google.cloud import bigquery

        return bigquery.QueryJobConfig(query_parameters=params or [], **kwargs)

    def metadados(self, tabela_id: str) -> MetadadosTabela:
        """Linhas e esquema pela API de metadados (não é query, não custa nada).

        tabela_id no formato projeto.dataset.tabela, sem crases.
        """
        t = self.client.get_table(tabela_id)
        return MetadadosTabela(
            linhas=t.num_rows, colunas=[(f.name, f.field_type) for f in t.schema]
        )

    def armazenamento(self, dataset: str) -> list[dict[str, Any]]:
        """Bytes armazenados de cada tabela do dataset, pela API de metadados (sem custo).

        Alternativa a INFORMATION_SCHEMA.TABLE_STORAGE, que exige permissão no nível da região.
        """
        linhas = []
        for item in self.client.list_tables(f"{self.settings.gcp_project}.{dataset}"):
            t = self.client.get_table(item.reference)
            p = t._properties
            linhas.append({
                "dataset": dataset,
                "tabela": t.table_id,
                "tipo": t.table_type,
                "linhas": t.num_rows,
                "bytes_logicos": t.num_bytes,
                "bytes_logicos_longo_prazo": int(p.get("numLongTermBytes", 0) or 0),
                "bytes_fisicos": int(p["numPhysicalBytes"]) if p.get("numPhysicalBytes") else None,
            })
        return linhas

    def copiar(self, origem_id: str, destino_id: str) -> str:
        """Copy job substituindo o destino (gratuito: não escaneia bytes). Devolve o job id."""
        from google.cloud import bigquery

        config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        job = self.client.copy_table(origem_id, destino_id, job_config=config)
        job.result()
        return job.job_id

    def remover(self, tabela_id: str) -> None:
        self.client.delete_table(tabela_id, not_found_ok=True)

    def estimar(self, sql: str, params: list | None = None) -> int:
        """Dry run: devolve quantos bytes a query escanearia. Não custa nada."""
        job = self.client.query(
            sql, job_config=self._config(params, dry_run=True, use_query_cache=False)
        )
        return int(job.total_bytes_processed or 0)

    def executar(
        self,
        sql: str,
        params: list | None = None,
        limite_bytes: int | None = None,
        retornar_linhas: bool = True,
    ) -> ResultadoQuery:
        limite = limite_bytes or self.settings.max_bytes_billed
        estimado = self.estimar(sql, params)
        verificar_limite(estimado, limite)

        inicio = time.monotonic()
        job = self.client.query(
            sql, job_config=self._config(params, maximum_bytes_billed=limite)
        )
        linhas_iter = job.result()  # espera; levanta exceção se a query falhar
        linhas = [dict(r.items()) for r in linhas_iter] if retornar_linhas else []
        duracao = time.monotonic() - inicio

        resultado = ResultadoQuery(
            job_id=job.job_id,
            bytes_estimados=estimado,
            bytes_processados=job.total_bytes_processed,
            bytes_faturados=job.total_bytes_billed,
            linhas_afetadas=job.num_dml_affected_rows,
            duracao_s=round(duracao, 3),
            linhas=linhas,
        )
        log.info(
            "query ok job=%s estimado=%s faturado=%s duracao=%.1fs",
            resultado.job_id,
            formatar_bytes(estimado),
            formatar_bytes(resultado.bytes_faturados),
            duracao,
        )
        return resultado
