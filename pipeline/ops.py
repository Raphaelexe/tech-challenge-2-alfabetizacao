"""Registro de execuções em ops.execucoes.

Uso:
    with registrar_etapa(gravador, id_exec, "perfil_fonte", "fonte") as reg:
        r = cliente.executar(sql)
        reg.acumular(r)

Se o bloco levantar exceção, a linha é gravada com status 'falhou' e a exceção
é propagada de novo — nunca engolida.
"""
from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

log = logging.getLogger(__name__)

Gravador = Callable[[dict[str, Any]], None]
GravadorLote = Callable[[list[dict[str, Any]]], None]


def novo_id_execucao() -> str:
    agora = datetime.now(timezone.utc)
    return f"{agora:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"


@dataclass
class RegistroEtapa:
    id_execucao: str
    etapa: str
    camada: str | None
    inicio: datetime
    fim: datetime | None = None
    duracao_s: float | None = None
    linhas_entrada: int | None = None
    linhas_saida: int | None = None
    bytes_estimados: int = 0
    bytes_processados: int = 0
    bytes_faturados: int = 0
    job_ids: list[str] = field(default_factory=list)
    status: str = "em_andamento"
    mensagem_erro: str | None = None
    latencia_p50_s: float | None = None
    latencia_p95_s: float | None = None

    def acumular(self, resultado: Any) -> None:
        """Soma bytes e job id de um ResultadoQuery a esta etapa."""
        self.job_ids.append(resultado.job_id)
        self.bytes_estimados += resultado.bytes_estimados or 0
        self.bytes_processados += resultado.bytes_processados or 0
        self.bytes_faturados += resultado.bytes_faturados or 0

    def como_linha(self) -> dict[str, Any]:
        linha = asdict(self)
        for chave in ("inicio", "fim"):
            if linha[chave] is not None:
                linha[chave] = linha[chave].isoformat()
        return linha


@contextmanager
def registrar_etapa(
    gravar: Gravador, id_execucao: str, etapa: str, camada: str | None = None
) -> Iterator[RegistroEtapa]:
    reg = RegistroEtapa(
        id_execucao=id_execucao,
        etapa=etapa,
        camada=camada,
        inicio=datetime.now(timezone.utc),
    )
    erro: BaseException | None = None
    try:
        yield reg
        reg.status = "sucesso"
    except BaseException as exc:
        erro = exc
        reg.status = "falhou"
        reg.mensagem_erro = f"{type(exc).__name__}: {exc}"[:2000]
        log.error("etapa %s falhou: %s", etapa, reg.mensagem_erro)
        raise
    finally:
        reg.fim = datetime.now(timezone.utc)
        reg.duracao_s = round((reg.fim - reg.inicio).total_seconds(), 3)
        try:
            gravar(reg.como_linha())
        except Exception:
            log.exception("não foi possível gravar ops.execucoes para %s", etapa)
            if erro is None:
                # Sem erro original: a falha de observabilidade precisa aparecer.
                raise


def gravador_bigquery(client: Any, tabela_id: str) -> Gravador:
    """Grava a linha por load job (gratuito e compatível com o sandbox do BigQuery).

    tabela_id no formato projeto.dataset.tabela, sem crases.
    """
    from google.cloud import bigquery

    esquema = client.get_table(tabela_id).schema

    def gravar(linha: dict[str, Any]) -> None:
        config = bigquery.LoadJobConfig(
            schema=esquema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client.load_table_from_json([linha], tabela_id, job_config=config).result()

    return gravar


def gravador_lote_bigquery(client: Any, tabela_id: str) -> GravadorLote:
    """Como gravador_bigquery, mas grava várias linhas num único load job."""
    from google.cloud import bigquery

    esquema = client.get_table(tabela_id).schema

    def gravar(linhas: list[dict[str, Any]]) -> None:
        if not linhas:
            return
        config = bigquery.LoadJobConfig(
            schema=esquema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        client.load_table_from_json(linhas, tabela_id, job_config=config).result()

    return gravar
