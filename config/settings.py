"""Configuração única do projeto.

Todo identificador de projeto, bucket, tópico ou dataset vem daqui (lido do .env
ou das variáveis de ambiente). Nenhum outro arquivo deve ter esses valores fixos.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent

_IDENTIFICADOR = re.compile(r"^[A-Za-z0-9_\-]+$")


class ConfiguracaoInvalidaError(RuntimeError):
    """Variável obrigatória ausente ou com formato inválido."""


def validar_identificador(valor: str, nome: str) -> str:
    """Garante que um nome usado dentro de SQL (projeto, dataset, tabela) é seguro."""
    if not valor or not _IDENTIFICADOR.match(valor):
        raise ConfiguracaoInvalidaError(
            f"{nome}={valor!r} inválido: use apenas letras, números, '_' e '-'."
        )
    return valor


@dataclass(frozen=True)
class Settings:
    gcp_project: str
    bq_location: str
    gcs_bucket: str
    pubsub_topic: str
    alert_email: str
    fonte_projeto: str
    fonte_dataset: str
    fonte_diretorios: str
    max_bytes_billed: int

    # Datasets do projeto (fixos por convenção da arquitetura medalhão)
    ds_bronze: str = "bronze"
    ds_silver: str = "silver"
    ds_gold: str = "gold"
    ds_ops: str = "ops"

    def tabela(self, dataset: str, tabela: str) -> str:
        """Nome totalmente qualificado de uma tabela do projeto, com crases."""
        validar_identificador(dataset, "dataset")
        validar_identificador(tabela, "tabela")
        return f"`{self.gcp_project}.{dataset}.{tabela}`"

    def dataset_fonte(self, chave: str = "inep") -> str:
        """Dataset da Base dos Dados: 'inep' (alfabetização) ou 'diretorios'."""
        return {"inep": self.fonte_dataset, "diretorios": self.fonte_diretorios}[chave]

    def fonte(self, tabela: str, dataset: str = "inep") -> str:
        """Nome totalmente qualificado de uma tabela da Base dos Dados."""
        validar_identificador(tabela, "tabela")
        return f"`{self.fonte_projeto}.{self.dataset_fonte(dataset)}.{tabela}`"


def carregar(env: dict[str, str] | None = None) -> Settings:
    """Lê a configuração do .env (se existir) e das variáveis de ambiente."""
    if env is None:
        try:
            from dotenv import load_dotenv

            load_dotenv(RAIZ / ".env")
        except ImportError:  # dotenv é opcional quando as variáveis já estão no ambiente
            pass
        env = dict(os.environ)

    faltando = [k for k in ("GCP_PROJECT", "GCS_BUCKET") if not env.get(k)]
    if faltando:
        raise ConfiguracaoInvalidaError(
            f"Variáveis obrigatórias ausentes: {', '.join(faltando)}. "
            "Copie .env.example para .env e preencha."
        )

    try:
        max_bytes = int(env.get("MAX_BYTES_BILLED", str(5 * 1024**3)))
    except ValueError as exc:
        raise ConfiguracaoInvalidaError("MAX_BYTES_BILLED precisa ser um inteiro.") from exc

    return Settings(
        gcp_project=validar_identificador(env["GCP_PROJECT"], "GCP_PROJECT"),
        bq_location=env.get("BQ_LOCATION", "US"),
        gcs_bucket=validar_identificador(env["GCS_BUCKET"], "GCS_BUCKET"),
        pubsub_topic=validar_identificador(
            env.get("PUBSUB_TOPIC", "eventos-alfabetizacao"), "PUBSUB_TOPIC"
        ),
        alert_email=env.get("ALERT_EMAIL", ""),
        fonte_projeto=validar_identificador(
            env.get("FONTE_PROJETO", "basedosdados"), "FONTE_PROJETO"
        ),
        fonte_dataset=validar_identificador(
            env.get("FONTE_DATASET", "br_inep_avaliacao_alfabetizacao"), "FONTE_DATASET"
        ),
        fonte_diretorios=validar_identificador(
            env.get("FONTE_DIRETORIOS", "br_bd_diretorios_brasil"), "FONTE_DIRETORIOS"
        ),
        max_bytes_billed=max_bytes,
    )
