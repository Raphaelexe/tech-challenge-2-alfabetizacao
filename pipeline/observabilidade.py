"""Logs da pipeline no Cloud Logging, base do alerta por e-mail.

O terminal recebe tudo (INFO para cima). O Cloud Logging recebe só WARNING para cima,
no log "pipeline-alfabetizacao" com o label pipeline=alfabetizacao: é o que a política
de alerta (scripts/criar_alerta.py) observa, e manter INFO fora evita pagar ingestão de
log sem necessidade. O envio é síncrono, para que o erro chegue antes de o processo sair.

Falhas que acontecem fora de registrar_etapa (configuração, permissão) também precisam
alertar: rodar_com_alerta registra qualquer exceção como CRITICAL e a propaga de novo.

LOG_NUVEM=0 desliga o envio (útil sem credenciais).
"""
from __future__ import annotations

import logging
import os
from typing import Callable

from config.settings import Settings

NOME_LOG = "pipeline-alfabetizacao"
LABEL_PIPELINE = "alfabetizacao"
FORMATO = "%(asctime)s %(levelname)s %(message)s"

log = logging.getLogger("observabilidade")


def filtro_alerta(projeto: str) -> str:
    """Filtro do Cloud Logging que a política de alerta usa."""
    return (
        f'logName="projects/{projeto}/logs/{NOME_LOG}" '
        f'AND labels.pipeline="{LABEL_PIPELINE}" AND severity>=ERROR'
    )


def configurar_logs(s: Settings, processo: str) -> None:
    logging.basicConfig(level=logging.INFO, format=FORMATO)
    if os.environ.get("LOG_NUVEM", "1") == "0":
        log.warning("LOG_NUVEM=0: erros não vão para o Cloud Logging e não disparam alerta")
        return
    try:
        import google.cloud.logging
        from google.cloud.logging.handlers import CloudLoggingHandler
        from google.cloud.logging.handlers.transports import SyncTransport

        cliente = google.cloud.logging.Client(project=s.gcp_project)
        handler = CloudLoggingHandler(
            cliente, name=NOME_LOG, transport=SyncTransport,
            labels={"pipeline": LABEL_PIPELINE, "processo": processo},
        )
    except Exception as exc:  # sem Cloud Logging a pipeline roda, mas o aviso precisa aparecer
        log.error("Cloud Logging indisponível (%s): falhas desta execução NÃO vão alertar", exc)
        return
    handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(handler)


def rodar_com_alerta(processo: str, principal: Callable[[], None]) -> None:
    try:
        principal()
    except SystemExit as exc:
        if exc.code not in (0, None):
            logging.getLogger(processo).critical("%s terminou com código %s", processo, exc.code)
        raise
    except BaseException as exc:
        logging.getLogger(processo).critical(
            "%s falhou: %s: %s", processo, type(exc).__name__, exc, exc_info=True
        )
        raise
