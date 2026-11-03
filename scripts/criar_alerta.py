"""Cria o alerta por e-mail da pipeline no Cloud Monitoring (idempotente).

1. canal de notificação do tipo e-mail para ALERT_EMAIL (reaproveita se já existir);
2. política de alerta baseada em log: qualquer entrada de severidade ERROR ou maior no log
   pipeline-alfabetizacao (pipeline/observabilidade.py) abre um incidente e manda e-mail.
   Limite de 1 notificação a cada 5 minutos, para uma falha em cascata não lotar a caixa.

Usa a API REST do Cloud Monitoring com as credenciais do gcloud (ADC).
Uso:  python -m scripts.criar_alerta
"""
from __future__ import annotations

import logging
from typing import Any

from config.settings import carregar
from pipeline.observabilidade import NOME_LOG, filtro_alerta

log = logging.getLogger("criar_alerta")

API = "https://monitoring.googleapis.com/v3"
NOME_POLITICA = "Pipeline alfabetização: falha"


def montar_canal(email: str) -> dict[str, Any]:
    return {
        "type": "email",
        "displayName": f"Pipeline alfabetização: {email}",
        "labels": {"email_address": email},
    }


def montar_politica(projeto: str, canal: str) -> dict[str, Any]:
    return {
        "displayName": NOME_POLITICA,
        "combiner": "OR",
        "conditions": [{
            "displayName": f"Log de ERROR em {NOME_LOG}",
            "conditionMatchedLog": {"filter": filtro_alerta(projeto)},
        }],
        "alertStrategy": {
            "notificationRateLimit": {"period": "300s"},
            "autoClose": "1800s",
        },
        "notificationChannels": [canal],
        "documentation": {
            "mimeType": "text/markdown",
            "content": (
                "Uma etapa da pipeline de alfabetização falhou.\n\n"
                "1. Veja a mensagem de erro neste incidente (log `pipeline-alfabetizacao`).\n"
                "2. Consulte `ops.execucoes WHERE status = 'falhou'` e, se for regra de "
                "qualidade, `ops.qualidade WHERE status = 'reprovada'`.\n"
                "3. A camada seguinte não foi publicada: a versão anterior continua no ar."
            ),
        },
        "severity": "ERROR",
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    s = carregar()
    if not s.alert_email:
        raise SystemExit("Defina ALERT_EMAIL no .env")
    credenciais, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    http = AuthorizedSession(credenciais)
    base = f"{API}/projects/{s.gcp_project}"

    def pedir(metodo: str, url: str, **kw) -> dict[str, Any]:
        r = http.request(metodo, url, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{metodo} {url}: {r.status_code} {r.text[:500]}")
        return r.json()

    canais = pedir("GET", f"{base}/notificationChannels", params={"filter": 'type="email"'})
    canal = next(
        (c["name"] for c in canais.get("notificationChannels", [])
         if c.get("labels", {}).get("email_address") == s.alert_email),
        None,
    )
    if canal:
        log.info("canal de e-mail já existe: %s", canal)
    else:
        canal = pedir("POST", f"{base}/notificationChannels", json=montar_canal(s.alert_email))["name"]
        log.info("canal de e-mail criado: %s", canal)

    politicas = pedir("GET", f"{base}/alertPolicies",
                      params={"filter": f'display_name="{NOME_POLITICA}"'})
    existente = next(iter(politicas.get("alertPolicies", [])), None)
    if existente:
        log.info("política já existe: %s", existente["name"])
    else:
        criada = pedir("POST", f"{base}/alertPolicies", json=montar_politica(s.gcp_project, canal))
        log.info("política criada: %s", criada["name"])
    log.info("filtro observado: %s", filtro_alerta(s.gcp_project))


if __name__ == "__main__":
    main()
