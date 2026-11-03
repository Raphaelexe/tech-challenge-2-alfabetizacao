"""Testes dos logs na nuvem e do alerta. Rodam sem GCP."""
import logging

import pytest

from config.settings import carregar
from pipeline.observabilidade import NOME_LOG, configurar_logs, filtro_alerta, rodar_com_alerta
from scripts.criar_alerta import montar_canal, montar_politica

S = carregar({"GCP_PROJECT": "meu-projeto", "GCS_BUCKET": "meu-bucket"})


def test_filtro_do_alerta_pega_so_erros_da_pipeline():
    f = filtro_alerta("meu-projeto")
    assert f'logName="projects/meu-projeto/logs/{NOME_LOG}"' in f
    assert 'labels.pipeline="alfabetizacao"' in f and "severity>=ERROR" in f


def test_politica_baseada_em_log_com_limite_de_notificacao():
    p = montar_politica("meu-projeto", "projects/meu-projeto/notificationChannels/123")
    cond = p["conditions"][0]["conditionMatchedLog"]
    assert cond["filter"] == filtro_alerta("meu-projeto")
    assert p["notificationChannels"] == ["projects/meu-projeto/notificationChannels/123"]
    assert p["alertStrategy"]["notificationRateLimit"]["period"] == "300s"
    assert "ops.execucoes" in p["documentation"]["content"]


def test_canal_de_email():
    assert montar_canal("grupo@exemplo.com") == {
        "type": "email", "displayName": "Pipeline alfabetização: grupo@exemplo.com",
        "labels": {"email_address": "grupo@exemplo.com"},
    }


def test_falha_vira_critical_e_continua_subindo(caplog):
    def principal():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        rodar_com_alerta("teste", principal)
    criticos = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(criticos) == 1 and "teste falhou: ValueError: boom" in criticos[0].getMessage()


def test_saida_normal_nao_alerta(caplog):
    rodar_com_alerta("teste", lambda: None)
    with pytest.raises(SystemExit):
        rodar_com_alerta("teste", lambda: (_ for _ in ()).throw(SystemExit(0)))
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


def test_saida_com_codigo_de_erro_alerta(caplog):
    with pytest.raises(SystemExit):
        rodar_com_alerta("teste", lambda: (_ for _ in ()).throw(SystemExit(2)))
    assert [r for r in caplog.records if r.levelno == logging.CRITICAL]


def test_log_nuvem_desligado_nao_instala_handler(monkeypatch):
    monkeypatch.setenv("LOG_NUVEM", "0")
    antes = list(logging.getLogger().handlers)
    configurar_logs(S, "teste")
    assert logging.getLogger().handlers == antes
