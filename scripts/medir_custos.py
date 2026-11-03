"""Quantidades medidas para a tabela de custos (FinOps) do README.

Coleta, sem estimar nada:
- bytes faturados e duração da execução bem-sucedida mais recente de cada etapa
  (ops.execucoes), e o total por camada;
- armazenamento lógico e físico de cada tabela, pela API de metadados
  (INFORMATION_SCHEMA.TABLE_STORAGE dá 403 neste projeto);
- volume de mensagens do Pub/Sub, pelo que a assinatura gravou em bronze.eventos_stream;
- tamanho dos arquivos Parquet exportados para o bucket (prefixo gold/).

Saída: docs/perfil/medicoes_custo.json e uma linha em ops.execucoes.
Uso:   python -m scripts.medir_custos
"""
from __future__ import annotations

import json
import logging
from typing import Any

from config.settings import RAIZ, Settings, carregar
from pipeline.bq import ClienteBQ, formatar_bytes
from pipeline.ops import gravador_bigquery, novo_id_execucao, registrar_etapa

log = logging.getLogger("medir_custos")


def consultas(s: Settings) -> dict[str, str]:
    execucoes = s.tabela(s.ds_ops, "execucoes")
    return {
        "ultima_execucao_por_etapa": f"""
            SELECT etapa, camada, id_execucao, inicio, duracao_s, linhas_saida,
                   bytes_estimados, bytes_faturados, ARRAY_LENGTH(job_ids) AS jobs
            FROM {execucoes}
            WHERE status = 'sucesso'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY etapa ORDER BY inicio DESC) = 1
            ORDER BY camada, etapa""",
        "total_por_execucao": f"""
            SELECT id_execucao, MIN(inicio) AS inicio,
                   STRING_AGG(DISTINCT camada ORDER BY camada) AS camadas,
                   COUNT(*) AS etapas, SUM(bytes_faturados) AS bytes_faturados,
                   ROUND(SUM(duracao_s), 1) AS duracao_s,
                   COUNTIF(status = 'falhou') AS falhas
            FROM {execucoes}
            GROUP BY id_execucao ORDER BY inicio""",
        "total_geral": f"""
            SELECT COUNT(DISTINCT id_execucao) AS execucoes, COUNT(*) AS etapas,
                   SUM(bytes_faturados) AS bytes_faturados,
                   MIN(inicio) AS primeira, MAX(inicio) AS ultima
            FROM {execucoes}""",
        "pubsub": f"""
            SELECT COUNT(*) AS mensagens, SUM(BYTE_LENGTH(data)) AS bytes_dados,
                   SUM(BYTE_LENGTH(data) + IFNULL(BYTE_LENGTH(attributes), 0)) AS bytes_com_atributos,
                   MIN(publish_time) AS primeira, MAX(publish_time) AS ultima
            FROM {s.tabela(s.ds_bronze, 'eventos_stream')}
            WHERE publish_time IS NOT NULL""",
    }


def tamanho_bucket(s: Settings, prefixo: str = "gold/") -> dict[str, Any]:
    from google.cloud import storage

    por_tabela: dict[str, dict[str, int]] = {}
    for blob in storage.Client(project=s.gcp_project).list_blobs(s.gcs_bucket, prefix=prefixo):
        tabela = blob.name.split("/")[1]
        t = por_tabela.setdefault(tabela, {"arquivos": 0, "bytes": 0})
        t["arquivos"] += 1
        t["bytes"] += blob.size or 0
    return {
        "bucket": s.gcs_bucket,
        "prefixo": prefixo,
        "por_tabela": por_tabela,
        "bytes_total": sum(t["bytes"] for t in por_tabela.values()),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = carregar()
    cliente = ClienteBQ(s)
    gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")

    resultado: dict[str, Any] = {}
    with registrar_etapa(gravar, novo_id_execucao(), "medir_custos", "ops") as reg:
        for nome, sql in consultas(s).items():
            r = cliente.executar(sql)
            reg.acumular(r)
            resultado[nome] = r.linhas
        resultado["armazenamento"] = [
            t for ds in (s.ds_bronze, s.ds_silver, s.ds_gold, s.ds_ops) for t in cliente.armazenamento(ds)
        ]
        resultado["bucket"] = tamanho_bucket(s)
        resultado["_execucao"] = {"id_execucao": reg.id_execucao, "job_ids": reg.job_ids}

    destino = RAIZ / "docs" / "perfil" / "medicoes_custo.json"
    destino.write_text(json.dumps(resultado, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    total = resultado["total_geral"][0]
    armazenado = sum(t["bytes_logicos"] or 0 for t in resultado["armazenamento"])
    log.info("execuções: %s | faturado no total: %s | BigQuery armazenado: %s | bucket gold/: %s",
             total["execucoes"], formatar_bytes(total["bytes_faturados"]),
             formatar_bytes(armazenado), formatar_bytes(resultado["bucket"]["bytes_total"]))
    log.info("salvo em %s", destino.relative_to(RAIZ))


if __name__ == "__main__":
    main()
