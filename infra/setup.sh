#!/usr/bin/env bash
# Cria a infraestrutura da fase 1 no GCP. Pode ser executado mais de uma vez
# (cada passo verifica se o recurso já existe).
#
# Pré-requisitos: gcloud e bq instalados e autenticados
#   gcloud auth login
#   gcloud auth application-default login
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$RAIZ/.env" ]]; then
  set -a; source "$RAIZ/.env"; set +a
fi
: "${GCP_PROJECT:?Defina GCP_PROJECT no .env}"
: "${GCS_BUCKET:?Defina GCS_BUCKET no .env}"
BQ_LOCATION="${BQ_LOCATION:-US}"
PUBSUB_TOPIC="${PUBSUB_TOPIC:-eventos-alfabetizacao}"

echo "==> Projeto: $GCP_PROJECT | Local: $BQ_LOCATION"
gcloud config set project "$GCP_PROJECT" >/dev/null

echo "==> Habilitando APIs"
gcloud services enable \
  bigquery.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

echo "==> Datasets BigQuery"
for ds in bronze silver gold ops; do
  if bq --project_id="$GCP_PROJECT" show --dataset "$GCP_PROJECT:$ds" >/dev/null 2>&1; then
    echo "    $ds já existe"
  else
    bq --project_id="$GCP_PROJECT" --location="$BQ_LOCATION" mk --dataset \
      --description "Camada $ds da pipeline de alfabetização" "$GCP_PROJECT:$ds"
  fi
done

echo "==> Tabelas de observabilidade (ops)"
sed "s/\${GCP_PROJECT}/$GCP_PROJECT/g" "$RAIZ/infra/sql/ops.sql" \
  | bq --project_id="$GCP_PROJECT" --location="$BQ_LOCATION" query --use_legacy_sql=false

echo "==> Tabelas da Bronze"
sed "s/\${GCP_PROJECT}/$GCP_PROJECT/g" "$RAIZ/infra/sql/bronze.sql" \
  | bq --project_id="$GCP_PROJECT" --location="$BQ_LOCATION" query --use_legacy_sql=false

echo "==> Bucket gs://$GCS_BUCKET"
if gcloud storage buckets describe "gs://$GCS_BUCKET" >/dev/null 2>&1; then
  echo "    bucket já existe"
else
  gcloud storage buckets create "gs://$GCS_BUCKET" \
    --location="$BQ_LOCATION" --uniform-bucket-level-access
fi

echo "==> Tópico Pub/Sub $PUBSUB_TOPIC"
if gcloud pubsub topics describe "$PUBSUB_TOPIC" >/dev/null 2>&1; then
  echo "    tópico já existe"
else
  gcloud pubsub topics create "$PUBSUB_TOPIC"
fi

echo "==> Tabelas do streaming"
sed "s/\${GCP_PROJECT}/$GCP_PROJECT/g" "$RAIZ/infra/sql/streaming.sql" \
  | bq --project_id="$GCP_PROJECT" --location="$BQ_LOCATION" query --use_legacy_sql=false

# A assinatura BigQuery grava com a conta de serviço do Pub/Sub, que precisa poder
# escrever só nesta tabela (permissão no nível da tabela, não do projeto).
NUMERO_PROJETO="$(gcloud projects describe "$GCP_PROJECT" --format='value(projectNumber)')"
CONTA_PUBSUB="service-${NUMERO_PROJETO}@gcp-sa-pubsub.iam.gserviceaccount.com"
echo "==> Permissão de escrita em bronze.eventos_stream para $CONTA_PUBSUB"
bq add-iam-policy-binding --member="serviceAccount:$CONTA_PUBSUB" \
  --role=roles/bigquery.dataEditor "$GCP_PROJECT:bronze.eventos_stream" >/dev/null

ASSINATURA="${PUBSUB_TOPIC}-bq"
echo "==> Assinatura BigQuery $ASSINATURA -> bronze.eventos_stream"
if gcloud pubsub subscriptions describe "$ASSINATURA" >/dev/null 2>&1; then
  echo "    assinatura já existe"
else
  gcloud pubsub subscriptions create "$ASSINATURA" --topic="$PUBSUB_TOPIC" \
    --bigquery-table="$GCP_PROJECT:bronze.eventos_stream" --write-metadata
fi

echo "==> Pronto."
