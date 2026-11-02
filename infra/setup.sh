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
# A assinatura BigQuery é criada na fase 3, junto com a tabela bronze.eventos_stream.

echo "==> Pronto."
