-- Tabelas do streaming simulado. ${GCP_PROJECT} é substituído pelo infra/setup.sh.

-- Bronze: destino da assinatura BigQuery do Pub/Sub (write_metadata = true). O esquema
-- precisa ser exatamente este: o Pub/Sub grava os metadados nessas colunas e a mensagem,
-- sem transformação, em `data` (inclusive JSON quebrado, que vai para a quarentena).
-- Append-only por natureza: o Pub/Sub só insere.
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.eventos_stream` (
  subscription_name STRING,
  message_id        STRING,
  publish_time      TIMESTAMP,
  data              STRING,
  attributes        STRING
)
PARTITION BY DATE(publish_time)
OPTIONS (description = 'Bronze do streaming: mensagens do Pub/Sub como chegaram (assinatura BigQuery).');

-- Silver: eventos válidos, um por id_evento (MERGE idempotente no micro-lote).
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.silver.eventos_validos` (
  id_evento                STRING    NOT NULL,
  tipo                     STRING,
  id_municipio             STRING,
  sigla_uf                 STRING,
  ano                      INT64,
  rede_padrao              STRING,
  taxa_alfabetizacao       FLOAT64,
  ts_evento                TIMESTAMP,   -- relógio do produtor, no momento em que o evento foi criado
  publish_time             TIMESTAMP,   -- relógio do Pub/Sub, quando recebeu a mensagem
  ts_processamento         TIMESTAMP,   -- relógio do micro-lote
  latencia_ingestao_s      FLOAT64,     -- publish_time - ts_evento
  latencia_ponta_a_ponta_s FLOAT64,     -- ts_processamento - ts_evento
  message_id               STRING,
  id_execucao              STRING,
  origem                   STRING    NOT NULL
)
PARTITION BY DATE(publish_time)
CLUSTER BY id_municipio
OPTIONS (description = 'Silver do streaming: eventos válidos e deduplicados por id_evento.');

-- Silver: eventos rejeitados, com o motivo e a mensagem original.
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.silver.eventos_quarentena` (
  message_id       STRING    NOT NULL,
  publish_time     TIMESTAMP,
  data             STRING,
  motivo           STRING    NOT NULL,
  ts_processamento TIMESTAMP,
  id_execucao      STRING,
  origem           STRING
)
PARTITION BY DATE(publish_time)
OPTIONS (description = 'Silver do streaming: eventos inválidos e o motivo da rejeição.');

-- Marca d'água do micro-lote: até onde (publish_time) cada execução processou.
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops.marca_dagua_stream` (
  id_execucao   STRING    NOT NULL,
  desde         TIMESTAMP NOT NULL,
  ate           TIMESTAMP NOT NULL,
  eventos_lidos INT64     NOT NULL,
  ts_registro   TIMESTAMP NOT NULL
)
OPTIONS (description = 'Janelas processadas pelo micro-lote do streaming (append).');
