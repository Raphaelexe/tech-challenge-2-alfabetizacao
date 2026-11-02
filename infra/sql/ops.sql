-- Tabelas de observabilidade. Acumulam execuções (nunca são sobrescritas).
-- ${GCP_PROJECT} é substituído pelo infra/setup.sh.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops.execucoes` (
  id_execucao       STRING    NOT NULL,
  etapa             STRING    NOT NULL,
  camada            STRING,
  inicio            TIMESTAMP NOT NULL,
  fim               TIMESTAMP,
  duracao_s         FLOAT64,
  linhas_entrada    INT64,
  linhas_saida      INT64,
  bytes_estimados   INT64,
  bytes_processados INT64,
  bytes_faturados   INT64,
  job_ids           ARRAY<STRING>,
  status            STRING    NOT NULL,   -- sucesso | falhou
  mensagem_erro     STRING,
  latencia_p50_s    FLOAT64,              -- só streaming
  latencia_p95_s    FLOAT64               -- só streaming
)
PARTITION BY DATE(inicio)
OPTIONS (description = 'Uma linha por etapa executada da pipeline.');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops.qualidade` (
  id_execucao  STRING    NOT NULL,
  regra        STRING    NOT NULL,
  camada       STRING    NOT NULL,
  severidade   STRING    NOT NULL,   -- bloqueante | informativa
  valor_medido FLOAT64,
  limiar       FLOAT64,
  status       STRING    NOT NULL,   -- aprovada | reprovada
  detalhe      STRING,
  ts_validacao TIMESTAMP NOT NULL
)
PARTITION BY DATE(ts_validacao)
OPTIONS (description = 'Resultado de cada regra de qualidade em cada execução.');
