-- Micro-lote: números do lote para ops.execucoes e para o log.
SELECT
  COUNT(*) AS lidos,
  COUNTIF(motivo_invalido IS NULL AND NOT duplicado) AS validos_novos,
  COUNTIF(duplicado) AS duplicados,
  COUNTIF(motivo_invalido IS NOT NULL) AS invalidos,
  APPROX_QUANTILES(IF(motivo_invalido IS NULL, latencia_ingestao_s, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS latencia_p50_s,
  APPROX_QUANTILES(IF(motivo_invalido IS NULL, latencia_ingestao_s, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(95)] AS latencia_p95_s,
  APPROX_QUANTILES(IF(motivo_invalido IS NULL, latencia_ponta_a_ponta_s, NULL), 100 IGNORE NULLS)[SAFE_OFFSET(95)] AS latencia_ponta_a_ponta_p95_s
FROM {destino};
