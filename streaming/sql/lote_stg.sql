-- Micro-lote, passo 1: janela de mensagens da Bronze, interpretada e validada.
-- Janela: publish_time em (@desde, @ate]. @ate fica alguns segundos antes de "agora",
-- para não pular mensagem que o Pub/Sub ainda está gravando (ver micro_lote.py).
-- motivo_invalido: o primeiro defeito encontrado, ou NULL se o evento é válido.
-- duplicado: id_evento repetido dentro do lote (fica a 1ª publicação) ou já gravado em
-- silver.eventos_validos por um lote anterior. Pub/Sub entrega "pelo menos uma vez".
CREATE OR REPLACE TABLE {destino} AS
WITH janela AS (
  SELECT
    message_id,
    publish_time,
    data,
    JSON_VALUE(attributes, '$.origem') AS origem,
    JSON_VALUE(attributes, '$.tipo_envio') AS tipo_envio,
    SAFE.PARSE_JSON(data) AS j
  FROM {bronze}.eventos_stream
  WHERE publish_time > @desde AND publish_time <= @ate
),
campos AS (
  SELECT
    message_id,
    publish_time,
    data,
    origem,
    tipo_envio,
    j IS NULL AS json_invalido,
    JSON_VALUE(j, '$.id_evento') AS id_evento,
    JSON_VALUE(j, '$.tipo') AS tipo,
    JSON_VALUE(j, '$.id_municipio') AS id_municipio,
    SAFE_CAST(JSON_VALUE(j, '$.ano') AS INT64) AS ano,
    JSON_VALUE(j, '$.rede') AS rede,
    SAFE_CAST(JSON_VALUE(j, '$.taxa_alfabetizacao') AS FLOAT64) AS taxa_alfabetizacao,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_VALUE(j, '$.ts_evento')) AS ts_evento
  FROM janela
),
validados AS (
  SELECT
    c.*,
    dir.sigla_uf,
    CASE
      WHEN c.json_invalido THEN 'json_invalido'
      WHEN c.id_evento IS NULL OR c.tipo IS NULL OR c.id_municipio IS NULL OR c.ano IS NULL
        OR c.rede IS NULL OR c.taxa_alfabetizacao IS NULL OR c.ts_evento IS NULL
        THEN 'campo_obrigatorio_ausente'
      WHEN c.tipo != 'atualizacao_indicador' THEN 'tipo_desconhecido'
      WHEN dir.id_municipio IS NULL THEN 'municipio_inexistente'
      WHEN c.taxa_alfabetizacao < 0 OR c.taxa_alfabetizacao > 100 THEN 'taxa_fora_0_100'
      WHEN c.ano NOT BETWEEN 2023 AND 2030 THEN 'ano_fora_da_trajetoria'
      WHEN c.rede NOT IN ('municipal', 'publica') THEN 'rede_desconhecida'
    END AS motivo_invalido
  FROM campos AS c
  LEFT JOIN {silver}.diretorio_municipio AS dir ON dir.id_municipio = c.id_municipio
)
SELECT
  v.*,
  v.rede AS rede_padrao,
  @ts_processamento AS ts_processamento,
  TIMESTAMP_DIFF(v.publish_time, v.ts_evento, MILLISECOND) / 1000 AS latencia_ingestao_s,
  TIMESTAMP_DIFF(@ts_processamento, v.ts_evento, MILLISECOND) / 1000 AS latencia_ponta_a_ponta_s,
  v.motivo_invalido IS NULL AND (
    ROW_NUMBER() OVER (PARTITION BY v.id_evento ORDER BY v.publish_time, v.message_id) > 1
    OR EXISTS (SELECT 1 FROM {silver}.eventos_validos AS e WHERE e.id_evento = v.id_evento)
  ) AS duplicado
FROM validados AS v;
