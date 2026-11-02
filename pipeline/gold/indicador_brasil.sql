-- Indicador nacional × meta pactuada (rede pública).
-- taxa_alfabetizacao = taxa oficial publicada em meta_alfabetizacao_brasil (55,9 / 59,2 / 66).
-- Para comparação, três agregações das UFs, com nomes que dizem o que são:
--   taxa_recalculada_microdado: microdados ponderados por peso_aluno (validacao_microdado);
--     n_ufs_com_microdado diz quantas UFs entraram (2023: 24, sem SP; investigação §10);
--   media_ponderada_ufs: taxas de UF ponderadas pela soma de pesos dos avaliados de cada UF;
--   media_simples_ufs: média sem peso, que faz uma UF pequena pesar o mesmo que SP.
CREATE OR REPLACE TABLE {destino} AS
WITH meta AS (
  SELECT
    ano,
    rede_padrao,
    taxa_referencia_meta AS taxa_alfabetizacao,
    CASE ano
      WHEN 2024 THEN meta_alfabetizacao_2024
      WHEN 2025 THEN meta_alfabetizacao_2025
      WHEN 2026 THEN meta_alfabetizacao_2026
      WHEN 2027 THEN meta_alfabetizacao_2027
      WHEN 2028 THEN meta_alfabetizacao_2028
      WHEN 2029 THEN meta_alfabetizacao_2029
      WHEN 2030 THEN meta_alfabetizacao_2030
    END AS meta_pactuada_ano,
    meta_alfabetizacao_2030 AS meta_pactuada_2030,
    percentual_participacao
  FROM {silver}.meta_brasil
),
microdado AS (
  SELECT ano, rede_padrao, taxa_recalculada, n_avaliados, n_ufs_com_microdado
  FROM {gold}.validacao_microdado
  WHERE nivel = 'brasil'
),
ufs AS (
  SELECT
    u.ano,
    u.rede_padrao,
    ROUND(SAFE_DIVIDE(SUM(u.taxa_alfabetizacao * v.soma_pesos),
                      SUM(IF(u.taxa_alfabetizacao IS NULL, NULL, v.soma_pesos))), 2) AS media_ponderada_ufs,
    ROUND(AVG(u.taxa_alfabetizacao), 2) AS media_simples_ufs,
    COUNTIF(u.tem_resultado) AS n_ufs_com_resultado
  FROM {gold}.indicador_uf AS u
  LEFT JOIN {gold}.validacao_microdado AS v
    ON v.nivel = 'uf' AND v.ano = u.ano AND v.sigla_uf = u.sigla_uf AND v.rede_padrao = u.rede_padrao
  GROUP BY 1, 2
)
SELECT
  m.ano,
  m.rede_padrao,
  m.taxa_alfabetizacao,
  'meta_brasil' AS fonte_resultado,
  m.taxa_alfabetizacao IS NOT NULL AS tem_resultado,
  m.meta_pactuada_ano,
  m.meta_pactuada_2030,
  CASE
    WHEN m.taxa_alfabetizacao IS NULL THEN 'sem_resultado'
    WHEN m.meta_pactuada_ano IS NULL THEN 'sem_meta'
    WHEN m.taxa_alfabetizacao >= m.meta_pactuada_ano THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  ROUND(m.meta_pactuada_ano - m.taxa_alfabetizacao, 2) AS distancia_meta_pactuada,
  ROUND(m.meta_pactuada_2030 - m.taxa_alfabetizacao, 2) AS distancia_meta_pactuada_2030,
  ROUND(100 - m.taxa_alfabetizacao, 2) AS distancia_cenario_aspiracional,
  mi.taxa_recalculada AS taxa_recalculada_microdado,
  mi.n_avaliados,
  mi.n_ufs_com_microdado,
  u.media_ponderada_ufs,
  u.media_simples_ufs,
  u.n_ufs_com_resultado,
  m.percentual_participacao,
  'oficial' AS origem
FROM meta AS m
LEFT JOIN microdado AS mi ON mi.ano = m.ano AND mi.rede_padrao = m.rede_padrao
LEFT JOIN ufs AS u ON u.ano = m.ano AND u.rede_padrao = m.rede_padrao;
