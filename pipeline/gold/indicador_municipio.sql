-- Indicador por município × ano × rede comparado com a meta pactuada do município.
-- FULL JOIN: meta sem resultado aparece como 'sem_resultado' (nunca some e nunca vira
-- 'nao_atingiu'); rede sem meta pactuada aparece como 'sem_meta'.
-- Meta do ano Y = coluna meta_alfabetizacao_Y da publicação do mesmo ano Y
-- (docs/investigacao.md §5). 2023 é linha de base: não há meta para ele.
-- Resultado vem sempre de resultado_municipio; taxa_referencia_meta fica só para conferência (§2).
CREATE OR REPLACE TABLE {destino}
CLUSTER BY id_municipio, rede_padrao AS
WITH resultado AS (
  SELECT ano, id_municipio, rede_padrao, rede_descricao, taxa_alfabetizacao, media_portugues
  FROM {silver}.resultado_municipio
  WHERE serie = '2'
),
meta AS (
  SELECT
    ano,
    id_municipio,
    rede_padrao,
    taxa_referencia_meta,
    CASE ano
      WHEN 2024 THEN meta_alfabetizacao_2024
      WHEN 2025 THEN meta_alfabetizacao_2025
      WHEN 2026 THEN meta_alfabetizacao_2026
      WHEN 2027 THEN meta_alfabetizacao_2027
      WHEN 2028 THEN meta_alfabetizacao_2028
      WHEN 2029 THEN meta_alfabetizacao_2029
      WHEN 2030 THEN meta_alfabetizacao_2030
    END AS meta_pactuada_ano,
    meta_alfabetizacao_2030,
    percentual_participacao
  FROM {silver}.meta_municipio
),
juntos AS (
  SELECT
    COALESCE(r.ano, m.ano) AS ano,
    COALESCE(r.id_municipio, m.id_municipio) AS id_municipio,
    COALESCE(r.rede_padrao, m.rede_padrao) AS rede_padrao,
    r.rede_descricao,
    r.taxa_alfabetizacao,
    r.media_portugues,
    r.taxa_alfabetizacao IS NOT NULL AS tem_resultado,
    m.id_municipio IS NOT NULL AS tem_meta,
    m.meta_pactuada_ano,
    m.meta_alfabetizacao_2030 AS meta_pactuada_2030,
    m.taxa_referencia_meta,
    m.percentual_participacao
  FROM resultado AS r
  FULL OUTER JOIN meta AS m
    ON m.ano = r.ano AND m.id_municipio = r.id_municipio AND m.rede_padrao = r.rede_padrao
)
SELECT
  j.ano,
  j.id_municipio,
  dir.nome_municipio,
  dir.sigla_uf,
  dir.regiao,
  j.rede_padrao,
  j.rede_descricao,
  j.taxa_alfabetizacao,
  j.media_portugues,
  'resultado_municipio' AS fonte_resultado,
  j.tem_resultado,
  j.tem_meta,
  j.meta_pactuada_ano,
  j.meta_pactuada_2030,
  CASE
    WHEN NOT j.tem_resultado THEN 'sem_resultado'
    WHEN j.meta_pactuada_ano IS NULL THEN 'sem_meta'
    WHEN j.taxa_alfabetizacao >= j.meta_pactuada_ano THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  ROUND(j.meta_pactuada_ano - j.taxa_alfabetizacao, 2) AS distancia_meta_pactuada,
  ROUND(j.meta_pactuada_2030 - j.taxa_alfabetizacao, 2) AS distancia_meta_pactuada_2030,
  ROUND(100 - j.taxa_alfabetizacao, 2) AS distancia_cenario_aspiracional,
  j.taxa_referencia_meta,
  j.percentual_participacao,
  'oficial' AS origem
FROM juntos AS j
LEFT JOIN {silver}.diretorio_municipio AS dir
  ON dir.id_municipio = j.id_municipio;
