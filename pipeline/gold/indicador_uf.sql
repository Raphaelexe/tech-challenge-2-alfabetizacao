-- Indicador por UF × ano × rede comparado com a meta pactuada da UF (rede pública).
-- Ordem de fonte do resultado (coluna fonte_resultado, docs/investigacao.md §4):
--   1. resultado_uf;
--   2. resultado_municipio, quando a UF tem um único município no diretório e não tem
--      linha em resultado_uf (DF 2024 = Brasília, 59,13, idêntico à meta_uf);
--   3. taxa da própria meta_uf, só quando não há linha de resultado nenhuma no ano
--      (2025, que só existe nas tabelas de meta).
-- UF sem nenhuma das três fica 'sem_resultado' (AC 2023, DF 2023, RR 2023 e 2024).
CREATE OR REPLACE TABLE {destino}
CLUSTER BY sigla_uf, rede_padrao AS
WITH resultado_uf AS (
  SELECT ano, sigla_uf, rede_padrao, taxa_alfabetizacao, media_portugues
  FROM {silver}.resultado_uf
  WHERE serie = '2'
),
uf_de_um_municipio AS (
  SELECT sigla_uf, ANY_VALUE(id_municipio) AS id_municipio
  FROM {silver}.diretorio_municipio
  GROUP BY sigla_uf
  HAVING COUNT(*) = 1
),
resultado_unico_municipio AS (
  SELECT r.ano, u.sigla_uf, r.rede_padrao, r.taxa_alfabetizacao, r.media_portugues
  FROM {silver}.resultado_municipio AS r
  JOIN uf_de_um_municipio AS u ON u.id_municipio = r.id_municipio
  WHERE r.serie = '2'
),
resultado AS (
  SELECT *, 'resultado_uf' AS fonte_resultado FROM resultado_uf
  UNION ALL
  SELECT m.*, 'resultado_municipio' AS fonte_resultado
  FROM resultado_unico_municipio AS m
  WHERE NOT EXISTS (
    SELECT 1 FROM resultado_uf AS u
    WHERE u.ano = m.ano AND u.sigla_uf = m.sigla_uf AND u.rede_padrao = m.rede_padrao
  )
),
meta AS (
  SELECT
    ano,
    sigla_uf,
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
  FROM {silver}.meta_uf
),
juntos AS (
  SELECT
    COALESCE(r.ano, m.ano) AS ano,
    COALESCE(r.sigla_uf, m.sigla_uf) AS sigla_uf,
    COALESCE(r.rede_padrao, m.rede_padrao) AS rede_padrao,
    COALESCE(r.taxa_alfabetizacao, IF(r.ano IS NULL, m.taxa_referencia_meta, NULL)) AS taxa_alfabetizacao,
    r.media_portugues,
    COALESCE(r.fonte_resultado, IF(r.ano IS NULL AND m.taxa_referencia_meta IS NOT NULL, 'meta_uf', NULL)) AS fonte_resultado,
    m.sigla_uf IS NOT NULL AS tem_meta,
    m.meta_pactuada_ano,
    m.meta_alfabetizacao_2030 AS meta_pactuada_2030,
    m.taxa_referencia_meta,
    m.percentual_participacao
  FROM resultado AS r
  FULL OUTER JOIN meta AS m
    ON m.ano = r.ano AND m.sigla_uf = r.sigla_uf AND m.rede_padrao = r.rede_padrao
)
SELECT
  j.ano,
  j.sigla_uf,
  dir.nome_uf,
  dir.regiao,
  j.rede_padrao,
  j.taxa_alfabetizacao,
  j.media_portugues,
  j.fonte_resultado,
  j.taxa_alfabetizacao IS NOT NULL AS tem_resultado,
  j.tem_meta,
  j.meta_pactuada_ano,
  j.meta_pactuada_2030,
  CASE
    WHEN j.taxa_alfabetizacao IS NULL THEN 'sem_resultado'
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
LEFT JOIN (SELECT DISTINCT sigla_uf, nome_uf, regiao FROM {silver}.diretorio_municipio) AS dir
  ON dir.sigla_uf = j.sigla_uf;
