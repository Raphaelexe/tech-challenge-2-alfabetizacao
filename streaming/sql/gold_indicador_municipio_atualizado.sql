-- Gold do streaming: indicador municipal oficial + a medição simulada mais recente de
-- cada município × ano × rede. VIEW: o evento aparece na camada analítica assim que o
-- MERGE do micro-lote termina, sem outra cópia.
-- A coluna origem ('oficial' | 'simulado') separa os dois; nenhum número simulado
-- substitui ou se mistura a uma linha oficial.
-- Status do simulado: comparado com a meta pactuada do ano do evento, da publicação mais
-- recente da meta municipal (mesmo recorte de rede), nos mesmos 4 estados da Gold oficial.
CREATE OR REPLACE VIEW {destino} AS
WITH ultimo_evento AS (
  SELECT *
  FROM {silver}.eventos_validos
  WHERE origem = 'simulado'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_municipio, ano, rede_padrao ORDER BY ts_evento DESC, id_evento
  ) = 1
),
meta AS (
  SELECT id_municipio, rede_padrao,
    meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
    meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
    meta_alfabetizacao_2030
  FROM {silver}.meta_municipio
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id_municipio, rede_padrao ORDER BY ano DESC) = 1
),
simulado AS (
  SELECT
    e.ano, e.id_municipio, e.rede_padrao, e.taxa_alfabetizacao,
    CASE e.ano
      WHEN 2024 THEN m.meta_alfabetizacao_2024
      WHEN 2025 THEN m.meta_alfabetizacao_2025
      WHEN 2026 THEN m.meta_alfabetizacao_2026
      WHEN 2027 THEN m.meta_alfabetizacao_2027
      WHEN 2028 THEN m.meta_alfabetizacao_2028
      WHEN 2029 THEN m.meta_alfabetizacao_2029
      WHEN 2030 THEN m.meta_alfabetizacao_2030
    END AS meta_pactuada_ano,
    e.ts_evento, e.id_evento, e.latencia_ponta_a_ponta_s
  FROM ultimo_evento AS e
  LEFT JOIN meta AS m ON m.id_municipio = e.id_municipio AND m.rede_padrao = e.rede_padrao
)
SELECT
  ano, id_municipio, nome_municipio, sigla_uf, regiao, rede_padrao, taxa_alfabetizacao,
  meta_pactuada_ano, status_meta, origem,
  CAST(NULL AS TIMESTAMP) AS ts_evento, CAST(NULL AS STRING) AS id_evento,
  CAST(NULL AS FLOAT64) AS latencia_ponta_a_ponta_s
FROM {gold}.indicador_municipio
UNION ALL
SELECT
  s.ano, s.id_municipio, d.nome_municipio, d.sigla_uf, d.regiao, s.rede_padrao,
  s.taxa_alfabetizacao, s.meta_pactuada_ano,
  CASE
    WHEN s.taxa_alfabetizacao IS NULL THEN 'sem_resultado'
    WHEN s.meta_pactuada_ano IS NULL THEN 'sem_meta'
    WHEN s.taxa_alfabetizacao >= s.meta_pactuada_ano THEN 'atingiu'
    ELSE 'nao_atingiu'
  END AS status_meta,
  'simulado' AS origem,
  s.ts_evento, s.id_evento, s.latencia_ponta_a_ponta_s
FROM simulado AS s
LEFT JOIN {silver}.diretorio_municipio AS d ON d.id_municipio = s.id_municipio;
