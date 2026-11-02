-- Base de features para a fase 3: município × ano_base, rede municipal (a rede da meta
-- municipal). Tudo é do ano_base t; o alvo é a taxa do ano t+1.
-- Sem vazamento de alvo: nenhuma coluna é derivada da taxa de t+1. Ficam fora status da
-- meta, distâncias até a meta e faixa_taxa_alfabetizacao (= taxa em faixas, §6).
-- meta_pactuada_proximo_ano é conhecida antes do resultado (trajetória publicada em t).
-- Linhas do último ano têm alvo nulo: servem para previsão, não para treino.
CREATE OR REPLACE TABLE {destino}
CLUSTER BY id_municipio AS
WITH resultado AS (
  SELECT
    ano, id_municipio, taxa_alfabetizacao, media_portugues,
    proporcao_aluno_nivel_0, proporcao_aluno_nivel_1, proporcao_aluno_nivel_2,
    proporcao_aluno_nivel_3, proporcao_aluno_nivel_4, proporcao_aluno_nivel_5,
    proporcao_aluno_nivel_6, proporcao_aluno_nivel_7, proporcao_aluno_nivel_8
  FROM {silver}.resultado_municipio
  WHERE serie = '2' AND rede_padrao = 'municipal'
),
microdado AS (
  SELECT ano, id_municipio, n_avaliados, n_escolas, soma_pesos, proficiencia_media
  FROM {gold}.validacao_microdado
  WHERE nivel = 'municipio' AND rede_padrao = 'municipal'
),
uf AS (
  SELECT ano, sigla_uf, taxa_alfabetizacao AS taxa_uf_rede_publica
  FROM {gold}.indicador_uf
  WHERE rede_padrao = 'publica'
),
meta AS (
  SELECT
    ano, id_municipio, percentual_participacao,
    CASE ano + 1
      WHEN 2024 THEN meta_alfabetizacao_2024
      WHEN 2025 THEN meta_alfabetizacao_2025
      WHEN 2026 THEN meta_alfabetizacao_2026
      WHEN 2027 THEN meta_alfabetizacao_2027
      WHEN 2028 THEN meta_alfabetizacao_2028
      WHEN 2029 THEN meta_alfabetizacao_2029
      WHEN 2030 THEN meta_alfabetizacao_2030
    END AS meta_pactuada_proximo_ano
  FROM {silver}.meta_municipio
)
SELECT
  r.ano AS ano_base,
  r.id_municipio,
  dir.sigla_uf,
  dir.regiao,
  dir.capital_uf,
  dir.amazonia_legal,
  r.taxa_alfabetizacao AS taxa_alfabetizacao_ano_base,
  r.media_portugues,
  r.proporcao_aluno_nivel_0, r.proporcao_aluno_nivel_1, r.proporcao_aluno_nivel_2,
  r.proporcao_aluno_nivel_3, r.proporcao_aluno_nivel_4, r.proporcao_aluno_nivel_5,
  r.proporcao_aluno_nivel_6, r.proporcao_aluno_nivel_7, r.proporcao_aluno_nivel_8,
  mi.n_avaliados,
  mi.n_escolas,
  mi.soma_pesos,
  mi.proficiencia_media,
  uf.taxa_uf_rede_publica,
  m.percentual_participacao,
  m.meta_pactuada_proximo_ano,
  proximo.taxa_alfabetizacao AS alvo_taxa_alfabetizacao_proximo_ano,
  'oficial' AS origem
FROM resultado AS r
LEFT JOIN resultado AS proximo
  ON proximo.id_municipio = r.id_municipio AND proximo.ano = r.ano + 1
LEFT JOIN {silver}.diretorio_municipio AS dir ON dir.id_municipio = r.id_municipio
LEFT JOIN microdado AS mi ON mi.ano = r.ano AND mi.id_municipio = r.id_municipio
LEFT JOIN uf ON uf.ano = r.ano AND uf.sigla_uf = dir.sigla_uf
LEFT JOIN meta AS m ON m.ano = r.ano AND m.id_municipio = r.id_municipio;
