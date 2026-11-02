-- Chave comum de rede (rede_padrao) para resultados e metas.
-- Resultados (alunos, uf, municipio) usam código; a descrição vem do dicionário.
-- Metas usam texto sem entrada no dicionário; a correspondência foi medida em
-- docs/investigacao.md (§1 e §2): 'Pública' = código 5, 'Municipal' = código 3.
-- Valor sem mapeamento vira 'desconhecido', e a regra rede_desconhecida exige zero.
CREATE OR REPLACE TABLE {destino} AS
WITH codigos AS (
  SELECT DISTINCT chave AS valor, descricao
  FROM {silver}.dicionario
  WHERE nome_coluna = 'rede'
)
SELECT
  'codigo' AS origem,
  valor,
  descricao AS rede_descricao,
  CASE valor
    WHEN '0' THEN 'total'
    WHEN '1' THEN 'federal'
    WHEN '2' THEN 'estadual'
    WHEN '3' THEN 'municipal'
    WHEN '4' THEN 'privada'
    WHEN '5' THEN 'publica'
    WHEN '6' THEN 'publica_com_federal'
    ELSE 'desconhecido'
  END AS rede_padrao
FROM codigos
UNION ALL
SELECT 'texto_meta', valor, descricao, rede_padrao
FROM UNNEST([
  STRUCT('Pública' AS valor, 'Pública (Estadual e Municipal)' AS descricao, 'publica' AS rede_padrao),
  STRUCT('Municipal', 'Municipal', 'municipal')
]);
