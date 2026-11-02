-- Evolução do indicador por território (município, UF, Brasil) × rede, com variação em
-- relação ao ano anterior disponível. ano_anterior explicita lacunas (ex.: AC sem 2023).
-- Com 2 ou 3 anos de série, isto descreve o passado; projeção até 2030 seria cenário.
-- fonte_resultado muda em 2025 para UF e Brasil (tabelas de meta, valores arredondados).
CREATE OR REPLACE TABLE {destino}
CLUSTER BY nivel, territorio AS
WITH todos AS (
  SELECT 'municipio' AS nivel, id_municipio AS territorio, nome_municipio AS nome_territorio,
         sigla_uf, rede_padrao, ano, taxa_alfabetizacao, fonte_resultado, origem
  FROM {gold}.indicador_municipio WHERE tem_resultado
  UNION ALL
  SELECT 'uf', sigla_uf, nome_uf, sigla_uf, rede_padrao, ano, taxa_alfabetizacao,
         fonte_resultado, origem
  FROM {gold}.indicador_uf WHERE tem_resultado
  UNION ALL
  SELECT 'brasil', 'BR', 'Brasil', CAST(NULL AS STRING), rede_padrao, ano, taxa_alfabetizacao,
         fonte_resultado, origem
  FROM {gold}.indicador_brasil WHERE tem_resultado
)
SELECT
  *,
  LAG(ano) OVER w AS ano_anterior,
  LAG(taxa_alfabetizacao) OVER w AS taxa_ano_anterior,
  ROUND(taxa_alfabetizacao - LAG(taxa_alfabetizacao) OVER w, 2) AS variacao_pp
FROM todos
WINDOW w AS (PARTITION BY nivel, territorio, rede_padrao ORDER BY ano);
