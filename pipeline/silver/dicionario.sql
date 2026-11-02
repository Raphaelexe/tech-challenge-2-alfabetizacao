-- Dicionário oficial da fonte: tradução de códigos por (id_tabela, nome_coluna, chave).
CREATE OR REPLACE TABLE {destino} AS
SELECT
  TRIM(id_tabela)   AS id_tabela,
  TRIM(nome_coluna) AS nome_coluna,
  TRIM(chave)       AS chave,
  TRIM(valor)       AS descricao
FROM {bronze}.dicionario
WHERE {carga_dicionario};
