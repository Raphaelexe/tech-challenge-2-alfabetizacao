-- Município -> UF e região (br_bd_diretorios_brasil). Indicadores 0/1 viram BOOL.
CREATE OR REPLACE TABLE {destino}
CLUSTER BY id_municipio AS
SELECT
  TRIM(id_municipio)      AS id_municipio,
  nome                    AS nome_municipio,
  UPPER(TRIM(sigla_uf))   AS sigla_uf,
  nome_uf,
  nome_regiao             AS regiao,
  capital_uf = 1          AS capital_uf,
  amazonia_legal = 1      AS amazonia_legal
FROM {bronze}.diretorio_municipio
WHERE {carga_diretorio_municipio};
