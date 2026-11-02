-- Demonstração de falha (python -m pipeline.orquestrador --injetar-falha ...).
-- Duplica a linha de São Paulo (3550308), rede municipal, 2024, como faria uma carga
-- reprocessada sem deduplicação. resultado_municipio_chave_duplicada (bloqueante)
-- precisa reprovar antes da publicação.
INSERT INTO {destino} (
  ano, id_municipio, nome_municipio, sigla_uf, regiao, serie, serie_descricao,
  rede, rede_descricao, rede_padrao, taxa_alfabetizacao, media_portugues
)
SELECT
  ano, id_municipio, nome_municipio, sigla_uf, regiao, serie, serie_descricao,
  rede, rede_descricao, rede_padrao, taxa_alfabetizacao, media_portugues
FROM {destino}
WHERE id_municipio = '3550308' AND ano = 2024 AND rede = '3';
