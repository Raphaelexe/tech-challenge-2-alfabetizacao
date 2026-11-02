-- Demonstração de falha (python -m pipeline.orquestrador --injetar-falha ...).
-- Simula a fonte trazendo um código de rede novo ('9'), sem entrada no dicionário,
-- para São Paulo (3550308) em 2024. Roda só na staging, depois que ela é montada e antes
-- das regras: resultado_municipio_rede_desconhecida (bloqueante) precisa reprovar, e
-- silver.resultado_municipio publicada não pode receber a linha.
INSERT INTO {destino} (
  ano, id_municipio, nome_municipio, sigla_uf, regiao, serie, serie_descricao,
  rede, rede_descricao, rede_padrao, taxa_alfabetizacao, media_portugues
)
SELECT
  ano, id_municipio, nome_municipio, sigla_uf, regiao, serie, serie_descricao,
  '9', NULL, 'desconhecido', taxa_alfabetizacao, media_portugues
FROM {destino}
WHERE id_municipio = '3550308' AND ano = 2024 AND rede = '3';
