-- Cada linha é uma publicação (ano) com a trajetória pactuada 2024-2030 (docs/investigacao.md §5).
-- taxa_alfabetizacao da tabela de meta vira taxa_referencia_meta: o resultado oficial vem
-- das tabelas de resultado (§2). Metas nulas continuam nulas (nunca zero).
CREATE OR REPLACE TABLE {destino}
CLUSTER BY sigla_uf AS
SELECT
  m.ano,
  UPPER(TRIM(m.sigla_uf)) AS sigla_uf,
  dir.nome_uf,
  dir.regiao,
  m.rede AS rede_origem,
  COALESCE(r.rede_padrao, 'desconhecido') AS rede_padrao,
  m.taxa_alfabetizacao AS taxa_referencia_meta,
  m.meta_alfabetizacao_2024,
  m.meta_alfabetizacao_2025,
  m.meta_alfabetizacao_2026,
  m.meta_alfabetizacao_2027,
  m.meta_alfabetizacao_2028,
  m.meta_alfabetizacao_2029,
  m.meta_alfabetizacao_2030,
  m.percentual_participacao
FROM {bronze}.meta_uf AS m
LEFT JOIN (SELECT DISTINCT sigla_uf, nome_uf, regiao FROM {silver}.diretorio_municipio) AS dir
  ON dir.sigla_uf = UPPER(TRIM(m.sigla_uf))
LEFT JOIN {silver}.rede AS r
  ON r.origem = 'texto_meta' AND r.valor = m.rede
WHERE {carga_meta_uf};
