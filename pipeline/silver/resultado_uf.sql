-- Resultado oficial por uf × ano × rede × série, com códigos traduzidos pelo dicionário.
-- Nulos preservados: nenhuma taxa ou proporção é preenchida com zero.
CREATE OR REPLACE TABLE {destino}
CLUSTER BY sigla_uf, rede_padrao AS
SELECT
  m.ano,
  UPPER(TRIM(m.sigla_uf)) AS sigla_uf,
  dir.nome_uf,
  dir.regiao,
  m.serie,
  ds.descricao AS serie_descricao,
  m.rede,
  r.rede_descricao,
  COALESCE(r.rede_padrao, 'desconhecido') AS rede_padrao,
  m.taxa_alfabetizacao,
  m.media_portugues,
  m.proporcao_aluno_nivel_0,
  m.proporcao_aluno_nivel_1,
  m.proporcao_aluno_nivel_2,
  m.proporcao_aluno_nivel_3,
  m.proporcao_aluno_nivel_4,
  m.proporcao_aluno_nivel_5,
  m.proporcao_aluno_nivel_6,
  m.proporcao_aluno_nivel_7,
  m.proporcao_aluno_nivel_8
FROM {bronze}.uf AS m
LEFT JOIN (SELECT DISTINCT sigla_uf, nome_uf, regiao FROM {silver}.diretorio_municipio) AS dir
  ON dir.sigla_uf = UPPER(TRIM(m.sigla_uf))
LEFT JOIN {silver}.rede AS r
  ON r.origem = 'codigo' AND r.valor = m.rede
LEFT JOIN {silver}.dicionario AS ds
  ON ds.id_tabela = 'uf' AND ds.nome_coluna = 'serie' AND ds.chave = m.serie
WHERE {carga_uf};
