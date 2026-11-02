-- Universo avaliado: presente (presenca = '1') E caderno preenchido
-- (preenchimento_caderno = '1'), códigos do dicionário. Ausentes têm alfabetizado = '0' na
-- fonte (docs/investigacao.md §7) e puxariam a taxa para baixo se entrassem no denominador.
-- Taxa recalculada = SUM(peso_aluno * alfabetizado) / SUM(peso_aluno).
-- Colunas explícitas e filtro de partição na Bronze (require_partition_filter).
CREATE OR REPLACE TABLE {destino}
PARTITION BY RANGE_BUCKET(ano, GENERATE_ARRAY(2023, 2031, 1))
CLUSTER BY id_municipio, rede_padrao AS
SELECT
  a.ano,
  TRIM(a.id_municipio) AS id_municipio,
  dir.sigla_uf,
  dir.regiao,
  a.id_escola,
  a.id_aluno,
  a.rede,
  COALESCE(r.rede_padrao, 'desconhecido') AS rede_padrao,
  CASE a.alfabetizado WHEN '1' THEN 1 WHEN '0' THEN 0 END AS alfabetizado,
  a.proficiencia,
  a.peso_aluno
FROM {bronze}.alunos AS a
LEFT JOIN {silver}.diretorio_municipio AS dir
  ON dir.id_municipio = TRIM(a.id_municipio)
LEFT JOIN {silver}.rede AS r
  ON r.origem = 'codigo' AND r.valor = a.rede
WHERE {carga_alunos}
  AND a.presenca = '1'
  AND a.preenchimento_caderno = '1';
