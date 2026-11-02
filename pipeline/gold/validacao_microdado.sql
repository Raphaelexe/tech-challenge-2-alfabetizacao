-- Validação cruzada: taxa recalculada dos microdados × taxa publicada, por município, UF
-- e Brasil, para cada rede. Uma única leitura de silver.alunos_avaliados: as somas de
-- peso são aditivas, então UF e Brasil saem da soma dos municípios.
-- Taxa recalculada = 100 * SUM(peso_aluno * alfabetizado) / SUM(peso_aluno), só sobre
-- avaliados (presentes com caderno preenchido; docs/investigacao.md §7).
-- Rede: a de cada aluno, mais 'publica' (estadual + municipal, = código 5) e 'total'.
-- n_ufs_com_microdado mostra quantas UFs entram no recálculo: SP 2023 não está nos
-- microdados da fonte (docs/investigacao.md §10), então o Brasil 2023 recalculado é sem SP.
CREATE OR REPLACE TABLE {destino}
CLUSTER BY nivel, rede_padrao AS
WITH por_municipio AS (
  SELECT
    a.ano,
    a.sigla_uf,
    a.id_municipio,
    grupo AS rede_padrao,
    COUNT(*) AS n_avaliados,
    COUNT(DISTINCT a.id_escola) AS n_escolas,
    SUM(a.peso_aluno) AS soma_pesos,
    SUM(a.peso_aluno * a.alfabetizado) AS soma_pesos_alfabetizados,
    SUM(a.peso_aluno * a.proficiencia) AS soma_pesos_proficiencia
  FROM {silver}.alunos_avaliados AS a,
    UNNEST(ARRAY_CONCAT(
      [a.rede_padrao],
      IF(a.rede_padrao IN ('estadual', 'municipal'), ['publica'], []),
      ['total']
    )) AS grupo
  GROUP BY 1, 2, 3, 4
),
niveis AS (
  SELECT 'municipio' AS nivel, ano, rede_padrao, sigla_uf, id_municipio,
         n_avaliados, n_escolas, soma_pesos, soma_pesos_alfabetizados, soma_pesos_proficiencia,
         1 AS n_ufs_com_microdado
  FROM por_municipio
  UNION ALL
  SELECT 'uf', ano, rede_padrao, sigla_uf, CAST(NULL AS STRING),
         SUM(n_avaliados), SUM(n_escolas), SUM(soma_pesos), SUM(soma_pesos_alfabetizados),
         SUM(soma_pesos_proficiencia), 1
  FROM por_municipio GROUP BY ano, rede_padrao, sigla_uf
  UNION ALL
  SELECT 'brasil', ano, rede_padrao, CAST(NULL AS STRING), CAST(NULL AS STRING),
         SUM(n_avaliados), SUM(n_escolas), SUM(soma_pesos), SUM(soma_pesos_alfabetizados),
         SUM(soma_pesos_proficiencia), COUNT(DISTINCT sigla_uf)
  FROM por_municipio GROUP BY ano, rede_padrao
),
publicada AS (
  SELECT 'municipio' AS nivel, ano, rede_padrao, sigla_uf, id_municipio, taxa_alfabetizacao
  FROM {silver}.resultado_municipio WHERE serie = '2'
  UNION ALL
  SELECT 'uf', ano, rede_padrao, sigla_uf, CAST(NULL AS STRING), taxa_alfabetizacao
  FROM {silver}.resultado_uf WHERE serie = '2'
  UNION ALL
  SELECT 'brasil', ano, rede_padrao, CAST(NULL AS STRING), CAST(NULL AS STRING), taxa_referencia_meta
  FROM {silver}.meta_brasil
),
juntos AS (
  SELECT
    COALESCE(n.nivel, p.nivel) AS nivel,
    COALESCE(n.ano, p.ano) AS ano,
    COALESCE(n.rede_padrao, p.rede_padrao) AS rede_padrao,
    COALESCE(n.sigla_uf, p.sigla_uf) AS sigla_uf,
    COALESCE(n.id_municipio, p.id_municipio) AS id_municipio,
    n.n_avaliados,
    n.n_escolas,
    n.n_ufs_com_microdado,
    n.soma_pesos,
    ROUND(100 * SAFE_DIVIDE(n.soma_pesos_alfabetizados, n.soma_pesos), 2) AS taxa_recalculada,
    ROUND(SAFE_DIVIDE(n.soma_pesos_proficiencia, n.soma_pesos), 2) AS proficiencia_media,
    p.taxa_alfabetizacao AS taxa_publicada,
    n.nivel IS NOT NULL AS tem_microdado,
    p.taxa_alfabetizacao IS NOT NULL AS tem_publicada
  FROM niveis AS n
  FULL OUTER JOIN publicada AS p
    ON p.nivel = n.nivel AND p.ano = n.ano AND p.rede_padrao = n.rede_padrao
   AND IFNULL(p.sigla_uf, 'BR') = IFNULL(n.sigla_uf, 'BR')
   AND IFNULL(p.id_municipio, '-') = IFNULL(n.id_municipio, '-')
)
SELECT
  *,
  ROUND(taxa_recalculada - taxa_publicada, 2) AS diferenca_pp
FROM juntos;
