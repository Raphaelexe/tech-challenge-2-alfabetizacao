-- Tabelas da Bronze. Colunas da fonte com nomes e tipos de docs/dicionario_dados.md,
-- mais as de controle: dt_ingestao (partição), id_execucao e fonte.
-- Este DDL é o contrato: o orquestrador compara o esquema da fonte com ele antes de
-- cada carga e para se uma coluna sumir ou mudar de tipo.
-- Append-only: nunca CREATE OR REPLACE nem WRITE_TRUNCATE aqui.
-- ${GCP_PROJECT} é substituído pelo infra/setup.sh.

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.dicionario` (
  id_tabela          STRING,
  nome_coluna        STRING,
  chave              STRING,
  cobertura_temporal STRING,
  valor              STRING,
  dt_ingestao        DATE   NOT NULL,
  id_execucao        STRING NOT NULL,
  fonte              STRING NOT NULL
)
PARTITION BY dt_ingestao
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.dicionario');

-- Subconjunto de br_bd_diretorios_brasil.municipio (27 colunas na fonte): só o que liga
-- município a UF e região. Fora: centroide (GEOGRAPHY), códigos TSE/RF/BCB, regiões de
-- saúde, imediata, intermediária, micro, meso e metropolitana, comarca e DDD.
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.diretorio_municipio` (
  id_municipio   STRING,
  nome           STRING,
  capital_uf     INT64,
  id_uf          STRING,
  sigla_uf       STRING,
  nome_uf        STRING,
  nome_regiao    STRING,
  amazonia_legal INT64,
  dt_ingestao    DATE   NOT NULL,
  id_execucao    STRING NOT NULL,
  fonte          STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY id_municipio
OPTIONS (description = 'Bronze append-only de br_bd_diretorios_brasil.municipio (subconjunto de colunas)');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.meta_brasil` (
  ano                     INT64,
  rede                    STRING,
  taxa_alfabetizacao      FLOAT64,
  meta_alfabetizacao_2024 FLOAT64,
  meta_alfabetizacao_2025 FLOAT64,
  meta_alfabetizacao_2026 FLOAT64,
  meta_alfabetizacao_2027 FLOAT64,
  meta_alfabetizacao_2028 FLOAT64,
  meta_alfabetizacao_2029 FLOAT64,
  meta_alfabetizacao_2030 FLOAT64,
  percentual_participacao FLOAT64,
  dt_ingestao             DATE   NOT NULL,
  id_execucao             STRING NOT NULL,
  fonte                   STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.meta_uf` (
  ano                     INT64,
  sigla_uf                STRING,
  rede                    STRING,
  taxa_alfabetizacao      FLOAT64,
  meta_alfabetizacao_2024 FLOAT64,
  meta_alfabetizacao_2025 FLOAT64,
  meta_alfabetizacao_2026 FLOAT64,
  meta_alfabetizacao_2027 FLOAT64,
  meta_alfabetizacao_2028 FLOAT64,
  meta_alfabetizacao_2029 FLOAT64,
  meta_alfabetizacao_2030 FLOAT64,
  percentual_participacao FLOAT64,
  dt_ingestao             DATE   NOT NULL,
  id_execucao             STRING NOT NULL,
  fonte                   STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano, sigla_uf
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.meta_municipio` (
  ano                     INT64,
  id_municipio            STRING,
  rede                    STRING,
  taxa_alfabetizacao      FLOAT64,
  meta_alfabetizacao_2024 FLOAT64,
  meta_alfabetizacao_2025 FLOAT64,
  meta_alfabetizacao_2026 FLOAT64,
  meta_alfabetizacao_2027 FLOAT64,
  meta_alfabetizacao_2028 FLOAT64,
  meta_alfabetizacao_2029 FLOAT64,
  meta_alfabetizacao_2030 FLOAT64,
  nivel_alfabetizacao     INT64,
  percentual_participacao FLOAT64,
  dt_ingestao             DATE   NOT NULL,
  id_execucao             STRING NOT NULL,
  fonte                   STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano, id_municipio
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.uf` (
  ano                     INT64,
  sigla_uf                STRING,
  serie                   STRING,
  rede                    STRING,
  taxa_alfabetizacao      FLOAT64,
  media_portugues         FLOAT64,
  proporcao_aluno_nivel_0 FLOAT64,
  proporcao_aluno_nivel_1 FLOAT64,
  proporcao_aluno_nivel_2 FLOAT64,
  proporcao_aluno_nivel_3 FLOAT64,
  proporcao_aluno_nivel_4 FLOAT64,
  proporcao_aluno_nivel_5 FLOAT64,
  proporcao_aluno_nivel_6 FLOAT64,
  proporcao_aluno_nivel_7 FLOAT64,
  proporcao_aluno_nivel_8 FLOAT64,
  dt_ingestao             DATE   NOT NULL,
  id_execucao             STRING NOT NULL,
  fonte                   STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano, sigla_uf
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.uf');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.municipio` (
  ano                     INT64,
  id_municipio            STRING,
  serie                   STRING,
  rede                    STRING,
  taxa_alfabetizacao      FLOAT64,
  media_portugues         FLOAT64,
  proporcao_aluno_nivel_0 FLOAT64,
  proporcao_aluno_nivel_1 FLOAT64,
  proporcao_aluno_nivel_2 FLOAT64,
  proporcao_aluno_nivel_3 FLOAT64,
  proporcao_aluno_nivel_4 FLOAT64,
  proporcao_aluno_nivel_5 FLOAT64,
  proporcao_aluno_nivel_6 FLOAT64,
  proporcao_aluno_nivel_7 FLOAT64,
  proporcao_aluno_nivel_8 FLOAT64,
  dt_ingestao             DATE   NOT NULL,
  id_execucao             STRING NOT NULL,
  fonte                   STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano, id_municipio
OPTIONS (description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.municipio');

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.bronze.alunos` (
  ano                   INT64,
  id_municipio          STRING,
  id_escola             STRING,
  id_aluno              STRING,
  caderno               STRING,
  serie                 STRING,
  rede                  STRING,
  presenca              STRING,
  preenchimento_caderno STRING,
  alfabetizado          STRING,
  proficiencia          FLOAT64,
  peso_aluno            FLOAT64,
  dt_ingestao           DATE   NOT NULL,
  id_execucao           STRING NOT NULL,
  fonte                 STRING NOT NULL
)
PARTITION BY dt_ingestao
CLUSTER BY ano, id_municipio
OPTIONS (
  description = 'Bronze append-only de br_inep_avaliacao_alfabetizacao.alunos',
  -- tabela grande: toda leitura precisa escolher a carga (partição)
  require_partition_filter = TRUE
);
