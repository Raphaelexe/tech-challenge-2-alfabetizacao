-- Carga append da Bronze: alunos. Colunas explícitas, iguais ao contrato em infra/sql/bronze.sql.
-- O NOT EXISTS torna a retentativa idempotente: se esta carga já foi gravada, não grava de novo.
INSERT INTO {destino} (
  ano,
  id_municipio,
  id_escola,
  id_aluno,
  caderno,
  serie,
  rede,
  presenca,
  preenchimento_caderno,
  alfabetizado,
  proficiencia,
  peso_aluno,
  dt_ingestao,
  id_execucao,
  fonte
)
SELECT
  ano,
  id_municipio,
  id_escola,
  id_aluno,
  caderno,
  serie,
  rede,
  presenca,
  preenchimento_caderno,
  alfabetizado,
  proficiencia,
  peso_aluno,
  @dt_ingestao,
  @id_execucao,
  @fonte
FROM {fonte}
WHERE NOT EXISTS (
  SELECT 1
  FROM {destino}
  WHERE dt_ingestao = @dt_ingestao
    AND id_execucao = @id_execucao
);
