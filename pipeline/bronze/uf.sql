-- Carga append da Bronze: uf. Colunas explícitas, iguais ao contrato em infra/sql/bronze.sql.
-- O NOT EXISTS torna a retentativa idempotente: se esta carga já foi gravada, não grava de novo.
INSERT INTO {destino} (
  ano,
  sigla_uf,
  serie,
  rede,
  taxa_alfabetizacao,
  media_portugues,
  proporcao_aluno_nivel_0,
  proporcao_aluno_nivel_1,
  proporcao_aluno_nivel_2,
  proporcao_aluno_nivel_3,
  proporcao_aluno_nivel_4,
  proporcao_aluno_nivel_5,
  proporcao_aluno_nivel_6,
  proporcao_aluno_nivel_7,
  proporcao_aluno_nivel_8,
  dt_ingestao,
  id_execucao,
  fonte
)
SELECT
  ano,
  sigla_uf,
  serie,
  rede,
  taxa_alfabetizacao,
  media_portugues,
  proporcao_aluno_nivel_0,
  proporcao_aluno_nivel_1,
  proporcao_aluno_nivel_2,
  proporcao_aluno_nivel_3,
  proporcao_aluno_nivel_4,
  proporcao_aluno_nivel_5,
  proporcao_aluno_nivel_6,
  proporcao_aluno_nivel_7,
  proporcao_aluno_nivel_8,
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
