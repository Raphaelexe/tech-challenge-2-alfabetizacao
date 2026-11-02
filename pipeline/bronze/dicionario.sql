-- Carga append da Bronze: dicionario. Colunas explícitas, iguais ao contrato em infra/sql/bronze.sql.
-- O NOT EXISTS torna a retentativa idempotente: se esta carga já foi gravada, não grava de novo.
INSERT INTO {destino} (
  id_tabela,
  nome_coluna,
  chave,
  cobertura_temporal,
  valor,
  dt_ingestao,
  id_execucao,
  fonte
)
SELECT
  id_tabela,
  nome_coluna,
  chave,
  cobertura_temporal,
  valor,
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
