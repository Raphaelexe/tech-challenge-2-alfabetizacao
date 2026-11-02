-- Carga append da Bronze: meta_uf. Colunas explícitas, iguais ao contrato em infra/sql/bronze.sql.
-- O NOT EXISTS torna a retentativa idempotente: se esta carga já foi gravada, não grava de novo.
INSERT INTO {destino} (
  ano,
  sigla_uf,
  rede,
  taxa_alfabetizacao,
  meta_alfabetizacao_2024,
  meta_alfabetizacao_2025,
  meta_alfabetizacao_2026,
  meta_alfabetizacao_2027,
  meta_alfabetizacao_2028,
  meta_alfabetizacao_2029,
  meta_alfabetizacao_2030,
  percentual_participacao,
  dt_ingestao,
  id_execucao,
  fonte
)
SELECT
  ano,
  sigla_uf,
  rede,
  taxa_alfabetizacao,
  meta_alfabetizacao_2024,
  meta_alfabetizacao_2025,
  meta_alfabetizacao_2026,
  meta_alfabetizacao_2027,
  meta_alfabetizacao_2028,
  meta_alfabetizacao_2029,
  meta_alfabetizacao_2030,
  percentual_participacao,
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
