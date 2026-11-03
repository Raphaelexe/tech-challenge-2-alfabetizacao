-- Micro-lote, passo 3: eventos válidos e não duplicados entram na Silver.
-- MERGE por id_evento: reprocessar a mesma janela não insere de novo (idempotente).
MERGE {silver}.eventos_validos AS t
USING (
  SELECT * FROM {destino}
  WHERE motivo_invalido IS NULL AND NOT duplicado
) AS s
ON t.id_evento = s.id_evento
WHEN NOT MATCHED THEN INSERT (
  id_evento, tipo, id_municipio, sigla_uf, ano, rede_padrao, taxa_alfabetizacao,
  ts_evento, publish_time, ts_processamento, latencia_ingestao_s, latencia_ponta_a_ponta_s,
  message_id, id_execucao, origem
) VALUES (
  s.id_evento, s.tipo, s.id_municipio, s.sigla_uf, s.ano, s.rede_padrao, s.taxa_alfabetizacao,
  s.ts_evento, s.publish_time, s.ts_processamento, s.latencia_ingestao_s, s.latencia_ponta_a_ponta_s,
  s.message_id, @id_execucao, s.origem
);
