-- Micro-lote, passo 4: eventos inválidos vão para a quarentena com o motivo e a mensagem
-- original. MERGE por message_id: a mesma mensagem não entra duas vezes.
MERGE {silver}.eventos_quarentena AS t
USING (
  SELECT message_id, ANY_VALUE(publish_time) AS publish_time, ANY_VALUE(data) AS data,
         ANY_VALUE(motivo_invalido) AS motivo, ANY_VALUE(ts_processamento) AS ts_processamento,
         ANY_VALUE(origem) AS origem
  FROM {destino}
  WHERE motivo_invalido IS NOT NULL
  GROUP BY message_id
) AS s
ON t.message_id = s.message_id
WHEN NOT MATCHED THEN INSERT (message_id, publish_time, data, motivo, ts_processamento, id_execucao, origem)
VALUES (s.message_id, s.publish_time, s.data, s.motivo, s.ts_processamento, @id_execucao, s.origem);
