# Tech Challenge — Alfabetização (POS TECH, Fase 2)

Pipeline medalhão (Bronze, Silver, Gold) no GCP sobre o dataset
`basedosdados.br_inep_avaliacao_alfabetizacao`, com batch + streaming simulado via Pub/Sub.

O plano completo, com requisitos, regras de negócio e roteiro, está em @docs/plano.md.
Ele é a fonte de verdade; o PDF em `docs/` é só o original. Ao concluir uma tarefa do
roteiro, marque o checkbox lá e registre decisões e medições na seção
"Decisões e medições registradas".

## Regras que não podem ser quebradas

- **Bronze só em append** (`INSERT INTO ... SELECT <colunas explícitas>`), com
  `dt_ingestao`, `id_execucao` e `fonte`. Nunca `WRITE_TRUNCATE` nem `CREATE OR REPLACE` na
  Bronze. Silver e Gold podem usar `CREATE OR REPLACE`.
- **Sem `SELECT *`** sobre a fonte ou a tabela de alunos; sempre colunas explícitas e filtro
  de partição.
- **Toda query passa por `pipeline/bq.py`** (`ClienteBQ`): dry run + `maximum_bytes_billed`.
  Não chamar o cliente BigQuery direto.
- **Toda etapa roda dentro de `registrar_etapa`** (`pipeline/ops.py`), que grava em
  `ops.execucoes`. Exceções nunca são engolidas: o único `except` registra a falha e faz
  `raise` de novo.
- **Nenhum ID de projeto, bucket ou tópico fixo no código.** Tudo vem de
  `config/settings.py` (`carregar()`, `settings.tabela()`, `settings.fonte()`).
- **Nomes de coluna da fonte precisam ser confirmados** em `docs/dicionario_dados.md`
  (gerado por `make perfil`) antes de serem usados em SQL. Não inventar colunas.
- Gold: status da meta em três estados (`atingiu`, `nao_atingiu`, `sem_meta`); distância
  usa a meta pactuada; 100% só em `distancia_cenario_aspiracional`; agregação ponderada por
  alunos; coluna `origem` onde houver dado do streaming.
- Nulos são preservados: nunca preencher proficiência ou meta com zero.
- Datasets na multi-região `US`.

## Convenções de código

- Python 3.11+, código, nomes, comentários e mensagens em **português** (ex.:
  `registrar_etapa`, `CustoAcimaDoLimiteError`), seguindo o estilo de `pipeline/` e
  `config/`.
- SQL de cada camada em `pipeline/<camada>/*.sql`.
- Testes em `tests/` com pytest; **não podem depender do GCP** (BigQuery simulado com
  objetos falsos, como em `tests/test_fase1.py`).
- O README descreve só o que foi implementado; o resto aparece como "planejado".

## Comandos

```bash
make setup      # venv + dependências
make testes     # pytest (sem GCP)
make infra      # cria datasets, tabelas ops, bucket, tópico
make perfil     # perfil da fonte -> docs/dicionario_dados.md
make bronze     # carga append da Bronze
make pipeline   # todas as camadas implementadas (pipeline/orquestrador.py)
```

Ambiente Windows: o venv fica em `.venv/Scripts/python` (o Makefile já trata isso), mas
`make` não está instalado. Rodar direto: `.venv/Scripts/python -m pytest -q`,
`.venv/Scripts/python -m pipeline.orquestrador --camadas bronze`.

Nova camada = arquivos `pipeline/<camada>/<tabela>.sql` (com `{destino}` e `{fonte}`) mais
uma entrada em `CAMADAS` no `pipeline/orquestrador.py`.

## Segurança

`.env` e credenciais nunca vão para o Git (já estão no `.gitignore`). Não ler nem exibir o
conteúdo do `.env` sem necessidade.
