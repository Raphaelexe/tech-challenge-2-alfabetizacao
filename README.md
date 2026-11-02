# Pipeline Híbrido para Análise da Alfabetização no Brasil

Tech Challenge — POS TECH, Fase 2. Pipeline medalhão (Bronze, Silver, Gold) no GCP,
com ingestão batch e streaming dos dados do Indicador Criança Alfabetizada
(dataset `br_inep_avaliacao_alfabetizacao` da Base dos Dados).

> **Status:** fase 1 implementada e executada (fundação, perfil e investigação da fonte).
> Ingestão Bronze implementada e executada duas vezes
> (`evidencias/fase2_bronze_duas_cargas_municipio.csv`). Silver e regras de qualidade
> implementadas e executadas (`evidencias/fase2_silver_qualidade.csv`: 51 regras, 44
> bloqueantes aprovadas), com falha demonstrada (`evidencias/fase2_pipeline_falhou.jpg`). Gold, streaming e alertas estão
> **planejados para as próximas etapas**.

## O que já existe

| Componente | Arquivo | O que faz |
| --- | --- | --- |
| Configuração | `config/settings.py`, `.env.example` | Único lugar com IDs de projeto, bucket e tópico |
| Infraestrutura | `infra/setup.sh`, `infra/sql/ops.sql` | Cria datasets `bronze`, `silver`, `gold`, `ops` (US), tabelas de observabilidade, bucket e tópico Pub/Sub |
| Cliente BigQuery | `pipeline/bq.py` | Dry run antes de toda query, aborta acima de `MAX_BYTES_BILLED` e executa com `maximum_bytes_billed` |
| Registro de execução | `pipeline/ops.py` | Grava cada etapa em `ops.execucoes` (duração, bytes, job ids, status); falhas são gravadas e propagadas |
| Perfil da fonte | `scripts/perfil_fonte.py` | Colunas reais, tamanhos, valores distintos de `rede`/`serie`/etc. e divergências entre tabelas |
| Investigação da fonte | `scripts/investigar_fonte.py` | Consultas que respondem as divergências do perfil (ver [Achados do perfil](#achados-do-perfil-etapa-1)) |
| Ingestão Bronze | `pipeline/orquestrador.py`, `pipeline/bronze/*.sql`, `infra/sql/bronze.sql` | Carga append das 7 tabelas e do diretório de municípios, com contrato de esquema, conferência de contagem e retentativa |
| Silver | `pipeline/silver/*.sql`, `pipeline/orquestrador.py` | 9 tabelas tratadas, gravadas em staging e publicadas só depois das regras |
| Qualidade | `quality/regras.yaml`, `quality/runner.py` | Regras bloqueantes e informativas, gravadas em `ops.qualidade` |
| Demonstração de falha | `scripts/injetar_falha/*.sql`, `--injetar-falha` | Injeta dado ruim na staging para provar que a regra bloqueante impede a publicação |
| Testes | `tests/test_fase1.py`, `tests/test_bronze.py`, `tests/test_silver.py` | 151 testes, sem dependência do GCP |

## Como executar

Pré-requisitos: Python 3.11+, [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
e um projeto GCP com faturamento ou sandbox do BigQuery.

```bash
gcloud auth login
gcloud auth application-default login

cp .env.example .env      # preencha GCP_PROJECT e GCS_BUCKET
make setup                # ambiente virtual + dependências
make testes               # deve passar sem acesso ao GCP
make infra                # cria os recursos no GCP
make perfil               # gera docs/dicionario_dados.md e grava em ops.execucoes
make investigar           # gera docs/perfil/investigacao.json
make bronze               # carga append das 8 tabelas em bronze.*
make silver               # Silver: staging -> regras -> publicação
```

Para olhar a fonte antes de criar a infraestrutura: `make perfil-sem-ops`.

Sem `make` (comum no Windows), os mesmos passos no Git Bash:

```bash
python -m venv .venv && .venv/Scripts/python -m pip install -r requirements.txt
.venv/Scripts/python -m pytest -q
bash infra/setup.sh
.venv/Scripts/python -m scripts.perfil_fonte
.venv/Scripts/python -m scripts.investigar_fonte
.venv/Scripts/python -m pipeline.orquestrador --camadas bronze   # --tabelas uf municipio
.venv/Scripts/python -m pipeline.orquestrador --camadas silver
```

`infra/setup.sh` pode ser rodado de novo sem risco: ele só cria o que falta (por
exemplo, as tabelas `bronze.*`, adicionadas depois da fase 1).

### Evidência da fase 1

Depois do `make perfil`, rode no console do BigQuery e salve o print em `evidencias/`:

```sql
SELECT id_execucao, etapa, status, duracao_s, bytes_faturados, ARRAY_LENGTH(job_ids) AS jobs
FROM `SEU_PROJETO.ops.execucoes`
ORDER BY inicio DESC;
```

## Achados do perfil (etapa 1)

Fonte: `docs/dicionario_dados.md` (execução `20261102T160721Z_9ebf14`), a tabela
`dicionario` agregada por coluna e chave (`evidencias/fase1_dicionario_agregado.csv`) e
`docs/perfil/investigacao.json`. Evidências e decisões completas em
[`docs/investigacao.md`](docs/investigacao.md).

| # | Achado | Consequência para a Silver |
| --- | --- | --- |
| 1 | O `dicionario` é consistente: cada par (coluna, chave) tem **um único significado** em todas as tabelas onde aparece (coluna `significados` = 1 nas 14 linhas). | Uma única tabela de tradução serve para `alunos`, `uf` e `municipio`. |
| 2 | `rede` é **código** (`'0'`…`'6'`, STRING) em `alunos`, `uf` e `municipio`, e **texto** nas metas: `'Pública'` em `meta_alfabetizacao_brasil`/`_uf`, `'Municipal'` em `meta_alfabetizacao_municipio`. O texto das metas não está no dicionário. | Join direto por `rede` casaria 0 linhas. Medido pela taxa igual dos dois lados: `'Pública'` = código `5` (42 de 49 pares UF-ano) e `'Municipal'` = código `3` (10.654 pares de 10.704 metas). A Silver cria `rede_padrao` com esse mapeamento. |
| 3 | Cada tabela tem um universo de rede diferente: `uf`/`municipio` têm `0, 2, 3, 5`; `alunos` tem `2, 3, 4`; os códigos `1` (Federal) e `6` existem no dicionário mas não aparecem nos dados. | Resultado só é comparado com meta do mesmo recorte de rede. Um check exige zero linhas em `rede_padrao = 'desconhecido'`. |
| 4 | `uf` não tem **DF** nem **RR**; `meta_alfabetizacao_uf` tem as 27 UFs. | RR não tem resultado em 2023–2024 em nenhuma tabela, e AC não tem em 2023. DF não tem resultado em 2023 e, em 2024, só aparece em `municipio` (Brasília, 59,13, igual à meta de UF) e `alunos`. Meta sem resultado recebe o estado `sem_resultado`, não "não atingiu". |
| 5 | **2025** só existe em `meta_alfabetizacao_brasil` e `meta_alfabetizacao_uf` (com `taxa_alfabetizacao`). `alunos`, `uf`, `municipio` e `meta_alfabetizacao_municipio` vão até 2024. | A série temporal por município termina em 2024; o ponto de 2025 só existe para Brasil e UF e vem das tabelas de meta. |
| 6 | `serie` tem um único valor (`'2'`, 2º ano). | Coluna mantida para o filtro de recorte, sem variação analítica. |
| 7 | `meta_alfabetizacao_municipio.nivel_alfabetizacao` (INT64) não tem entrada no dicionário. | É a taxa em faixas (0: <40 … 5: ≥80). Fica fora de `gold.features_ml` por vazamento de alvo. |
| 8 | Em `alunos`, o ausente tem `alfabetizado = '0'`, e não nulo; peso e proficiência são nulos fora do universo avaliado (presente **e** caderno preenchido: 1.502.809 alunos em 2023 e 1.851.852 em 2024). | A taxa recalculada usa só os avaliados como denominador; incluir os ~245–268 mil ausentes por ano puxaria a taxa para baixo. |
| 9 | A meta nacional de 2025 vem publicada com a trajetória arredondada (60, 64, 67…), e não existe meta para 2023, que é a linha de base. | Resultado do ano *Y* comparado com `meta_alfabetizacao_Y` da linha do mesmo ano; 2023 = `sem_meta`. |

## Bronze

`pipeline/orquestrador.py` copia as 7 tabelas do dataset de alfabetização, mais o
diretório de municípios da Base dos Dados, para o dataset `bronze`. A ordem é
`dicionario`, `diretorio_municipio`, `meta_brasil`, `meta_uf`, `meta_municipio`, `uf`,
`municipio`, `alunos`.

O diretório (`br_bd_diretorios_brasil.municipio`) entra porque `municipio` e
`meta_alfabetizacao_municipio` não têm `sigla_uf`. Sem ele, a Gold não liga município a
UF nem a região. Ele é o mesmo diretório que as consultas geradas pelo site da Base dos
Dados usam (`consultas_basedosdados/`). Das 27 colunas, a Bronze guarda 8:
`id_municipio`, `nome`, `capital_uf`, `id_uf`, `sigla_uf`, `nome_uf`, `nome_regiao` e
`amazonia_legal`. O resto (centroide GEOGRAPHY, códigos TSE/RF/BCB, outras
regionalizações) não serve a nenhuma análise prevista. Por isso as colunas fora do
subconjunto não geram aviso de "coluna nova". Cada tabela tem um SQL em `pipeline/bronze/<tabela>.sql` com as colunas
listadas uma a uma, sem `SELECT *`. O DDL em `infra/sql/bronze.sql`, criado pelo
`infra/setup.sh`, é o contrato de esquema.

**Política de escrita (histórico preservado).** A carga é sempre
`INSERT INTO ... SELECT`. Na Bronze nunca se usa `WRITE_TRUNCATE`, `CREATE OR REPLACE`
nem sobrescrita de partição. Cada linha leva três colunas de controle:

| Coluna | Conteúdo |
| --- | --- |
| `dt_ingestao` | data (UTC) da carga; é a coluna de partição |
| `id_execucao` | execução que gravou a carga (o mesmo id de `ops.execucoes`) |
| `fonte` | tabela de origem, ex.: `basedosdados.br_inep_avaliacao_alfabetizacao.uf` |

Duas execuções no mesmo dia geram duas cargas na mesma partição, com `id_execucao`
diferentes. A Silver vai ler a carga mais recente por `id_execucao`. Cada `INSERT` tem
um `NOT EXISTS` por (`dt_ingestao`, `id_execucao`), então a retentativa do orquestrador
não duplica uma carga que já tinha sido gravada.

**Quando a fonte é atualizada.** Toda execução grava uma carga nova ao lado das
anteriores, e a Silver passa a usar a mais recente. As versões antigas continuam
disponíveis para comparação.

**Travas por tabela.** Cada uma interrompe a execução com erro e grava
`status = 'falhou'` em `ops.execucoes`:
1. Antes do `INSERT`, o esquema da fonte é comparado com o da tabela Bronze. Coluna
   removida ou com tipo alterado para a carga; coluna nova só gera aviso no log.
2. Depois do `INSERT`, as linhas gravadas precisam ser iguais às linhas da fonte.

**Retentativa.** Erros transitórios do BigQuery (5xx, 429) são repetidos até 3 vezes,
com espera de 5 s e 10 s. Os demais erros sobem direto.

**Custo.** A carga de `alunos` escaneia 256,1 MiB na fonte (dry run medido no perfil);
as outras seis tabelas somam menos de 3 MiB. O tamanho do diretório não pôde ser lido
(`__TABLES__` de `br_bd_diretorios_brasil` dá 403) e será medido pela primeira carga em
`ops.execucoes`. As tabelas são clusterizadas por `ano` e
`id_municipio` ou `sigla_uf`. `bronze.alunos` exige filtro de partição
(`require_partition_filter`), para nenhuma leitura varrer todas as cargas sem querer.

## Silver

Os SQLs ficam em `pipeline/silver/<tabela>.sql` e são executados por
`pipeline/orquestrador.py --camadas silver`. A Silver é reconstruível a partir da
Bronze, por isso usa `CREATE OR REPLACE`.

**Qual carga da Bronze é lida.** Para cada tabela, a carga mais recente que terminou com
`status = 'sucesso'` em `ops.execucoes`, ou seja, uma que passou pelas travas de esquema e
contagem. No SQL, o marcador `{carga_<tabela>}` vira o filtro
`dt_ingestao = DATE '…' AND id_execucao = '…'`. Ele inclui a partição, que é obrigatória
em `bronze.alunos`.

**Publicação.** Cada tabela passa por três passos:
1. É gravada em `silver.<tabela>_stg`.
2. As regras de `quality/regras.yaml` rodam sobre a staging.
3. Se nenhuma bloqueante reprovar, a staging é copiada para `silver.<tabela>` (copy job,
   sem custo de bytes) e removida.

Se uma bloqueante reprovar, a execução para com erro. A versão anterior de
`silver.<tabela>` continua publicada, e a staging fica no dataset para inspeção.

| Tabela | Grão | O que faz |
| --- | --- | --- |
| `dicionario` | id_tabela × coluna × chave | Tradução oficial de códigos |
| `diretorio_municipio` | município | Nome, UF, região, capital e Amazônia Legal |
| `rede` | origem × valor | Chave comum `rede_padrao`: código `5` e texto `'Pública'` viram `publica`, código `3` e `'Municipal'` viram `municipal` ([investigação](docs/investigacao.md) §1 e §2) |
| `resultado_uf` | ano × UF × rede × série | Resultado oficial com rede, série e região traduzidas |
| `resultado_municipio` | ano × município × rede × série | Idem, com nome e UF do município |
| `meta_brasil`, `meta_uf`, `meta_municipio` | publicação (ano) × território | Trajetória pactuada 2024–2030. A taxa da tabela de meta vira `taxa_referencia_meta`, e `nivel_alfabetizacao` vira `faixa_taxa_alfabetizacao` |
| `alunos_avaliados` | ano × aluno | Só presentes com caderno preenchido, `alfabetizado` 0/1 e peso amostral. Particionada por ano |

Nulos são preservados: nenhuma taxa, meta ou proficiência é preenchida com zero, e um
teste reprova qualquer `COALESCE(…, 0)` nos SQLs da Silver.

## Qualidade de dados

`quality/regras.yaml` declara cada regra com tabela, severidade, operador, limiar e uma
consulta que devolve um número. `quality/runner.py` junta as regras de uma tabela numa
única query: o BigQuery cobra no mínimo 10 MB por tabela referenciada em cada query, e
uma query por regra multiplicaria esse piso. O runner grava todos os resultados em
`ops.qualidade` (um load job por tabela) antes de decidir. Se alguma bloqueante
reprovar, levanta `QualidadeBloqueanteError`. Nenhum `except` engole esse erro: o
`registrar_etapa` grava `status = 'falhou'` em `ops.execucoes` e propaga a exceção.

51 regras: 44 bloqueantes e 7 informativas.

| Tipo | Regras | Severidade |
| --- | ---: | --- |
| Duplicidade de chave (uma por tabela) | 9 | bloqueante |
| Código sem tradução: `rede_padrao = 'desconhecido'`, série sem descrição, código de presença/caderno/alfabetizado fora do dicionário, código com dois significados | 12 | bloqueante |
| Chave de relacionamento: `id_municipio` com 7 dígitos, município sem UF no diretório | 4 | bloqueante |
| Faixa de valor: taxa e metas entre 0 e 100, peso amostral positivo | 6 | bloqueante |
| **Taxa de correspondência dos joins** com o diretório, limiar 100% | 5 | bloqueante |
| Tabela vazia ou cobertura mínima (diretório ≥ 5.570 municípios, 27 UFs por publicação de meta) | 8 | bloqueante |
| Nulos e universo: taxa nula, meta sem 2030, meta 2030 ≠ 80, proficiência nula, proporção de avaliados ≥ 80% | 7 | informativa |

**Por que 100% nos joins com o diretório.** O diretório é a lista oficial de municípios
do IBGE (5.571). Um código que não casa não é lacuna da fonte: é chave errada ou
município novo. Nos dois casos, a linha ficaria sem UF e sumiria das agregações sem
aviso. Os joins entre resultado e meta, com limiares medidos na investigação (99% para
município e cobertura observada para UF), entram na Gold.

### Demonstração de falha

Um relatório só com aprovações não prova que a trava funciona. `--injetar-falha` roda um
cenário de `scripts/injetar_falha/` na **staging**, depois que ela é montada e antes das
regras. A Bronze e a Silver publicada não são alteradas.

| Cenário | O que simula | Regra que precisa reprovar |
| --- | --- | --- |
| `resultado_municipio__rede_desconhecida` | a fonte passa a mandar a rede `'9'`, sem entrada no dicionário | `resultado_municipio_rede_desconhecida` |
| `resultado_municipio__chave_duplicada` | carga reprocessada que duplica São Paulo / municipal / 2024 | `resultado_municipio_chave_duplicada` |

```bash
.venv/Scripts/python -m pipeline.orquestrador --camadas silver --tabelas resultado_municipio     --injetar-falha resultado_municipio__rede_desconhecida
echo "código de saída: $?"     # diferente de 0
```

Evidências que a execução deixa (consultas no console do BigQuery):

```sql
-- 1. a regra bloqueante reprovou
SELECT id_execucao, regra, severidade, valor_medido, limiar, status
FROM `SEU_PROJETO.ops.qualidade`
WHERE status = 'reprovada' AND severidade = 'bloqueante'
ORDER BY ts_validacao DESC;

-- 2. a etapa terminou como falha, com a mensagem do erro
SELECT id_execucao, etapa, status, mensagem_erro
FROM `SEU_PROJETO.ops.execucoes`
WHERE status = 'falhou' ORDER BY inicio DESC;

-- 3. a linha ruim ficou na staging e não chegou à tabela publicada
SELECT 'staging' AS tabela, COUNTIF(rede = '9') AS linhas_rede_9
FROM `SEU_PROJETO.silver.resultado_municipio_stg`
UNION ALL
SELECT 'publicada', COUNTIF(rede = '9') FROM `SEU_PROJETO.silver.resultado_municipio`;
```

**Execução registrada** (`20261102T204419Z_22674a`, cenário
`resultado_municipio__rede_desconhecida`):

| Evidência | Arquivo | O que mostra |
| --- | --- | --- |
| Terminal | `evidencias/fase2_pipeline_falhou.jpg` | `FALHA INJETADA`, `REPROVADA [bloqueante] resultado_municipio_rede_desconhecida: valor=1.0 limiar = 0.0`, `QualidadeBloqueanteError` e `código de saída: 1` |
| Consultas 1 e 2 | `evidencias/fase2_demonstracao_falha1.jpg`, `..._falha2.jpg` | regra `reprovada` em `ops.qualidade` e etapa `falhou` em `ops.execucoes` |
| Consulta 3 | `evidencias/fase2_demonstracao_falha3.jpg` | 1 linha com rede `'9'` na staging e **0** na tabela publicada |

As outras 8 regras da tabela aprovaram na mesma execução. A regra
`resultado_municipio_vazio` mediu 23.996 linhas: as 23.995 da fonte mais a linha injetada.

Para voltar ao estado normal, rode a mesma tabela sem a opção. A staging é recriada a
partir da Bronze, aprovada e removida:
`.venv/Scripts/python -m pipeline.orquestrador --camadas silver --tabelas resultado_municipio`.

## Decisões já tomadas

- **Acesso à fonte:** BigQuery público da Base dos Dados, sem download. Não há
  transferência entre nuvens; o custo é de bytes escaneados, medido por dry run.
- **Região:** todos os datasets em `US`, a mesma do projeto `basedosdados`. O BigQuery
  não faz join entre locais diferentes.
- **Gravação em `ops.execucoes` por load job**, não por streaming insert: é gratuito e
  funciona no sandbox do BigQuery.
- **Trava de custo dupla:** dry run com teto no Python e `maximum_bytes_billed` no
  próprio job, para o caso de a estimativa errar.

## Limitações conhecidas

- `__TABLES__` pode ser bloqueado em alguns projetos; nesse caso o relatório sai sem
  linhas e tamanhos, e o resto continua.
- A Bronze guarda uma cópia inteira a cada execução, mesmo que a fonte não tenha
  mudado: ~260 MiB de armazenamento lógico por carga, quase todo de `alunos`.
