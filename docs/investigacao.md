# Investigação da fonte

Anomalias e divergências encontradas na fonte, com a evidência e a decisão tomada.
Números de `docs/perfil/investigacao.json`, gerado por `scripts/investigar_fonte.py`
(execuções `20261102T165554Z_c2e682`, `20261102T194947Z_acb82c` e `20261102T195613Z_d251e8`). Para reproduzir:
`python -m scripts.investigar_fonte`.

Status: **confirmado** quando a evidência fecha a questão; **hipótese** quando ainda
depende de uma consulta de acompanhamento (indicada).

## 1. `'Pública'` nas metas de UF e Brasil é o código de rede `5`

**Pergunta.** As metas trazem `rede = 'Pública'`, texto que não aparece no dicionário.
O dicionário tem duas redes públicas: `5` (Estadual e Municipal) e `6` (Federal,
Estadual e Municipal).

**Evidência.** No join `meta_alfabetizacao_uf × uf` por (ano, sigla_uf), a taxa da meta
é igual à da tabela `uf` (tolerância de 0,01 p.p.) em:

| rede em `uf` | pares | taxa igual |
| --- | ---: | ---: |
| `5` Pública (Est. + Mun.) | 49 | **42** |
| `3` Municipal | 49 | 5 |
| `2` Estadual | 46 | 0 |
| `0` Total | 1 | 1 |

O código `6` não aparece em nenhuma tabela de resultado.

**Conclusão (confirmado).** `'Pública'` = código `5`. Na Silver: `rede_padrao = 'publica'`
para `5` nos resultados e para `'Pública'` nas metas.

As 7 divergências restantes (`rede_meta_uf_por_ano`) são todas de 2023 e nenhuma passa de
0,05 p.p.: BA 36,8 × 36,78; CE 84,5 × 84,46; GO 66,7 × 66,74; MT 55,06 × 55,03;
PA 48,4 × 48,38; PE 59,0 × 58,95; RS 63,4 × 63,43. É arredondamento da tabela de meta.

## 2. `'Municipal'` na meta municipal é o código `3`; a taxa de 2023 vem arredondada

**Evidência.** No join `meta_alfabetizacao_municipio × municipio` por (ano, id_municipio):

| rede em `municipio` | pares | taxa igual (0,01 p.p.) |
| --- | ---: | ---: |
| `3` Municipal | 10.654 | 6.620 (62%) |
| `5` Pública | 10.125 | 5.285 (52%) |
| `0` Total | 389 | 380 |
| `2` Estadual | 1.998 | 5 |

A rede `3` tem um par para quase toda meta (10.654 de 10.704). A taxa, porém, só bate em
62% dos casos. Em municípios sem escola estadual, as redes `3`, `5` e `0` têm a mesma
taxa, e por isso as três batem em boa parte dos pares.

Na meta municipal de 2023, a taxa tem 1 casa decimal (faixas de `nivel_alfabetizacao`:
40.0, 70.1, 80.0). Em 2024 tem 2 casas (39.99, 69.98). Em `municipio`, sempre 2 casas.

Por ano (`rede_meta_municipio_por_ano`):

| ano | rede | pares | igual (0,01) | igual (0,05) | maior diferença |
| --- | --- | ---: | ---: | ---: | ---: |
| 2023 | `3` | 5.302 | 1.268 | **5.046** | 36,07 |
| 2023 | `5` | 4.773 | 895 | 3.574 | 48,36 |
| 2024 | `3` | 5.352 | **5.352** | 5.352 | 0,00 |
| 2024 | `5` | 5.352 | 4.390 | 4.404 | 37,70 |

**Conclusão (confirmado).** `'Municipal'` = código `3`. Em 2024 as taxas são idênticas
em 100% dos municípios. Em 2023, 95,2% batem dentro de 0,05 p.p. (arredondamento para
1 casa).

**Os 256 casos de 2023 fora da tolerância.** Eles se dividem em três grupos:

- **70** têm resultado na rede `3`, mas a taxa na tabela de meta é nula. Como a comparação
  com nulo é falsa, eles entraram na conta de "não bate".
- **183** têm diferença de exatamente 0,05 p.p., como 67,35 contra 67,4 ou 64,85 contra
  64,9. É arredondamento para 1 casa decimal, que o ponto flutuante registra como um
  pouco acima de 0,05 (`meta_municipio_2023_divergentes`, 186 casos com `> 0.05`).
- **3** têm divergência real (os maiores casos da consulta):

| id_municipio | UF | taxa na meta | rede `3` em `municipio` | rede `5` |
| --- | --- | ---: | ---: | ---: |
| 2305100 | CE | 55,6 | 91,67 | 91,67 |
| 2304905 | CE | 83,1 | 99,15 | 99,15 |
| 5106752 | MT | 68,0 | 65,94 | 65,94 |

Os 186 se espalham por 24 UFs, sem concentração: o máximo é 26, em MG. Nos 3 casos reais,
a taxa da meta não bate com nenhuma rede. A hipótese é que a tabela de meta tenha usado
um resultado de 2023 anterior a alguma revisão do INEP, mas isso não dá para verificar
com este dataset.

**Decisão.** O resultado do município vem sempre de `municipio.taxa_alfabetizacao`
(rede `3`), a tabela de resultado. A taxa que vem na tabela de meta fica na Silver como
`taxa_referencia_meta`, só para conferência. Uma regra informativa conta os municípios
com diferença acima de **0,06 p.p.**, margem que absorve o arredondamento para 1 casa e o
erro de ponto flutuante. O valor esperado é 3 em 2023 e 0 em 2024.

## 3. Cobertura do join município × meta municipal

| ano | metas | sem resultado na rede `3` | correspondência |
| --- | ---: | ---: | ---: |
| 2023 | 5.352 | 50 | 99,07% |
| 2024 | 5.352 | 0 | 100% |

`meta_alfabetizacao_municipio` cobre 5.352 municípios por ano, sem chave repetida.
`meta_alfabetizacao_2030` nunca é nula. Em 2023, `taxa_alfabetizacao` é nula para 120
municípios.

**Decisão.** Limiar bloqueante de **99%** para o match `meta_municipio → municipio
(rede 3)`. Ele fica abaixo do pior ano observado (99,07%) e acima do que um erro de chave
produziria: um `id_municipio` em formato diferente derruba o match para perto de 0%.

## 4. DF e RR não estão na tabela `uf`

| | DF (53) | RR (14) |
| --- | --- | --- |
| `uf` | ausente | ausente |
| `municipio` | só 2024 (redes `2` e `5`, 1 linha cada) | ausente |
| `alunos` | só 2024 (27.698 linhas) | ausente |
| `meta_alfabetizacao_uf` taxa | 2023 nula · 2024 **59,13** · 2025 65,0 | 2023 nula · 2024 nula · 2025 57,0 |
| `meta_alfabetizacao_uf` meta 2030 | 80 | nula em 2023 e 2024 · 80 em 2025 |

**Conclusão (confirmado para RR, parcial para DF).**
- **RR** não tem nenhum resultado em 2023 e 2024 em nenhuma tabela. A única informação
  é a de 2025, na tabela de meta.
- **DF** não tem resultado em 2023. Em 2024 tem resultado em `municipio` (Brasília,
  `5300108`, que é a UF inteira) e em `alunos`, mas falta na tabela `uf`. A taxa de 2024
  existe na meta de UF.
- DF não tem rede municipal: a rede distrital aparece como estadual (`2`).
- Brasília (`5300108`) em 2024 tem taxa **59,13** nas redes `2` e `5`, idêntica à de DF
  em `meta_alfabetizacao_uf` (`df_2024_municipio_vs_meta_uf`). DF e RR não têm nenhuma
  linha em `meta_alfabetizacao_municipio`.

**Decisão.** Na Gold, UF sem resultado recebe o estado `sem_resultado`, nunca
`nao_atingiu`. O match `meta_uf → uf (rede 5)` não pode exigir 27/27 UFs. Hoje são 49 de
54 pares UF-ano (27 UFs × 2 anos). Os 5 pares sem resultado (`uf_rede_5_faltante`) são
AC 2023, DF 2023, DF 2024, RR 2023 e RR 2024. Em nenhum deles há linha em `uf`, e só DF
2024 tem taxa na meta. Com o resultado de Brasília para DF 2024, a cobertura esperada é
24 UFs em 2023 e 26 em 2024. O limiar será fixado no valor observado, para que
qualquer perda nova reprove. Para DF 2024, a Gold de UF usa o valor de Brasília, com
`fonte_resultado = 'municipio'`. 

## 5. Meta nacional: a edição de 2025 tem a trajetória arredondada

| linha (`ano`) | taxa | meta 2024 | meta 2025 | meta 2026 | … | meta 2030 | participação |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 2023 | 55,9 | 59,9 | 63,77 | 67,47 | | 80 | 86,0 |
| 2024 | 59,2 | 59,9 | 63,77 | 67,47 | | 80 | 87,37 |
| 2025 | 66,0 | 60,0 | 64,0 | 67,0 | | 80 | 88,0 |

Cada linha da tabela é uma publicação. A de 2025 traz a mesma trajetória com valores
inteiros. Não existe `meta_alfabetizacao_2023`: 2023 é a linha de base do Compromisso.

**Decisão.** O resultado do ano *Y* é comparado com `meta_alfabetizacao_Y` da linha do
mesmo ano *Y*. Isso dá:
- 2024: 59,2 contra a meta de 59,9, `nao_atingiu`;
- 2025: 66 contra a meta de 64, `atingiu`;
- 2023: `sem_meta`, por ser a linha de base.

A meta pactuada de 2030 é 80 nas três linhas. A universalização (100%) só aparece na
coluna `distancia_cenario_aspiracional`.

## 6. `nivel_alfabetizacao` é a taxa em faixas: vazamento de alvo

| nível | faixa da taxa (2024) |
| --- | --- |
| 0 | menor que 40 |
| 1 | 40 a 50 |
| 2 | 50 a 60 |
| 3 | 60 a 70 |
| 4 | 70 a 80 |
| 5 | 80 ou mais |

O nível é nulo exatamente quando a taxa é nula (120 municípios em 2023).

**Decisão.** A coluna fica na Silver com o nome `faixa_taxa_alfabetizacao`. Ela é
**excluída de `gold.features_ml`**, porque é derivada do alvo do mesmo ano.

## 7. Universo de alunos: o ausente tem `alfabetizado = '0'`, e não nulo

| ano | presença | caderno | linhas | proficiência | soma dos pesos |
| --- | --- | --- | ---: | --- | ---: |
| 2023 | ausente | não preenchido | 244.381 | nula | nula |
| 2023 | presente | não preenchido | 249 | nula | nula |
| 2023 | presente | preenchido | 1.502.809 | preenchida | 1.743.618 |
| 2024 | ausente | não preenchido | 267.772 | nula | nula |
| 2024 | presente | não preenchido | 936 | nula | nula |
| 2024 | presente | preenchido | 1.851.852 | preenchida | 2.109.175 |

`alfabetizado` nunca é nulo, nem para ausentes. Uma taxa sobre todos os registros contaria
cerca de 245 mil (2023) e 268 mil (2024) ausentes como "não alfabetizados". O peso é nulo
fora do universo avaliado, e `id_aluno` não se repete dentro do ano.

**Decisão.** `silver.alunos_avaliados` = `presenca = '1' AND preenchimento_caderno = '1'`.
A taxa recalculada é `SUM(peso_aluno × alfabetizado) / SUM(peso_aluno)` sobre esse
universo. Uma regra bloqueante exige `peso_aluno` não nulo em todo aluno avaliado.

## 8. Recortes de rede diferentes por ano em `municipio`

| ano | rede `0` | rede `2` | rede `3` | rede `5` |
| --- | ---: | ---: | ---: | ---: |
| 2023 | — | 1.149 | 5.448 | 4.950 |
| 2024 | 398 | 1.086 | 5.448 | 5.516 |

A rede `0` (Total, com a privada) só existe em 2024, para 398 municípios. Na tabela
`uf`, aparece em uma única linha: BA 2024, com 35,96 (`rede_0_uf`). Não há chave
(ano, rede, id_municipio) repetida.

**Decisão.** A evolução temporal por município usa só recortes presentes nos dois anos:
rede `3`, e rede `5` onde houver nos dois. A rede `0` não entra na série histórica.

## 9. Diretório de municípios: esquema e recorte

`municipio` e `meta_alfabetizacao_municipio` só têm `id_municipio`, então a ligação com
UF e região vem de `basedosdados.br_bd_diretorios_brasil.municipio`. As consultas
geradas pelo site da Base dos Dados (`consultas_basedosdados/`) usam a mesma tabela.

Esquema real (`diretorio_colunas`, via `INFORMATION_SCHEMA.COLUMNS`): 27 colunas. As 8
levadas para a Bronze são `id_municipio`, `nome`, `capital_uf` (INT64), `id_uf`,
`sigla_uf`, `nome_uf`, `nome_regiao` e `amazonia_legal` (INT64). As outras 19 ficam
fora: `id_municipio_6`, `id_municipio_tse`, `id_municipio_rf`, `id_municipio_bcb`,
`id_comarca`, as regiões de saúde, imediata, intermediária, micro, meso e metropolitana,
`ddd` e `centroide` (GEOGRAPHY). A tabela `uf` do diretório (`id_uf`, `sigla`, `nome`,
`regiao`) não é necessária, porque o diretório de municípios já traz `nome_uf` e
`nome_regiao`.

`__TABLES__` desse dataset dá **403** para o nosso projeto, ao contrário do dataset de
alfabetização. O tamanho do diretório sai da primeira carga da Bronze.
