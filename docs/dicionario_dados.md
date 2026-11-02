# Dicionário de dados da fonte

Gerado por `scripts/perfil_fonte.py` em 2026-11-02 (execução `20261102T160721Z_9ebf14`), a partir de `basedosdados.br_inep_avaliacao_alfabetizacao`. Não edite à mão: rode o script de novo.

## Tabelas

| Tabela | Linhas | Tamanho lógico | Colunas |
| --- | ---: | ---: | ---: |
| `alunos` | 3.867.999 | 256.1 MiB | 12 |
| `dicionario` | 27 | 1.1 KiB | 5 |
| `meta_alfabetizacao_brasil` | 3 | 270 B | 11 |
| `meta_alfabetizacao_municipio` | 10.704 | 1.1 MiB | 13 |
| `meta_alfabetizacao_uf` | 81 | 7.2 KiB | 12 |
| `municipio` | 23.995 | 1.7 MiB | 15 |
| `uf` | 145 | 10.1 KiB | 15 |

Dry run de `SELECT *` em `alunos`: **256.1 MiB** escaneados. É o número de referência para a otimização de FinOps.

## Divergências entre tabelas

Colunas que aparecem em mais de uma tabela com conjuntos de valores diferentes. Cada divergência precisa de tratamento na Silver (ex.: `rede_padrao`) antes dos joins.

- **`ano`** (alunos, meta_alfabetizacao_brasil, meta_alfabetizacao_municipio, meta_alfabetizacao_uf, municipio, uf)
    - `alunos` não tem: `2025`
    - `meta_alfabetizacao_municipio` não tem: `2025`
    - `municipio` não tem: `2025`
    - `uf` não tem: `2025`
- **`rede`** (alunos, meta_alfabetizacao_brasil, meta_alfabetizacao_municipio, meta_alfabetizacao_uf, municipio, uf)
    - `alunos` não tem: `0`, `5`, `Municipal`, `Pública`
    - `meta_alfabetizacao_brasil` não tem: `0`, `2`, `3`, `4`, `5`, `Municipal`
    - `meta_alfabetizacao_municipio` não tem: `0`, `2`, `3`, `4`, `5`, `Pública`
    - `meta_alfabetizacao_uf` não tem: `0`, `2`, `3`, `4`, `5`, `Municipal`
    - `municipio` não tem: `4`, `Municipal`, `Pública`
    - `uf` não tem: `4`, `Municipal`, `Pública`
- **`sigla_uf`** (meta_alfabetizacao_uf, uf)
    - `uf` não tem: `DF`, `RR`

## Valores distintos por tabela

### `alunos`

- `alfabetizado` (2 valores): `0`, `1`
- `ano` (2 valores): `2023`, `2024`
- `preenchimento_caderno` (2 valores): `0`, `1`
- `presenca` (2 valores): `0`, `1`
- `rede` (3 valores): `2`, `3`, `4`
- `serie` (1 valores): `2`

### `meta_alfabetizacao_brasil`

- `ano` (3 valores): `2023`, `2024`, `2025`
- `rede` (1 valores): `Pública`

### `meta_alfabetizacao_municipio`

- `ano` (2 valores): `2023`, `2024`
- `rede` (1 valores): `Municipal`

### `meta_alfabetizacao_uf`

- `ano` (3 valores): `2023`, `2024`, `2025`
- `rede` (1 valores): `Pública`
- `sigla_uf` (27 valores): `AC`, `AL`, `AM`, `AP`, `BA`, `CE`, `DF`, `ES`, `GO`, `MA`, `MG`, `MS`, `MT`, `PA`, `PB`, `PE`, `PI`, `PR`, `RJ`, `RN`, `RO`, `RR`, `RS`, `SC`, `SE`, `SP`, `TO`

### `municipio`

- `ano` (2 valores): `2023`, `2024`
- `rede` (4 valores): `0`, `2`, `3`, `5`
- `serie` (1 valores): `2`

### `uf`

- `ano` (2 valores): `2023`, `2024`
- `rede` (4 valores): `0`, `2`, `3`, `5`
- `serie` (1 valores): `2`
- `sigla_uf` (25 valores): `AC`, `AL`, `AM`, `AP`, `BA`, `CE`, `ES`, `GO`, `MA`, `MG`, `MS`, `MT`, `PA`, `PB`, `PE`, `PI`, `PR`, `RJ`, `RN`, `RO`, `RS`, `SC`, `SE`, `SP`, `TO`

## Colunas e tipos

### `alunos`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `id_municipio` | STRING |
| `id_escola` | STRING |
| `id_aluno` | STRING |
| `caderno` | STRING |
| `serie` | STRING |
| `rede` | STRING |
| `presenca` | STRING |
| `preenchimento_caderno` | STRING |
| `alfabetizado` | STRING |
| `proficiencia` | FLOAT64 |
| `peso_aluno` | FLOAT64 |

### `dicionario`

| Coluna | Tipo |
| --- | --- |
| `id_tabela` | STRING |
| `nome_coluna` | STRING |
| `chave` | STRING |
| `cobertura_temporal` | STRING |
| `valor` | STRING |

### `meta_alfabetizacao_brasil`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `rede` | STRING |
| `taxa_alfabetizacao` | FLOAT64 |
| `meta_alfabetizacao_2024` | FLOAT64 |
| `meta_alfabetizacao_2025` | FLOAT64 |
| `meta_alfabetizacao_2026` | FLOAT64 |
| `meta_alfabetizacao_2027` | FLOAT64 |
| `meta_alfabetizacao_2028` | FLOAT64 |
| `meta_alfabetizacao_2029` | FLOAT64 |
| `meta_alfabetizacao_2030` | FLOAT64 |
| `percentual_participacao` | FLOAT64 |

### `meta_alfabetizacao_municipio`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `id_municipio` | STRING |
| `rede` | STRING |
| `taxa_alfabetizacao` | FLOAT64 |
| `meta_alfabetizacao_2024` | FLOAT64 |
| `meta_alfabetizacao_2025` | FLOAT64 |
| `meta_alfabetizacao_2026` | FLOAT64 |
| `meta_alfabetizacao_2027` | FLOAT64 |
| `meta_alfabetizacao_2028` | FLOAT64 |
| `meta_alfabetizacao_2029` | FLOAT64 |
| `meta_alfabetizacao_2030` | FLOAT64 |
| `nivel_alfabetizacao` | INT64 |
| `percentual_participacao` | FLOAT64 |

### `meta_alfabetizacao_uf`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `sigla_uf` | STRING |
| `rede` | STRING |
| `taxa_alfabetizacao` | FLOAT64 |
| `meta_alfabetizacao_2024` | FLOAT64 |
| `meta_alfabetizacao_2025` | FLOAT64 |
| `meta_alfabetizacao_2026` | FLOAT64 |
| `meta_alfabetizacao_2027` | FLOAT64 |
| `meta_alfabetizacao_2028` | FLOAT64 |
| `meta_alfabetizacao_2029` | FLOAT64 |
| `meta_alfabetizacao_2030` | FLOAT64 |
| `percentual_participacao` | FLOAT64 |

### `municipio`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `id_municipio` | STRING |
| `serie` | STRING |
| `rede` | STRING |
| `taxa_alfabetizacao` | FLOAT64 |
| `media_portugues` | FLOAT64 |
| `proporcao_aluno_nivel_0` | FLOAT64 |
| `proporcao_aluno_nivel_1` | FLOAT64 |
| `proporcao_aluno_nivel_2` | FLOAT64 |
| `proporcao_aluno_nivel_3` | FLOAT64 |
| `proporcao_aluno_nivel_4` | FLOAT64 |
| `proporcao_aluno_nivel_5` | FLOAT64 |
| `proporcao_aluno_nivel_6` | FLOAT64 |
| `proporcao_aluno_nivel_7` | FLOAT64 |
| `proporcao_aluno_nivel_8` | FLOAT64 |

### `uf`

| Coluna | Tipo |
| --- | --- |
| `ano` | INT64 |
| `sigla_uf` | STRING |
| `serie` | STRING |
| `rede` | STRING |
| `taxa_alfabetizacao` | FLOAT64 |
| `media_portugues` | FLOAT64 |
| `proporcao_aluno_nivel_0` | FLOAT64 |
| `proporcao_aluno_nivel_1` | FLOAT64 |
| `proporcao_aluno_nivel_2` | FLOAT64 |
| `proporcao_aluno_nivel_3` | FLOAT64 |
| `proporcao_aluno_nivel_4` | FLOAT64 |
| `proporcao_aluno_nivel_5` | FLOAT64 |
| `proporcao_aluno_nivel_6` | FLOAT64 |
| `proporcao_aluno_nivel_7` | FLOAT64 |
| `proporcao_aluno_nivel_8` | FLOAT64 |
