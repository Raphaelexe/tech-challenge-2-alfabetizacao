WITH 
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'serie'
        AND id_tabela = 'alunos'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'rede'
        AND id_tabela = 'alunos'
),
dicionario_presenca AS (
    SELECT
        chave AS chave_presenca,
        valor AS descricao_presenca
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'presenca'
        AND id_tabela = 'alunos'
),
dicionario_preenchimento_caderno AS (
    SELECT
        chave AS chave_preenchimento_caderno,
        valor AS descricao_preenchimento_caderno
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'preenchimento_caderno'
        AND id_tabela = 'alunos'
),
dicionario_alfabetizado AS (
    SELECT
        chave AS chave_alfabetizado,
        valor AS descricao_alfabetizado
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'alfabetizado'
        AND id_tabela = 'alunos'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.id_escola as id_escola,
    dados.id_aluno as id_aluno,
    dados.caderno as caderno,
    descricao_serie AS serie,
    descricao_rede AS rede,
    descricao_presenca AS presenca,
    descricao_preenchimento_caderno AS preenchimento_caderno,
    descricao_alfabetizado AS alfabetizado,
    dados.proficiencia as proficiencia,
    dados.peso_aluno as peso_aluno
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie`
    ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede`
    ON dados.rede = chave_rede
LEFT JOIN `dicionario_presenca`
    ON dados.presenca = chave_presenca
LEFT JOIN `dicionario_preenchimento_caderno`
    ON dados.preenchimento_caderno = chave_preenchimento_caderno
LEFT JOIN `dicionario_alfabetizado`
    ON dados.alfabetizado = chave_alfabetizado