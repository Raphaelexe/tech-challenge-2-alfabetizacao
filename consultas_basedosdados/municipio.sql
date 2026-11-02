WITH 
dicionario_serie AS (
    SELECT
        chave AS chave_serie,
        valor AS descricao_serie
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'serie'
        AND id_tabela = 'municipio'
),
dicionario_rede AS (
    SELECT
        chave AS chave_rede,
        valor AS descricao_rede
    FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'rede'
        AND id_tabela = 'municipio'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    descricao_serie AS serie,
    descricao_rede AS rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.media_portugues as media_portugues,
    dados.proporcao_aluno_nivel_0 as proporcao_aluno_nivel_0,
    dados.proporcao_aluno_nivel_1 as proporcao_aluno_nivel_1,
    dados.proporcao_aluno_nivel_2 as proporcao_aluno_nivel_2,
    dados.proporcao_aluno_nivel_3 as proporcao_aluno_nivel_3,
    dados.proporcao_aluno_nivel_4 as proporcao_aluno_nivel_4,
    dados.proporcao_aluno_nivel_5 as proporcao_aluno_nivel_5,
    dados.proporcao_aluno_nivel_6 as proporcao_aluno_nivel_6,
    dados.proporcao_aluno_nivel_7 as proporcao_aluno_nivel_7,
    dados.proporcao_aluno_nivel_8 as proporcao_aluno_nivel_8
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_serie`
    ON dados.serie = chave_serie
LEFT JOIN `dicionario_rede`
    ON dados.rede = chave_rede