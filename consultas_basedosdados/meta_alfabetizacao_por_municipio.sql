SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    dados.rede as rede,
    dados.taxa_alfabetizacao as taxa_alfabetizacao,
    dados.meta_alfabetizacao_2024 as meta_alfabetizacao_2024,
    dados.meta_alfabetizacao_2025 as meta_alfabetizacao_2025,
    dados.meta_alfabetizacao_2026 as meta_alfabetizacao_2026,
    dados.meta_alfabetizacao_2027 as meta_alfabetizacao_2027,
    dados.meta_alfabetizacao_2028 as meta_alfabetizacao_2028,
    dados.meta_alfabetizacao_2029 as meta_alfabetizacao_2029,
    dados.meta_alfabetizacao_2030 as meta_alfabetizacao_2030,
    dados.nivel_alfabetizacao as nivel_alfabetizacao,
    dados.percentual_participacao as percentual_participacao
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio