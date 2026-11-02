"""Investigação das divergências apontadas pelo perfil (fecha a etapa 1).

Cada consulta responde uma pergunta que a Silver precisa decidir antes dos joins:

- rede_meta_uf / rede_meta_municipio: as metas usam rede em texto ('Pública',
  'Municipal'), sem entrada no dicionario. A qual código de uf/municipio cada uma
  corresponde? Critério: taxa_alfabetizacao igual nos dois lados, no mesmo ano e chave.
- meta_municipio_sem_par: quantos municípios com meta não têm resultado na rede
  correspondente (base da taxa de correspondência do join na Silver).
- df_rr: DF e RR estão em meta_alfabetizacao_uf, mas não na tabela uf. Eles têm
  dados em municipio e alunos?
- meta_brasil: as 3 linhas da tabela (inclui 2025, que nenhuma tabela de resultado tem).
- nivel_alfabetizacao: coluna sem entrada no dicionario.
- chaves_municipio / alunos_*: duplicidade de chave, nulos e o universo de avaliados.

Saída: docs/perfil/investigacao.json (mescla com o que já existe) e uma linha em
ops.execucoes.
Uso:   python -m scripts.investigar_fonte
       python -m scripts.investigar_fonte --consultas rede_meta_uf_por_ano df_rr_meta_municipio
"""
from __future__ import annotations

import argparse
import json
import logging

from config.settings import RAIZ, Settings, carregar
from pipeline.bq import ClienteBQ, formatar_bytes
from pipeline.ops import gravador_bigquery, novo_id_execucao, registrar_etapa

log = logging.getLogger("investigar_fonte")

TOLERANCIA = 0.01  # pontos percentuais; as taxas vêm com 2 casas na fonte


def consultas(s: Settings) -> dict[str, str]:
    F = s.fonte
    return {
        "rede_meta_uf": f"""
            SELECT u.rede, COUNT(*) AS pares,
                   COUNTIF(ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) <= {TOLERANCIA}) AS taxa_igual
            FROM {F('meta_alfabetizacao_uf')} m
            JOIN {F('uf')} u USING (ano, sigla_uf)
            GROUP BY u.rede ORDER BY u.rede""",
        "rede_meta_municipio": f"""
            SELECT u.rede, COUNT(*) AS pares,
                   COUNTIF(ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) <= {TOLERANCIA}) AS taxa_igual
            FROM {F('meta_alfabetizacao_municipio')} m
            JOIN {F('municipio')} u USING (ano, id_municipio)
            GROUP BY u.rede ORDER BY u.rede""",
        "meta_municipio_sem_par": f"""
            SELECT m.ano, COUNT(*) AS metas, COUNTIF(u.id_municipio IS NULL) AS sem_resultado_rede_3
            FROM {F('meta_alfabetizacao_municipio')} m
            LEFT JOIN (SELECT DISTINCT ano, id_municipio FROM {F('municipio')} WHERE rede = '3') u
              USING (ano, id_municipio)
            GROUP BY m.ano ORDER BY m.ano""",
        "df_rr_municipio": f"""
            SELECT SUBSTR(id_municipio, 1, 2) AS cod_uf, ano, rede, COUNT(*) AS linhas
            FROM {F('municipio')} WHERE SUBSTR(id_municipio, 1, 2) IN ('14', '53')
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""",
        "df_rr_alunos": f"""
            SELECT SUBSTR(id_municipio, 1, 2) AS cod_uf, ano, COUNT(*) AS linhas
            FROM {F('alunos')} WHERE SUBSTR(id_municipio, 1, 2) IN ('14', '53')
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "df_rr_meta_uf": f"""
            SELECT ano, sigla_uf, rede, taxa_alfabetizacao, meta_alfabetizacao_2030
            FROM {F('meta_alfabetizacao_uf')} WHERE sigla_uf IN ('DF', 'RR') ORDER BY 2, 1""",
        "meta_brasil": f"SELECT * FROM {F('meta_alfabetizacao_brasil')} ORDER BY ano",
        "nivel_alfabetizacao": f"""
            SELECT ano, nivel_alfabetizacao, COUNT(*) AS linhas,
                   MIN(taxa_alfabetizacao) AS taxa_min, MAX(taxa_alfabetizacao) AS taxa_max
            FROM {F('meta_alfabetizacao_municipio')} GROUP BY 1, 2 ORDER BY 1, 2""",
        "nulos_meta_municipio": f"""
            SELECT ano, COUNT(*) AS linhas, COUNT(DISTINCT id_municipio) AS municipios,
                   COUNTIF(taxa_alfabetizacao IS NULL) AS taxa_nula,
                   COUNTIF(meta_alfabetizacao_2030 IS NULL) AS meta_2030_nula
            FROM {F('meta_alfabetizacao_municipio')} GROUP BY 1 ORDER BY 1""",
        "chaves_municipio": f"""
            SELECT ano, rede, COUNT(*) AS linhas, COUNT(DISTINCT id_municipio) AS municipios,
                   COUNTIF(taxa_alfabetizacao IS NULL) AS taxa_nula
            FROM {F('municipio')} GROUP BY 1, 2 ORDER BY 1, 2""",
        "alunos_universo": f"""
            SELECT ano, presenca, preenchimento_caderno,
                   alfabetizado IS NULL AS alfabetizado_nulo, proficiencia IS NULL AS proficiencia_nula,
                   COUNT(*) AS linhas, ROUND(SUM(peso_aluno)) AS soma_pesos
            FROM {F('alunos')} GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 3, 4, 5""",
        "alunos_duplicidade": f"""
            SELECT ano, COUNT(*) AS linhas, COUNT(DISTINCT id_aluno) AS alunos_distintos
            FROM {F('alunos')} GROUP BY 1 ORDER BY 1""",
        # Acompanhamento: a taxa da meta bate pouco com a do resultado. Hipótese: as
        # metas de 2023 vêm com 1 casa decimal (arredondamento até 0,05 p.p.).
        "rede_meta_municipio_por_ano": f"""
            SELECT m.ano, u.rede, COUNT(*) AS pares,
                   COUNTIF(ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) <= {TOLERANCIA}) AS igual_001,
                   COUNTIF(ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) <= 0.05) AS igual_005,
                   COUNTIF(ROUND(u.taxa_alfabetizacao, 1) = m.taxa_alfabetizacao) AS igual_arredondado_1,
                   ROUND(MAX(ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao)), 2) AS maior_diferenca
            FROM {F('meta_alfabetizacao_municipio')} m
            JOIN {F('municipio')} u USING (ano, id_municipio)
            WHERE u.rede IN ('3', '5')
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "rede_meta_uf_por_ano": f"""
            SELECT m.ano, m.sigla_uf, m.taxa_alfabetizacao AS taxa_meta,
                   u.taxa_alfabetizacao AS taxa_uf_rede_5,
                   ROUND(m.taxa_alfabetizacao - u.taxa_alfabetizacao, 2) AS diferenca
            FROM {F('meta_alfabetizacao_uf')} m
            JOIN {F('uf')} u USING (ano, sigla_uf)
            WHERE u.rede = '5'
              AND (m.taxa_alfabetizacao IS NULL
                   OR ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) > {TOLERANCIA})
            ORDER BY 1, 2""",
        # DF tem dado em municipio/alunos em 2024, mas não na tabela uf. O valor do
        # município 5300108 (Brasília, rede 5) bate com a meta_uf de DF?
        "df_2024_municipio_vs_meta_uf": f"""
            SELECT u.ano, u.rede, u.taxa_alfabetizacao AS taxa_municipio_brasilia,
                   m.taxa_alfabetizacao AS taxa_meta_uf_df
            FROM {F('municipio')} u
            LEFT JOIN {F('meta_alfabetizacao_uf')} m
              ON m.ano = u.ano AND m.sigla_uf = 'DF'
            WHERE u.id_municipio = '5300108' ORDER BY 1, 2""",
        "df_rr_meta_municipio": f"""
            SELECT SUBSTR(id_municipio, 1, 2) AS cod_uf, ano, COUNT(*) AS linhas,
                   COUNTIF(taxa_alfabetizacao IS NULL) AS taxa_nula
            FROM {F('meta_alfabetizacao_municipio')}
            WHERE SUBSTR(id_municipio, 1, 2) IN ('14', '53') GROUP BY 1, 2 ORDER BY 1, 2""",
        "rede_0_uf": f"""
            SELECT ano, sigla_uf, taxa_alfabetizacao FROM {F('uf')} WHERE rede = '0' ORDER BY 1, 2""",
        # 256 municípios de 2023 com taxa da meta a mais de 0,05 p.p. da rede 3. A taxa
        # da meta bate com outra rede, ou é um valor que não aparece em lugar nenhum?
        "meta_municipio_2023_divergentes": f"""
            WITH r AS (
              SELECT id_municipio,
                     MAX(IF(rede = '3', taxa_alfabetizacao, NULL)) AS r3,
                     MAX(IF(rede = '5', taxa_alfabetizacao, NULL)) AS r5,
                     MAX(IF(rede = '2', taxa_alfabetizacao, NULL)) AS r2
              FROM {F('municipio')} WHERE ano = 2023 GROUP BY 1),
            d AS (
              SELECT m.id_municipio, m.taxa_alfabetizacao AS taxa_meta, r.r3, r.r5, r.r2
              FROM {F('meta_alfabetizacao_municipio')} m JOIN r USING (id_municipio)
              WHERE m.ano = 2023 AND ABS(m.taxa_alfabetizacao - r.r3) > 0.05)
            SELECT COUNT(*) AS divergentes,
                   COUNTIF(ABS(taxa_meta - r5) <= 0.05) AS bate_rede_5,
                   COUNTIF(ABS(taxa_meta - r2) <= 0.05) AS bate_rede_2,
                   COUNTIF(r5 IS NULL) AS sem_rede_5,
                   COUNTIF(SUBSTR(id_municipio, 1, 2) = '35') AS em_sp,
                   ARRAY_AGG(STRUCT(id_municipio, taxa_meta, r3, r5, r2) ORDER BY ABS(taxa_meta - r3) DESC LIMIT 10) AS exemplos
            FROM d""",
        "meta_municipio_2023_divergentes_por_uf": f"""
            SELECT SUBSTR(m.id_municipio, 1, 2) AS cod_uf, COUNT(*) AS divergentes
            FROM {F('meta_alfabetizacao_municipio')} m
            JOIN {F('municipio')} u USING (ano, id_municipio)
            WHERE m.ano = 2023 AND u.rede = '3' AND ABS(m.taxa_alfabetizacao - u.taxa_alfabetizacao) > 0.05
            GROUP BY 1 ORDER BY 2 DESC""",
        "uf_rede_5_faltante": f"""
            SELECT m.ano, m.sigla_uf, m.taxa_alfabetizacao AS taxa_meta,
                   ARRAY_AGG(DISTINCT u.rede IGNORE NULLS) AS redes_em_uf
            FROM {F('meta_alfabetizacao_uf')} m
            LEFT JOIN {F('uf')} u USING (ano, sigla_uf)
            WHERE m.ano IN (2023, 2024)
            GROUP BY 1, 2, 3
            HAVING COUNTIF(u.rede = '5') = 0
            ORDER BY 1, 2""",
        # Diretório de municípios (8ª tabela da Bronze): colunas reais antes de usar.
        "diretorio_colunas": f"""
            SELECT table_name, ordinal_position, column_name, data_type
            FROM `{s.fonte_projeto}.{s.fonte_diretorios}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name IN ('municipio', 'uf')
            ORDER BY table_name, ordinal_position""",
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    todas = consultas(carregar({"GCP_PROJECT": "x", "GCS_BUCKET": "x"}))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--consultas", nargs="+", choices=list(todas), default=list(todas))
    args = parser.parse_args()

    s = carregar()
    cliente = ClienteBQ(s)
    gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")

    destino = RAIZ / "docs" / "perfil" / "investigacao.json"
    resultado = json.loads(destino.read_text(encoding="utf-8")) if destino.exists() else {}
    execucoes = resultado.pop("_execucoes", [])
    if "_execucao" in resultado:  # formato da primeira versão do script
        execucoes.append(resultado.pop("_execucao"))

    with registrar_etapa(gravar, novo_id_execucao(), "investigar_fonte", "fonte") as reg:
        for nome, sql in consultas(s).items():
            if nome not in args.consultas:
                continue
            r = cliente.executar(sql)
            reg.acumular(r)
            resultado[nome] = r.linhas
            print(f"\n### {nome}")
            for linha in r.linhas:
                print("  ", json.dumps(linha, ensure_ascii=False, default=str))
        execucoes.append({"id_execucao": reg.id_execucao, "consultas": args.consultas,
                          "job_ids": reg.job_ids})
        resultado["_execucoes"] = execucoes

    destino.write_text(
        json.dumps(resultado, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    log.info("salvo em %s | bytes faturados: %s",
             destino.relative_to(RAIZ), formatar_bytes(reg.bytes_faturados))


if __name__ == "__main__":
    main()
