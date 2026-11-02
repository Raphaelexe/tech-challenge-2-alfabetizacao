"""Perfil da fonte (tarefa 1 do roteiro).

Levanta, direto do BigQuery público da Base dos Dados:
- colunas e tipos reais de todas as tabelas do dataset;
- linhas e tamanho de cada tabela;
- valores distintos das colunas categóricas (rede, série, ano...) em cada tabela,
  apontando onde a mesma coluna usa valores diferentes entre tabelas;
- quanto um SELECT * na tabela de alunos escanearia (dry run).

Saídas:
- docs/perfil/perfil_fonte.json   (dados brutos do perfil)
- docs/dicionario_dados.md        (relatório legível, vai para o README)
- uma linha em ops.execucoes      (evidência de execução)

Uso:  python -m scripts.perfil_fonte            (grava em ops.execucoes)
      python -m scripts.perfil_fonte --sem-ops  (antes de rodar infra/setup.sh)
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

from config.settings import RAIZ, Settings, carregar, validar_identificador
from pipeline.bq import ClienteBQ, formatar_bytes
from pipeline.ops import gravador_bigquery, novo_id_execucao, registrar_etapa

log = logging.getLogger("perfil_fonte")

# Colunas cujo conteúdo queremos comparar entre tabelas. Só são consultadas as
# que existirem de fato; colunas citadas na tabela dicionario também entram.
COLUNAS_CATEGORICAS = [
    "ano", "rede", "serie", "sigla_uf", "tipo_rede", "dependencia_administrativa",
    "presenca", "preenchimento_caderno", "alfabetizado", "localizacao",
]
MAX_DISTINTOS = 50  # acima disso a coluna não é categórica; mostramos só a contagem


# ---------------------------------------------------------------- funções puras

def agrupar_colunas(linhas: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    """INFORMATION_SCHEMA.COLUMNS -> {tabela: [{coluna, tipo}, ...]} na ordem original."""
    tabelas: dict[str, list[dict[str, str]]] = defaultdict(list)
    for l in sorted(linhas, key=lambda x: (x["table_name"], x["ordinal_position"])):
        tabelas[l["table_name"]].append({"coluna": l["column_name"], "tipo": l["data_type"]})
    return dict(tabelas)


def colunas_do_dicionario(linhas_dicionario: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Da tabela dicionario, extrai {tabela: {colunas com códigos}} se o formato permitir."""
    resultado: dict[str, set[str]] = defaultdict(set)
    for l in linhas_dicionario:
        tabela, coluna = l.get("id_tabela"), l.get("nome_coluna")
        if tabela and coluna:
            resultado[str(tabela)].add(str(coluna))
    return dict(resultado)


def escolher_colunas_categoricas(
    colunas: dict[str, list[dict[str, str]]],
    do_dicionario: dict[str, set[str]],
) -> dict[str, list[str]]:
    escolhidas = {}
    for tabela, cols in colunas.items():
        if tabela == "dicionario":
            continue
        nomes = {c["coluna"] for c in cols}
        alvo = (set(COLUNAS_CATEGORICAS) | do_dicionario.get(tabela, set())) & nomes
        if alvo:
            escolhidas[tabela] = sorted(alvo)
    return escolhidas


def comparar_valores(
    valores: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, Any]]:
    """Para cada coluna presente em 2+ tabelas, indica se os conjuntos de valores divergem.

    valores = {tabela: {coluna: [valores distintos como texto]}}
    """
    por_coluna: dict[str, dict[str, set[str]]] = defaultdict(dict)
    for tabela, cols in valores.items():
        for coluna, vals in cols.items():
            por_coluna[coluna][tabela] = set(vals)

    comparacao = {}
    for coluna, por_tabela in sorted(por_coluna.items()):
        if len(por_tabela) < 2:
            continue
        uniao = set().union(*por_tabela.values())
        exclusivos = {
            t: sorted(uniao - v) for t, v in por_tabela.items() if uniao - v
        }
        comparacao[coluna] = {
            "tabelas": sorted(por_tabela),
            "diverge": bool(exclusivos),
            "ausentes_por_tabela": exclusivos,
        }
    return comparacao


def montar_markdown(perfil: dict[str, Any]) -> str:
    l = [
        "# Dicionário de dados da fonte",
        "",
        f"Gerado por `scripts/perfil_fonte.py` em {perfil['data']} "
        f"(execução `{perfil['id_execucao']}`), a partir de "
        f"`{perfil['fonte']}`. Não edite à mão: rode o script de novo.",
        "",
        "## Tabelas",
        "",
        "| Tabela | Linhas | Tamanho lógico | Colunas |",
        "| --- | ---: | ---: | ---: |",
    ]
    for t in sorted(perfil["colunas"]):
        tam = perfil["tamanhos"].get(t, {})
        linhas = tam.get("linhas")
        linhas_txt = f"{linhas:,}".replace(",", ".") if isinstance(linhas, int) else "-"
        l.append(
            f"| `{t}` | {linhas_txt} | {formatar_bytes(tam.get('bytes'))} "
            f"| {len(perfil['colunas'][t])} |"
        )
    l += [
        "",
        f"Dry run de `SELECT *` em `alunos`: **{formatar_bytes(perfil['dry_run_alunos'])}** "
        "escaneados. É o número de referência para a otimização de FinOps.",
        "",
        "## Divergências entre tabelas",
        "",
        "Colunas que aparecem em mais de uma tabela com conjuntos de valores diferentes. "
        "Cada divergência precisa de tratamento na Silver (ex.: `rede_padrao`) antes dos joins.",
        "",
    ]
    divergentes = {c: d for c, d in perfil["comparacao"].items() if d["diverge"]}
    if not divergentes:
        l.append("Nenhuma divergência encontrada nas colunas comparadas.")
    for coluna, d in divergentes.items():
        l.append(f"- **`{coluna}`** ({', '.join(d['tabelas'])})")
        for tabela, faltam in d["ausentes_por_tabela"].items():
            l.append(f"    - `{tabela}` não tem: {', '.join(f'`{v}`' for v in faltam)}")
    l += ["", "## Valores distintos por tabela", ""]
    for tabela, cols in sorted(perfil["valores"].items()):
        l.append(f"### `{tabela}`")
        l.append("")
        for coluna, vals in cols.items():
            n = perfil["n_distintos"][tabela][coluna]
            amostra = ", ".join(f"`{v}`" for v in vals) if n <= MAX_DISTINTOS else "(não categórica)"
            l.append(f"- `{coluna}` ({n} valores): {amostra}")
        l.append("")
    l += ["## Colunas e tipos", ""]
    for tabela, cols in sorted(perfil["colunas"].items()):
        l.append(f"### `{tabela}`")
        l.append("")
        l.append("| Coluna | Tipo |")
        l.append("| --- | --- |")
        l += [f"| `{c['coluna']}` | {c['tipo']} |" for c in cols]
        l.append("")
    return "\n".join(l)


# ----------------------------------------------------------------- consultas

def consultar(cliente: ClienteBQ, s: Settings, reg) -> dict[str, Any]:
    ds = f"`{s.fonte_projeto}.{s.fonte_dataset}"

    r = cliente.executar(
        f"SELECT table_name, ordinal_position, column_name, data_type "
        f"FROM {ds}.INFORMATION_SCHEMA.COLUMNS`"
    )
    reg.acumular(r)
    colunas = agrupar_colunas(r.linhas)
    log.info("tabelas encontradas: %s", ", ".join(sorted(colunas)))

    tamanhos: dict[str, dict[str, int]] = {}
    try:
        r = cliente.executar(f"SELECT table_id, row_count, size_bytes FROM {ds}.__TABLES__`")
        reg.acumular(r)
        tamanhos = {
            x["table_id"]: {"linhas": x["row_count"], "bytes": x["size_bytes"]} for x in r.linhas
        }
    except Exception as exc:  # __TABLES__ pode ser bloqueado em alguns projetos
        log.warning("sem acesso a __TABLES__ (%s); tamanhos ficam em branco", exc)

    do_dic: dict[str, set[str]] = {}
    if "dicionario" in colunas:
        r = cliente.executar(f"SELECT * FROM {s.fonte('dicionario')}")
        reg.acumular(r)
        do_dic = colunas_do_dicionario(r.linhas)
        dicionario_linhas = r.linhas
    else:
        dicionario_linhas = []

    valores: dict[str, dict[str, list[str]]] = {}
    n_distintos: dict[str, dict[str, int]] = {}
    for tabela, cols in escolher_colunas_categoricas(colunas, do_dic).items():
        valores[tabela], n_distintos[tabela] = {}, {}
        for coluna in cols:
            validar_identificador(coluna, "coluna")
            r = cliente.executar(
                f"SELECT CAST({coluna} AS STRING) AS valor, COUNT(*) AS n "
                f"FROM {s.fonte(tabela)} GROUP BY 1 ORDER BY 1 LIMIT {MAX_DISTINTOS + 1}"
            )
            reg.acumular(r)
            vals = ["NULL" if x["valor"] is None else x["valor"] for x in r.linhas]
            n_distintos[tabela][coluna] = len(vals)
            valores[tabela][coluna] = vals[:MAX_DISTINTOS]

    dry_alunos = cliente.estimar(f"SELECT * FROM {s.fonte('alunos')}") if "alunos" in colunas else None

    return {
        "data": date.today().isoformat(),
        "id_execucao": reg.id_execucao,
        "fonte": f"{s.fonte_projeto}.{s.fonte_dataset}",
        "colunas": colunas,
        "tamanhos": tamanhos,
        "valores": valores,
        "n_distintos": n_distintos,
        "comparacao": comparar_valores(
            {t: {c: v for c, v in cols.items() if n_distintos[t][c] <= MAX_DISTINTOS}
             for t, cols in valores.items()}
        ),
        "dry_run_alunos": dry_alunos,
        "dicionario": dicionario_linhas,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sem-ops", action="store_true", help="não grava em ops.execucoes")
    args = parser.parse_args()

    s = carregar()
    cliente = ClienteBQ(s)
    if args.sem_ops:
        gravar = lambda linha: log.info("ops (não gravado): %s", linha["status"])  # noqa: E731
    else:
        gravar = gravador_bigquery(cliente.client, f"{s.gcp_project}.{s.ds_ops}.execucoes")

    with registrar_etapa(gravar, novo_id_execucao(), "perfil_fonte", "fonte") as reg:
        perfil = consultar(cliente, s, reg)

    destino = RAIZ / "docs"
    (destino / "perfil").mkdir(parents=True, exist_ok=True)
    (destino / "perfil" / "perfil_fonte.json").write_text(
        json.dumps(perfil, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    (destino / "dicionario_dados.md").write_text(montar_markdown(perfil), encoding="utf-8")
    log.info(
        "perfil salvo em docs/dicionario_dados.md | bytes faturados: %s | jobs: %d",
        formatar_bytes(reg.bytes_faturados), len(reg.job_ids),
    )


if __name__ == "__main__":
    main()
