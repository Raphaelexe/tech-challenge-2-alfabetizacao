"""Execução das regras de qualidade (quality/regras.yaml) e registro em ops.qualidade.

As regras de uma tabela viram uma única query (uma coluna por regra), porque o
BigQuery cobra no mínimo 10 MB por tabela referenciada em cada query.

Todas as regras são avaliadas e gravadas antes de qualquer decisão. Se alguma
bloqueante reprovar, levanta QualidadeBloqueanteError: quem chamou não publica a
tabela, e a exceção encerra a pipeline com erro.
"""
from __future__ import annotations

import logging
import operator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from config.settings import RAIZ, validar_identificador

log = logging.getLogger("qualidade")

ARQUIVO_REGRAS = RAIZ / "quality" / "regras.yaml"
SEVERIDADES = {"bloqueante", "informativa"}
OPERADORES: dict[str, Callable[[float, float], bool]] = {
    "=": operator.eq, "<=": operator.le, ">=": operator.ge,
}


class QualidadeBloqueanteError(RuntimeError):
    """Uma ou mais regras bloqueantes reprovaram."""


class RegraInvalidaError(ValueError):
    """Regra mal declarada em regras.yaml."""


@dataclass(frozen=True)
class Regra:
    nome: str
    tabela: str
    severidade: str
    operador: str
    limiar: float
    sql: str
    descricao: str = ""


@dataclass
class ResultadoRegra:
    regra: Regra
    valor: float | None

    @property
    def aprovada(self) -> bool:
        # Valor nulo (tabela vazia, divisão por zero) nunca aprova.
        return self.valor is not None and OPERADORES[self.regra.operador](self.valor, self.regra.limiar)

    def como_linha(self, id_execucao: str, camada: str, ts: datetime) -> dict[str, Any]:
        return {
            "id_execucao": id_execucao,
            "regra": self.regra.nome,
            "camada": camada,
            "severidade": self.regra.severidade,
            "valor_medido": self.valor,
            "limiar": self.regra.limiar,
            "status": "aprovada" if self.aprovada else "reprovada",
            "detalhe": f"{self.regra.tabela}: valor {self.valor} {self.regra.operador} {self.regra.limiar}?",
            "ts_validacao": ts.isoformat(),
        }


def carregar_regras(caminho: Path = ARQUIVO_REGRAS) -> list[Regra]:
    dados = yaml.safe_load(caminho.read_text(encoding="utf-8"))
    regras = []
    for bruta in dados["regras"]:
        r = Regra(**{**bruta, "limiar": float(bruta["limiar"])})
        validar_identificador(r.nome, "nome da regra")
        if r.severidade not in SEVERIDADES:
            raise RegraInvalidaError(f"{r.nome}: severidade {r.severidade!r}")
        if r.operador not in OPERADORES:
            raise RegraInvalidaError(f"{r.nome}: operador {r.operador!r}")
        regras.append(r)
    nomes = [r.nome for r in regras]
    repetidos = {n for n in nomes if nomes.count(n) > 1}
    if repetidos:
        raise RegraInvalidaError(f"regras com nome repetido: {sorted(repetidos)}")
    return regras


def montar_consulta(regras: list[Regra], marcadores: dict[str, str]) -> str:
    """Uma query com uma coluna por regra; cada regra é uma subconsulta escalar."""
    colunas = [
        f"  ({r.sql.format(**marcadores)}) AS {r.nome}" for r in regras
    ]
    return "SELECT\n" + ",\n".join(colunas)


def validar_tabela(
    cliente: Any,
    regras: list[Regra],
    marcadores: dict[str, str],
    id_execucao: str,
    camada: str,
    gravar: Callable[[list[dict[str, Any]]], None],
    reg: Any = None,
) -> list[ResultadoRegra]:
    """Roda as regras da tabela, grava todas em ops.qualidade e bloqueia se preciso."""
    if not regras:
        return []
    r = cliente.executar(montar_consulta(regras, marcadores))
    if reg is not None:
        reg.acumular(r)
    linha = r.linhas[0]
    ts = datetime.now(timezone.utc)
    resultados = [
        ResultadoRegra(regra, None if linha[regra.nome] is None else float(linha[regra.nome]))
        for regra in regras
    ]
    gravar([res.como_linha(id_execucao, camada, ts) for res in resultados])
    for res in resultados:
        nivel = logging.INFO if res.aprovada else (
            logging.ERROR if res.regra.severidade == "bloqueante" else logging.WARNING
        )
        log.log(nivel, "%s [%s] %s: valor=%s limiar %s %s",
                 "ok" if res.aprovada else "REPROVADA", res.regra.severidade,
                 res.regra.nome, res.valor, res.regra.operador, res.regra.limiar)

    bloqueios = [x for x in resultados if not x.aprovada and x.regra.severidade == "bloqueante"]
    if bloqueios:
        raise QualidadeBloqueanteError(
            "regras bloqueantes reprovadas: "
            + "; ".join(f"{x.regra.nome} (valor {x.valor}, limiar {x.regra.operador} {x.regra.limiar})"
                        for x in bloqueios)
        )
    return resultados
