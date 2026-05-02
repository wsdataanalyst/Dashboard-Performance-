from __future__ import annotations

import math
from dataclasses import dataclass

from .domain import Seller

# Meta oficial: prazo médio (dias) deve ser menor ou igual a este valor.
META_PRAZO_MEDIO_DIAS: float = 43.0


@dataclass(frozen=True)
class BonusResult:
    nome: str
    margem_pct: float | None
    alcance_pct: float | None
    prazo_medio: int | None
    conversao_pct: float | None
    tme_minutos: float | None
    interacoes: int | None
    qtd_faturadas: int | None
    elegivel_margem: bool
    bateu_prazo: bool | None
    bateu_conversao: bool | None
    bateu_tme: bool | None
    bateu_interacao: bool | None
    bonus_margem: float
    bonus_prazo: float
    bonus_conversao: float
    bonus_tme: float
    bonus_interacao: float
    bonus_total: float


def calc_interacoes(s: Seller) -> int | None:
    # Regra do projeto: Interações = Iniciados + Recebidos + Chamadas
    if s.iniciados is None and s.recebidos is None and s.chamadas is None:
        return None
    return (s.iniciados or 0) + (s.recebidos or 0) + (s.chamadas or 0)


def calc_conversao(s: Seller) -> float | None:
    interacoes = calc_interacoes(s)
    if interacoes is None or s.qtd_faturadas in (None, 0):
        return None
    # Conversão = faturadas / interações
    if interacoes == 0:
        return None
    return round((s.qtd_faturadas / interacoes) * 100, 2)


def _coerce_meta_compare_float(valor: object) -> float | None:
    """Converte valor para float seguro para comparação com meta (rejeita bool, NaN, etc.)."""
    if valor is None or isinstance(valor, bool):
        return None
    try:
        if isinstance(valor, str) and not str(valor).strip():
            return None
        x = float(valor)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def bate_meta(valor: object, meta: float, direcao: str) -> bool | None:
    x = _coerce_meta_compare_float(valor)
    if x is None:
        return None
    m = float(meta)
    return x >= m if direcao == ">=" else x <= m


def calcular_bonus(s: Seller) -> BonusResult:
    interacoes = calc_interacoes(s)
    conversao = calc_conversao(s)

    elegivel_margem = bool(
        s.alcance_projetado_pct is not None
        and s.margem_pct is not None
        and s.alcance_projetado_pct >= 90.0
        and s.margem_pct >= 26.0
    )

    bateu_prazo = bate_meta(s.prazo_medio, META_PRAZO_MEDIO_DIAS, "<=")
    bateu_conversao = bate_meta(conversao, 12.0, ">=")
    bateu_tme = bate_meta(s.tme_minutos, 5.0, "<=")
    bateu_interacao = bate_meta(interacoes, 200, ">=")

    # Regras oficiais do bônus (conforme você definiu)
    bonus_margem = 150.0 if elegivel_margem else 0.0
    # Somente True conta (None = sem dado → não soma; evita ambiguidade com tipos numpy/pandas)
    bonus_prazo = 100.0 if bateu_prazo is True else 0.0
    bonus_conversao = 100.0 if bateu_conversao is True else 0.0
    bonus_tme = 150.0 if bateu_tme is True else 0.0
    bonus_interacao = 100.0 if bateu_interacao is True else 0.0
    bonus = bonus_margem + bonus_prazo + bonus_conversao + bonus_tme + bonus_interacao

    return BonusResult(
        nome=s.nome,
        margem_pct=s.margem_pct,
        alcance_pct=s.alcance_projetado_pct,
        prazo_medio=s.prazo_medio,
        conversao_pct=conversao,
        tme_minutos=s.tme_minutos,
        interacoes=interacoes,
        qtd_faturadas=s.qtd_faturadas,
        elegivel_margem=elegivel_margem,
        bateu_prazo=bateu_prazo,
        bateu_conversao=bateu_conversao,
        bateu_tme=bateu_tme,
        bateu_interacao=bateu_interacao,
        bonus_margem=bonus_margem,
        bonus_prazo=bonus_prazo,
        bonus_conversao=bonus_conversao,
        bonus_tme=bonus_tme,
        bonus_interacao=bonus_interacao,
        bonus_total=bonus,
    )


def calcular_time(sellers: list[Seller]) -> tuple[list[BonusResult], float]:
    results = [calcular_bonus(s) for s in sellers]
    results.sort(key=lambda r: r.bonus_total, reverse=True)
    total = sum(r.bonus_total for r in results)
    return results, total

