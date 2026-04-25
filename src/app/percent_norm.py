from __future__ import annotations

from typing import Any

"""
Normalização de percentual vinda de planilha (Excel, HTML, exportações).

- Participação e margem costumam ficar no intervalo “Excel” 0–3 (ex.: 0,26, 1,0, 2,5) ou
  já vêm em pontos de % (ex.: 26, 15).
- Alcance projetado de departamentos pode passar muito de 100%: o Excel guarda
  1000% como 10,0; 1250% como 12,5; 1500% como 15,0, etc. A faixa 1–30 como
  fração ×100 cobre isso. Valores > 30 (ex.: 35, 64, 1500) são tratados como
  percentual já em pontos.
"""

PCT_DEC = 4  # casas decimais ao gravar / exibir cálculo


def to_float(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x != x or x in (float("inf"), float("-inf")):  # NaN / inf
            return None
        return x
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace("%", "").strip()
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") >= 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def normalize_small_excel_percent(v: Any) -> float | None:
    """
    Campos de margem/participação e indicadores raramente > 300%: fração 0–3 vira %.
    """
    f = to_float(v)
    if f is None:
        return None
    if abs(f) <= 3.0:
        return round(float(f) * 100.0, PCT_DEC)
    return round(float(f), PCT_DEC)


def normalize_alcance_projetado(v: Any) -> float | None:
    """
    Alcance projetado: pode ser fração 0–1, ou fração 1–30 (10 = 1000%, 12,5 = 1250%),
    ou já em pontos (35 = 35%, 1500 = 1500%).
    """
    f = to_float(v)
    if f is None:
        return None
    af = abs(f)
    if af < 1.0:
        return round(f * 100.0, PCT_DEC)
    if 1.0 <= af <= 30.0:
        return round(f * 100.0, PCT_DEC)
    return round(f, PCT_DEC)
