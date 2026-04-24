from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class KpiImportResult:
    kpis: dict[str, Any]
    warnings: list[str]


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("R$", "").replace("%", "").strip()
    # pt-BR: 1.234,56
    if s.count(",") == 1 and s.count(".") >= 1:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _to_int(v: Any) -> int | None:
    f = _to_float(v)
    return int(f) if f is not None else None


def import_faturamento_atendidos_xlsx(file_bytes: bytes) -> KpiImportResult:
    """
    Lê o arquivo no formato do seu export "Faturamento e Atendidos.xlsx".

    Estrutura observada:
    - Linhas iniciais com filtros
    - Linha de header real contendo: "Data - Mês", "Data - Dia", "Faturamento", "Meta",
      "# Clientes Atendidos", "# Notas Emitidas"
    """
    warnings: list[str] = []
    df0 = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", sheet_name=0, header=None)
    if df0.empty:
        return KpiImportResult(kpis={}, warnings=["Arquivo vazio."])

    header_row = None
    for i in range(min(len(df0), 60)):
        row = df0.iloc[i].astype(str).fillna("").tolist()
        joined = " | ".join(row).lower()
        if "faturamento" in joined and "meta" in joined and "clientes" in joined and "notas" in joined:
            header_row = i
            break

    if header_row is None:
        return KpiImportResult(kpis={}, warnings=["Não encontrei cabeçalho (Faturamento/Meta/Clientes/Notas)."])

    df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", sheet_name=0, header=header_row)
    df = df.rename(columns=lambda c: str(c).strip())

    # normaliza nomes (alguns vêm com encoding quebrado)
    col_mes = next((c for c in df.columns if "mês" in c.lower() or "mes" in c.lower()), None)
    col_dia = next((c for c in df.columns if "dia" in c.lower() and "data" in c.lower()), None)
    col_fat = next((c for c in df.columns if "fatur" in c.lower()), None)
    col_meta = next((c for c in df.columns if re.search(r"\bmeta\b", c.lower())), None)
    col_cli = next((c for c in df.columns if "clientes" in c.lower()), None)
    col_nf = next((c for c in df.columns if "notas" in c.lower()), None)

    if not (col_dia and col_fat and col_meta and col_cli and col_nf):
        warnings.append(f"Colunas detectadas: {list(df.columns)}")
        return KpiImportResult(kpis={}, warnings=["Cabeçalho encontrado, mas colunas essenciais faltando."] + warnings)

    # limpa linhas sem dia numérico
    df["_dia"] = pd.to_numeric(df[col_dia], errors="coerce")
    df = df[df["_dia"].notna()].copy()
    if df.empty:
        return KpiImportResult(kpis={}, warnings=["Não encontrei linhas de dia (numéricas)."])

    # considera "dia anterior" como último dia com faturamento (ou último dia do arquivo)
    df["_fat"] = pd.to_numeric(df[col_fat], errors="coerce")
    df2 = df[df["_fat"].notna()].copy()
    ref = df2.iloc[-1] if not df2.empty else df.iloc[-1]

    fat_total = float(pd.to_numeric(df[col_fat], errors="coerce").fillna(0).sum())
    # meta geralmente constante por dia; pega o último valor não nulo
    meta_series = pd.to_numeric(df[col_meta], errors="coerce").dropna()
    meta_val = float(meta_series.iloc[-1]) if len(meta_series) else None

    kpis = {
        "faturamento_total": fat_total,
        # Meta neste arquivo é diária (não a meta geral do mês/time)
        "meta_dia": meta_val,
        "faturamento_dia_anterior": _to_float(ref.get(col_fat)),
        "nf_dia_anterior": _to_int(ref.get(col_nf)),
        "clientes_dia_anterior": _to_int(ref.get(col_cli)),
        "nf_acumulado": _to_int(pd.to_numeric(df[col_nf], errors="coerce").fillna(0).sum()),
        "clientes_acumulado": _to_int(pd.to_numeric(df[col_cli], errors="coerce").fillna(0).sum()),
        "dia_referencia": int(ref["_dia"]),
        "mes_referencia": str(ref.get(col_mes)) if col_mes else None,
    }
    return KpiImportResult(kpis=kpis, warnings=warnings)

