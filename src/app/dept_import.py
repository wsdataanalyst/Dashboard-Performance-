from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .percent_norm import normalize_alcance_projetado, normalize_small_excel_percent


@dataclass(frozen=True)
class DeptImportResult:
    payload: dict[str, Any]
    meta: dict[str, str]
    warnings: list[str]


def _looks_like_html(b: bytes) -> bool:
    head = (b or b"")[:256].lstrip().lower()
    return head.startswith(b"<") or b"<html" in head or b"<table" in head or b"<style" in head


def _read_excel_or_html(file_name: str, b: bytes) -> list[pd.DataFrame]:
    if (b or b"").startswith(b"Token is expired"):
        raise ValueError(f"Arquivo '{file_name}' inválido (conteúdo: Token is expired). Reexporte o arquivo.")
    if _looks_like_html(b):
        html = b.decode("utf-8", errors="ignore")
        return list(pd.read_html(io.StringIO(html)))

    ext = Path(file_name).suffix.lower()
    if ext == ".xlsx":
        return [pd.read_excel(io.BytesIO(b), engine="openpyxl")]
    if ext == ".xls":
        return [pd.read_excel(io.BytesIO(b), engine="xlrd")]
    return [pd.read_excel(io.BytesIO(b))]


def _norm_col(c: Any) -> str:
    s = str(c or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _col_lookup(df: pd.DataFrame) -> dict[str, str]:
    return {_norm_col(c): str(c) for c in df.columns}


def _find_col(df: pd.DataFrame, *needles: str) -> str | None:
    cols = _col_lookup(df)
    for n in needles:
        n2 = _norm_col(n)
        for k, orig in cols.items():
            if n2 in k:
                return orig
    return None


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
    s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") >= 1 else s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _clean_dept(name: Any) -> str:
    s = str(name or "").strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def import_departamentos(files: list[tuple[str, bytes]]) -> DeptImportResult:
    """
    Importa base de departamentos (produtos) a partir de Excel/HTML exportado.

    Colunas esperadas (flexível por match):
    - departamento / categoria / grupo
    - faturamento
    - meta
    - participacao (%)
    - alcance projetado (%)
    - margem (%)
    """
    warnings: list[str] = []
    departamentos: dict[str, dict[str, Any]] = {}

    for fname, b in files:
        tables = _read_excel_or_html(fname, b)
        handled = False
        for df in tables:
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            c_dept = _find_col(df, "depart", "categoria", "grupo", "setor")
            c_fat = _find_col(df, "fatur", "receita", "valor")
            c_meta = _find_col(df, "meta")
            c_part = _find_col(df, "particip", "% particip")
            c_alc = _find_col(df, "alcance", "alcance projet")
            c_marg = _find_col(df, "margem", "% margem")

            if not c_dept or not (c_fat or c_meta or c_part or c_alc or c_marg):
                continue

            for _, r in df.iterrows():
                dept = _clean_dept(r.get(c_dept) if c_dept else None)
                if not dept or dept.lower() in {"total", "geral"}:
                    continue
                rec = departamentos.setdefault(dept, {"departamento": dept})
                if c_fat:
                    v = _to_float(r.get(c_fat))
                    if v is not None:
                        rec["faturamento"] = v
                if c_meta:
                    v = _to_float(r.get(c_meta))
                    if v is not None:
                        rec["meta_faturamento"] = v
                if c_part:
                    v = normalize_small_excel_percent(r.get(c_part))
                    if v is not None:
                        rec["participacao_pct"] = v
                if c_alc:
                    v = normalize_alcance_projetado(r.get(c_alc))
                    if v is not None:
                        rec["alcance_projetado_pct"] = v
                if c_marg:
                    v = normalize_small_excel_percent(r.get(c_marg))
                    if v is not None:
                        rec["margem_pct"] = v

            handled = True
            break

        if not handled:
            warnings.append(
                f"Arquivo '{fname}' importado, mas não reconheci tabela de departamentos. "
                "Verifique colunas (departamento, faturamento, meta, participação, alcance, margem)."
            )

    payload: dict[str, Any] = {"departamentos": list(departamentos.values())}
    payload["departamentos"].sort(key=lambda x: str(x.get("departamento") or ""))
    return DeptImportResult(payload=payload, meta={"provider": "dept_excel_import", "model": "pandas"}, warnings=warnings)

