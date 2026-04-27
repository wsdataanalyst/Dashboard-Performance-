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


def _find_meta_faturamento_col(df: pd.DataFrame) -> str | None:
    """Meta de faturamento (R$), não '% Meta Margem'."""
    cols = _col_lookup(df)
    exact: str | None = None
    fallback: str | None = None
    for k, orig in cols.items():
        if "meta" not in k:
            continue
        if "margem" in k:
            continue
        if k.strip() == "meta":
            return orig
        if exact is None and k.startswith("meta "):
            exact = orig
        if fallback is None:
            fallback = orig
    return exact or fallback


def _find_meta_margem_col(df: pd.DataFrame) -> str | None:
    """Meta de margem (%) por departamento (não confundir com meta de faturamento)."""
    cols = _col_lookup(df)
    best: str | None = None
    for k, orig in cols.items():
        if "margem" not in k:
            continue
        if "meta" not in k:
            continue
        # Preferir colunas que explicitam "% meta margem"
        if "% meta" in k or k.startswith("% meta"):
            return orig
        if best is None:
            best = orig
    return best


def _find_margem_result_col(df: pd.DataFrame, *, skip: str | None) -> str | None:
    """
    Coluna de margem RESULTADO (% Margem), evitando confundir com '% Meta Margem'.
    Se `skip` for a coluna de meta, nunca retorna ela.
    """
    cols = _col_lookup(df)
    # Preferir explicitamente "% margem" / "margem %" sem "meta"
    preferred: list[str] = []
    fallback: list[str] = []
    for k, orig in cols.items():
        if skip is not None and orig == skip:
            continue
        if "margem" not in k:
            continue
        if "meta" in k:
            continue
        if "% margem" in k or k.startswith("% margem") or k.endswith(" % margem") or "margem %" in k:
            preferred.append(orig)
        else:
            fallback.append(orig)
    return preferred[0] if preferred else (fallback[0] if fallback else None)


def _col_by_excel_pos(df: pd.DataFrame, excel_col_letter: str) -> str | None:
    """
    Fallback por posição do Excel (A=0, B=1, ...).
    Útil quando o export vem com cabeçalho não reconhecível.
    """
    if df is None or df.empty:
        return None
    letter = str(excel_col_letter or "").strip().upper()
    if not letter or len(letter) != 1 or not ("A" <= letter <= "Z"):
        return None
    idx = ord(letter) - ord("A")
    if idx < 0 or idx >= len(df.columns):
        return None
    return str(df.columns[idx])


def _looks_like_percent_series(s: pd.Series) -> bool:
    try:
        v = pd.to_numeric(s, errors="coerce")
        v = v.dropna()
        if v.empty:
            return False
        # Normalmente meta/resultado de margem ficam entre 0 e 100 (ou 0-1 no Excel).
        # Se a mediana for absurda (ex.: milhares), não é campo de %.
        med = float(v.median())
        return abs(med) <= 120.0 or abs(med) <= 2.5
    except Exception:
        return False


def _find_faturamento_projetado_acumulado_col(df: pd.DataFrame) -> str | None:
    """Colunas tipo 'Fat. Projetado Acumulado' (não confundir só com 'Faturamento')."""
    return _find_col(
        df,
        "projetado acumulado",
        "projetado acum",
        "fat projetado acum",
        "fat. projetado acum",
        "faturamento projetado acumulado",
        "faturamento projetado",
        "proj acumulado",
    )


def _find_faturamento_real_col(df: pd.DataFrame, skip: str | None) -> str | None:
    """Faturamento realizado / período (evita reusar a coluna de projetado)."""
    for cand in ("faturamento", "fat real", "fatur real", "realizado", "fatur", "receita", "valor"):
        c = _find_col(df, cand)
        if c and (skip is None or c != skip):
            return c
    return None


def _recalc_alcance_projetado_pct(rec: dict[str, Any]) -> None:
    """
    Alcance projetado = (Fat. Projetado Acumulado / Meta faturamento) * 100
    quando ambos existem e meta > 0 — bate com a planilha (ex.: 3604,71%).
    Sobrescreve a coluna % da planilha para evitar erro de importação.
    """
    meta = rec.get("meta_faturamento")
    fp = rec.get("faturamento_projetado_acumulado")
    if meta is None or fp is None:
        return
    try:
        m = float(meta)
        p = float(fp)
    except (TypeError, ValueError):
        return
    if m <= 0:
        return
    rec["alcance_projetado_pct"] = round((p / m) * 100.0, 4)


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
    - meta (faturamento em R$, não % meta margem)
    - faturamento (realizado) e/ou Fat. Projetado Acumulado
    - participacao (%)
    - alcance projetado (%) — se existir **meta** e **faturamento projetado acumulado**,
      o app recalcula: (projetado / meta) * 100
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
            c_fat_proj = _find_faturamento_projetado_acumulado_col(df)
            c_fat = _find_faturamento_real_col(df, skip=c_fat_proj)
            c_meta = _find_meta_faturamento_col(df) or _find_col(df, "meta")
            c_meta_marg = _find_meta_margem_col(df) or _find_col(df, "meta margem", "% meta margem", "margem meta")
            c_part = _find_col(df, "particip", "% particip")
            c_alc = _find_col(df, "alcance", "alcance projet")
            c_marg = _find_margem_result_col(df, skip=c_meta_marg) or _find_col(df, "% margem")

            # Fallback pedido: na base "Faturamento por departamento", col G = % Meta Margem, col H = % Margem (resultado)
            # Só aplica quando ainda não identificou as colunas por nome.
            try:
                if c_meta_marg is None:
                    cand_g = _col_by_excel_pos(df, "G")
                    if cand_g and _looks_like_percent_series(df[cand_g]):
                        c_meta_marg = cand_g
                if c_marg is None:
                    cand_h = _col_by_excel_pos(df, "H")
                    if cand_h and _looks_like_percent_series(df[cand_h]):
                        c_marg = cand_h
            except Exception:
                pass

            if not c_dept or not (c_fat or c_fat_proj or c_meta or c_part or c_alc or c_marg):
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
                if c_fat_proj:
                    v = _to_float(r.get(c_fat_proj))
                    if v is not None:
                        rec["faturamento_projetado_acumulado"] = v
                if c_meta:
                    v = _to_float(r.get(c_meta))
                    if v is not None:
                        rec["meta_faturamento"] = v
                if c_meta_marg and c_meta_marg != c_meta:
                    v = normalize_small_excel_percent(r.get(c_meta_marg))
                    if v is not None:
                        rec["meta_margem_pct"] = v
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
                _recalc_alcance_projetado_pct(rec)

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

