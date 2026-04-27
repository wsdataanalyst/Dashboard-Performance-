from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

import pandas as pd

from .percent_norm import to_float


def _norm_col(c: Any) -> str:
    s = str(c or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _looks_like_html(b: bytes) -> bool:
    head = (b or b"")[:256].lstrip().lower()
    return head.startswith(b"<") or b"<html" in head or b"<table" in head or b"<style" in head


def _read_excel_any(file_bytes: bytes) -> pd.DataFrame:
    if _looks_like_html(file_bytes):
        html = file_bytes.decode("utf-8", errors="ignore")
        tables = pd.read_html(io.StringIO(html))
        return tables[0] if tables else pd.DataFrame()
    # tenta openpyxl e cai para xlrd
    try:
        return pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", header=None)
    except Exception:
        return pd.read_excel(io.BytesIO(file_bytes), engine="xlrd", header=None)


def _detect_header_row(df0: pd.DataFrame) -> int:
    """
    Detecta a linha do cabeçalho olhando por colunas-chave: orçamento, emissão, filial, vendedor, valor.
    """
    if df0 is None or df0.empty:
        return 0
    for i in range(min(len(df0), 40)):
        row = df0.iloc[i].astype(str).fillna("").tolist()
        joined = " | ".join(row).lower()
        if ("orc" in joined or "orçamento" in joined) and ("emiss" in joined or "filial" in joined) and ("vend" in joined or "consult" in joined):
            return i
    return 0


def _read_budget_df(file_bytes: bytes) -> pd.DataFrame:
    df0 = _read_excel_any(file_bytes)
    if df0.empty:
        return pd.DataFrame()
    header_row = _detect_header_row(df0)
    # relê com header encontrado
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", header=header_row)
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd", header=header_row)
        except Exception:
            # fallback: usa df0 e injeta header
            df = df0.copy()
            df.columns = df.iloc[header_row].astype(str).tolist()
            df = df.iloc[header_row + 1 :].copy()
    df = df.rename(columns=lambda c: str(c).strip())
    # remove colunas "Unnamed"
    drop = [c for c in df.columns if str(c).strip().lower().startswith("unnamed")]
    if drop:
        df = df.drop(columns=drop)
    return df


def _pick_col(df: pd.DataFrame, *needles: str) -> str | None:
    cols = { _norm_col(c): str(c) for c in df.columns }
    for n in needles:
        nn = _norm_col(n)
        for k, orig in cols.items():
            if nn in k:
                return orig
    return None


def _pick_valor_col(df: pd.DataFrame) -> str | None:
    # preferir "vlr bruto" / "valor bruto"
    c = _pick_col(df, "vlr bruto", "valor bruto", "vlr", "valor")
    return c


def _to_date(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, pd.Timestamp):
        return v.date().isoformat()
    try:
        dt = pd.to_datetime(v, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.date().isoformat()
    except Exception:
        return None


@dataclass(frozen=True)
class OrcamentosParsed:
    pendentes_df: pd.DataFrame
    finalizados_df: pd.DataFrame
    meta: dict[str, Any]


def parse_orcamentos(pend_bytes: bytes, fin_bytes: bytes) -> OrcamentosParsed:
    pend = _read_budget_df(pend_bytes)
    fin = _read_budget_df(fin_bytes)

    # mapeamento de colunas (flexível)
    col_orc_p = _pick_col(pend, "orc", "orçamento", "orcamento") if not pend.empty else None
    col_orc_f = _pick_col(fin, "orc", "orçamento", "orcamento") if not fin.empty else None

    col_filial_p = _pick_col(pend, "filial") if not pend.empty else None
    col_filial_f = _pick_col(fin, "filial") if not fin.empty else None

    col_emissao_p = _pick_col(pend, "emiss") if not pend.empty else None
    col_emissao_f = _pick_col(fin, "emiss") if not fin.empty else None

    col_finaliz_f = _pick_col(fin, "dt finaliz", "finaliz") if not fin.empty else None

    col_vend_p = _pick_col(pend, "vendedor", "nome vend", "consult") if not pend.empty else None
    col_vend_f = _pick_col(fin, "vendedor", "nome vend", "consult") if not fin.empty else None

    col_cnpj_p = _pick_col(pend, "cnpj?") if not pend.empty else None
    col_cnpj_f = _pick_col(fin, "cnpj?") if not fin.empty else None

    col_val_p = _pick_valor_col(pend) if not pend.empty else None
    col_val_f = _pick_valor_col(fin) if not fin.empty else None

    def _normalize(df: pd.DataFrame, *, kind: str) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()
        # colunas "sintéticas" para cálculos, sem perder as originais
        if kind == "pendentes":
            col_orc = col_orc_p
            col_filial = col_filial_p
            col_em = col_emissao_p
            col_v = col_vend_p
            col_c = col_cnpj_p
            col_val = col_val_p
        else:
            col_orc = col_orc_f
            col_filial = col_filial_f
            col_em = col_emissao_f
            col_v = col_vend_f
            col_c = col_cnpj_f
            col_val = col_val_f

        out["_orcamento"] = out[col_orc].astype(str).str.strip() if col_orc and col_orc in out.columns else ""
        out["_filial"] = out[col_filial].astype(str).str.strip() if col_filial and col_filial in out.columns else ""
        out["_emissao"] = out[col_em].apply(_to_date) if col_em and col_em in out.columns else None
        out["_consultor"] = out[col_v].astype(str).str.strip() if col_v and col_v in out.columns else ""
        out["_tipo_cliente"] = out[col_c].astype(str).str.strip().str.upper() if col_c and col_c in out.columns else ""
        out["_valor"] = out[col_val].apply(to_float) if col_val and col_val in out.columns else None
        if kind == "finalizados":
            out["_dt_finaliz"] = out[col_finaliz_f].apply(_to_date) if col_finaliz_f and col_finaliz_f in out.columns else None
        return out

    pend2 = _normalize(pend, kind="pendentes")
    fin2 = _normalize(fin, kind="finalizados")

    meta: dict[str, Any] = {
        "cols": {
            "pendentes": {
                "orcamento": col_orc_p,
                "filial": col_filial_p,
                "emissao": col_emissao_p,
                "consultor": col_vend_p,
                "tipo_cliente": col_cnpj_p,
                "valor": col_val_p,
            },
            "finalizados": {
                "orcamento": col_orc_f,
                "filial": col_filial_f,
                "emissao": col_emissao_f,
                "dt_finaliz": col_finaliz_f,
                "consultor": col_vend_f,
                "tipo_cliente": col_cnpj_f,
                "valor": col_val_f,
            },
        }
    }
    return OrcamentosParsed(pendentes_df=pend2, finalizados_df=fin2, meta=meta)

