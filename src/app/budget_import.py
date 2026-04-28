from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from .percent_norm import to_float
from .spreadsheet_bytes import assert_excel_or_html_bytes


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


def _read_excel_any(file_bytes: bytes, *, file_name: str = "planilha") -> pd.DataFrame:
    if _looks_like_html(file_bytes):
        html = file_bytes.decode("utf-8", errors="ignore")
        tables = pd.read_html(io.StringIO(html))
        return tables[0] if tables else pd.DataFrame()
    assert_excel_or_html_bytes(file_name, file_bytes)
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


def _read_budget_df(file_bytes: bytes, *, file_name: str = "planilha de orçamentos") -> pd.DataFrame:
    df0 = _read_excel_any(file_bytes, file_name=file_name)
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


def is_orcamento_workbook(file_bytes: bytes, *, file_name: str) -> bool:
    try:
        assert_excel_or_html_bytes(file_name, file_bytes)
    except ValueError:
        return False
    try:
        df = _read_budget_df(file_bytes, file_name=file_name)
    except Exception:
        return False
    if df.empty:
        return False
    col_orc = _pick_col(df, "orc", "orçamento", "orcamento")
    if not col_orc:
        return False
    return bool(_pick_col(df, "emiss") or _pick_col(df, "filial"))


def _finaliz_date_fill_ratio(df: pd.DataFrame) -> float:
    c = _pick_col(df, "dt finaliz", "finaliz", "data finaliz")
    if not c or df.empty:
        return 0.0
    ser = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
    return float(ser.notna().mean())


def classify_orcamento_workbook(file_name: str, file_bytes: bytes) -> Literal["pendentes", "finalizados"] | None:
    """
    Distingue export de orçamentos pendentes vs finalizados (nome do arquivo ou coluna de data de finalização).
    """
    if not is_orcamento_workbook(file_bytes, file_name=file_name):
        return None
    df = _read_budget_df(file_bytes, file_name=file_name)
    fn = (file_name or "").lower()
    if "pendent" in fn or "pendencia" in fn or "pendência" in fn:
        return "pendentes"
    if "finaliz" in fn or "conclu" in fn:
        return "finalizados"
    col_f = _pick_col(df, "dt finaliz", "finaliz", "data finaliz")
    if col_f:
        ratio = _finaliz_date_fill_ratio(df)
        if ratio >= 0.12:
            return "finalizados"
    return "pendentes"


def resolve_orcamentos_pend_fin_bytes(pairs: list[tuple[str, bytes]]) -> tuple[bytes, bytes]:
    """
    Recebe exatamente dois (nome, bytes) de planilhas de orçamento.
    Devolve (bytes_pendentes, bytes_finalizados).
    """
    if len(pairs) != 2:
        raise ValueError("Para orçamentos são necessários exatamente 2 arquivos (pendentes e finalizados).")
    (n1, b1), (n2, b2) = pairs[0], pairs[1]
    if not is_orcamento_workbook(b1, file_name=n1) or not is_orcamento_workbook(b2, file_name=n2):
        raise ValueError("Um dos arquivos não parece planilha de orçamentos (colunas Orçamento + Emissão/Filial).")

    r1 = classify_orcamento_workbook(n1, b1)
    r2 = classify_orcamento_workbook(n2, b2)
    if r1 is None or r2 is None:
        raise ValueError("Não foi possível classificar os arquivos de orçamentos.")

    if r1 != r2:
        pend_b, fin_b = (b1, b2) if r1 == "pendentes" else (b2, b1)
        return (pend_b, fin_b)

    df1 = _read_budget_df(b1, file_name=n1)
    df2 = _read_budget_df(b2, file_name=n2)
    s1, s2 = _finaliz_date_fill_ratio(df1), _finaliz_date_fill_ratio(df2)
    if abs(s1 - s2) < 1e-9:
        raise ValueError(
            "Não consegui distinguir pendentes vs finalizados. Use nomes com **pendente** e **finalizado** no arquivo "
            "ou confirme que a base de finalizados tem a coluna de data de finalização preenchida na maioria das linhas."
        )
    if s1 > s2:
        return (b2, b1)
    return (b1, b2)


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
    pend = _read_budget_df(pend_bytes, file_name="Orçamentos pendentes")
    fin = _read_budget_df(fin_bytes, file_name="Orçamentos finalizados")

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

