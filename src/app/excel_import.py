from __future__ import annotations

import io
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

from .domain import filter_excluded_sellers_from_payload, is_excluded_seller_name


@dataclass(frozen=True)
class ImportResult:
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

    # excel real
    ext = Path(file_name).suffix.lower()
    if ext == ".xlsx":
        return [pd.read_excel(io.BytesIO(b), engine="openpyxl")]
    if ext == ".xls":
        # xls real (binário antigo)
        return [pd.read_excel(io.BytesIO(b), engine="xlrd")]
    # fallback: tenta como excel
    return [pd.read_excel(io.BytesIO(b))]


def _clean_name(name: str) -> str:
    s = str(name or "").strip()
    s = s.replace("_", " ")
    s = re.sub(r"\(\s*\d+\s*\)", "", s).strip()
    # remove sujeiras comuns
    s = re.sub(r"r\$\s*[\d\.,]+", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _name_key(nome: str) -> str:
    s = _clean_name(nome).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    parts = [p for p in s.split() if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]}_{parts[-1]}"


def _norm_name_match(nome: str) -> str:
    s = _clean_name(nome).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z\s]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _should_merge_names(a_norm: str, b_norm: str) -> bool:
    ta = [p for p in a_norm.split() if p]
    tb = [p for p in b_norm.split() if p]
    if not ta or not tb:
        return False
    short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    # substring quando primeiro token bate
    if short[0] == long_[0] and (" ".join(short) in " ".join(long_) or " ".join(long_) in " ".join(short)):
        return True
    # mesmo último sobrenome + primeiro nome compatível
    if short[-1] == long_[-1] and (short[0] == long_[0] or short[0] in long_ or long_[0] in short):
        return True
    # tokens do curto contidos no longo (ex.: "ediones lima" dentro de "antonio ediones de lima")
    if set(short).issubset(set(long_)):
        return True
    # "wesley cavalcante" vs "joao wesley ... cavalcante"
    if short[-1] == long_[-1] and short[0] in long_:
        return True
    return False


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


def _merge(base: dict[str, dict], updates: list[dict[str, Any]]) -> None:
    for u in updates:
        nome = _clean_name(u.get("nome") or "")
        if _should_skip_name(nome) or is_excluded_seller_name(nome):
            continue
        key = _name_key(nome)
        if not key:
            continue
        r = base.setdefault(key, {"nome": nome})
        if len(nome) > len(str(r.get("nome") or "")):
            r["nome"] = nome
        for k, v in u.items():
            if k == "nome":
                continue
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            r[k] = v


def _to_int(v: Any) -> int | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if isinstance(v, (int, float)):
            return int(round(float(v)))
        return int(float(str(v).replace(".", "").replace(",", ".")))
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace("R$", "").replace("%", "").strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None


def _as_pct_0_100(v: Any) -> float | None:
    x = _to_float(v)
    if x is None:
        return None
    # Alguns relatórios exportam 194,97% como 1.9497
    if 0 <= x <= 3.0:
        return round(x * 100.0, 2)
    return round(x, 2)


def _pick_name_col(df: pd.DataFrame) -> str:
    for n in ("vendedor", "usuário", "usuario", "agente", "user"):
        c = _find_col(df, n)
        if c:
            return c
    return str(df.columns[0])


def _should_skip_name(nome: str) -> bool:
    s = _clean_name(nome).strip().lower()
    return (not s) or s in {"total", "nan", "none"} or s.startswith("central de vendas")


def _parse_hms_to_minutes(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    # "00h 35m 52s"
    mt = re.search(r"(\d+)\s*h\s*(\d+)\s*m\s*(\d+)\s*s", s.lower())
    if mt:
        hh = int(mt.group(1))
        mm = int(mt.group(2))
        ss = int(mt.group(3))
        return round(hh * 60 + mm + ss / 60.0, 2)
    m = re.search(r"(\d+):(\d+):(\d+)", s)
    if not m:
        return _to_float(v)
    hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return round(hh * 60 + mm + ss / 60.0, 2)


def _pick_table_with_cols(tables: list[pd.DataFrame], required_any: list[str]) -> pd.DataFrame | None:
    for t in tables:
        cols = [str(c).lower() for c in t.columns]
        if all(any(r in c for c in cols) for r in required_any):
            return t
    return None


def import_5_files_to_payload(files: list[tuple[str, bytes]]) -> ImportResult:
    """
    Importa até 5 arquivos (excel real ou html exportado como .xls/.xlsx) e monta payload.
    Espera arquivos relacionados a:
    - Alcance/Margem/Meta/Faturamento (print 1)
    - Prazo médio (print 2)
    - Qtd faturadas + faturamento (print 3)
    - Chamadas (print 4)
    - TME + iniciados/recebidos/finalizados (print 5)
    """
    warnings: list[str] = []
    base: dict[str, dict] = {}

    for fname, b in files:
        tables = _read_excel_or_html(fname, b)
        handled = False

        # Detecta pelo CONTEÚDO (colunas), não pelo nome do arquivo
        for t in tables:
            # Chamadas: Agente + Chamadas
            if _find_col(t, "agente") and _find_col(t, "chamadas"):
                c_nome = _pick_name_col(t)
                c_ch = _find_col(t, "chamadas")
                updates: list[dict[str, Any]] = []
                for _, r in t.iterrows():
                    nome = _clean_name(r.get(c_nome) or "")
                    if not nome:
                        continue
                    updates.append({"nome": nome, "chamadas": _to_int(r.get(c_ch))})
                _merge(base, updates)
                handled = True
                break

            # TME / Atendimentos: Usuario + Iniciados + Recebidos (+ TME)
            if (_find_col(t, "usuario") or _find_col(t, "user")) and _find_col(t, "iniciados") and _find_col(t, "recebidos"):
                c_nome = _pick_name_col(t)
                c_ini = _find_col(t, "iniciados")
                c_rec = _find_col(t, "recebidos")
                c_fin = _find_col(t, "finalizados")
                c_tme = _find_col(t, "tme")
                updates = []
                for _, r in t.iterrows():
                    nome = _clean_name(r.get(c_nome) or "")
                    if not nome:
                        continue
                    updates.append(
                        {
                            "nome": nome,
                            "iniciados": _to_int(r.get(c_ini)),
                            "recebidos": _to_int(r.get(c_rec)),
                            "finalizados": _to_int(r.get(c_fin)) if c_fin else None,
                            "tme_minutos": _parse_hms_to_minutes(r.get(c_tme)) if c_tme else None,
                        }
                    )
                _merge(base, updates)
                handled = True
                break

            # Prazo médio: P. Médio
            if _find_col(t, "p medio", "p. medio", "p médio", "p. médio"):
                c_nome = _pick_name_col(t)
                c_pmedio = _find_col(t, "p medio", "p. medio", "p médio", "p. médio")
                updates = []
                for _, r in t.iterrows():
                    nome = _clean_name(r.get(c_nome) or "")
                    if not nome:
                        continue
                    updates.append({"nome": nome, "prazo_medio": _to_int(r.get(c_pmedio))})
                _merge(base, updates)
                handled = True
                break

            # Qtd. faturadas
            if _find_col(t, "qtd fatur", "qtd. fatur", "qtd faturadas", "qtd. faturadas"):
                c_nome = _pick_name_col(t)
                c_qtd = _find_col(t, "qtd fatur", "qtd. fatur", "qtd faturadas", "qtd. faturadas")
                c_fat = _find_col(t, "faturamento")
                updates = []
                for _, r in t.iterrows():
                    nome = _clean_name(r.get(c_nome) or "")
                    if not nome:
                        continue
                    rec: dict[str, Any] = {"nome": nome, "qtd_faturadas": _to_int(r.get(c_qtd))}
                    if c_fat:
                        rec["faturamento"] = _to_float(r.get(c_fat))
                    updates.append(rec)
                _merge(base, updates)
                handled = True
                break

            # Alcance & Margem
            if _find_col(t, "alcance projet") and _find_col(t, "% margem"):
                c_nome = _pick_name_col(t)
                c_alc = _find_col(t, "alcance projet")
                c_marg = _find_col(t, "% margem")
                c_fat = _find_col(t, "faturamento")
                c_meta = _find_col(t, "meta")
                updates = []
                for _, r in t.iterrows():
                    nome = _clean_name(r.get(c_nome) or "")
                    if not nome:
                        continue
                    updates.append(
                        {
                            "nome": nome,
                            "alcance_projetado_pct": _as_pct_0_100(r.get(c_alc)),
                            "margem_pct": _as_pct_0_100(r.get(c_marg)),
                            "faturamento": _to_float(r.get(c_fat)) if c_fat else None,
                            "meta_faturamento": _to_float(r.get(c_meta)) if c_meta else None,
                        }
                    )
                _merge(base, updates)
                handled = True
                break

        if not handled:
            warnings.append(
                f"Arquivo '{fname}' importado, mas não reconheci nenhuma tabela válida. "
                "Verifique se o arquivo tem as colunas esperadas."
            )

    # Consolida possíveis duplicatas por similaridade (ex.: "Antonio Lima" vs "Antonio Ediones De Lima")
    recs = list(base.values())
    consolidated: list[dict[str, Any]] = []
    keys: list[str] = []
    for r in recs:
        nome = str(r.get("nome") or "").strip()
        if _should_skip_name(nome) or is_excluded_seller_name(nome):
            continue
        nk = _norm_name_match(nome)
        if not nk:
            continue
        best_i = -1
        best = 0.0
        # Primeiro: merge mais determinístico por tokens (subconjunto / sobrenome / substring)
        for i, kk in enumerate(keys):
            if _should_merge_names(nk, kk):
                best_i = i
                best = 1.0
                break
        for i, kk in enumerate(keys):
            score = SequenceMatcher(None, nk, kk).ratio()
            if score > best:
                best = score
                best_i = i
        if best_i >= 0:
            # Se a similaridade for boa, consolidar
            if best >= 0.78:
                cur = consolidated[best_i]
                if len(nome) > len(str(cur.get("nome") or "")):
                    cur["nome"] = nome
                for k, v in r.items():
                    if k == "nome":
                        continue
                    if cur.get(k) is None and v is not None:
                        cur[k] = v
                continue
        # novo cluster
        consolidated.append(dict(r))
        keys.append(nk)

    payload: dict[str, Any] = {"vendedores": consolidated}
    payload = filter_excluded_sellers_from_payload(payload)
    payload["vendedores"].sort(key=lambda x: str(x.get("nome") or ""))

    return ImportResult(payload=payload, meta={"provider": "excel_import", "model": "pandas"}, warnings=warnings)

