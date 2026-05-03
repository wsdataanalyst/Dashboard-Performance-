from __future__ import annotations

import copy
import re
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Seller:
    nome: str
    margem_pct: float | None = None
    # % Alcance (real) = faturamento / meta * 100 (quando existir)
    alcance_pct: float | None = None
    alcance_projetado_pct: float | None = None
    # Quando informado no payload (ex.: Ajuste rápido), substitui Iniciados+Recebidos+Chamadas.
    interacoes: int | None = None
    # Quando informado no payload (ex.: Ajuste rápido), substitui o cálculo faturadas/interações.
    conversao_pct: float | None = None
    prazo_medio: int | None = None
    qtd_faturadas: int | None = None
    iniciados: int | None = None
    recebidos: int | None = None
    chamadas: int | None = None
    finalizados: int | None = None
    tme_minutos: float | None = None
    faturamento: float | None = None
    meta_faturamento: float | None = None
    desconto_valor: float | None = None
    desconto_pct: float | None = None
    qtd_desconto: int | None = None
    qtd_desconto_pct: float | None = None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def is_excluded_seller_name(nome: str) -> bool:
    """Reservado para futuras regras de exclusão. Nenhum vendedor é excluído (Laila Rodrigues participa das análises)."""
    return False


def _norm_vendedor_key(nome: str) -> str:
    """Chave estável para detectar duplicatas do mesmo vendedor (alinhado ao acúmulo no Streamlit)."""
    txt = str(nome or "").strip().lower()
    txt = txt.replace("_", " ")
    txt = re.sub(r"\(\s*\d+\s*\)", "", txt).strip()
    txt = re.sub(r"r\$\s*[\d\.,]+", "", txt, flags=re.IGNORECASE).strip()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9\s]+", " ", txt)
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    return txt


def _dedupe_vendedores_dicts(items: list[dict]) -> list[dict]:
    """
    Mescla linhas duplicadas do mesmo vendedor (mesmo _norm_vendedor_key).

    Import/merge às vezes gera duas entradas com KPIs diferentes; para bônus usamos regra
    conservadora: prazo médio = máximo (pior), TME = máximo, conversão/alcance/margem/interações = mínimo.
    Demais campos numéricos: preenche faltantes a partir das outras linhas; volume = máximo.
    """
    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for item in items:
        nome = str(item.get("nome") or "").strip()
        if not nome:
            continue
        k = _norm_vendedor_key(nome)
        if k not in groups:
            groups[k] = []
        groups[k].append(dict(item))

    out: list[dict] = []
    for bucket in groups.values():
        if len(bucket) == 1:
            out.append(bucket[0])
            continue

        canonical = max(bucket, key=lambda x: len(str(x.get("nome") or "").strip()))
        merged: dict = dict(canonical)
        merged["nome"] = str(canonical.get("nome") or "").strip()

        for b in bucket:
            if b is canonical:
                continue
            for key, v in b.items():
                if key == "nome" or v is None:
                    continue
                if merged.get(key) is None:
                    merged[key] = v

        prazos = [_to_int(b.get("prazo_medio")) for b in bucket]
        prazos = [p for p in prazos if p is not None]
        if prazos:
            merged["prazo_medio"] = max(prazos)

        tmes = [_to_float(b.get("tme_minutos")) for b in bucket]
        tmes = [t for t in tmes if t is not None]
        if tmes:
            merged["tme_minutos"] = max(tmes)

        convs = [_to_float(b.get("conversao_pct")) for b in bucket]
        convs = [c for c in convs if c is not None]
        if convs:
            merged["conversao_pct"] = min(convs)

        inters = [_to_int(b.get("interacoes")) for b in bucket]
        inters = [i for i in inters if i is not None]
        if inters:
            merged["interacoes"] = min(inters)

        margs = [_to_float(b.get("margem_pct")) for b in bucket]
        margs = [m for m in margs if m is not None]
        if margs:
            merged["margem_pct"] = min(margs)

        alc_proj = [_to_float(b.get("alcance_projetado_pct")) for b in bucket]
        alc_proj = [a for a in alc_proj if a is not None]
        if alc_proj:
            merged["alcance_projetado_pct"] = min(alc_proj)

        alc_real = [_to_float(b.get("alcance_pct")) for b in bucket]
        alc_real = [a for a in alc_real if a is not None]
        if alc_real:
            merged["alcance_pct"] = min(alc_real)

        fats = [_to_float(b.get("faturamento")) for b in bucket]
        fats = [f for f in fats if f is not None]
        if fats:
            merged["faturamento"] = max(fats)

        nfs = [_to_int(b.get("qtd_faturadas")) for b in bucket]
        nfs = [n for n in nfs if n is not None]
        if nfs:
            merged["qtd_faturadas"] = max(nfs)

        for fld in ("iniciados", "recebidos", "chamadas", "finalizados"):
            vals = [_to_int(b.get(fld)) for b in bucket]
            vals = [v for v in vals if v is not None]
            if vals:
                merged[fld] = max(vals)

        out.append(merged)
    return out


def refresh_payload_totais_from_vendedores(payload: dict[str, Any]) -> None:
    """Recalcula totais a partir da lista `vendedores` (após filtros)."""
    vendors = payload.get("vendedores")
    if not isinstance(vendors, list) or not vendors:
        return
    fs = 0.0
    ms = 0.0
    cf = False
    cm = False
    for item in vendors:
        if not isinstance(item, dict):
            continue
        f = _to_float(item.get("faturamento"))
        m = _to_float(item.get("meta_faturamento") or item.get("meta_total"))
        if f is not None:
            fs += f
            cf = True
        if m is not None:
            ms += m
            cm = True
    tot = payload.get("totais")
    if isinstance(tot, dict):
        tot = dict(tot)
        if cf:
            tot["faturamento_total"] = fs
        if cm:
            tot["meta_total"] = ms
        payload["totais"] = tot
    elif cf or cm:
        payload["totais"] = {
            "faturamento_total": fs if cf else None,
            "meta_total": ms if cm else None,
        }


def _sum_meta_total_from_raw_vendors(vendors: list[Any]) -> float | None:
    ms = 0.0
    cm = False
    for item in vendors:
        if not isinstance(item, dict):
            continue
        m = _to_float(item.get("meta_faturamento") or item.get("meta_total"))
        if m is not None:
            ms += m
            cm = True
    return ms if cm else None


def filter_excluded_sellers_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Normaliza o payload: mantém todos os vendedores, recalcula totais a partir da lista
    e preserva `totais.meta_total` quando já vier do print (linha TOTAL).
    """
    out = copy.deepcopy(payload)
    raw = out.get("vendedores")
    if not isinstance(raw, list):
        return out
    existing_totais = out.get("totais") if isinstance(out.get("totais"), dict) else {}
    existing_meta_total = _to_float(existing_totais.get("meta_total")) if isinstance(existing_totais, dict) else None
    meta_total_incl_excl = _sum_meta_total_from_raw_vendors(raw)
    out["vendedores"] = [x for x in raw if isinstance(x, dict)]
    refresh_payload_totais_from_vendedores(out)
    if existing_meta_total is not None and existing_meta_total > 0:
        tot = out.get("totais")
        if isinstance(tot, dict):
            tot = dict(tot)
            tot["meta_total"] = float(existing_meta_total)
            out["totais"] = tot
        else:
            out["totais"] = {"meta_total": float(existing_meta_total)}
    elif meta_total_incl_excl is not None:
        tot = out.get("totais")
        if isinstance(tot, dict):
            tot = dict(tot)
            tot["meta_total"] = meta_total_incl_excl
            out["totais"] = tot
        else:
            out["totais"] = {"meta_total": meta_total_incl_excl}
    return out


def parse_sellers(payload: dict[str, Any]) -> list[Seller]:
    raw = payload.get("vendedores") or []
    sellers: list[Seller] = []
    if not isinstance(raw, list):
        return sellers

    cleaned: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        nome = str(item.get("nome") or "").strip()
        if not nome:
            continue
        cleaned.append(dict(item))

    for item in _dedupe_vendedores_dicts(cleaned):
        nome = str(item.get("nome") or "").strip()
        sellers.append(
            Seller(
                nome=nome,
                margem_pct=_to_float(item.get("margem_pct")),
                alcance_pct=_to_float(item.get("alcance_pct")),
                alcance_projetado_pct=_to_float(item.get("alcance_projetado_pct")),
                interacoes=_to_int(item.get("interacoes")),
                conversao_pct=_to_float(item.get("conversao_pct")),
                prazo_medio=_to_int(item.get("prazo_medio")),
                qtd_faturadas=_to_int(item.get("qtd_faturadas")),
                iniciados=_to_int(item.get("iniciados")),
                recebidos=_to_int(item.get("recebidos")),
                chamadas=_to_int(item.get("chamadas")),
                finalizados=_to_int(item.get("finalizados")),
                tme_minutos=_to_float(item.get("tme_minutos")),
                faturamento=_to_float(item.get("faturamento")),
                meta_faturamento=_to_float(item.get("meta_faturamento") or item.get("meta_total")),
                desconto_valor=_to_float(item.get("desconto_valor") or item.get("desconto")),
                desconto_pct=_to_float(item.get("desconto_pct")),
                qtd_desconto=_to_int(item.get("qtd_desconto")),
                qtd_desconto_pct=_to_float(item.get("qtd_desconto_pct")),
            )
        )
    return sellers

