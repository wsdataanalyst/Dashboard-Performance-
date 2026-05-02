from __future__ import annotations

import copy
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
    """Vendedores cujo primeiro nome é Laila não entram em cálculos nem no armazenamento."""
    parts = (nome or "").strip().lower().split()
    return bool(parts and parts[0] == "laila")


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
    Remove vendedores excluídos (ex.: Laila) do payload.

    Regra de negócio: o(a) vendedor(a) excluído(a) não aparece em detalhes/dashboards,
    mas a META do time pode permanecer a mesma. Por isso, preservamos `totais.meta_total`
    considerando também os vendedores excluídos quando houver meta por vendedor.
    """
    out = copy.deepcopy(payload)
    raw = out.get("vendedores")
    if not isinstance(raw, list):
        return out
    # Se o payload já vier com totais (linha TOTAL do print), preserve-os como fonte oficial.
    existing_totais = out.get("totais") if isinstance(out.get("totais"), dict) else {}
    existing_meta_total = _to_float(existing_totais.get("meta_total")) if isinstance(existing_totais, dict) else None
    # Fallback: somar apenas a meta do vendedor excluído (Laila) sem trazer ela para detalhes
    laila_meta = 0.0
    for item in raw:
        if not isinstance(item, dict):
            continue
        nome = str(item.get("nome") or "")
        if is_excluded_seller_name(nome):
            m = _to_float(item.get("meta_faturamento") or item.get("meta_total"))
            if m is not None:
                laila_meta += float(m)
    meta_total_incl_excl = _sum_meta_total_from_raw_vendors(raw)
    out["vendedores"] = [
        x
        for x in raw
        if isinstance(x, dict) and not is_excluded_seller_name(str(x.get("nome") or ""))
    ]
    refresh_payload_totais_from_vendedores(out)
    # Preferir meta_total já fornecida pelos prints; senão, usar soma incluindo excluídos.
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
    elif laila_meta > 0:
        tot = out.get("totais") if isinstance(out.get("totais"), dict) else {}
        cur = _to_float(tot.get("meta_total")) or 0.0
        tot2 = dict(tot) if isinstance(tot, dict) else {}
        tot2["meta_total"] = float(cur) + float(laila_meta)
        out["totais"] = tot2
    return out


def parse_sellers(payload: dict[str, Any]) -> list[Seller]:
    raw = payload.get("vendedores") or []
    sellers: list[Seller] = []
    if not isinstance(raw, list):
        return sellers

    for item in raw:
        if not isinstance(item, dict):
            continue
        nome = str(item.get("nome") or "").strip()
        if not nome:
            continue
        if is_excluded_seller_name(nome):
            continue
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

