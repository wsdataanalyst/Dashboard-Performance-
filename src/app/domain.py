from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Seller:
    nome: str
    margem_pct: float | None = None
    alcance_projetado_pct: float | None = None
    prazo_medio: int | None = None
    qtd_faturadas: int | None = None
    iniciados: int | None = None
    recebidos: int | None = None
    chamadas: int | None = None
    finalizados: int | None = None
    tme_minutos: float | None = None
    faturamento: float | None = None
    meta_faturamento: float | None = None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> int | None:
    if v is None:
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


def filter_excluded_sellers_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove vendedores excluídos (ex.: Laila) do JSON persistido e atualiza totais."""
    out = copy.deepcopy(payload)
    raw = out.get("vendedores")
    if not isinstance(raw, list):
        return out
    out["vendedores"] = [
        x
        for x in raw
        if isinstance(x, dict) and not is_excluded_seller_name(str(x.get("nome") or ""))
    ]
    refresh_payload_totais_from_vendedores(out)
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
                alcance_projetado_pct=_to_float(item.get("alcance_projetado_pct")),
                prazo_medio=_to_int(item.get("prazo_medio")),
                qtd_faturadas=_to_int(item.get("qtd_faturadas")),
                iniciados=_to_int(item.get("iniciados")),
                recebidos=_to_int(item.get("recebidos")),
                chamadas=_to_int(item.get("chamadas")),
                finalizados=_to_int(item.get("finalizados")),
                tme_minutos=_to_float(item.get("tme_minutos")),
                faturamento=_to_float(item.get("faturamento")),
                meta_faturamento=_to_float(item.get("meta_faturamento") or item.get("meta_total")),
            )
        )
    return sellers

