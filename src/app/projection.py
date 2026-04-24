from __future__ import annotations

from dataclasses import dataclass

from .domain import Seller
from .bonus import calc_interacoes, calc_conversao


@dataclass(frozen=True)
class Projection:
    dias_uteis_total: int
    dias_uteis_trabalhados: int
    dias_restantes: int
    qtd_faturadas_atual: int
    interacoes_atual: int
    conversao_atual_pct: float | None
    media_diaria_faturas: float
    media_diaria_interacoes: float
    projecao_faturas: float
    projecao_interacoes: float
    projecao_conversao_pct: float | None
    ticket_medio: float | None
    faturamento_atual: float | None
    meta_faturamento: float | None
    faturamento_dia_atual: float | None
    projecao_faturamento: float | None
    faturamento_faltando: float | None
    nfs_por_dia_necessarias: float | None
    ticket_necessario_com_mesmo_ritmo: float | None
    status: str


def projetar_resultados(
    seller: Seller,
    *,
    dias_uteis_total: int,
    dias_uteis_trabalhados: int,
    meta_faturamento: float | None = None,
    ticket_medio_override: float | None = None,
) -> Projection:
    dias_uteis_total = max(1, int(dias_uteis_total))
    dias_uteis_trabalhados = max(1, min(int(dias_uteis_trabalhados), dias_uteis_total))

    qtd = int(seller.qtd_faturadas or 0)
    inter = int(calc_interacoes(seller) or 0)
    conv = calc_conversao(seller)

    media_fat = qtd / dias_uteis_trabalhados
    media_int = inter / dias_uteis_trabalhados
    dias_restantes = max(0, dias_uteis_total - dias_uteis_trabalhados)

    proj_fat = qtd + (media_fat * dias_restantes)
    proj_int = inter + (media_int * dias_restantes)
    proj_conv = None
    if proj_int > 0:
        proj_conv = round((proj_fat / proj_int) * 100, 2)

    # Ticket médio e faturamento
    faturamento_atual = seller.faturamento
    ticket_medio = None
    if ticket_medio_override and ticket_medio_override > 0:
        ticket_medio = float(ticket_medio_override)
    elif faturamento_atual is not None and qtd > 0:
        ticket_medio = float(faturamento_atual) / float(qtd)

    faturamento_dia_atual = None
    if ticket_medio is not None:
        faturamento_dia_atual = round(media_fat * ticket_medio, 2)

    proj_faturamento = None
    if ticket_medio is not None:
        proj_faturamento = round(proj_fat * ticket_medio, 2)

    faltando = None
    nfs_dia = None
    ticket_necessario = None
    if meta_faturamento is not None and meta_faturamento > 0:
        if faturamento_atual is None and ticket_medio is not None:
            faturamento_atual = round(qtd * ticket_medio, 2)
        if faturamento_atual is not None:
            faltando = round(max(0.0, float(meta_faturamento) - float(faturamento_atual)), 2)
            if dias_restantes > 0 and ticket_medio and ticket_medio > 0:
                nfs_dia = round(faltando / (dias_restantes * ticket_medio), 2)
            if dias_restantes > 0 and media_fat > 0:
                # Se mantiver o mesmo ritmo de NFs/dia, qual ticket precisa para bater a meta?
                ticket_necessario = round(faltando / (dias_restantes * media_fat), 2)

    # Status: usa meta de faturamento quando fornecida; senão, apenas ritmo vs. projeção de NFs.
    if meta_faturamento is not None and meta_faturamento > 0 and proj_faturamento is not None:
        pct_meta = (proj_faturamento / meta_faturamento) * 100
        if pct_meta >= 110:
            status = "✅ Acima da meta"
        elif pct_meta >= 90:
            status = "⚠️ Próximo da meta"
        else:
            status = "🔴 Abaixo da meta"
    else:
        status = "📈 Projeção calculada"

    return Projection(
        dias_uteis_total=dias_uteis_total,
        dias_uteis_trabalhados=dias_uteis_trabalhados,
        dias_restantes=dias_restantes,
        qtd_faturadas_atual=qtd,
        interacoes_atual=inter,
        conversao_atual_pct=conv,
        media_diaria_faturas=round(media_fat, 2),
        media_diaria_interacoes=round(media_int, 2),
        projecao_faturas=round(proj_fat, 1),
        projecao_interacoes=round(proj_int, 1),
        projecao_conversao_pct=proj_conv,
        ticket_medio=round(ticket_medio, 2) if ticket_medio is not None else None,
        faturamento_atual=round(faturamento_atual, 2) if faturamento_atual is not None else None,
        meta_faturamento=round(meta_faturamento, 2) if meta_faturamento is not None else None,
        faturamento_dia_atual=faturamento_dia_atual,
        projecao_faturamento=proj_faturamento,
        faturamento_faltando=faltando,
        nfs_por_dia_necessarias=nfs_dia,
        ticket_necessario_com_mesmo_ritmo=ticket_necessario,
        status=status,
    )

