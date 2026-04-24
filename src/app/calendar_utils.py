from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class CalendarInfo:
    ano: int
    mes: int
    hoje: date
    dias_uteis_total: int
    dias_uteis_trabalhados: int
    dias_uteis_restantes: int


def _month_range(ano: int, mes: int) -> tuple[date, date]:
    start = date(ano, mes, 1)
    if mes == 12:
        end = date(ano + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(ano, mes + 1, 1) - timedelta(days=1)
    return start, end


def compute_calendar_info(
    *,
    ano: int,
    mes: int,
    country: str = "BR",
    subdiv: str | None = None,
    hoje: date | None = None,
) -> CalendarInfo:
    """
    Calcula dias úteis do mês (seg-sex) excluindo feriados.
    `subdiv` pode ser UF (ex: "CE", "SP") para feriados estaduais (quando suportado).
    """
    hoje = hoje or date.today()
    start, end = _month_range(int(ano), int(mes))

    # holidays é opcional: se não estiver instalado, ignora feriados.
    feriados: set[date] = set()
    try:
        import holidays  # type: ignore

        if subdiv:
            h = holidays.country_holidays(country, subdiv=subdiv)  # type: ignore[arg-type]
        else:
            h = holidays.country_holidays(country)  # type: ignore[arg-type]
        feriados = {d for d in h if isinstance(d, date)}
    except Exception:
        feriados = set()

    def is_business_day(d: date) -> bool:
        if d.weekday() >= 5:  # 5=sábado,6=domingo
            return False
        if d in feriados:
            return False
        return True

    total = 0
    worked = 0
    cur = start
    while cur <= end:
        if is_business_day(cur):
            total += 1
            if cur <= hoje:
                worked += 1
        cur += timedelta(days=1)

    remaining = max(0, total - worked)
    return CalendarInfo(
        ano=int(ano),
        mes=int(mes),
        hoje=hoje,
        dias_uteis_total=total,
        dias_uteis_trabalhados=worked,
        dias_uteis_restantes=remaining,
    )

