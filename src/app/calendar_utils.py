from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


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


def hoje_fuso_brasil() -> date:
    """
    'Hoje' no fuso de Brasília. Evita o app 'comer' ou ganhar 1 dia quando o
    processo roda em UTC (Streamlit em nuvem) enquanto o time está no BR.
    """
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    except Exception:
        return date.today()


def compute_calendar_info(
    *,
    ano: int,
    mes: int,
    country: str = "BR",
    subdiv: str | None = None,
    hoje: date | None = None,
) -> CalendarInfo:
    """
    Calcula dias úteis do mês (seg–sex) excluindo feriados nacionais/estaduais
    (quando a lib `holidays` e a UF forem fornecidos).

    Regras de contagem (mês = start..end):
    - `A` = úteis com d < hoje (dias "passados" estritos, sem hoje).
    - `dias_uteis_trabalhados` = úteis com d ≤ hoje = **A + 1{hoje é dia útil no mês}**
      (mesma lógica de "ritmo" / médias: corridos no mês até a data de hoje, inclusive
      o dia atual quando for útil).
    - `dias_uteis_restantes` = úteis com d ≥ hoje = **total - A** (hoje e os próximos úteis
      ainda a ocorrer no mês, inclusive a própria sexta se hoje for domingo, etc.).

    Assim: (dias úteis "corridos até hoje" no sentido A+H) + (úteis *após* hoje) = total,
    ou seja `dias_uteis_trabalhados + (B só depois) = total`, com B = d > hoje. O número
    `dias_uteis_restantes` = H+B é a quantidade a partir de hoje no calendário do mês.

    Data de hoje: fuso `America/Sao_Paulo` (evita diferença de 1 em deploy UTC).

    `subdiv` pode ser UF (ex: "CE", "SP") para feriados estaduais (quando suportado).
    """
    hoje = hoje or hoje_fuso_brasil()
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
    ate_ontem = 0
    cur = start
    while cur <= end:
        if is_business_day(cur):
            total += 1
            if hoje > end:
                pass
            elif hoje < start:
                pass
            else:
                if cur < hoje:
                    ate_ontem += 1
        cur += timedelta(days=1)

    if hoje < start:
        # Mês ainda é futuro.
        corrimos_ate = 0
        restantes_ate = total
    elif hoje > end:
        # Mês já "fechou" frente a hoje.
        corrimos_ate = total
        restantes_ate = 0
    else:
        hoje_uteis = bool(is_business_day(hoje) and start <= hoje <= end)
        # Ritmo: úteis no mês até e incluindo a referência (d ≤ hoje) — padrão original + projeções
        corrimos_ate = ate_ontem + (1 if hoje_uteis else 0)
        # Ainda no mês a partir de hoje (hoje e futuros do mês) — ajusta "falta um dia"
        restantes_ate = max(0, total - ate_ontem)
    return CalendarInfo(
        ano=int(ano),
        mes=int(mes),
        hoje=hoje,
        dias_uteis_total=total,
        dias_uteis_trabalhados=corrimos_ate,
        dias_uteis_restantes=restantes_ate,
    )

