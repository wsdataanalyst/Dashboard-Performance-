from __future__ import annotations

import datetime
import math
import unicodedata
from dataclasses import dataclass
from io import BytesIO

from fpdf import FPDF

# Assinatura padrão do gestor no PDF (Feedback STAR)
STAR_GESTOR_PADRAO = "Willame Sousa - Coordenador de Vendas"

# Título fixo da seção (anexada ao feedback após a IA, para sempre aparecer no app/PDF)
TITULO_SECAO_SIMULACAO = "Simulando Capacidade de Venda"


@dataclass(frozen=True)
class StarInput:
    periodo: str
    nome: str
    bonus_total: float
    margem_pct: float | None
    alcance_pct: float | None
    prazo_medio: int | None
    conversao_pct: float | None
    tme_minutos: float | None
    interacoes: int | None
    qtd_faturadas: int | None
    faturamento: float | None
    meta_faturamento: float | None
    ticket_medio: float | None


def format_simulacao_capacidade_venda(x: StarInput) -> str:
    """
    Cenários hipotéticos (mesmo faturamento = NFs * ticket, funil: NFs ≈ interações * conversão%)
    para a seção "Simulando Capacidade de Venda" do feedback. Texto puro, sem markdown.
    """
    lines: list[str] = []
    M = x.meta_faturamento
    F = x.faturamento
    N = x.qtd_faturadas
    I = x.interacoes
    c = x.conversao_pct

    t_eff: float | None
    if x.ticket_medio is not None and x.ticket_medio > 0:
        t_eff = float(x.ticket_medio)
    elif F is not None and N and int(N) > 0:
        t_eff = float(F) / float(int(N))
    else:
        t_eff = None

    if M is None or float(M) <= 0:
        lines.append("Meta de faturamento ausente no período; não é possível simular cenarios de alcance de meta.")
        return "\n".join(lines)
    m = float(M)

    if F is None or t_eff is None or not N or int(N) <= 0:
        lines.append("Faturamento, NFs ou ticket insuficientes; simulacoes de capacidade de venda ficam indisponiveis.")
        return "\n".join(lines)

    f = float(F)
    n = int(N)
    t = float(t_eff)
    if f <= 0:
        lines.append("Faturamento nulo; simulacoes nao se aplicam (sem base de desempenho).")
        return "\n".join(lines)

    gap = m - f
    lines.append(f"Meta faturamento: R$ {m:,.2f} | Entregue: R$ {f:,.2f} | Gap: R$ {max(0.0, gap):,.2f}.")
    lines.append(f"Base do periodo: {n} NFs, ticket medio ~ R$ {t:,.2f}.")

    if gap <= 0:
        sobra = -gap
        lines.append(
            f"Meta de faturamento ja atingida; sobra aproximada (vs meta): R$ {sobra:,.2f}."
        )
        lines.append(
            "Ainda pode citar, se fizer sentido, que a meta foi superada: simulacoes de 'falta' nao se aplicam."
        )
        return "\n".join(lines)

    # 1) Só volume (mesmo ticket)
    nfs_meta = m / t
    nfs_falt = max(0.0, nfs_meta - float(n))
    nfs_total_int = int(math.ceil(m / t))  # NFs inteiras no mínimo para alcançar M com ticket t
    nfs_extra_int = max(0, nfs_total_int - n)
    lines.append(
        f"Se mantivesse o ticket em R$ {t:,.2f}: precisaria de ~{nfs_meta:.1f} NFs no total (~{nfs_falt:.1f} a mais em media; no minimo cerca de {nfs_extra_int} NFs a mais, em numeros inteiros, vs o atual {n})."
    )

    # 2) Só ticket (mesma quantidade de NFs)
    t_p_m = m / float(n)
    d_t = t_p_m - t
    lines.append(
        f"Com as mesmas {n} NFs: o ticket medio precisaria subir de R$ {t:,.2f} para ~R$ {t_p_m:,.2f} (delta de ~R$ {d_t:,.2f} por NF) para alcancar a meta."
    )

    # 3) Meio termo (crescimento equilibrado no volume e no ticket): factor sqrt(M/F)
    factor = math.sqrt(m / f)
    n_eq = n * factor
    t_eq = t * factor
    lines.append(
        f"Cenario meio termo (volume e ticket crescendo na mesma proporcao ~x{factor:.3f}): aprox. {n_eq:.1f} NFs e ticket R$ {t_eq:,.2f} (cada eixo cerca de {(factor - 1) * 100:+.1f}%)."
    )

    # 4) Funil: faturas = I * c/100; F = faturas * t  =>  M = I' * (c'/100) * t
    if I is not None and int(I) > 0 and c is not None and float(c) > 0 and t > 0:
        i0 = int(I)
        c0 = float(c)
        # Mesmas interacoes, conversao necessaria (%)
        # M = t * (c1/100) * i0  => c1 = 100*M / (t*i0)
        c1 = 100.0 * m / (t * float(i0))
        # Mesma conversao, interacoes necessarias
        i1 = 100.0 * m / (t * c0)
        # Meio termo: escalar I e c pelo mesmo fator
        i_eq = float(i0) * factor
        c_eq = c0 * factor
        lines.append(
            f"Funil (NFs aprox. interacoes * conversao/100; ticket {t:,.2f}): com as mesmas {i0} interacoes, a conversao precisaria ir de {c0:.1f}% para ~{c1:.1f}% (hipotetico) para bater a meta. "
            f"Com a mesma conversao {c0:.1f}%, precisaria de ~{i1:.0f} interacoes. "
            f"Meio termo no funil: ~{i_eq:.0f} interacoes e ~{c_eq:.1f}% de conversao (cada eixo em torno de {(factor - 1) * 100:+.1f}% em relacao ao atual)."
        )
        if c1 > 100.0:
            lines.append(
                f"Aviso: conversao alvo {c1:.0f}% passa de 100% — o modelo de funil nao bate so com conversao; reforce a necessidade de volume (interacoes) e/ou ticket."
            )
    else:
        lines.append(
            "Dados de interacoes e/ou conversao incompletos; nao foi possivel simular a parte de funil (contatos x conversao)."
        )

    lines.append(
        "Lembre: estes numeros sao ilustrativos (relacao aproximada faturamento = NFs * ticket) e nao consideram prazo, mix, sazonalidade."
    )
    return "\n".join(lines)


def append_secao_simulacao_capacidade_venda(x: StarInput, texto_gerado_pela_ia: str) -> str:
    """
    Garante a seção no PDF e no app: a IA gera STAR + fechamento; o app anexa os cenários
    (números calculados), evitando omissão pelo modelo.
    """
    s = (texto_gerado_pela_ia or "").rstrip()
    corpo = format_simulacao_capacidade_venda(x).strip()
    if not corpo:
        return s
    if s and not s.endswith(("\n", "\r")):
        s = s + "\n"
    return f"{s}\n{TITULO_SECAO_SIMULACAO}\n\n{corpo}\n".strip() + "\n"


FEEDBACK_ANTERIOR_MAX_CHARS = 10_000


def _trunc_feedback_anterior(s: str, max_chars: int = FEEDBACK_ANTERIOR_MAX_CHARS) -> str:
    t = (s or "").strip()
    if len(t) <= max_chars:
        return t
    return t[:max_chars] + "\n[... texto do feedback anterior truncado para o limite do prompt ...]"


def build_prompt_star(
    x: StarInput,
    *,
    feedback_anterior_texto: str | None = None,
    periodo_analise_anterior: str | None = None,
    feedback_anterior_registrado_em: str | None = None,
) -> str:
    if feedback_anterior_texto and str(feedback_anterior_texto).strip():
        tprev = _trunc_feedback_anterior(str(feedback_anterior_texto).strip())
        p_an = (periodo_analise_anterior or "—").strip()
        em_an = (feedback_anterior_registrado_em or "—").strip()
        # concat: texto anterior pode conter "{"; não interpolar tprev com f-string
        bloco_hist = (
            "HISTORICO: FEEDBACK ANTERIOR (ultimo registro salvo no sistema para ESTE vendedor)\n"
            f"Periodo da analise (do feedback anterior): {p_an}\n"
            f"Registrado em: {em_an}\n\n"
            "TEXTO INTEIRO DO FEEDBACK ANTERIOR (leia tudo; use na comparacao):\n"
            + tprev
            + "\n"
        )
    else:
        bloco_hist = (
            "HISTORICO: Nao existe feedback anterior deste vendedor no banco acessivel (primeiro registro, "
            "ou sem historico). Diga 1 frase: comparativo com feedback anterior nao se aplica.\n"
        )

    return (
        f"""
Gere um feedback individual usando a metodologia STAR (Situação, Tarefa, Ação, Resultado).

{bloco_hist}

Regras gerais:
- Seja direto, humano e acionável (gestor → vendedor).
- Use bullets curtos em Ação.
- Não use markdown. Texto puro.
- Sempre que existir meta, mostre no texto como: "Meta: X | Entrega: Y".
- Se não existir meta para um indicador, mostre apenas o resultado.
- NÃO escreva uma seção "Simulando Capacidade de Venda" (o sistema anexa essa parte depois, com os números exatos).
- Sobre a liderança / remetente do feedback (Willame Sousa, coordenação de vendas): gênero masculino — o coordenador, ele, dele, o gestor, seu coordenador. Não use feminino (ela, a coordenadora, a gestora) para se referir à gestão ou à pessoa que assina; o nome "Willame" é masculino.
- Sobre o vendedor (destinatário do texto): trate com "você" ou use o nome; se usar 3ª pessoa, não assuma o gênero em nomes ambíguos; prefira o nome ou "o colaborador" / "a colaboradora" só se o contexto for claro.
- EVOLUÇÃO: quando houver "TEXTO INTEIRO" do feedback anterior acima, voce DEVE comparar o desempenho e os indicadores atuais (do periodo corrente) com o conteudo daquele feedback. Destaque: o que aparenta ter melhorado, piorado, ou se manteve (com base nos numeros de agora e no tom/prioridades do feedback anterior). Seja especifico. Se o historico disser que nao ha feedback anterior, cumpra a 1 frase.
- Nao recopie o texto do feedback anterior por extenso; interprete e compare com dados atuais.

Dados do período ATUAL: {x.periodo}
Vendedor: {x.nome}
Bônus total: R$ {x.bonus_total:,.2f}

Indicadores e metas (use todos no panorama inicial):
- Faturamento: Meta: {x.meta_faturamento} | Entrega: {x.faturamento}
- Ticket médio: {x.ticket_medio}
- Qtd. NFs (faturadas): {x.qtd_faturadas}
- Interações: Meta: 200 | Entrega: {x.interacoes}
- Conversão (%): Meta: 12 | Entrega: {x.conversao_pct}
- Margem (%): Meta: 26 (elegível com Alcance >= 90) | Entrega margem: {x.margem_pct} | Alcance: {x.alcance_pct}
- Prazo médio (dias): Meta: <= 43 | Entrega: {x.prazo_medio}
- TME (min): Meta: <= 5 | Entrega: {x.tme_minutos}

Estrutura obrigatória do texto (texto puro):

1) PANORAMA GERAL (comece por aqui): um bloco inicial que passe por TODOS os indicadores acima,
   cada um com meta e entrega quando existir meta (formato "Meta: X | Entrega: Y"), em linguagem clara e sintética.
   Inclua na sequencia um sub-bloco intitulado: Evolucao em relacao ao feedback anterior (3 a 7 frases).
   Cumpra as regras de EVOLUÇÃO acima; se nao houver historico, use 1 frase e deixe claro.

2) STAR com FOCO OPERACIONAL: em seguida, desenvolva Situação, Tarefa, Ação e Resultado dando ênfase especial a:
   - NFs (volume, ritmo, consistência),
   - ticket médio (valor médio e oportunidade de mix),
   - conversão (eficiência do funil),
   - interações (cadência e volume frente à meta),
   - potencial de vendas (onde pode acelerar, gaps, prioridades para o próximo período).
   (Quando fizer sentido, alinhe acoes a gaps ja citados no feedback anterior.)

3) Fechamento: 2-4 frases motivadoras ligando esforço a resultado e próximos passos.

Não escreva assinaturas nem rodapé de data no texto (o PDF acrescenta isso depois).

Saída (JSON):
- Responda apenas com um único objeto JSON (sem markdown).
- O campo feedback_star deve ser uma string JSON válida: use \\n para quebras de linha
  dentro do texto, nunca uma quebra de linha real entre as aspas do JSON.
""".strip()
    )


def _sanitize_pdf_text(s: str) -> str:
    if not s:
        return ""
    t = unicodedata.normalize("NFKD", s)
    t = "".join(c for c in t if not unicodedata.combining(c))
    repl = {
        "≤": "<=",
        "≥": ">=",
        "–": "-",
        "—": "-",
        "•": "-",
    }
    for a, b in repl.items():
        t = t.replace(a, b)
    return t.encode("latin-1", "replace").decode("latin-1")


def render_pdf_star(
    *,
    titulo: str,
    periodo: str,
    vendedor: str,
    texto: str,
    gestor_nome_cargo: str = STAR_GESTOR_PADRAO,
    data_assinatura: str | None = None,
) -> BytesIO:
    if data_assinatura is None:
        data_assinatura = datetime.date.today().strftime("%d/%m/%Y")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)

    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, _sanitize_pdf_text(titulo), ln=True, align="C")
    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 6, _sanitize_pdf_text(f"Periodo: {periodo}"), ln=True, align="C")
    pdf.cell(0, 6, _sanitize_pdf_text(f"Colaborador(a): {vendedor}"), ln=True, align="C")
    pdf.ln(8)

    pdf.set_font("Arial", "", 11)
    pdf.multi_cell(0, 6, _sanitize_pdf_text(texto))

    pdf.ln(14)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, _sanitize_pdf_text("Assinaturas"), ln=True)
    pdf.set_draw_color(120, 120, 120)
    pdf.ln(2)

    pdf.set_font("Arial", "B", 10)
    pdf.cell(0, 6, _sanitize_pdf_text("Gestor"), ln=True)
    pdf.set_font("Arial", "", 10)
    pdf.multi_cell(0, 5, _sanitize_pdf_text(gestor_nome_cargo))
    pdf.ln(2)
    pdf.set_font("Arial", "", 9)
    pdf.cell(0, 5, _sanitize_pdf_text("_" * 72), ln=True)
    pdf.cell(0, 5, _sanitize_pdf_text("Assinatura do gestor"), ln=True)
    pdf.ln(8)

    pdf.set_font("Arial", "B", 10)
    pdf.cell(0, 6, _sanitize_pdf_text("Colaborador(a)"), ln=True)
    pdf.set_font("Arial", "", 10)
    pdf.multi_cell(0, 5, _sanitize_pdf_text(vendedor))
    pdf.ln(2)
    pdf.set_font("Arial", "", 9)
    pdf.cell(0, 5, _sanitize_pdf_text("_" * 72), ln=True)
    pdf.cell(0, 5, _sanitize_pdf_text("Assinatura do colaborador"), ln=True)
    pdf.ln(10)

    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 6, _sanitize_pdf_text(f"Data: {data_assinatura}"), ln=True)

    buf = BytesIO()
    pdf.output(buf)
    buf.seek(0)
    return buf
