from __future__ import annotations

import datetime
import unicodedata
from dataclasses import dataclass
from io import BytesIO

from fpdf import FPDF

# Assinatura padrão do gestor no PDF (Feedback STAR)
STAR_GESTOR_PADRAO = "Willame Sousa - Coordenador de Vendas"


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


def build_prompt_star(x: StarInput) -> str:
    return f"""
Gere um feedback individual usando a metodologia STAR (Situação, Tarefa, Ação, Resultado).

Regras gerais:
- Seja direto, humano e acionável (gestor → vendedor).
- Use bullets curtos em Ação.
- Não use markdown. Texto puro.
- Sempre que existir meta, mostre no texto como: "Meta: X | Entrega: Y".
- Se não existir meta para um indicador, mostre apenas o resultado.

Dados do período: {x.periodo}
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

2) STAR com FOCO OPERACIONAL: em seguida, desenvolva Situação, Tarefa, Ação e Resultado dando ênfase especial a:
   - NFs (volume, ritmo, consistência),
   - ticket médio (valor médio e oportunidade de mix),
   - conversão (eficiência do funil),
   - interações (cadência e volume frente à meta),
   - potencial de vendas (onde pode acelerar, gaps, prioridades para o próximo período).

3) Fechamento: 2-4 frases motivadoras ligando esforço a resultado e próximos passos.

Não escreva assinaturas nem rodapé de data no texto (o PDF acrescenta isso depois).

Saída (JSON):
- Responda apenas com um único objeto JSON (sem markdown).
- O campo feedback_star deve ser uma string JSON válida: use \\n para quebras de linha
  dentro do texto, nunca uma quebra de linha real entre as aspas do JSON.
""".strip()


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
    pdf.cell(0, 6, _sanitize_pdf_text("Gestor(a)"), ln=True)
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
