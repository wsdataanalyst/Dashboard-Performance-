from __future__ import annotations

import streamlit as st


def _profile_css(profile: str) -> str:
    p = (profile or "desktop").lower().strip()
    if p == "mobile":
        return """
  /* Perfil Mobile (reforço além das media queries) */
  .main .block-container {
    padding-top: 1.25rem !important;
    padding-bottom: 2.25rem !important;
    padding-left: 0.65rem !important;
    padding-right: 0.65rem !important;
    max-width: 100% !important;
  }
  .dp-header { padding: 14px 14px !important; border-radius: 14px !important; }
  .dp-title{ font-size:1.18rem !important; line-height: 1.25 !important; }
  .dp-sub{ font-size:0.95rem !important; line-height: 1.5 !important; }
  .dp-kpi-value{ font-size:1.28rem !important; }
  .dp-kpi-label{ font-size: 0.72rem !important; }
  .dp-card { padding: 12px 12px !important; border-radius: 14px !important; }
  .main h3 { font-size: 1.08rem !important; }
  [data-testid="stSidebar"] > div { padding-left: .55rem !important; padding-right: .55rem !important; }
  .stButton > button { padding: 0.7rem 0.85rem !important; min-height: 46px !important; font-size: 0.98rem !important; }
"""
    if p == "tablet":
        return """
  /* Perfil Tablet / iPad — mais largura útil, fonte menor para não cortar números */
  .main .block-container {
    padding-top: 1.5rem !important;
    padding-left: 0.55rem !important;
    padding-right: 0.55rem !important;
    max-width: 100% !important;
  }
  /* Tipografia base um pouco menor no iPad */
  .main, .main p, .main li, .main label { font-size: 0.95rem !important; }
  .stCaption, .main .stCaption { font-size: 0.82rem !important; }
  .dp-title{ font-size:1.14rem !important; }
  .dp-sub{ font-size: 0.92rem !important; }
  .dp-kpi-label{ font-size: 0.72rem !important; }
  .dp-kpi-value{ font-size:1.22rem !important; }
  .dp-card { padding: 12px 12px !important; }
  .main h3 { font-size: 1.06rem !important; }
  .stButton > button { min-height: 44px !important; }
  /* Métricas Streamlit (evita cortar valores longos) */
  [data-testid="stMetricLabel"] { font-size: 0.85rem !important; }
  [data-testid="stMetricValue"] { font-size: 1.25rem !important; }
  /* Dataframe: texto menor para caber colunas */
  div[data-testid="stDataFrame"] { font-size: 0.9rem !important; }
"""
    # desktop (default)
    return """
  /* Perfil Desktop */
  .main .block-container { max-width: 1500px; }
"""


def _responsive_viewport_css() -> str:
    """
    CSS por largura real do navegador — melhora iPad/iPhone mesmo com perfil “desktop”.
    Inclui fonte 16px em inputs (Safari não dá zoom ao focar) e alvos de toque maiores.
    """
    return """
  /* ——— Telas médias (iPad retrato, tablets) ——— */
  @media screen and (max-width: 1100px) {
    html {
      -webkit-text-size-adjust: 100%;
      text-size-adjust: 100%;
    }
    .stButton > button {
      touch-action: manipulation;
    }
    .main .block-container {
      max-width: 100% !important;
      padding-left: 1rem !important;
      padding-right: 1rem !important;
    }
    .dp-title { font-size: clamp(1.1rem, 2.8vw, 1.35rem) !important; }
    .dp-kpi-value { font-size: clamp(1.2rem, 3.2vw, 1.45rem) !important; }
    [data-testid="stTabs"] [role="tab"],
    [data-testid="stTabs"] button {
      font-size: 0.95rem !important;
      padding: 0.55rem 0.75rem !important;
      min-height: 44px !important;
    }
    [data-testid="stTabs"] [role="tablist"],
    [data-testid="stTabs"] [data-baseweb="tab-list"] {
      flex-wrap: wrap !important;
      gap: 4px !important;
    }
  }

  /* ——— iPad / tablet (não celular): KPIs e colunas deixam de esmagar números ——— */
  @media screen and (max-width: 1400px) and (min-width: 641px) {
    .main .block-container {
      padding-left: 0.5rem !important;
      padding-right: 0.5rem !important;
    }
    .main [data-testid="stHorizontalBlock"] {
      flex-wrap: wrap !important;
      row-gap: 0.45rem !important;
      column-gap: 0.35rem !important;
      align-items: flex-start !important;
    }
    .main [data-testid="stHorizontalBlock"] > div[data-testid="column"] {
      flex: 1 1 calc(33.333% - 8px) !important;
      min-width: min(100%, 220px) !important;
      max-width: 100% !important;
    }
    [data-testid="stMetric"] {
      min-width: 0 !important;
      overflow: visible !important;
    }
    [data-testid="stMetric"] [data-testid="stMetricLabel"] p {
      white-space: normal !important;
      line-height: 1.25 !important;
      word-break: break-word !important;
    }
    [data-testid="stMetricValue"] {
      overflow: visible !important;
    }
    [data-testid="stMetricValue"] > div {
      overflow: visible !important;
      text-overflow: clip !important;
      white-space: normal !important;
    }
    .bonus-table-wrap {
      overflow-x: auto !important;
      -webkit-overflow-scrolling: touch;
    }
    .bonus-table-wrap .bonus-table {
      min-width: 700px;
    }
    .bonus-table .bonus-cell-num,
    th.bonus-col-bonus,
    td.bonus-col-bonus {
      white-space: nowrap !important;
    }
    div[data-testid="stDataFrame"] {
      overflow-x: auto !important;
      -webkit-overflow-scrolling: touch;
    }
    .js-plotly-plot, .plotly-graph-div {
      max-width: 100% !important;
    }
  }

  /* ——— Smartphones ——— */
  @media screen and (max-width: 640px) {
    .main .block-container {
      padding-top: 1.1rem !important;
      padding-left: 0.65rem !important;
      padding-right: 0.65rem !important;
    }
    .main h1, .main h2, .main h3 {
      margin-top: 1rem !important;
      font-size: clamp(1rem, 4.2vw, 1.15rem) !important;
    }
    .dp-header { margin-bottom: 12px !important; }
    .dp-title { font-size: 1.14rem !important; }
    .dp-sub { font-size: 0.9rem !important; }
    .dp-kpi-value { font-size: 1.22rem !important; }
    .bonus-panel-title { font-size: 1.05rem !important; }
    .bonus-metric-value { font-size: 1.45rem !important; }
    .bonus-table { font-size: 0.82rem !important; }
    .bonus-table thead th { padding: 10px 10px !important; font-size: 0.65rem !important; }
    .bonus-table tbody td { padding: 9px 10px !important; }
    .bonus-vendedor { min-width: 110px !important; }
    [data-testid="stMetricLabel"] { font-size: 0.82rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.35rem !important; }
    .stButton > button {
      min-height: 46px !important;
      padding: 0.65rem 0.9rem !important;
      font-size: 0.95rem !important;
    }
    /* iOS: fonte < 16px em input dispara zoom ao focar */
    .stTextInput input,
    .stTextArea textarea,
    [data-testid="stNumberInput"] input,
    [data-baseweb="input"] input,
    [data-baseweb="select"] > div {
      font-size: 16px !important;
      line-height: 1.35 !important;
    }
    [data-testid="stExpander"] details summary,
    .streamlit-expanderHeader {
      font-size: 1rem !important;
      min-height: 44px !important;
      align-items: center !important;
    }
    div[data-testid="stDataFrame"] { margin-left: -2px; margin-right: -2px; }
  }
"""


def inject_styles(profile: str = "desktop") -> None:
    css = """
<style>
  :root{
    --bg0:#0B1220;
    --bg1:#0F172A;
    --panel:#111A2E;
    --panel2:#0F1B33;
    --border:rgba(255,255,255,.08);
    --text:#E5E7EB;
    --muted:#94A3B8;
    --brand:#6EE7B7;
    --amber:#FBBF24;
    --red:#FB7185;
    --shadow: 0 10px 30px rgba(0,0,0,.35);
  }

  /* Conteúdo principal: afasta do topo do viewport e alinha margens */
  .main .block-container {
    padding-top: 2.25rem !important;
    padding-bottom: 2.5rem !important;
    padding-left: 1.5rem !important;
    padding-right: 1.5rem !important;
  }

  /* Títulos de seção (###) — evita “colado” no topo / entre blocos */
  .main h1, .main h2, .main h3 {
    margin-top: 1.25rem !important;
    margin-bottom: 0.65rem !important;
    line-height: 1.35;
  }
  /* H3 como "header moderno" (card) */
  .main h3 {
    font-size: 1.05rem !important;
    font-weight: 900 !important;
    letter-spacing: .2px;
    color: var(--text) !important;
    padding: 12px 14px !important;
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    background:
      radial-gradient(900px 220px at 12% 0%, rgba(59,130,246,.16), transparent 60%),
      radial-gradient(900px 220px at 88% 12%, rgba(110,231,183,.10), transparent 55%),
      linear-gradient(180deg, rgba(17,26,46,.90), rgba(11,18,32,.92)) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.22);
  }

  /* Sidebar: mais legível e aproveitada */
  [data-testid="stSidebar"] > div {
    padding-top: 1.25rem !important;
    padding-left: 0.75rem !important;
    padding-right: 0.75rem !important;
  }
  /* Sidebar — visual moderno */
  [data-testid="stSidebar"] {
    background:
      radial-gradient(1200px 520px at 15% 0%, rgba(59,130,246,.20), transparent 50%),
      radial-gradient(900px 420px at 85% 20%, rgba(110,231,183,.14), transparent 55%),
      linear-gradient(180deg, rgba(11,18,32,.98), rgba(9,14,26,.98)) !important;
    border-right: 1px solid rgba(255,255,255,.08);
  }
  /* Texto do sidebar com contraste melhor */
  [data-testid="stSidebar"] * {
    color: rgba(229,231,235,.94);
  }
  [data-testid="stSidebar"] .stCaption,
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] small,
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p small {
    color: rgba(148,163,184,.95) !important;
  }
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] li,
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] span {
    color: rgba(229,231,235,.94) !important;
  }

  [data-testid="stSidebar"] .stMarkdown h3 {
    font-size: 0.9rem !important;
    padding: 10px 12px !important;
    border-radius: 12px !important;
  }
  /* Radios no sidebar com "pills" */
  [data-testid="stSidebar"] div[role="radiogroup"] > label {
    padding: 10px 10px !important;
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    background: rgba(255,255,255,.02) !important;
    margin-bottom: 8px !important;
    transition: transform .12s ease, background .12s ease, border-color .12s ease;
  }
  /* Radio selecionado: destaque + texto bem legível */
  [data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) {
    background: rgba(59,130,246,.10) !important;
    border-color: rgba(59,130,246,.30) !important;
    box-shadow: 0 10px 22px rgba(0,0,0,.22);
  }
  [data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) * {
    color: rgba(229,231,235,.98) !important;
    font-weight: 850 !important;
  }
  [data-testid="stSidebar"] div[role="radiogroup"] > label:hover {
    transform: translateY(-1px);
    background: rgba(255,255,255,.04) !important;
    border-color: rgba(59,130,246,.22) !important;
  }
  /* Expander do sidebar */
  [data-testid="stSidebar"] [data-testid="stExpander"] details summary {
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    background: rgba(255,255,255,.02) !important;
    padding: 10px 12px !important;
  }
  /* Inputs no sidebar */
  [data-testid="stSidebar"] [data-baseweb="input"] > div,
  [data-testid="stSidebar"] [data-baseweb="select"] > div {
    border-radius: 14px !important;
    border-color: rgba(255,255,255,.12) !important;
    background: rgba(255,255,255,.03) !important;
  }
  /* Texto dentro de inputs/selects no sidebar */
  [data-testid="stSidebar"] input,
  [data-testid="stSidebar"] textarea,
  [data-testid="stSidebar"] [data-baseweb="select"] * {
    color: rgba(229,231,235,.96) !important;
  }
  [data-testid="stSidebar"] [data-baseweb="select"] [aria-selected="true"] {
    color: rgba(229,231,235,.98) !important;
  }
  [data-testid="stSidebar"] [data-baseweb="select"] svg {
    fill: rgba(148,163,184,.95) !important;
  }
  [data-testid="stSidebar"] .stButton > button {
    border-radius: 14px !important;
    border: 1px solid rgba(255,255,255,.12) !important;
    background: rgba(255,255,255,.04) !important;
  }
  [data-testid="stSidebar"] .stButton > button * {
    color: rgba(229,231,235,.98) !important;
  }
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
  [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] li {
    font-size: 0.98rem;
    line-height: 1.45;
  }
  [data-testid="stSidebar"] .stCaption { font-size: 0.88rem !important; }

  /* Header */
  .dp-header{
    background: radial-gradient(1200px 500px at 20% 0%, rgba(110,231,183,.18), transparent 45%),
                radial-gradient(900px 420px at 80% 20%, rgba(59,130,246,.14), transparent 55%),
                linear-gradient(180deg, rgba(17,26,46,.95), rgba(11,18,32,.90));
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 18px 18px;
    box-shadow: var(--shadow);
    margin-bottom: 18px;
    margin-top: 4px;
  }
  .dp-title{ margin:0; font-size:1.35rem; font-weight:800; color:var(--text); letter-spacing:.2px;}
  .dp-sub{ margin:.35rem 0 0 0; color:var(--muted); font-size:.95rem; line-height: 1.45; }
  .dp-pill{
    display:inline-flex; align-items:center; gap:8px;
    padding:6px 10px; border-radius:999px;
    border:1px solid var(--border);
    background: rgba(255,255,255,.03);
    color:var(--muted); font-size:.78rem;
  }
  .dot{ width:8px; height:8px; border-radius:50%; background:var(--brand); display:inline-block; }

  /* Seletor de Dashboard (topo) em formato de cards */
  /* Alvo: primeiro radiogroup horizontal do main */
  section.main div[role="radiogroup"]{
    gap: 10px !important;
    flex-wrap: wrap !important;
    margin: 6px 0 6px 0 !important;
  }
  section.main div[role="radiogroup"] > label{
    flex: 1 1 260px !important;
    min-width: 220px !important;
    padding: 14px 14px !important;
    border-radius: 16px !important;
    border: 1px solid rgba(255,255,255,.12) !important;
    background:
      radial-gradient(900px 220px at 12% 0%, rgba(59,130,246,.16), transparent 60%),
      radial-gradient(900px 220px at 88% 12%, rgba(110,231,183,.10), transparent 55%),
      linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94)) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.22);
    transition: transform .12s ease, border-color .12s ease, background .12s ease;
  }
  section.main div[role="radiogroup"] > label:hover{
    transform: translateY(-1px);
    border-color: rgba(59,130,246,.26) !important;
  }
  /* Texto dentro do card */
  section.main div[role="radiogroup"] > label *{
    color: rgba(229,231,235,.96) !important;
    font-weight: 850 !important;
    font-size: 1.02rem !important;
    letter-spacing: .2px;
  }
  /* Selecionado */
  section.main div[role="radiogroup"] > label:has(input:checked){
    border-color: rgba(110,231,183,.38) !important;
    background:
      radial-gradient(900px 240px at 14% 0%, rgba(110,231,183,.22), transparent 62%),
      radial-gradient(900px 240px at 88% 12%, rgba(59,130,246,.16), transparent 58%),
      linear-gradient(180deg, rgba(17,26,46,.94), rgba(11,18,32,.96)) !important;
    box-shadow: 0 18px 42px rgba(0,0,0,.28);
  }
  section.main div[role="radiogroup"] > label:has(input:checked) *{
    color: rgba(229,231,235,.99) !important;
  }

  /* Cards */
  .dp-card{
    background: linear-gradient(180deg, rgba(17,26,46,.9), rgba(15,23,42,.9));
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 14px 14px;
    box-shadow: 0 6px 18px rgba(0,0,0,.25);
  }
  .dp-kpi-label{ color:var(--muted); font-size:.78rem; letter-spacing:.5px; text-transform:uppercase; }
  .dp-kpi-value{ color:var(--text); font-size:1.45rem; font-weight:900; margin-top:6px; }
  .dp-kpi-help{ color:var(--muted); font-size:.82rem; margin-top:6px; }

  /* Status */
  .ok{ color: var(--brand); font-weight: 700; }
  .warn{ color: var(--amber); font-weight: 700; }
  .bad{ color: var(--red); font-weight: 700; }

  /* Buttons */
  .stButton > button{
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,.12) !important;
    /* Botão padrão neutro (evita “barras verdes” em cards clicáveis) */
    background: rgba(255,255,255,.04) !important;
    color: rgba(229,231,235,.96) !important;
    font-weight: 850 !important;
    padding: .65rem 1rem !important;
  }
  .stButton > button:hover{
    background: rgba(255,255,255,.06) !important;
    border-color: rgba(59,130,246,.22) !important;
    transform: translateY(-1px);
  }

  /* Tabelas Streamlit: rolagem horizontal (evita cortar números no iPad) */
  div[data-testid="stDataFrame"]{
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow-x: auto;
    overflow-y: visible;
    -webkit-overflow-scrolling: touch;
    margin-top: 6px;
    margin-bottom: 8px;
  }

  /* Tabelas: mais legíveis (centraliza e melhora números) */
  div[data-testid="stDataFrame"] table th,
  div[data-testid="stDataFrame"] table td{
    text-align: center !important;
    vertical-align: middle !important;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  /* Expander (ex.: calendário na home) — título mais visível */
  .streamlit-expanderHeader {
    font-size: 1.05rem !important;
    font-weight: 600 !important;
  }

  /* Painel “Central de Vendas | Resultados de Bônus” (aba indicadores) */
  .bonus-panel-wrap {
    background: linear-gradient(180deg, rgba(17,26,46,.94), rgba(11,18,32,.97));
    border: 1px solid rgba(255,255,255,.1);
    border-radius: 18px;
    padding: 22px 20px 20px;
    margin-bottom: 16px;
    box-shadow: 0 12px 40px rgba(0,0,0,.4);
  }
  .bonus-panel-title {
    margin: 0 0 6px 0;
    font-size: 1.26rem;
    font-weight: 800;
    color: #f8fafc;
    letter-spacing: 0.02em;
    line-height: 1.3;
  }
  .bonus-panel-note {
    margin: 0 0 18px 0;
    font-size: 0.84rem;
    color: #94a3b8;
    font-style: italic;
    line-height: 1.45;
  }
  .bonus-metric-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    margin-bottom: 20px;
  }
  @media (max-width: 900px) {
    .bonus-metric-grid { grid-template-columns: 1fr; }
  }
  .bonus-metric-card {
    background: rgba(15,23,42,.88);
    border: 1px solid rgba(255,255,255,.08);
    border-radius: 14px;
    padding: 16px 18px;
  }
  .bonus-metric-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: .1em;
    color: #94a3b8;
    font-weight: 600;
  }
  .bonus-metric-value-row {
    display: flex;
    align-items: baseline;
    gap: 8px;
    margin-top: 8px;
  }
  .bonus-metric-value {
    font-size: 1.82rem;
    font-weight: 900;
    color: #6EE7B7;
    text-shadow: 0 0 22px rgba(110,231,183,.3);
    font-variant-numeric: tabular-nums;
  }
  .bonus-metric-arrow {
    font-size: 1.35rem;
    color: #4ade80;
    opacity: 0.95;
  }
  .bonus-metric-sub {
    margin-top: 8px;
    font-size: 0.78rem;
    color: #64748b;
    line-height: 1.4;
  }
  .bonus-bar-track {
    margin-top: 12px;
    height: 8px;
    border-radius: 999px;
    background: rgba(255,255,255,.06);
    overflow: hidden;
  }
  .bonus-bar-fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #22c55e, #6EE7B7);
  }
  .bonus-table-wrap {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,.08);
  }
  .bonus-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 0.86rem;
  }
  .bonus-table thead th {
    text-align: left;
    padding: 12px 14px;
    background: rgba(15,23,42,.98);
    color: #94a3b8;
    font-weight: 700;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid rgba(255,255,255,.08);
    white-space: normal;
    word-break: break-word;
    hyphens: auto;
  }
  .bonus-table tbody td {
    padding: 11px 14px;
    border-bottom: 1px solid rgba(255,255,255,.05);
    color: #e2e8f0;
    vertical-align: middle;
  }
  .bonus-table tbody tr:hover td { background: rgba(255,255,255,.025); }
  .bonus-vendedor { font-weight: 700; color: #f1f5f9; min-width: 120px; white-space: normal; word-break: break-word; }
  .bonus-cell-num { font-variant-numeric: tabular-nums; }
  .bonus-pill-sim {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 8px;
    background: rgba(34,197,94,.28);
    color: #bbf7d0;
    font-weight: 700;
    font-size: 0.78rem;
  }
  .bonus-pill-nao {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 8px;
    background: rgba(71,85,105,.5);
    color: #cbd5e1;
    font-weight: 600;
    font-size: 0.78rem;
  }
  .bonus-ico { margin-left: 6px; font-size: 0.95rem; }
  .bonus-col-bonus {
    background: rgba(110,231,183,.07) !important;
    box-shadow: inset 0 0 0 1px rgba(110,231,183,.4);
    color: #6EE7B7 !important;
    font-weight: 800 !important;
    font-size: 0.94rem !important;
    text-align: right !important;
    white-space: nowrap;
  }
  .bonus-table thead th.bonus-col-bonus {
    text-align: right !important;
    box-shadow: inset 0 0 24px rgba(110,231,183,.1);
  }
  .bonus-legend {
    margin: 12px 0 0 0;
    font-size: 0.8rem;
    color: #94a3b8;
  }
  .bonus-footer {
    display: flex;
    flex-wrap: wrap;
    align-items: flex-end;
    justify-content: space-between;
    gap: 18px;
    margin-top: 18px;
    padding-top: 16px;
    border-top: 1px solid rgba(255,255,255,.08);
  }
  .bonus-footer-narr {
    flex: 1;
    min-width: 220px;
    font-size: 0.87rem;
    color: #cbd5e1;
    line-height: 1.55;
  }
  .bonus-footer-total-block { text-align: right; }
  .bonus-footer-total-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: .12em;
    color: #64748b;
    font-weight: 600;
  }
  .bonus-footer-total-box {
    margin-top: 8px;
    display: inline-block;
    padding: 10px 20px;
    border-radius: 12px;
    background: rgba(15,23,42,.92);
    border: 1px solid rgba(110,231,183,.4);
    color: #6EE7B7;
    font-weight: 900;
    font-size: 1.12rem;
    font-variant-numeric: tabular-nums;
    box-shadow: 0 0 26px rgba(110,231,183,.18);
  }

__PROFILE_CSS__
__RESPONSIVE_CSS__
</style>
"""
    st.markdown(
        css.replace("__PROFILE_CSS__", _profile_css(profile)).replace(
            "__RESPONSIVE_CSS__", _responsive_viewport_css()
        ),
        unsafe_allow_html=True,
    )


def render_header(title: str, subtitle: str, right: str | None = None) -> None:
    right_html = f"<span class='dp-pill'><span class='dot'></span>{right}</span>" if right else ""
    st.markdown(
        f"""
<div class="dp-header">
  <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:14px; flex-wrap:wrap;">
    <div>
      <p class="dp-title">{title}</p>
      <p class="dp-sub">{subtitle}</p>
    </div>
    <div>{right_html}</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

