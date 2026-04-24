from __future__ import annotations

import html
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.bonus import calcular_time
from src.app.config import load_settings
from src.app.domain import parse_sellers
from src.app.security import build_admin_auth, constant_time_equals, sha256_hex
from src.app.storage import (
    base_data_dir,
    connect,
    delete_analysis,
    get_analysis,
    init_db,
    list_analyses,
    list_feedbacks,
    list_uploads,
    purge_excluded_sellers_from_all_analyses,
    save_analysis,
    save_feedback,
    save_upload_file,
)
from src.app.theme import inject_styles, render_header
from src.app.ai.router import Provider, extract_json_from_images, json_from_text
from src.app.feedback_star import STAR_GESTOR_PADRAO, StarInput, build_prompt_star, render_pdf_star
from src.app.excel_import import import_5_files_to_payload
from src.app.ocr_fallback import extract_payload_from_prints_ocr
from src.app.projection import projetar_resultados
from src.app.calendar_utils import compute_calendar_info


PROMPT_EXTRACAO = """
Você é um especialista em análise de dados de vendas. Analise as imagens fornecidas que são prints de painéis de gestão de uma central de vendas.

Extraia TODOS os dados disponíveis e retorne um JSON estruturado com o seguinte formato EXATO:

{
  "vendedores": [
    {
      "nome": "Nome completo do vendedor",
      "margem_pct": 26.04,
      "alcance_projetado_pct": 64.25,
      "prazo_medio": 57,
      "qtd_faturadas": 21,
      "faturamento": 234686.0,
      "meta_faturamento": 867518.0,
      "iniciados": 134,
      "recebidos": 27,
      "chamadas": 80,
      "tme_minutos": 33.22
    }
  ],
  "totais": {
    "faturamento_total": 234686.0,
    "meta_total": 867518.0
  },
  "periodo": "Abril (até 15/04)",
  "observacoes": "qualquer observação relevante encontrada nos prints"
}

REGRAS IMPORTANTES:
1) Para TME: converta "00h 33m 13s" para minutos decimais (33.22).
2) Combine dados pelo NOME do vendedor (pode vir abreviado).
3) Se um dado não existir em nenhum print, use null.
4) Retorne APENAS o JSON, sem markdown, sem explicações.
5) Use ponto (.) como separador decimal.
6) Se existirem valores de Meta/Faturamento no print 1, preencha `faturamento` e `meta_faturamento` (por vendedor, se disponível).
7) O Print 1 contém indicadores como: Alcance, Margem, Meta, %Meta, %Venda, Desconto, Faturamento. Extraia ao máximo por vendedor.
8) Onde buscar cada indicador:
   - Print 5 (TME): contém TME e Atendimentos (Iniciados + Recebidos).
   - Print 4 (Chamadas): contém o número de Chamadas por vendedor.
   - Print 3: contém Qtd. Faturadas (NFs).
   - Print 2: contém Prazo Médio.
   - Print 1: contém Alcance, Margem, Meta, %Meta, %Venda, Desconto e Faturamento.
9) Interações (para este projeto) = Iniciados + Recebidos + Chamadas.
9) Se existirem totais do time (Meta Total e Faturamento Total) em qualquer print, preencha em `totais`.
"""


PROMPT_INSIGHTS = """
Você é um especialista em gestão de times de vendas e análise de performance.
Analise os dados abaixo e retorne um JSON com EXATAMENTE este formato.

Foque nos indicadores: NFs (qtd_faturadas), faturamento, ticket médio, interações, conversão (%), margem (%).
Se existir meta de faturamento, compare "Meta: X | Entrega: Y" no resumo executivo.
Indique claramente quais indicadores estão mais comprometendo a meta.

{{
  "resumo_executivo": "2-3 frases",
  "destaques_positivos": [
    {{"vendedor":"Nome","indicador":"Indicador","valor":"Valor","insight":"Por que é bom"}}
  ],
  "pontos_atencao": [
    {{"vendedor":"Nome","indicador":"Indicador","valor":"Valor","insight":"O que fazer"}}
  ],
  "recomendacoes_time": [
    {{"prioridade":"Alta/Média/Baixa","acao":"Ação concreta","impacto":"Impacto esperado"}}
  ],
  "prioridades_vendedores": [
    {{"vendedor":"Nome","prioridade":"Alta/Média/Baixa","motivos":["1-3 motivos curtos"]}}
  ],
  "vendedor_destaque": "Nome",
  "vendedor_foco": "Nome"
}}

Retorne APENAS o JSON.

DADOS:
{dados_json}
"""


PROMPT_HIGHLIGHTS = """
Você é um analista sênior de performance comercial. Gere uma análise profunda (texto) a partir dos dados abaixo.

Objetivo:
- Produzir um resumo gerencial do desempenho no período, com foco em: NFs, faturamento, ticket médio, interações, conversão, margem.
- Apontar os principais gargalos que comprometem a meta e as alavancas mais rápidas.
- Separar em blocos: "Resumo", "O que está puxando para baixo", "O que está puxando para cima", "Ações da semana", "Ações do mês".

Regras:
- Texto direto e bem trabalhado (sem markdown).
- Cite vendedores quando necessário e use números do dataset.
- Não invente dados.

Retorne um JSON com EXATAMENTE este formato:
{{
  "texto": "..."
}}

DADOS:
{dados_json}
"""


def _fmt_pct_cell(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.2f}"


def _fmt_int_cell(v: object) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return str(int(float(v)))


def _fmt_float_cell(v: object, nd: int = 2) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.{nd}f}"


def _icon_meta(ok: bool | None) -> str:
    if ok is True:
        return '<span class="bonus-ico" title="Somou bônus">✅</span>'
    if ok is False:
        return '<span class="bonus-ico" title="Não somou bônus">❌</span>'
    return '<span class="bonus-ico" title="Sem dado">◽</span>'


def _bonus_panel_narrative(src: pd.DataFrame, total: float, n_eleg: int) -> str:
    parts: list[str] = [
        "Time fechou <strong style='color:#6EE7B7;'>R$ {:,.2f}</strong> em bônus no período.".format(total),
        "Só <strong>{}</strong> vendedor(es) ficaram elegíveis ao bônus de margem.".format(n_eleg),
    ]
    extras: list[str] = []
    for _, r in src.iterrows():
        if not bool(r.get("elegivel_margem")):
            m = r.get("margem_pct")
            a = r.get("alcance_pct")
            if m is not None and not pd.isna(m) and float(m) >= 26.0:
                if a is not None and not pd.isna(a) and float(a) < 90.0:
                    extras.append(
                        "<strong>{}</strong> bateu a meta de margem, mas ficou inelegível por alcance abaixo do gatilho.".format(
                            html.escape(str(r.get("nome", "")))
                        )
                    )
    if extras:
        parts.append(" ".join(extras[:3]))
    return " ".join(parts)


def render_bonus_central_panel_html(df: pd.DataFrame, *, periodo: str, total: float) -> str:
    """Painel estilo ‘Central de Vendas | Resultados de Bônus’ (HTML + classes em theme)."""
    src = df.reset_index(drop=True)
    n = len(src)
    n_eleg = int(src["elegivel_margem"].sum()) if n and "elegivel_margem" in src.columns else 0
    bar_pct = min(100.0, (n_eleg / n * 100.0)) if n else 0.0
    periodo_esc = html.escape(str(periodo or "Período"))

    rows: list[str] = []
    for _, r in src.iterrows():
        nome = html.escape(str(r.get("nome", "")))
        alc = _fmt_pct_cell(r.get("alcance_pct"))
        elig = bool(r.get("elegivel_margem"))
        pill = '<span class="bonus-pill-sim">Sim</span>' if elig else '<span class="bonus-pill-nao">Não</span>'
        marg_v = _fmt_pct_cell(r.get("margem_pct"))
        marg_ok = float(r.get("bonus_margem") or 0) > 0
        prazo_v = _fmt_int_cell(r.get("prazo_medio"))
        prazo_ok = r.get("bateu_prazo")
        conv_v = _fmt_pct_cell(r.get("conversao_pct"))
        conv_ok = r.get("bateu_conversao")
        tme_v = _fmt_float_cell(r.get("tme_minutos"), 2)
        tme_ok = r.get("bateu_tme")
        inter_v = _fmt_int_cell(r.get("interacoes"))
        inter_ok = r.get("bateu_interacao")
        btot = float(r.get("bonus_total") or 0)
        rows.append(
            "<tr>"
            f'<td class="bonus-vendedor">{nome}</td>'
            f'<td class="bonus-cell-num">{alc}</td>'
            f"<td>{pill}</td>"
            f'<td class="bonus-cell-num">{marg_v}{_icon_meta(marg_ok if marg_v != "—" else None)}</td>'
            f'<td class="bonus-cell-num">{prazo_v}{_icon_meta(prazo_ok if prazo_v != "—" else None)}</td>'
            f'<td class="bonus-cell-num">{conv_v}{_icon_meta(conv_ok if conv_v != "—" else None)}</td>'
            f'<td class="bonus-cell-num">{tme_v}{_icon_meta(tme_ok if tme_v != "—" else None)}</td>'
            f'<td class="bonus-cell-num">{inter_v}{_icon_meta(inter_ok if inter_v != "—" else None)}</td>'
            f'<td class="bonus-col-bonus">R$ {btot:,.2f}</td>'
            "</tr>"
        )

    narr = _bonus_panel_narrative(src, total, n_eleg)
    rows_joined = "\n".join(rows)

    return f"""
<div class="bonus-panel-wrap">
  <h2 class="bonus-panel-title">Central de Vendas | Resultados de Bônus — {periodo_esc}</h2>
  <p class="bonus-panel-note">TME considerado dentro da meta por instabilidade na plataforma (quando aplicável ao período).</p>
  <div class="bonus-metric-grid">
    <div class="bonus-metric-card">
      <div class="bonus-metric-label">Bônus total do time</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value">R$ {total:,.2f}</span>
        <span class="bonus-metric-arrow" aria-hidden="true">↑</span>
      </div>
      <div class="bonus-metric-sub">Soma dos bônus individuais na análise ativa.</div>
    </div>
    <div class="bonus-metric-card">
      <div class="bonus-metric-label">Elegíveis bônus de margem</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value">{n_eleg}</span>
        <span style="font-size:0.9rem;color:#64748b;font-weight:600;">/ {n}</span>
      </div>
      <div class="bonus-metric-sub">Margem ≥ 26% e Alcance projetado ≥ 90%.</div>
      <div class="bonus-bar-track"><div class="bonus-bar-fill" style="width:{bar_pct:.1f}%"></div></div>
    </div>
  </div>
  <div class="bonus-table-wrap">
    <table class="bonus-table">
      <thead>
        <tr>
          <th>Vendedor</th>
          <th>Alcance projetado (%)</th>
          <th>Elegível margem?</th>
          <th>% Margem</th>
          <th>Prazo médio</th>
          <th>Conversão (%)</th>
          <th>TME (min)</th>
          <th>Interações</th>
          <th class="bonus-col-bonus">Bônus total (R$)</th>
        </tr>
      </thead>
      <tbody>
        {rows_joined}
      </tbody>
    </table>
  </div>
  <p class="bonus-legend">✅ = somou bônus &nbsp;|&nbsp; ❌ = não somou bônus</p>
  <div class="bonus-footer">
    <div class="bonus-footer-narr">{narr}</div>
    <div class="bonus-footer-total-block">
      <div class="bonus-footer-total-label">Total do time</div>
      <div class="bonus-footer-total-box">R$ {total:,.2f}</div>
    </div>
  </div>
</div>
"""


def _enrich_results_df_for_performance(results_df: pd.DataFrame, sellers: list) -> pd.DataFrame:
    """Enriquece df de BonusResult com dados brutos (NFs, faturamento, meta, ticket)."""
    if results_df.empty:
        return results_df
    raw_map = {getattr(s, "nome", None): s for s in sellers or []}
    df = results_df.copy()
    df["faturamento"] = df["nome"].apply(lambda n: getattr(raw_map.get(n), "faturamento", None))
    df["meta_faturamento"] = df["nome"].apply(lambda n: getattr(raw_map.get(n), "meta_faturamento", None))
    df["qtd_faturadas"] = df["qtd_faturadas"] if "qtd_faturadas" in df.columns else None
    df["ticket_medio"] = df.apply(
        lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"]))
        if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0)
        else None,
        axis=1,
    )
    return df


def _priority_from_metrics(row: pd.Series) -> tuple[str, list[str]]:
    """Regra simples e transparente para priorização (Alta/Média/Baixa)."""
    reasons: list[str] = []

    # metas fixas do projeto
    if pd.notna(row.get("conversao_pct")) and float(row["conversao_pct"]) < 12.0:
        reasons.append("Conversão abaixo de 12%")
    if pd.notna(row.get("interacoes")) and float(row["interacoes"]) < 200:
        reasons.append("Interações abaixo de 200")
    # margem: elegibilidade depende de alcance + margem
    if bool(row.get("margem_pct") is not None) and (row.get("elegivel_margem") is False):
        # só destacar se margem existe e não elegível
        reasons.append("Margem inelegível (alcance < 90% ou margem < 26%)")

    # faturamento vs meta (se existir)
    meta = row.get("meta_faturamento")
    fat = row.get("faturamento")
    if pd.notna(meta) and float(meta) > 0 and pd.notna(fat):
        ratio = float(fat) / float(meta)
        if ratio < 0.7:
            reasons.append("Faturamento < 70% da meta")
        elif ratio < 0.9:
            reasons.append("Faturamento < 90% da meta")

    # NFs
    if pd.notna(row.get("qtd_faturadas")) and float(row["qtd_faturadas"]) < 20:
        reasons.append("Baixo volume de NFs")

    # decidir prioridade
    if any("70%" in r for r in reasons) or len(reasons) >= 3:
        return "Alta", reasons[:3]
    if reasons:
        return "Média", reasons[:3]
    return "Baixa", ["Indicadores dentro do esperado"]


def _build_priority_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows: list[dict] = []
    for _, r in df.iterrows():
        prio, motivos = _priority_from_metrics(r)
        rows.append(
            {
                "Vendedor": r.get("nome"),
                "Prioridade": prio,
                "Motivos (curto)": " | ".join(motivos),
            }
        )
    out = pd.DataFrame(rows)
    order = {"Alta": 0, "Média": 1, "Baixa": 2}
    out["_o"] = out["Prioridade"].map(order).fillna(99)
    out = out.sort_values(["_o", "Vendedor"]).drop(columns=["_o"])
    return out


def _render_insights_moderno(data: dict) -> None:
    resumo = str(data.get("resumo_executivo") or "").strip()
    vendedor_destaque = str(data.get("vendedor_destaque") or "").strip()
    vendedor_foco = str(data.get("vendedor_foco") or "").strip()

    st.markdown(
        """
<div class="dp-card" style="padding:16px 18px;">
  <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;justify-content:space-between;">
    <div>
      <div style="color:#94A3B8;font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;font-weight:700;">
        Painel executivo
      </div>
      <div style="color:#E5E7EB;font-size:1.18rem;font-weight:900;margin-top:6px;">
        Insights do time
      </div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;">
      <span class="dp-pill"><span class="dot"></span>Leitura gerencial</span>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns([2.2, 1, 1])
    with c1:
        st.markdown("### Resumo executivo")
        if resumo:
            st.markdown(
                f"""
<div class="dp-card" style="padding:16px 18px;">
  <div style="color:#E5E7EB;font-size:1.02rem;line-height:1.55;">
    {html.escape(resumo)}
  </div>
</div>
""",
                unsafe_allow_html=True,
            )
        else:
            st.info("Sem resumo executivo no retorno da IA.")
    with c2:
        st.markdown("### Destaque")
        st.markdown(
            f"""
<div class="dp-card" style="padding:14px 14px;">
  <div class="dp-kpi-label">Vendedor destaque</div>
  <div class="dp-kpi-value" style="font-size:1.05rem;">{html.escape(vendedor_destaque or "—")}</div>
</div>
""",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown("### Foco")
        st.markdown(
            f"""
<div class="dp-card" style="padding:14px 14px;">
  <div class="dp-kpi-label">Vendedor foco</div>
  <div class="dp-kpi-value" style="font-size:1.05rem;">{html.escape(vendedor_foco or "—")}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    tab1, tab2, tab3 = st.tabs(["Destaques positivos", "Pontos de atenção", "Recomendações do time"])

    def _render_cards(items: list, kind: str) -> None:
        if not isinstance(items, list) or not items:
            st.caption("Nada retornado pela IA.")
            return
        for it in items:
            if not isinstance(it, dict):
                continue
            vend = str(it.get("vendedor") or it.get("prioridade") or "").strip()
            ind = str(it.get("indicador") or it.get("acao") or "").strip()
            val = str(it.get("valor") or it.get("impacto") or "").strip()
            ins = str(it.get("insight") or "").strip()
            badge = (
                "<span class='dp-pill' style='border-color:rgba(110,231,183,.35);color:#6EE7B7;'>Positivo</span>"
                if kind == "pos"
                else "<span class='dp-pill' style='border-color:rgba(251,113,133,.35);color:#FB7185;'>Atenção</span>"
                if kind == "att"
                else "<span class='dp-pill' style='border-color:rgba(251,191,36,.35);color:#FBBF24;'>Ação</span>"
            )
            st.markdown(
                f"""
<div class="dp-card" style="padding:14px 16px;margin-bottom:10px;">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap;">
    <div style="color:#E5E7EB;font-weight:800;font-size:1.02rem;">{html.escape(vend) or "—"}</div>
    <div>{badge}</div>
  </div>
  <div style="color:#94A3B8;margin-top:6px;font-size:.9rem;line-height:1.45;">
    <strong style="color:#E5E7EB;">{html.escape(ind) or "—"}</strong>
    {' · ' + html.escape(val) if val else ''}
  </div>
  {'<div style=\"margin-top:8px;color:#CBD5E1;line-height:1.55;\">' + html.escape(ins) + '</div>' if ins else ''}
</div>
""",
                unsafe_allow_html=True,
            )

    with tab1:
        st.markdown("### Destaques positivos")
        _render_cards(data.get("destaques_positivos") or [], "pos")
    with tab2:
        st.markdown("### Pontos de atenção")
        _render_cards(data.get("pontos_atencao") or [], "att")
    with tab3:
        st.markdown("### Recomendações do time")
        recs = data.get("recomendacoes_time") or []
        if not isinstance(recs, list) or not recs:
            st.caption("Nada retornado pela IA.")
        else:
            for r in recs:
                if not isinstance(r, dict):
                    continue
                prio = str(r.get("prioridade") or "—").strip()
                acao = str(r.get("acao") or "—").strip()
                imp = str(r.get("impacto") or "").strip()
                prio_color = "#FBBF24" if prio.lower().startswith("a") else "#6EE7B7" if prio.lower().startswith("b") else "#94A3B8"
                st.markdown(
                    f"""
<div class="dp-card" style="padding:14px 16px;margin-bottom:10px;">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">
    <div style="color:#E5E7EB;font-weight:850;font-size:1.0rem;">{html.escape(acao)}</div>
    <span class="dp-pill" style="border-color:rgba(255,255,255,.12);color:{prio_color};">
      Prioridade: {html.escape(prio)}
    </span>
  </div>
  {('<div style=\"margin-top:8px;color:#CBD5E1;line-height:1.55;\">Impacto: ' + html.escape(imp) + '</div>') if imp else ''}
</div>
""",
                    unsafe_allow_html=True,
                )


def _ensure_db():
    settings = load_settings()
    conn = connect(settings.db_path)
    init_db(conn)
    return settings, conn


def _maybe_login(settings) -> None:
    # Segurança sempre ativa: se não estiver configurado via secrets/env,
    # usa as credenciais padrão (você pediu).
    admin_user = settings.admin_username or "wsdataanalyst"
    admin_pass = settings.admin_password or "#P161217m"

    if st.session_state.get("auth_ok"):
        return

    auth = build_admin_auth(admin_user, admin_pass)
    st.markdown(
        """
<style>
  /* Esta tela termina em st.stop — afeta só o login */
  section.main [data-testid="stTextInput"] label p {
    font-size: 1.08rem !important;
    font-weight: 600 !important;
  }
  section.main [data-testid="stTextInput"] input {
    font-size: 1.06rem !important;
  }
  section.main .stButton > button {
    font-size: 1.06rem !important;
    padding: 0.75rem 1rem !important;
  }
</style>
""",
        unsafe_allow_html=True,
    )
    render_header(
        "Dashboard Performance",
        "Acesso restrito — informe usuário e senha.",
        right="Segurança ativa",
    )
    # Colunas laterais estreitas: formulário mais central e “perto” do conteúdo
    _, mid, _ = st.columns([0.18, 1.15, 0.18])
    with mid:
        st.markdown(
            """
<div class="dp-card" style="padding:20px 22px 6px;">
  <p style="margin:0;font-size:1.42rem;font-weight:800;color:#E5E7EB;">🔐 Entrar</p>
  <p style="margin:8px 0 0 0;color:#94A3B8;font-size:1rem;line-height:1.45;">
    Campos abaixo — depois clique em <strong>Entrar</strong>.
  </p>
</div>
""",
            unsafe_allow_html=True,
        )
        u = st.text_input("Usuário", placeholder="Digite o usuário")
        p = st.text_input("Senha", type="password")
        if st.button("Entrar", use_container_width=True):
            ok_user = constant_time_equals((u or "").strip(), auth.username)
            ok_pass = constant_time_equals(
                build_admin_auth(auth.username, (p or "")).password_hash,
                auth.password_hash,
            )
            if ok_user and ok_pass:
                st.session_state["auth_ok"] = True
                st.rerun()
            else:
                st.error("Usuário ou senha inválidos.")
    st.stop()


def _uploads_dir(settings) -> Path:
    data_dir = base_data_dir(settings.db_path)
    p = data_dir / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def page_upload(settings, conn) -> None:
    render_header(
        "Upload e extração",
        "Envie prints (ou Excel) → extrai JSON → você revisa → salva no histórico.",
        right="Fallback Gemini ↔ OpenAI",
    )

    def default_provider_index() -> int:
        # Preferir Auto quando as 2 chaves existem; senão, cair para a que existir.
        if settings.google_api_key and settings.openai_api_key:
            return 0  # auto
        if settings.google_api_key:
            return 1  # gemini
        return 2  # openai (ou última opção)

    provider: Provider = st.selectbox(
        "Provedor de IA",
        options=["auto", "gemini", "openai"],
        format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
        index=default_provider_index(),
    )

    k1, k2 = st.columns(2)
    with k1:
        st.write("**GOOGLE_API_KEY**:", "✅" if settings.google_api_key else "❌")
    with k2:
        st.write("**OPENAI_API_KEY**:", "✅" if settings.openai_api_key else "❌")
    st.caption("Se as duas chaves estiverem ✅, use **Auto** para ter fallback.")

    # Mantém exatamente a ordem/nome do dashboard original (seu print)
    nomes = [
        "Print 1 - Alcance, Margem, Meta, %Meta, %Venda, Desconto, Faturamento",
        "Print 2 - Prazo Médio",
        "Print 3 - Qtd. Faturadas",
        "Print 4 - Chamadas",
        "Print 5 - TME, Iniciados e Recebidos",
    ]

    images: list[tuple[str, bytes, str | None]] = []
    cols = st.columns(2)
    for i, nome in enumerate(nomes):
        with cols[i % 2]:
            f = st.file_uploader(nome, type=["png", "jpg", "jpeg"], key=f"up_{i}")
            if f:
                images.append((nome, f.read(), getattr(f, "type", None)))

    if images:
        st.markdown("### Preview")
        pcols = st.columns(min(3, len(images)))
        for i, (n, b, _) in enumerate(images):
            with pcols[i % len(pcols)]:
                st.image(b, caption=n, use_container_width=True)

    st.markdown("---")
    # `periodo` é usado por fluxos de import (Excel/OCR/IA), então precisa existir antes.
    left, right = st.columns([1, 1])
    with left:
        periodo = st.text_input("Período", value="")
    with right:
        st.caption("Dica: algo como `Abril/2026` ou `Abril (até 15/04)`.")

    st.markdown("### 📄 Importar Excel (mais confiável que OCR)")
    excel_files = st.file_uploader(
        "Envie os 5 arquivos (um por print) — aceita .xlsx / .xls (inclui export HTML).",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        key="excel_upload",
    )
    if excel_files:
        if st.button("📥 Importar arquivos (Excel/HTML)", use_container_width=True):
            try:
                with st.spinner("Importando arquivos..."):
                    res = import_5_files_to_payload([(f.name, f.read()) for f in excel_files])
                if periodo and isinstance(res.payload, dict):
                    res.payload["periodo"] = periodo
                st.session_state["payload"] = res.payload
                st.session_state["extraction_meta"] = res.meta
                if res.warnings:
                    st.warning("Importação concluída com avisos.")
                    for w in res.warnings:
                        st.caption(w)
                else:
                    st.success("Importação concluída.")
            except Exception as e:
                st.error("Falha ao importar Excel/HTML.")
                st.caption(str(e))

    b1, b2, b3 = st.columns([1, 1, 1])
    with b1:
        run_ia = st.button("🤖 Extrair com IA", use_container_width=True, disabled=not images)
    with b2:
        use_manual = st.button("✍️ Usar JSON manual", use_container_width=True)
    with b3:
        clear = st.button("🧹 Limpar dados", use_container_width=True)
    ocr_debug = st.toggle("Debug OCR (mostrar diagnóstico)", value=False, disabled=not images)
    run_ocr = st.button("🧾 Extrair sem IA (OCR)", use_container_width=True, disabled=not images)

    if clear:
        st.session_state.pop("payload", None)
        st.session_state.pop("extraction_meta", None)
        st.session_state.pop("insights", None)
        st.rerun()

    if run_ia:
        imgs = [(n, b) for (n, b, _) in images]
        try:
            with st.spinner("Extraindo dados (com fallback automático)..."):
                payload, provider_used, model_used = extract_json_from_images(
                    settings=settings,
                    provider=provider,
                    images=imgs,
                    prompt=PROMPT_EXTRACAO,
                )
            if periodo and isinstance(payload, dict):
                payload["periodo"] = periodo
            st.session_state["payload"] = payload
            st.session_state["extraction_meta"] = {"provider": provider_used, "model": model_used}
            st.success(f"Extração concluída usando **{provider_used}** (`{model_used}`).")
        except Exception as e:
            st.error("Não consegui extrair com IA.")
            st.caption(str(e))
            st.info(
                "Se você está usando apenas OpenAI: confirme que `OPENAI_API_KEY` está preenchida. "
                "Se o erro for **429 / insufficient_quota**, sua conta/projeto OpenAI está sem crédito/quota."
            )

    if run_ocr:
        imgs = [(n, b) for (n, b, _) in images]
        try:
            with st.spinner("Extraindo via OCR (sem IA)..."):
                if ocr_debug:
                    payload, dbg = extract_payload_from_prints_ocr(imgs, debug=True)
                    st.session_state["ocr_debug"] = dbg
                else:
                    payload = extract_payload_from_prints_ocr(imgs, debug=False)
            if periodo and isinstance(payload, dict):
                payload["periodo"] = periodo
            st.session_state["payload"] = payload
            st.session_state["extraction_meta"] = {"provider": "ocr", "model": "tesseract"}
            st.success("OCR concluído. Revise os dados antes de salvar (pode precisar ajustes).")
            if ocr_debug:
                st.info("Debug OCR habilitado: veja o diagnóstico no final da página.")
        except Exception as e:
            st.error("Não consegui extrair via OCR.")
            st.caption(str(e))
            st.info(
                "No Windows local, você precisa ter o **Tesseract** instalado para OCR funcionar. "
                "No Streamlit Cloud, isso é instalado via `packages.txt`."
            )
    dbg = st.session_state.get("ocr_debug")
    if isinstance(dbg, dict) and dbg.get("prints"):
        with st.expander("🧪 Diagnóstico OCR (debug)", expanded=False):
            for p in dbg.get("prints", []):
                st.markdown(f"#### {p.get('nome_print')} ({p.get('kind')})")
                st.write("**Headers detectados (centro X):**", p.get("headers_detectados"))
                st.text_area(
                    "Amostra de texto OCR (primeiras linhas)",
                    value="\n".join(p.get("amostra_texto") or []),
                    height=180,
                    key=f"dbg_{p.get('kind')}_{p.get('nome_print')}",
                )

    if use_manual:
        example = {
            "periodo": periodo or "Abril/2026",
            "vendedores": [
                {
                    "nome": "João Silva",
                    "margem_pct": 27.2,
                    "alcance_projetado_pct": 92.0,
                    "prazo_medio": 40,
                    "qtd_faturadas": 20,
                    "iniciados": 120,
                    "recebidos": 40,
                    "finalizados": 60,
                    "tme_minutos": 4.5,
                }
            ],
        }
        txt = st.text_area("Cole o JSON aqui", value=json.dumps(example, ensure_ascii=False, indent=2), height=260)
        if st.button("💾 Carregar JSON manual", use_container_width=True):
            try:
                payload = json.loads(txt)
                if not isinstance(payload, dict):
                    raise ValueError("JSON precisa ser um objeto.")
                st.session_state["payload"] = payload
                st.session_state["extraction_meta"] = {"provider": "manual", "model": "manual"}
                st.success("JSON carregado.")
            except Exception as e:
                st.error(f"JSON inválido: {e}")

    payload = st.session_state.get("payload")
    if isinstance(payload, dict):
        st.markdown("---")
        st.subheader("✅ Validação dos dados (prévia)")

        sellers = parse_sellers(payload)
        results, total = calcular_time(sellers) if sellers else ([], 0.0)

        if sellers:
            df_prev = pd.DataFrame([r.__dict__ for r in results])
            # tenta enriquecer validação com faturamento/meta/ticket quando existirem no payload
            raw_map = {s.nome: s for s in sellers}
            df_prev["chamadas"] = df_prev["nome"].apply(lambda n: raw_map.get(n).chamadas if raw_map.get(n) else None)
            df_prev["faturamento"] = df_prev["nome"].apply(lambda n: raw_map.get(n).faturamento if raw_map.get(n) else None)
            df_prev["meta_faturamento"] = df_prev["nome"].apply(lambda n: raw_map.get(n).meta_faturamento if raw_map.get(n) else None)
            df_prev["ticket_medio"] = df_prev.apply(
                lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"])) if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0) else None,
                axis=1,
            )
            st.dataframe(
                df_prev[
                    [
                        "nome",
                        "alcance_pct",
                        "margem_pct",
                        "prazo_medio",
                        "conversao_pct",
                        "tme_minutos",
                        "interacoes",
                        "chamadas",
                        "bonus_total",
                        "qtd_faturadas",
                        "faturamento",
                        "meta_faturamento",
                        "ticket_medio",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.warning("Não encontrei vendedores no payload. Confira a extração/JSON manual.")

        with st.expander("Ver JSON completo (opcional)"):
            st.json(payload)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown('<div class="dp-card"><div class="dp-kpi-label">Vendedores</div>'
                        f'<div class="dp-kpi-value">{len(sellers)}</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="dp-card"><div class="dp-kpi-label">Bônus total</div>'
                        f'<div class="dp-kpi-value">R$ {total:,.2f}</div></div>', unsafe_allow_html=True)
        with c3:
            meta = st.session_state.get("extraction_meta") or {}
            st.markdown('<div class="dp-card"><div class="dp-kpi-label">Origem</div>'
                        f'<div class="dp-kpi-value" style="font-size:1.05rem">{meta.get("provider","—")} / {meta.get("model","—")}</div></div>',
                        unsafe_allow_html=True)

        st.markdown("### Salvar no histórico")
        if st.button("✅ Salvar análise", use_container_width=True):
            meta = st.session_state.get("extraction_meta") or {"provider": "manual", "model": "manual"}
            periodo_final = str(payload.get("periodo") or periodo or "Período não informado")
            analysis_id = save_analysis(
                conn,
                periodo=periodo_final,
                provider_used=str(meta.get("provider", "unknown")),
                model_used=str(meta.get("model", "unknown")),
                parent_analysis_id=None,
                payload=payload,
                total_bonus=float(total),
            )

            # Persistir uploads para auditoria (se houver)
            up_dir = _uploads_dir(settings) / str(analysis_id)
            up_dir.mkdir(parents=True, exist_ok=True)
            for n, b, ctype in images:
                digest = sha256_hex(b)
                safe_name = "".join(ch for ch in n if ch.isalnum() or ch in (" ", "-", "_")).strip().replace(" ", "_")
                filename = f"{safe_name}_{digest[:10]}.png"
                rel_path = str(Path("uploads") / str(analysis_id) / filename)
                (up_dir / filename).write_bytes(b)
                save_upload_file(
                    conn,
                    analysis_id=analysis_id,
                    filename=filename,
                    content_type=ctype,
                    sha256=digest,
                    rel_path=rel_path,
                )

            st.success(f"Análise salva com ID **{analysis_id}**.")
            st.session_state["active_analysis_id"] = analysis_id


def page_dashboard(settings, conn) -> None:
    render_header("Dashboard", "Visualize a análise ativa (ou carregue do histórico).")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Vá em **Upload e extração** ou carregue no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise ativa não encontrada (talvez tenha sido apagada).")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    results, total = calcular_time(sellers) if sellers else ([], 0.0)

    top = results[0].nome if results else "—"
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"<div class='dp-card'><div class='dp-kpi-label'>Período</div><div class='dp-kpi-value' style='font-size:1.1rem'>{row.periodo}</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='dp-card'><div class='dp-kpi-label'>Bônus total</div><div class='dp-kpi-value'>R$ {total:,.2f}</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='dp-card'><div class='dp-kpi-label'>Top (bônus)</div><div class='dp-kpi-value' style='font-size:1.1rem'>{top}</div></div>", unsafe_allow_html=True)
    c4.markdown(f"<div class='dp-card'><div class='dp-kpi-label'>IA usada</div><div class='dp-kpi-value' style='font-size:1.0rem'>{row.provider_used} / {row.model_used}</div></div>", unsafe_allow_html=True)

    if not results:
        st.warning("Sem vendedores no payload.")
        return

    df = pd.DataFrame([r.__dict__ for r in results])
    # Enriquecer com chamadas/faturamento/meta/ticket para visão completa
    raw_map = {s.nome: s for s in sellers}
    df["chamadas"] = df["nome"].apply(lambda n: raw_map.get(n).chamadas if raw_map.get(n) else None)
    df["faturamento"] = df["nome"].apply(lambda n: raw_map.get(n).faturamento if raw_map.get(n) else None)
    df["meta_faturamento"] = df["nome"].apply(lambda n: raw_map.get(n).meta_faturamento if raw_map.get(n) else None)
    df["ticket_medio"] = df.apply(
        lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"])) if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0) else None,
        axis=1,
    )

    tab_resumo, tab_bonus = st.tabs(["Resumo completo", "Central de Vendas | Bônus"])

    with tab_resumo:
        st.markdown("### Resultado por vendedor")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.markdown("### Gráfico de bônus")
        try:
            import plotly.express as px

            fig = px.bar(df, x="nome", y="bonus_total", title="Bônus por vendedor")
            fig.update_layout(height=380)
            st.plotly_chart(fig, use_container_width=True, key="bonus_chart_dashboard")
        except Exception as e:
            st.info(f"Não foi possível renderizar gráfico: {e}")

        st.markdown("### Auditoria (uploads salvos)")
        ups = list_uploads(conn, int(analysis_id))
        if not ups:
            st.caption("Sem uploads salvos para esta análise.")
        else:
            st.dataframe(pd.DataFrame(ups), use_container_width=True, hide_index=True)

    with tab_bonus:
        st.markdown(
            render_bonus_central_panel_html(df, periodo=row.periodo, total=float(total)),
            unsafe_allow_html=True,
        )
        st.caption(
            "Detalhamento por coluna de R$ (margem, prazo, etc.) permanece na aba **Resumo completo**."
        )


def page_evolution(settings, conn) -> None:
    render_header("Evolução", "Acompanhe a evolução do bônus ao longo do tempo.")
    rows = list_analyses(conn, limit=200)
    if len(rows) < 2:
        st.info("Você precisa de pelo menos 2 análises salvas para ver a evolução.")
        return

    df = pd.DataFrame(
        [{"id": r.id, "created_at": r.created_at, "periodo": r.periodo, "total_bonus": r.total_bonus} for r in rows]
    )
    # ordem cronológica
    df = df.sort_values("id", ascending=True)

    c1, c2 = st.columns(2)
    c1.metric("Análises", f"{len(df)}")
    c2.metric("Último bônus", f"R$ {df.iloc[-1]['total_bonus']:,.2f}")

    try:
        import plotly.express as px

        fig = px.line(df, x="periodo", y="total_bonus", markers=True, title="Evolução do bônus total")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True, key="bonus_chart_evolution")
    except Exception as e:
        st.info(f"Não foi possível renderizar gráfico: {e}")

    st.dataframe(df[["id", "created_at", "periodo", "total_bonus"]], use_container_width=True, hide_index=True)


def _team_stats(df_results: pd.DataFrame) -> dict[str, float]:
    def _mean(col: str) -> float:
        if col not in df_results.columns:
            return 0.0
        s = pd.to_numeric(df_results[col], errors="coerce")
        s = s.dropna()
        return float(s.mean()) if len(s) else 0.0

    def _sum(col: str) -> float:
        if col not in df_results.columns:
            return 0.0
        s = pd.to_numeric(df_results[col], errors="coerce")
        s = s.dropna()
        return float(s.sum()) if len(s) else 0.0

    return {
        "media_margem": _mean("margem_pct"),
        "media_alcance": _mean("alcance_pct"),
        "media_prazo": _mean("prazo_medio"),
        "media_conversao": _mean("conversao_pct"),
        "media_tme": _mean("tme_minutos"),
        "media_interacoes": _mean("interacoes"),
        "total_faturas": _sum("qtd_faturadas"),
        "total_bonus": _sum("bonus_total"),
    }


def page_performance(settings, conn, *, key_prefix: str = "perf") -> None:
    render_header("Performance", "Visão gerencial do time (metas, padrões e prioridades).")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    results, total = calcular_time(sellers) if sellers else ([], 0.0)
    if not results:
        st.warning("Sem vendedores.")
        return

    df = pd.DataFrame([r.__dict__ for r in results])
    raw_map = {s.nome: s for s in sellers}
    df["chamadas"] = df["nome"].apply(lambda n: raw_map.get(n).chamadas if raw_map.get(n) else None)
    df["faturamento"] = df["nome"].apply(lambda n: raw_map.get(n).faturamento if raw_map.get(n) else None)
    df["meta_faturamento"] = df["nome"].apply(lambda n: raw_map.get(n).meta_faturamento if raw_map.get(n) else None)
    df["ticket_medio"] = df.apply(
        lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"])) if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0) else None,
        axis=1,
    )
    stats = _team_stats(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bônus total", f"R$ {stats['total_bonus']:,.2f}")
    c2.metric("Margem média", f"{stats['media_margem']:.1f}%")
    c3.metric("Conversão média", f"{stats['media_conversao']:.1f}%")
    c4.metric("TME médio", f"{stats['media_tme']:.1f} min")

    # Evolução de conversão por período (últimas análises salvas)
    st.markdown("### Conversão x Interações (comparativo por análise salva)")
    rows = list_analyses(conn, limit=12)
    if len(rows) >= 2:
        hist: list[dict] = []
        for r in reversed(rows):  # cronológico (antigo -> novo)
            try:
                payload_r = json.loads(r.payload_json)
            except Exception:
                continue
            base = _extract_perf_summary_from_payload(r.periodo, payload_r)
            inter = float(base.get("tot_interacoes") or 0)
            nfs = float(base.get("tot_nfs") or 0)
            conv_total = (nfs / inter * 100.0) if inter > 0 else None
            hist.append(
                {
                    "id": int(r.id),
                    "created_at": str(r.created_at),
                    "periodo": str(r.periodo),
                    "interacoes": inter,
                    "nfs": nfs,
                    "conversao_total_pct": conv_total,
                }
            )
        hdf = pd.DataFrame(hist)
        if not hdf.empty:
            last = hdf.iloc[-1]
            prev = hdf.iloc[-2] if len(hdf) >= 2 else None

            def _fmt_conv(v) -> str:
                return f"{float(v):.1f}%" if v is not None and not pd.isna(v) else "—"

            m1, m2, m3 = st.columns(3)
            if prev is not None:
                m1.metric(
                    "Interações (time)",
                    f"{int(last['interacoes'])}",
                    delta=int(last["interacoes"] - prev["interacoes"]),
                )
                m2.metric("NFs (time)", f"{int(last['nfs'])}", delta=int(last["nfs"] - prev["nfs"]))
                if pd.notna(last.get("conversao_total_pct")) and pd.notna(prev.get("conversao_total_pct")):
                    delta_pp = float(last["conversao_total_pct"]) - float(prev["conversao_total_pct"])
                    # Streamlit só colore automaticamente (verde/vermelho) quando `delta` é numérico.
                    m3.metric(
                        "Conversão (NFs/Interações)",
                        _fmt_conv(last["conversao_total_pct"]),
                        delta=round(delta_pp, 1),
                        help="Delta em pontos percentuais (pp) vs análise anterior.",
                    )
                else:
                    m3.metric("Conversão (NFs/Interações)", _fmt_conv(last.get("conversao_total_pct")))

            # “Melhor momento” (maior conversão total)
            best_idx = None
            if "conversao_total_pct" in hdf.columns:
                s = pd.to_numeric(hdf["conversao_total_pct"], errors="coerce")
                if s.notna().any():
                    best_idx = int(s.idxmax())
                    best = hdf.loc[best_idx]
                    st.caption(
                        f"Melhor conversão no histórico carregado: **ID {int(best['id'])}** "
                        f"({best['periodo']}) → **{float(best['conversao_total_pct']):.1f}%** "
                        f"com **{int(best['interacoes'])}** interações e **{int(best['nfs'])}** NFs."
                    )

            # Gráfico combinado (barras + linha)
            try:
                import plotly.graph_objects as go
                from plotly.subplots import make_subplots

                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(
                    go.Bar(x=hdf["id"], y=hdf["interacoes"], name="Interações", marker_color="rgba(59,130,246,0.55)"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Bar(x=hdf["id"], y=hdf["nfs"], name="NFs", marker_color="rgba(110,231,183,0.75)"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Scatter(
                        x=hdf["id"],
                        y=hdf["conversao_total_pct"],
                        name="Conversão (%)",
                        mode="lines+markers",
                        line=dict(color="rgba(251,191,36,0.95)", width=3),
                    ),
                    secondary_y=True,
                )
                if best_idx is not None:
                    fig.add_trace(
                        go.Scatter(
                            x=[hdf.loc[best_idx, "id"]],
                            y=[hdf.loc[best_idx, "conversao_total_pct"]],
                            mode="markers",
                            marker=dict(size=14, color="rgba(251,191,36,1)", symbol="star"),
                            name="Melhor conversão",
                        ),
                        secondary_y=True,
                    )
                fig.update_layout(
                    title="Interações e NFs vs Conversão (por análise salva)",
                    height=420,
                    barmode="group",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                    margin=dict(l=10, r=10, t=60, b=10),
                )
                fig.update_xaxes(title_text="ID da análise")
                fig.update_yaxes(title_text="Volume", secondary_y=False)
                fig.update_yaxes(title_text="Conversão (%)", secondary_y=True, rangemode="tozero")
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_conv_history_combo")
            except Exception as e:
                st.caption(f"Gráfico combinado indisponível: {e}")
    else:
        st.caption("Salve pelo menos 2 análises para comparar conversão vs interações ao longo do tempo.")

    st.markdown("### Indicadores (ranking)")
    indicador = st.selectbox(
        "Escolha o indicador",
        options=[
            ("bonus_total", "Bônus (R$)"),
            ("margem_pct", "Margem (%)"),
            ("conversao_pct", "Conversão (%)"),
            ("prazo_medio", "Prazo médio (dias)"),
            ("tme_minutos", "TME (min)"),
            ("interacoes", "Interações"),
            ("qtd_faturadas", "Qtd faturadas"),
        ],
        format_func=lambda x: x[1],
        key=f"{key_prefix}_indicador",
    )
    col = indicador[0]
    dfp = df.sort_values(col, ascending=False)

    try:
        import plotly.express as px

        fig = px.bar(dfp, x="nome", y=col, title=f"Ranking — {indicador[1]}")
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_rank_{col}")
    except Exception as e:
        st.info(f"Não foi possível renderizar gráfico: {e}")

    st.dataframe(
        dfp[
            [
                "nome",
                "bonus_total",
                "margem_pct",
                "conversao_pct",
                "prazo_medio",
                "tme_minutos",
                "interacoes",
                "chamadas",
                "qtd_faturadas",
                "ticket_medio",
                "faturamento",
                "meta_faturamento",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


def page_edit(settings, conn) -> None:
    render_header("Edição manual", "Corrija dados rapidamente e salve uma nova versão no histórico.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    if not sellers:
        st.warning("Sem vendedores para editar.")
        return

    df = pd.DataFrame([s.__dict__ for s in sellers])
    st.caption("Edite os campos necessários e clique em **Salvar nova versão**.")
    edited = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
    )

    if st.button("💾 Salvar nova versão", use_container_width=True):
        novos = edited.to_dict(orient="records")
        new_payload = dict(payload)
        new_payload["vendedores"] = novos

        sellers2 = parse_sellers(new_payload)
        results2, total2 = calcular_time(sellers2) if sellers2 else ([], 0.0)
        new_id = save_analysis(
            conn,
            periodo=str(new_payload.get("periodo") or row.periodo),
            provider_used="manual_edit",
            model_used="manual_edit",
            parent_analysis_id=int(analysis_id),
            payload=new_payload,
            total_bonus=float(total2),
        )
        st.session_state["active_analysis_id"] = new_id
        st.success(f"Nova versão salva como **#{new_id}** e definida como ativa.")


def page_projection(settings, conn) -> None:
    render_header("Simulação / Projeções", "Mantendo o ritmo atual: NFs projetadas, interações, conversão e meta.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    if not sellers:
        st.warning("Sem vendedores para projeção.")
        return

    totais = payload.get("totais") if isinstance(payload, dict) else None
    if not isinstance(totais, dict):
        totais = {}

    cal = st.session_state.get("calendar_info")
    default_total = int(cal["dias_uteis_total"]) if isinstance(cal, dict) and "dias_uteis_total" in cal else 22
    default_trab = int(cal["dias_uteis_trabalhados"]) if isinstance(cal, dict) and "dias_uteis_trabalhados" in cal else min(15, default_total)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        dias_total = st.number_input("Total de dias úteis no mês", min_value=1, max_value=31, value=int(default_total))
    with col2:
        dias_trab = st.number_input("Dias úteis trabalhados até agora", min_value=1, max_value=int(dias_total), value=min(int(default_trab), int(dias_total)))
    with col3:
        meta_faturamento = st.number_input("Meta de faturamento (R$)", min_value=0.0, max_value=1e9, value=0.0, step=1000.0, format="%.2f")
    with col4:
        modo = st.selectbox("Modo", options=["Por vendedor", "Time (somado)"], key="proj_mode")

    if modo == "Por vendedor":
        nome = st.selectbox("Vendedor", options=[s.nome for s in sellers], key="proj_seller")
        s = next(x for x in sellers if x.nome == nome)
        ticket_auto = (float(s.faturamento) / float(s.qtd_faturadas)) if (s.faturamento is not None and (s.qtd_faturadas or 0) > 0) else 0.0
        ticket_override = st.number_input(
            "Ticket médio (R$) — opcional",
            min_value=0.0,
            max_value=1e8,
            value=float(ticket_auto),
            step=50.0,
            format="%.2f",
            help="Se o faturamento não veio do print, você pode informar o ticket médio aqui.",
        )
        meta_auto = float(s.meta_faturamento) if (s.meta_faturamento is not None and s.meta_faturamento > 0) else 0.0
        meta_faturamento_eff = float(meta_faturamento) if meta_faturamento > 0 else (meta_auto if meta_auto > 0 else None)
        proj = projetar_resultados(
            s,
            dias_uteis_total=int(dias_total),
            dias_uteis_trabalhados=int(dias_trab),
            meta_faturamento=meta_faturamento_eff,
            ticket_medio_override=float(ticket_override) if ticket_override > 0 else None,
        )
        titulo = f"Projeção — {s.nome}"
    else:
        # soma do time (modelo simples: soma dos indicadores atuais e projeta linearmente)
        from src.app.domain import Seller as SellerDC
        # Totais do time (se vierem do print) ajudam no ticket/meta.
        fat_total = totais.get("faturamento_total")
        meta_total = totais.get("meta_total")

        soma = SellerDC(
            nome="Time",
            qtd_faturadas=sum(int(x.qtd_faturadas or 0) for x in sellers),
            iniciados=sum(int(x.iniciados or 0) for x in sellers),
            recebidos=sum(int(x.recebidos or 0) for x in sellers),
            chamadas=sum(int(x.chamadas or 0) for x in sellers),
            faturamento=float(fat_total) if isinstance(fat_total, (int, float)) else None,
            meta_faturamento=float(meta_total) if isinstance(meta_total, (int, float)) else None,
        )
        meta_faturamento_eff = float(meta_faturamento) if meta_faturamento > 0 else soma.meta_faturamento
        proj = projetar_resultados(
            soma,
            dias_uteis_total=int(dias_total),
            dias_uteis_trabalhados=int(dias_trab),
            meta_faturamento=meta_faturamento_eff,
            ticket_medio_override=None,
        )
        titulo = "Projeção — Time"

    st.markdown(f"### {titulo}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturadas (atual)", f"{proj.qtd_faturadas_atual}")
    c2.metric("Interações (atual)", f"{proj.interacoes_atual}")
    c3.metric("Projeção faturadas", f"{proj.projecao_faturas}")
    c4.metric("Status", proj.status)

    st.markdown("### Ritmo diário")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Média faturas/dia", f"{proj.media_diaria_faturas}")
    k2.metric("Média interações/dia", f"{proj.media_diaria_interacoes}")
    k3.metric("Dias restantes", f"{proj.dias_restantes}")
    k4.metric("Conversão proj.", f"{proj.projecao_conversao_pct:.2f}%" if proj.projecao_conversao_pct is not None else "—")

    st.markdown("### Meta em faturamento (mantendo o ritmo/ticket)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Ticket médio", f"R$ {proj.ticket_medio:,.2f}" if proj.ticket_medio is not None else "—")
    m2.metric("Faturamento atual", f"R$ {proj.faturamento_atual:,.2f}" if proj.faturamento_atual is not None else "—")
    m3.metric("Faturamento/dia (atual)", f"R$ {proj.faturamento_dia_atual:,.2f}" if proj.faturamento_dia_atual is not None else "—")
    m4.metric("Projeção faturamento", f"R$ {proj.projecao_faturamento:,.2f}" if proj.projecao_faturamento is not None else "—")

    if proj.meta_faturamento is not None and proj.meta_faturamento > 0:
        st.markdown("### O que falta para bater a meta")
        x1, x2, x3 = st.columns(3)
        x1.metric("Meta faturamento", f"R$ {proj.meta_faturamento:,.2f}")
        x2.metric("Falta (R$)", f"R$ {proj.faturamento_faltando:,.2f}" if proj.faturamento_faltando is not None else "—")
        x3.metric(
            "NFs/dia necessárias (mesmo ticket)",
            f"{proj.nfs_por_dia_necessarias}" if proj.nfs_por_dia_necessarias is not None else "—",
        )
        if proj.ticket_necessario_com_mesmo_ritmo is not None:
            st.caption(f"Se mantiver o mesmo ritmo de NFs/dia, o ticket médio necessário seria ~ **R$ {proj.ticket_necessario_com_mesmo_ritmo:,.2f}**.")


def page_star(settings, conn) -> None:
    render_header("Feedback STAR", "Gere feedback individual e baixe em PDF.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    results, total = calcular_time(sellers) if sellers else ([], 0.0)
    if not results:
        st.warning("Sem vendedores.")
        return

    nome = st.selectbox("Vendedor", options=[r.nome for r in results], key="star_seller")
    r = next(x for x in results if x.nome == nome)
    s_raw = next((s for s in sellers if s.nome == nome), None)
    ticket = None
    if s_raw and s_raw.faturamento is not None and (s_raw.qtd_faturadas or 0) > 0:
        ticket = float(s_raw.faturamento) / float(s_raw.qtd_faturadas or 1)

    st.markdown("### Resumo do vendedor")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Bônus", f"R$ {r.bonus_total:,.2f}")
    c2.metric("Margem", f"{r.margem_pct if r.margem_pct is not None else '—'}")
    c3.metric("Conversão", f"{r.conversao_pct if r.conversao_pct is not None else '—'}")
    c4.metric("Interações", f"{r.interacoes if r.interacoes is not None else '—'}")

    provider: Provider = st.selectbox(
        "Provedor de IA (feedback)",
        options=["auto", "gemini", "openai"],
        format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
        key="star_provider",
    )

    if st.button("✨ Gerar feedback STAR", use_container_width=True):
        star_in = StarInput(
            periodo=row.periodo,
            nome=r.nome,
            bonus_total=r.bonus_total,
            margem_pct=r.margem_pct,
            alcance_pct=r.alcance_pct,
            prazo_medio=r.prazo_medio,
            conversao_pct=r.conversao_pct,
            tme_minutos=r.tme_minutos,
            interacoes=r.interacoes,
            qtd_faturadas=r.qtd_faturadas,
            faturamento=s_raw.faturamento if s_raw else None,
            meta_faturamento=s_raw.meta_faturamento if s_raw else None,
            ticket_medio=round(ticket, 2) if ticket is not None else None,
        )
        prompt = build_prompt_star(star_in)
        star_user_prompt = (
            f'Retorne um JSON no formato {{"feedback_star":"...texto..."}}. {prompt}'
        )
        try:
            with st.spinner("Gerando feedback..."):
                resp, prov_used, model_used = json_from_text(
                    settings=settings,
                    provider=provider,
                    prompt=star_user_prompt,
                )
        except RuntimeError as e:
            st.error(str(e))
            st.info(
                "**Dicas:** se a OpenAI estiver sem cota (429), escolha **Gemini** no seletor. "
                "Com cota na OpenAI, **Auto** volta a funcionar como fallback. "
                "Se o Gemini falhar de novo, gere outra vez (às vezes o JSON vem malformado)."
            )
        else:
            texto = str(resp.get("feedback_star") or "").strip()
            if not texto:
                st.error("A IA não retornou `feedback_star`.")
            else:
                save_feedback(
                    conn,
                    analysis_id=int(analysis_id),
                    seller_name=r.nome,
                    provider_used=prov_used,
                    model_used=model_used,
                    feedback_text=texto,
                )
                st.session_state["last_star_text"] = texto

    texto = st.session_state.get("last_star_text")
    if isinstance(texto, str) and texto.strip():
        st.markdown("### Feedback")
        st.text_area("Feedback STAR", value=texto, height=260)
        pdf = render_pdf_star(
            titulo="Feedback STAR",
            periodo=row.periodo,
            vendedor=r.nome,
            texto=texto,
            gestor_nome_cargo=STAR_GESTOR_PADRAO,
        )
        st.download_button(
            "📄 Baixar PDF",
            data=pdf,
            file_name=f"Feedback_STAR_{r.nome.replace(' ', '_')}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )

    with st.expander("Histórico de feedbacks desta análise"):
        fb = list_feedbacks(conn, int(analysis_id))
        if not fb:
            st.caption("Nenhum feedback gerado ainda.")
        else:
            st.dataframe(pd.DataFrame(fb)[["created_at", "seller_name", "provider_used", "model_used"]], use_container_width=True, hide_index=True)


def page_history(settings, conn) -> None:
    render_header("Histórico", "Carregue análises anteriores sem perder informação.")

    rows = list_analyses(conn, limit=100)
    if not rows:
        st.info("Histórico vazio. Faça sua primeira análise em **Upload e extração**.")
        return

    options = {f"#{r.id} · {r.periodo} · {r.created_at} · R$ {r.total_bonus:,.2f}": r.id for r in rows}
    selected = st.selectbox("Selecione uma análise", options=list(options.keys()))
    selected_id = int(options[selected])

    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        if st.button("📌 Tornar ativa", use_container_width=True):
            st.session_state["active_analysis_id"] = selected_id
            st.success("Análise ativa atualizada.")
            st.rerun()
    with c2:
        if st.button("🗑️ Apagar", use_container_width=True):
            delete_analysis(conn, selected_id)
            if st.session_state.get("active_analysis_id") == selected_id:
                st.session_state.pop("active_analysis_id", None)
            st.success("Análise apagada.")
            st.rerun()
    with c3:
        st.caption("Dica: apagar remove o registro e os uploads vinculados (por cascata).")

    row = get_analysis(conn, selected_id)
    if row:
        st.markdown("---")
        st.subheader("Detalhe")
        st.write("**Período:**", row.periodo)
        st.write("**IA:**", f"{row.provider_used} / {row.model_used}")
        st.write("**Total bônus:**", f"R$ {row.total_bonus:,.2f}")
        st.json(json.loads(row.payload_json))


def page_insights(settings, conn) -> None:
    render_header("Insights", "IA gera recomendações e pontos de atenção do time.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico** ou crie em **Upload**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    def default_provider_index() -> int:
        if settings.google_api_key and settings.openai_api_key:
            return 0
        if settings.google_api_key:
            return 1
        return 2

    provider: Provider = st.selectbox(
        "Provedor de IA (para insights)",
        options=["auto", "gemini", "openai"],
        format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
        key="ins_provider",
        index=default_provider_index(),
    )

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    results, total = calcular_time(sellers) if sellers else ([], 0.0)

    df = pd.DataFrame([r.__dict__ for r in results]) if results else pd.DataFrame()
    df = _enrich_results_df_for_performance(df, sellers)
    dados_json = json.dumps(
        {
            "periodo": row.periodo,
            "total_bonus": total,
            "vendedores": df.to_dict(orient="records") if not df.empty else [],
        },
        ensure_ascii=False,
        indent=2,
    )
    prompt = PROMPT_INSIGHTS.format(dados_json=dados_json)

    # Visual rápido (antes de gerar IA)
    if not df.empty:
        st.markdown("### Visão rápida (performance)")
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("NFs (time)", f"{int(pd.to_numeric(df.get('qtd_faturadas'), errors='coerce').fillna(0).sum())}")
        k2.metric("Faturamento (time)", f"R$ {float(pd.to_numeric(df.get('faturamento'), errors='coerce').fillna(0).sum()):,.2f}")
        k3.metric("Interações (time)", f"{int(pd.to_numeric(df.get('interacoes'), errors='coerce').fillna(0).sum())}")
        conv = pd.to_numeric(df.get("conversao_pct"), errors="coerce").dropna()
        k4.metric("Conversão (média)", f"{float(conv.mean()):.2f}%" if len(conv) else "—")
        marg = pd.to_numeric(df.get("margem_pct"), errors="coerce").dropna()
        k5.metric("Margem (média)", f"{float(marg.mean()):.2f}%" if len(marg) else "—")

        try:
            import plotly.express as px

            st.markdown("### Gráficos")
            c1, c2 = st.columns(2)
            with c1:
                if "faturamento" in df.columns:
                    fig = px.bar(df, x="nome", y="faturamento", title="Faturamento por vendedor")
                    fig.update_layout(height=340)
                    st.plotly_chart(fig, use_container_width=True, key="ins_perf_faturamento_bar")
            with c2:
                if "qtd_faturadas" in df.columns:
                    fig = px.bar(df, x="nome", y="qtd_faturadas", title="NFs (Qtd. faturadas) por vendedor")
                    fig.update_layout(height=340)
                    st.plotly_chart(fig, use_container_width=True, key="ins_perf_nfs_bar")

            c3, c4 = st.columns(2)
            with c3:
                if "ticket_medio" in df.columns:
                    fig = px.bar(df, x="nome", y="ticket_medio", title="Ticket médio por vendedor")
                    fig.update_layout(height=340)
                    st.plotly_chart(fig, use_container_width=True, key="ins_perf_ticket_bar")
            with c4:
                if "conversao_pct" in df.columns and "interacoes" in df.columns:
                    fig = px.scatter(
                        df,
                        x="interacoes",
                        y="conversao_pct",
                        size="qtd_faturadas" if "qtd_faturadas" in df.columns else None,
                        color="elegivel_margem" if "elegivel_margem" in df.columns else None,
                        hover_name="nome",
                        title="Interações x Conversão (bolha = NFs)",
                    )
                    fig.update_layout(height=340)
                    st.plotly_chart(fig, use_container_width=True, key="ins_perf_inter_conv_scatter")
        except Exception as e:
            st.caption(f"Gráficos indisponíveis: {e}")

    if st.button("✨ Gerar insights", use_container_width=True):
        try:
            with st.spinner("Gerando insights (com fallback automático)..."):
                insights, prov_used, model_used = json_from_text(settings=settings, provider=provider, prompt=prompt)
            st.session_state["insights"] = {"data": insights, "provider": prov_used, "model": model_used}
        except Exception as e:
            st.error("Não consegui gerar insights com IA.")
            st.caption(str(e))
            st.info(
                "Verifique se você configurou `GOOGLE_API_KEY` e/ou `OPENAI_API_KEY`. "
                "No modo **Auto**, o ideal é ter as duas para garantir fallback."
            )

    ins = st.session_state.get("insights")
    if isinstance(ins, dict) and isinstance(ins.get("data"), dict):
        st.caption(f"Gerado por **{ins.get('provider')}** (`{ins.get('model')}`).")
        _render_insights_moderno(ins["data"])
        st.markdown("### Priorização (automática)")
        pr = _build_priority_table(df)
        if pr.empty:
            st.caption("Sem dados suficientes para priorização.")
        else:
            st.dataframe(pr, use_container_width=True, hide_index=True)
    else:
        st.info("Clique em **Gerar insights**.")


def _extract_perf_summary_from_payload(periodo: str, payload: dict) -> dict:
    sellers = parse_sellers(payload or {})
    results, total_bonus = calcular_time(sellers) if sellers else ([], 0.0)
    df = pd.DataFrame([r.__dict__ for r in results]) if results else pd.DataFrame()
    df = _enrich_results_df_for_performance(df, sellers)

    tot_fat = float(pd.to_numeric(df.get("faturamento"), errors="coerce").fillna(0).sum()) if not df.empty else 0.0
    tot_nf = float(pd.to_numeric(df.get("qtd_faturadas"), errors="coerce").fillna(0).sum()) if not df.empty else 0.0
    tot_inter = float(pd.to_numeric(df.get("interacoes"), errors="coerce").fillna(0).sum()) if not df.empty else 0.0
    ticket = pd.to_numeric(df.get("ticket_medio"), errors="coerce").dropna() if not df.empty else pd.Series([], dtype=float)
    conv = pd.to_numeric(df.get("conversao_pct"), errors="coerce").dropna() if not df.empty else pd.Series([], dtype=float)
    marg = pd.to_numeric(df.get("margem_pct"), errors="coerce").dropna() if not df.empty else pd.Series([], dtype=float)

    return {
        "periodo": periodo,
        "n_vendedores": int(len(df)) if not df.empty else 0,
        "tot_faturamento": tot_fat,
        "tot_nfs": tot_nf,
        "tot_interacoes": tot_inter,
        "media_ticket": float(ticket.mean()) if len(ticket) else None,
        "media_conversao": float(conv.mean()) if len(conv) else None,
        "media_margem": float(marg.mean()) if len(marg) else None,
        "total_bonus": float(total_bonus),
        "vendedores": df.to_dict(orient="records") if not df.empty else [],
    }


def page_highlights(settings, conn) -> None:
    render_header("Highlight semanal e mensal", "Leitura profunda e gráficos do período e do histórico.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    row = get_analysis(conn, int(analysis_id))
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    base = _extract_perf_summary_from_payload(row.periodo, payload)
    df = pd.DataFrame(base.get("vendedores") or [])

    # Cards do período atual
    st.markdown("### Período atual (análise ativa)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Faturamento (time)", f"R$ {float(base['tot_faturamento']):,.2f}")
    c2.metric("NFs (time)", f"{int(base['tot_nfs'])}")
    c3.metric("Interações (time)", f"{int(base['tot_interacoes'])}")
    c4.metric("Ticket médio (média)", f"R$ {base['media_ticket']:,.2f}" if base.get("media_ticket") else "—")

    try:
        import plotly.express as px

        st.markdown("### Gráficos do período")
        g1, g2 = st.columns(2)
        with g1:
            if not df.empty and "faturamento" in df.columns:
                fig = px.bar(df, x="nome", y="faturamento", title="Faturamento por vendedor")
                fig.update_layout(height=330)
                st.plotly_chart(fig, use_container_width=True, key="hl_current_faturamento_bar")
        with g2:
            if not df.empty and "qtd_faturadas" in df.columns:
                fig = px.bar(df, x="nome", y="qtd_faturadas", title="NFs por vendedor")
                fig.update_layout(height=330)
                st.plotly_chart(fig, use_container_width=True, key="hl_current_nfs_bar")
    except Exception as e:
        st.caption(f"Gráficos indisponíveis: {e}")

    # Histórico (últimas análises) para leitura semanal/mensal
    st.markdown("### Tendência (histórico)")
    rows = list_analyses(conn, limit=24)
    if len(rows) < 2:
        st.info("Salve mais análises para habilitar tendência semanal/mensal.")
        return

    hist: list[dict] = []
    for r in reversed(rows):
        try:
            p = json.loads(r.payload_json)
        except Exception:
            continue
        hist.append(_extract_perf_summary_from_payload(r.periodo, p) | {"id": r.id, "created_at": r.created_at})
    hdf = pd.DataFrame(hist)
    if hdf.empty:
        st.info("Não consegui montar o histórico.")
        return

    # “Semanal” = últimas 4 análises, “Mensal” = últimas 12 análises (aproximação por snapshots salvos)
    last4 = hdf.tail(4)
    last12 = hdf.tail(12)

    tab_w, tab_m, tab_ai = st.tabs(["Highlight semanal (últimas 4)", "Highlight mensal (últimas 12)", "Análise profunda (IA)"])

    def _render_trend(sub: pd.DataFrame, title: str) -> None:
        st.markdown(f"### {title}")
        try:
            import plotly.express as px

            c1, c2 = st.columns(2)
            with c1:
                fig = px.line(sub, x="id", y="tot_faturamento", markers=True, title="Faturamento do time")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_faturamento_{title}")
            with c2:
                fig = px.line(sub, x="id", y="tot_nfs", markers=True, title="NFs do time")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_nfs_{title}")

            c3, c4 = st.columns(2)
            with c3:
                fig = px.line(sub, x="id", y="media_ticket", markers=True, title="Ticket médio (média)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_ticket_{title}")
            with c4:
                fig = px.line(sub, x="id", y="media_conversao", markers=True, title="Conversão (média)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_conversao_{title}")
        except Exception as e:
            st.caption(f"Gráficos indisponíveis: {e}")

        st.dataframe(
            sub[
                [
                    "id",
                    "created_at",
                    "periodo",
                    "tot_faturamento",
                    "tot_nfs",
                    "tot_interacoes",
                    "media_ticket",
                    "media_conversao",
                    "media_margem",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    with tab_w:
        _render_trend(last4, "Últimas 4 análises (aprox. semanal)")
    with tab_m:
        _render_trend(last12, "Últimas 12 análises (aprox. mensal)")

    with tab_ai:
        provider: Provider = st.selectbox(
            "Provedor de IA (para highlight)",
            options=["auto", "gemini", "openai"],
            format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
            key="hl_provider",
        )
        dados_json = json.dumps(
            {
                "atual": base,
                "historico": hdf.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        )
        prompt = PROMPT_HIGHLIGHTS.format(dados_json=dados_json)
        if st.button("🧠 Gerar análise profunda", use_container_width=True):
            try:
                with st.spinner("Gerando análise profunda..."):
                    resp, prov_used, model_used = json_from_text(settings=settings, provider=provider, prompt=prompt)
                st.session_state["hl_text"] = {"t": str(resp.get("texto") or "").strip(), "p": prov_used, "m": model_used}
            except Exception as e:
                st.error("Não consegui gerar análise profunda com IA.")
                st.caption(str(e))

        t = st.session_state.get("hl_text")
        if isinstance(t, dict) and t.get("t"):
            st.caption(f"Gerado por **{t.get('p')}** (`{t.get('m')}`).")
            st.text_area("Highlight (texto)", value=str(t.get("t")), height=360)
        else:
            st.info("Clique em **Gerar análise profunda**.")


def main() -> None:
    st.set_page_config(page_title="Dashboard Performance", page_icon="📊", layout="wide")
    # Perfil de layout (Mobile / Tablet / Desktop)
    ui_profile = st.session_state.get("ui_profile") or "desktop"
    inject_styles(profile=str(ui_profile))

    settings, conn = _ensure_db()
    _maybe_login(settings)
    try:
        purge_excluded_sellers_from_all_analyses(conn)
    except Exception:
        pass

    with st.sidebar:
        st.markdown("### 🖥️ Layout / Dispositivo")
        prof = st.selectbox(
            "Perfil",
            options=["desktop", "tablet", "mobile"],
            format_func=lambda x: {"desktop": "Notebook / PC", "tablet": "iPad / Tablet", "mobile": "Smartphone"}[x],
            key="ui_profile",
        )
        st.caption("Dica: altere o perfil e recarregue para aplicar melhor o espaçamento.")
        st.markdown("---")
        st.markdown("### 📌 Sessão")
        aid = st.session_state.get("active_analysis_id")
        if aid is not None:
            st.success(f"Análise ativa: **#{aid}**")
        else:
            st.caption("Nenhuma análise ativa — use Upload ou Histórico.")
        if st.button("Sair da sessão", use_container_width=True):
            st.session_state.pop("auth_ok", None)
            st.session_state.pop("active_analysis_id", None)
            st.rerun()
        st.markdown("---")
        st.markdown("### 🔑 APIs de IA")
        st.write("Gemini:", "✅" if settings.google_api_key else "❌")
        st.write("OpenAI:", "✅" if settings.openai_api_key else "❌")
        st.markdown("---")
        cal = st.session_state.get("calendar_info")
        if isinstance(cal, dict) and cal.get("ano"):
            st.markdown("### 📅 Calendário (mês atual na home)")
            mes_v = int(cal.get("mes") or 1)
            st.metric("Dias úteis no mês", f"{cal.get('dias_uteis_total', '—')}")
            st.metric("Úteis restantes", f"{cal.get('dias_uteis_restantes', '—')}")
            st.caption(f"Referência: {cal.get('ano')}/{mes_v:02d}")
        st.markdown("---")
        st.markdown("### 🕘 Histórico rápido")
        rows = list_analyses(conn, limit=10)
        if rows:
            options = {f"#{r.id} · {r.periodo}": r.id for r in rows}
            pick = st.selectbox("Carregar análise", options=list(options.keys()))
            if st.button("📌 Tornar ativa", use_container_width=True):
                st.session_state["active_analysis_id"] = int(options[pick])
                st.rerun()
        else:
            st.caption("Nenhuma análise salva ainda.")
        st.markdown("---")
        with st.expander("ℹ️ Navegação", expanded=False):
            st.caption(
                "Use as **abas** no painel principal. Fluxo típico: Nova análise → "
                "revisar dados → salvar → abrir **Dashboard** ou **Histórico**."
            )

    # Aba inicial: calendário (dias úteis automáticos)
    with st.expander("📅 Calendário (dias úteis automáticos)", expanded=True):
        import datetime as _dt

        today = _dt.date.today()
        col1, col2, col3 = st.columns(3)
        with col1:
            ano = st.number_input("Ano", min_value=2020, max_value=2100, value=int(today.year))
        with col2:
            mes = st.number_input("Mês", min_value=1, max_value=12, value=int(today.month))
        with col3:
            uf = st.text_input("UF (opcional, ex: CE/SP)", value="").strip().upper() or None

        info = compute_calendar_info(ano=int(ano), mes=int(mes), subdiv=uf)
        st.session_state["calendar_info"] = {
            "ano": info.ano,
            "mes": info.mes,
            "hoje": info.hoje.isoformat(),
            "dias_uteis_total": info.dias_uteis_total,
            "dias_uteis_trabalhados": info.dias_uteis_trabalhados,
            "dias_uteis_restantes": info.dias_uteis_restantes,
        }

        c1, c2, c3 = st.columns(3)
        c1.metric("Dias úteis (mês)", info.dias_uteis_total)
        c2.metric("Dias úteis trabalhados", info.dias_uteis_trabalhados)
        c3.metric("Dias úteis restantes", info.dias_uteis_restantes)

        st.caption("Regras: não considera sábado/domingo e tenta excluir feriados. Para feriados estaduais, informe a UF.")

    st.markdown("### Selecione o Dashboard:")
    dash = st.radio(
        "",
        options=["Dashboard de Bônus", "Dashboard de Performance"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if dash == "Dashboard de Bônus":
        tabs = st.tabs(["1. Nova Análise", "2. Dashboard", "3. Evolução", "4. Edição Manual", "5. Análise com IA", "6. Histórico"])
        with tabs[0]:
            page_upload(settings, conn)
        with tabs[1]:
            page_dashboard(settings, conn)
        with tabs[2]:
            page_evolution(settings, conn)
        with tabs[3]:
            page_edit(settings, conn)
        with tabs[4]:
            page_insights(settings, conn)
        with tabs[5]:
            page_history(settings, conn)
    else:
        tabs = st.tabs(["Visão Geral", "Indicadores", "Highlights (Semanal/Mensal)", "Simulação/Projeção", "Feedback STAR", "Análise com IA", "Histórico"])
        with tabs[0]:
            page_performance(settings, conn, key_prefix="perf_overview")
        with tabs[1]:
            page_performance(settings, conn, key_prefix="perf_indicadores")
        with tabs[2]:
            page_highlights(settings, conn)
        with tabs[3]:
            page_projection(settings, conn)
        with tabs[4]:
            page_star(settings, conn)
        with tabs[5]:
            page_insights(settings, conn)
        with tabs[6]:
            page_history(settings, conn)


if __name__ == "__main__":
    main()

