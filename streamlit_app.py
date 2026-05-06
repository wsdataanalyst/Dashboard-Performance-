from __future__ import annotations

import html
import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.bonus import META_PRAZO_MEDIO_DIAS, calcular_time
from src.app.config import load_settings
from src.app.domain import parse_sellers, refresh_payload_totais_from_vendedores
from src.app.auth import hash_password, new_invite_code, verify_password
from src.app.security import sha256_hex
from src.app.storage import (
    backfill_owner_user_id,
    connect,
    create_invite,
    create_user_from_invite,
    delete_analysis,
    get_analysis,
    get_last_feedback_for_seller,
    get_latest_base_analysis_id,
    get_user_by_username,
    backup_database_to_bytes,
    count_all_analyses,
    resolve_data_dir,
    ensure_admin_user,
    init_db,
    list_analyses,
    list_invites,
    list_feedbacks,
    list_uploads,
    purge_excluded_sellers_from_all_analyses,
    save_analysis,
    save_feedback,
    save_upload_file,
    get_upload_blob_bytes,
    update_analysis_periodo,
)
from src.app.theme import inject_styles, render_header
from src.app.ai.router import Provider, extract_json_from_images, json_from_text
from src.app.feedback_star import (
    STAR_GESTOR_PADRAO,
    StarInput,
    append_secao_simulacao_capacidade_venda,
    build_prompt_star,
    render_pdf_star,
)
from src.app.excel_import import import_5_files_to_payload
from src.app.dept_import import import_departamentos
from src.app.kpi_import import import_faturamento_atendidos_daily_df, import_faturamento_atendidos_xlsx
from src.app.ocr_fallback import extract_payload_from_prints_ocr
from src.app.projection import projetar_resultados
from src.app.calendar_utils import compute_calendar_info
from src.app.budget_import import is_orcamento_workbook, parse_orcamentos, resolve_orcamentos_pend_fin_bytes


def _parse_iso_dt(s: object):
    try:
        from datetime import datetime

        txt = str(s or "").strip()
        if not txt:
            return None
        txt = txt.replace("Z", "+00:00")
        return datetime.fromisoformat(txt)
    except Exception:
        return None


def _fmt_created_at_local(created_at: object) -> str:
    """Formata timestamps para Fortaleza (UTC-03) como dd/mm/aaaa HH:MM."""
    from datetime import timedelta, timezone

    dt = _parse_iso_dt(created_at)
    if dt is None:
        return "—"
    tz_fortaleza = timezone(timedelta(hours=-3))
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=tz_fortaleza)
    else:
        dt = dt.astimezone(tz_fortaleza)
    return dt.strftime("%d/%m/%Y %H:%M")


def _norm_person_name(s: object) -> str:
    import re
    import unicodedata

    txt = str(s or "").strip().lower()
    # remove sufixos tipo "(2)" comuns em exports
    txt = txt.replace("_", " ")
    txt = re.sub(r"\(\s*\d+\s*\)", "", txt).strip()
    # remove sujeiras comuns que podem aparecer junto do nome
    txt = re.sub(r"r\$\s*[\d\.,]+", "", txt, flags=re.IGNORECASE).strip()
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-z0-9\\s]+", " ", txt)
    txt = re.sub(r"\\s{2,}", " ", txt).strip()
    return txt


def _quick_adjust_find_vendedores(raw_v: list, nome_editor: object) -> list[dict]:
    """
    Associa a linha do editor a entrada(s) em payload['vendedores'].
    Tenta nome exato (strip); se não achar, usa o mesmo critério de normalização do acúmulo (_norm_person_name).
    Atualiza todas as entradas que colidem (ex.: duplicata de nome no payload).
    """
    exact = str(nome_editor or "").strip()
    matches: list[dict] = []
    for item in raw_v:
        if isinstance(item, dict) and str(item.get("nome") or "").strip() == exact:
            matches.append(item)
    if matches:
        return matches
    want = _norm_person_name(exact)
    if not want:
        return []
    for item in raw_v:
        if not isinstance(item, dict):
            continue
        if _norm_person_name(item.get("nome")) == want:
            matches.append(item)
    return matches


def _as_float(x: object) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def _as_int(x: object) -> int | None:
    try:
        if x is None:
            return None
        v = int(float(x))
        return v
    except Exception:
        return None


def _accumulate_payload(base_payload: dict, delta_payload: dict) -> dict:
    """
    Soma campos numéricos (dia -> acumulado) preservando percentuais/metas.
    Usado quando o usuário envia apenas o resultado de um dia (ex.: 27/04) e quer
    somar no acumulado já existente.
    """
    out = dict(base_payload or {})
    base_v = out.get("vendedores") if isinstance(out.get("vendedores"), list) else []
    delta_v = delta_payload.get("vendedores") if isinstance(delta_payload.get("vendedores"), list) else []

    by_name: dict[str, dict] = {}
    for it in base_v:
        if isinstance(it, dict) and it.get("nome"):
            by_name[_norm_person_name(it.get("nome"))] = dict(it)

    numeric_sum_fields = {
        "faturamento",
        "qtd_faturadas",
        "chamadas",
        "iniciados",
        "recebidos",
        "finalizados",
        "desconto_valor",
        "qtd_desconto",
    }
    numeric_max_fields = {
        "meta_faturamento",
        "alcance_pct",
        "alcance_projetado_pct",
        "margem_pct",
        "prazo_medio",
        "tme_minutos",
        "desconto_pct",
        "qtd_desconto_pct",
        "interacoes",
    }

    for it in delta_v:
        if not isinstance(it, dict):
            continue
        nm = _norm_person_name(it.get("nome"))
        if not nm:
            continue
        cur = by_name.get(nm) or {"nome": it.get("nome")}
        # soma (acumulável)
        for k in numeric_sum_fields:
            dv = _as_float(it.get(k))
            if dv is None:
                continue
            cv = _as_float(cur.get(k)) or 0.0
            cur[k] = float(cv) + float(dv)
        # preserva/atualiza por "melhor esforço" (não soma %)
        for k in numeric_max_fields:
            if it.get(k) is None:
                continue
            # meta_faturamento: manter a maior
            if k in {"meta_faturamento"}:
                cv = _as_float(cur.get(k))
                dv = _as_float(it.get(k))
                if dv is not None and (cv is None or dv >= cv):
                    cur[k] = dv
                continue
            # demais: se não existe, preenche; senão mantém o existente (evita trocar % acumulada por % do dia)
            if cur.get(k) is None:
                cur[k] = it.get(k)
        by_name[nm] = cur

    out["vendedores"] = list(by_name.values())

    # Totais do time: soma faturamento_total e preserva meta_total (meta não é diária).
    bt = out.get("totais") if isinstance(out.get("totais"), dict) else {}
    dt = delta_payload.get("totais") if isinstance(delta_payload.get("totais"), dict) else {}
    bt = dict(bt) if isinstance(bt, dict) else {}
    fat_b = _as_float(bt.get("faturamento_total")) or 0.0
    fat_d = _as_float(dt.get("faturamento_total")) or 0.0
    if fat_d:
        bt["faturamento_total"] = float(fat_b) + float(fat_d)
    # meta_total: se delta tiver meta_total e base não, usa; se base já tem, preserva
    if bt.get("meta_total") in (None, 0, 0.0):
        mt = _as_float(dt.get("meta_total"))
        if mt is not None and mt > 0:
            bt["meta_total"] = float(mt)
    if bt:
        out["totais"] = bt
    return out


def _text_has_date_token(txt: object) -> bool:
    """Detecta datas comuns no campo Período (dd/mm, dd/mm/aa, dd/mm/aaaa, yyyy-mm-dd)."""
    import re

    s = str(txt or "").strip()
    if not s:
        return False
    if re.search(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)", s):
        return True
    if re.search(r"(?<!\d)\d{2}/\d{2}(?:/\d{2,4})?(?!\d)", s):
        return True
    return False


def _extract_ref_date_iso_from_periodo(txt: object) -> str | None:
    """
    Extrai uma data ISO (YYYY-MM-DD) do texto do período.
    Aceita: dd/mm, dd/mm/aa, dd/mm/aaaa, yyyy-mm-dd.
    Para dd/mm sem ano, assume o ano atual (Fortaleza).
    """
    import re

    s = str(txt or "").strip()
    if not s:
        return None

    m_iso = re.search(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)", s)
    if m_iso:
        yy, mm, dd = int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{yy:04d}-{mm:02d}-{dd:02d}"
        return None
    m_br = re.search(r"(?<!\d)(\d{2})/(\d{2})(?:/(\d{2}|\d{4}))?(?!\d)", s)
    if not m_br:
        return None
    dd, mm = int(m_br.group(1)), int(m_br.group(2))
    yy_raw = m_br.group(3)
    if yy_raw is None:
        try:
            from datetime import datetime, timedelta, timezone

            tz = timezone(timedelta(hours=-3))
            yy = int(datetime.now(tz).year)
        except Exception:
            return None
    else:
        yy = int(yy_raw)
        if yy < 100:
            yy = 2000 + yy
    if not (1 <= mm <= 12 and 1 <= dd <= 31):
        return None
    return f"{yy:04d}-{mm:02d}-{dd:02d}"


def _latest_saved_perf_date_key_iso(conn: object, *, owner_user_id: int | None, include_all: bool) -> str | None:
    """
    Retorna a maior data ISO (YYYY-MM-DD) entre análises de performance já salvas,
    considerando a regra do app: apenas payloads sem `_kind` e com vendedores.
    """
    try:
        rows = list_analyses(conn, limit=1200, owner_user_id=owner_user_id, include_all=include_all)
    except Exception:
        return None
    best: str | None = None
    for rr in rows:
        try:
            p = json.loads(getattr(rr, "payload_json", "") or "")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        if str(p.get("_kind") or ""):
            continue
        if not parse_sellers(p):
            continue
        try:
            dk, _ = _extract_date_label_from_periodo(
                str(getattr(rr, "periodo", "") or ""),
                str(getattr(rr, "created_at", "") or ""),
            )
        except Exception:
            dk = "0000-00-00"
        if not dk or dk == "0000-00-00":
            continue
        if best is None or str(dk) > str(best):
            best = str(dk)
    return best


def _perf_analysis_rows_chronological(conn: object, *, owner_user_id: int | None, include_all: bool, limit: int = 500) -> list[object]:
    """
    Retorna análises de performance (payload sem `_kind` e com vendedores) em ordem cronológica
    pela data extraída do `periodo` (dd/mm/aaaa). Empata por id.
    """
    try:
        rows = list_analyses(conn, limit=int(limit), owner_user_id=owner_user_id, include_all=include_all)
    except Exception:
        return []

    tmp: list[tuple[str, int, object]] = []
    for r in rows:
        try:
            p = json.loads(getattr(r, "payload_json", "") or "")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        if str(p.get("_kind") or ""):
            continue
        if not parse_sellers(p):
            continue
        try:
            dk, _ = _extract_date_label_from_periodo(str(getattr(r, "periodo", "") or ""), str(getattr(r, "created_at", "") or ""))
        except Exception:
            dk = "0000-00-00"
        if not dk or dk == "0000-00-00":
            continue
        tmp.append((str(dk), int(getattr(r, "id", 0) or 0), r))
    tmp.sort(key=lambda x: (x[0], x[1]))
    return [x[2] for x in tmp]


def _iso_to_br(iso_yyyy_mm_dd: str) -> str:
    try:
        yy, mm, dd = str(iso_yyyy_mm_dd).split("-")
        return f"{dd}/{mm}/{yy}"
    except Exception:
        return str(iso_yyyy_mm_dd)


def _iso_to_br(iso_yyyy_mm_dd: str) -> str:
    try:
        yy, mm, dd = iso_yyyy_mm_dd.split("-")
        return f"{dd}/{mm}/{yy}"
    except Exception:
        return str(iso_yyyy_mm_dd)


def _pdf_safe_text(s: object) -> str:
    """
    FPDF (Helvetica) não suporta unicode completo. Normaliza para ASCII/latin-1 seguro.
    Também evita caracteres que quebram layout.
    """
    if s is None:
        return ""
    out = str(s)
    out = out.replace("—", "-").replace("•", "-")
    out = out.replace("▲", "^").replace("▼", "v").replace("→", ">")
    out = out.replace("\u00A0", " ")
    try:
        out = out.encode("latin-1", errors="ignore").decode("latin-1")
    except Exception:
        pass
    return out


def _break_long_words(s: str, max_len: int = 60) -> str:
    """Evita erro do FPDF quando existe 'palavra' maior que a largura útil."""
    import re

    def _chunk(m: re.Match) -> str:
        w = m.group(0)
        return " ".join(w[i : i + max_len] for i in range(0, len(w), max_len))

    return re.sub(r"\S{" + str(max_len + 1) + r",}", _chunk, s)


def _build_text_pdf_bytes(*, title: str, text: str) -> bytes:
    """PDF simples só com texto (para análises de IA)."""
    from fpdf import FPDF

    pdf = FPDF(format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    epw = float(pdf.w - pdf.l_margin - pdf.r_margin)

    # Título
    pdf.set_x(pdf.l_margin)
    pdf.set_font("Helvetica", style="B", size=13)
    pdf.multi_cell(epw, 7, _break_long_words(_pdf_safe_text(title)))
    pdf.ln(1)

    pdf.set_font("Helvetica", size=11)
    for line in (text or "").splitlines():
        if not line.strip():
            pdf.ln(2)
            continue
        safe = _break_long_words(_pdf_safe_text(line))
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(epw, 6, safe)

    return bytes(pdf.output(dest="S"))  # type: ignore[arg-type]

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

    # Insight curto para o título (ocupa o "vazio" ao lado do período)
    try:
        def _cnt_false(col: str) -> int:
            if col not in src.columns:
                return 0
            s = src[col]
            return int((s == False).sum())  # noqa: E712

        def _cnt_true(col: str) -> int:
            if col not in src.columns:
                return 0
            s = src[col]
            return int((s == True).sum())  # noqa: E712

        def _fmt_ind(name: str, ok: int) -> str:
            if not n:
                return f"{name}: —"
            pct = round((ok / n) * 100.0)
            return f"{name}: {ok}/{n} ({pct}%)"

        # Top bônus
        top_nome = None
        top_bonus = None
        if "bonus_total" in src.columns:
            b = pd.to_numeric(src["bonus_total"], errors="coerce")
            if b.notna().any():
                i = int(b.idxmax())
                top_nome = str(src.loc[i].get("nome") or "").strip() or None
                top_bonus = float(b.loc[i]) if pd.notna(b.loc[i]) else None

        # Média e total por vendedor
        avg_bonus = (float(total) / float(n)) if n else None

        # Quem está "quase lá": faltando 1 indicador para somar bônus (entre prazo/conversao/tme/interacao + margem elegível)
        near = 0
        try:
            if n and all(c in src.columns for c in ["elegivel_margem", "bateu_prazo", "bateu_conversao", "bateu_tme", "bateu_interacao"]):
                def _ok(v):
                    return bool(v) is True
                for _, rr in src.iterrows():
                    okm = bool(rr.get("elegivel_margem")) is True
                    okp = _ok(rr.get("bateu_prazo"))
                    okc = _ok(rr.get("bateu_conversao"))
                    okt = _ok(rr.get("bateu_tme"))
                    oki = _ok(rr.get("bateu_interacao"))
                    oks = [okm, okp, okc, okt, oki]
                    if sum(1 for x in oks if x) == 4:
                        near += 1
        except Exception:
            near = 0

        # Gargalos (top 2)
        misses = {
            "margem/alcance": int((~src["elegivel_margem"]).sum()) if "elegivel_margem" in src.columns else 0,
            "prazo": _cnt_false("bateu_prazo"),
            "conversão": _cnt_false("bateu_conversao"),
            "TME": _cnt_false("bateu_tme"),
            "interações": _cnt_false("bateu_interacao"),
        }
        top2 = sorted(misses.items(), key=lambda kv: kv[1], reverse=True)[:2]
        garg = " e ".join([f"{k} ({v})" for k, v in top2 if v is not None]) if top2 else "—"

        # Entrega por indicador (quantos bateram / %)
        ok_marg = int(src["elegivel_margem"].sum()) if "elegivel_margem" in src.columns else 0
        ok_prazo = _cnt_true("bateu_prazo")
        ok_conv = _cnt_true("bateu_conversao")
        ok_tme = _cnt_true("bateu_tme")
        ok_inter = _cnt_true("bateu_interacao")
        entrega = " | ".join(
            [
                _fmt_ind("Margem/Alc", ok_marg),
                _fmt_ind("Prazo", ok_prazo),
                _fmt_ind("Conv", ok_conv),
                _fmt_ind("TME", ok_tme),
                _fmt_ind("Inter", ok_inter),
            ]
        )

        parts = []
        parts.append(entrega)
        if top_nome and top_bonus is not None:
            parts.append(f"Top: {top_nome} (R$ {top_bonus:,.0f})")
        if avg_bonus is not None:
            parts.append(f"Média: R$ {avg_bonus:,.0f}/vendedor")
        if near:
            parts.append(f"Quase lá: {near} a 1 indicador do bônus")
        parts.append(f"Gargalos: {garg}")

        insight = " • ".join([p for p in parts if p])
    except Exception:
        insight = "Insight: —"
    insight_esc = html.escape(insight)

    return f"""
<div class="bonus-panel-wrap">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <h2 class="bonus-panel-title" style="margin:0;">Central de Vendas | Resultados de Bônus — {periodo_esc}</h2>
    <div class="dp-pill" style="max-width: 980px; white-space: normal; line-height: 1.35; font-size:0.95rem; padding:10px 12px;">
      {insight_esc}
    </div>
  </div>
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


def render_bonus_sdr_panel_html(
    *,
    periodo: str,
    nome: str,
    cargo: str,
    indicadores: list[dict[str, object]],
    total_sdr: float,
    margin_top_px: int = 18,
) -> str:
    """
    Painel no mesmo estilo da Central de Bônus, para critérios SDR (metas do time + campo manual).
    Cada item de `indicadores`: indicador, origem, entrega, meta, ok (bool|None), bonus (float).
    """
    periodo_esc = html.escape(str(periodo or "Período"))
    nome_esc = html.escape(str(nome or ""))
    cargo_esc = html.escape(str(cargo or ""))
    rows_html: list[str] = []
    for it in indicadores:
        ind = html.escape(str(it.get("indicador", "")))
        orig = html.escape(str(it.get("origem", "")))
        ent = html.escape(str(it.get("entrega", "—")))
        meta = html.escape(str(it.get("meta", "")))
        ok = it.get("ok")
        bonus = float(it.get("bonus") or 0.0)
        rows_html.append(
            "<tr>"
            f'<td class="bonus-vendedor">{ind}</td>'
            f'<td>{orig}</td>'
            f'<td class="bonus-cell-num">{ent}</td>'
            f'<td class="bonus-cell-num">{meta}</td>'
            f"<td>{_icon_meta(ok)}</td>"
            f'<td class="bonus-col-bonus">R$ {bonus:,.2f}</td>'
            "</tr>"
        )
    rows_joined = "\n".join(rows_html)
    mt = max(0, int(margin_top_px))
    return f"""
<div class="bonus-panel-wrap" style="margin-top: {mt}px;">
  <h2 class="bonus-panel-title" style="margin:0 0 6px 0;">Bônus SDR — {nome_esc}</h2>
  <p class="bonus-panel-note" style="margin-top:0;">{cargo_esc} · Período: {periodo_esc}</p>
  <p class="bonus-panel-note">
    Conversão, TME e margem usam a <strong>média do time</strong> desta análise (mesma base do dashboard).
    <strong>Participação em vendas</strong> é informada manualmente (não existe KPI automático).
    Regras: conversão time ≥ 17% (R$ 150) · TME médio ≤ 5 min (R$ 150) · participação ≥ 20% (R$ 100) · margem média ≥ 26% (R$ 150).
  </p>
  <div class="bonus-metric-grid">
    <div class="bonus-metric-card">
      <div class="bonus-metric-label">Bônus SDR ({nome_esc})</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value">R$ {total_sdr:,.2f}</span>
        <span class="bonus-metric-arrow" aria-hidden="true">↑</span>
      </div>
      <div class="bonus-metric-sub">Soma das faixas atendidas na tabela abaixo.</div>
    </div>
  </div>
  <div class="bonus-table-wrap">
    <table class="bonus-table">
      <thead>
        <tr>
          <th>Indicador</th>
          <th>Origem do dado</th>
          <th>Entrega</th>
          <th>Meta</th>
          <th>Bateu?</th>
          <th class="bonus-col-bonus">Bônus (R$)</th>
        </tr>
      </thead>
      <tbody>
        {rows_joined}
      </tbody>
    </table>
  </div>
  <p class="bonus-legend">✅ = somou bônus &nbsp;|&nbsp; ❌ = não somou &nbsp;|&nbsp; ◽ = sem dado</p>
</div>
"""


def render_bonus_consolidated_footer_html(*, total_vendedores: float, total_sdr: float) -> str:
    """Faixa final: subtotais + total consolidado (vendedores + SDR)."""
    g = float(total_vendedores) + float(total_sdr)
    return f"""
<div class="bonus-panel-wrap" style="margin-top: 14px;">
  <div class="bonus-metric-grid">
    <div class="bonus-metric-card">
      <div class="bonus-metric-label">Subtotal — vendedores (Central)</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value">R$ {total_vendedores:,.2f}</span>
      </div>
    </div>
    <div class="bonus-metric-card">
      <div class="bonus-metric-label">Subtotal — SDR</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value">R$ {total_sdr:,.2f}</span>
      </div>
    </div>
    <div class="bonus-metric-card" style="border-color: rgba(110,231,183,.35);">
      <div class="bonus-metric-label">Total consolidado (painel)</div>
      <div class="bonus-metric-value-row">
        <span class="bonus-metric-value" style="color:#6EE7B7;">R$ {g:,.2f}</span>
        <span class="bonus-metric-arrow" aria-hidden="true">↑</span>
      </div>
      <div class="bonus-metric-sub">Vendedores + Mayara Barros (SDR).</div>
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
    df["desconto_pct"] = df["nome"].apply(lambda n: getattr(raw_map.get(n), "desconto_pct", None))
    df["qtd_desconto_pct"] = df["nome"].apply(lambda n: getattr(raw_map.get(n), "qtd_desconto_pct", None))
    # Alcance real: SEMPRE prioriza (Faturamento / Meta) * 100 quando houver ambos
    # (regra pedida: bater com a planilha "Alcance e Margem")
    df["alcance_real_pct"] = None
    df["qtd_faturadas"] = df["qtd_faturadas"] if "qtd_faturadas" in df.columns else None
    df["ticket_medio"] = df.apply(
        lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"]))
        if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0)
        else None,
        axis=1,
    )
    # calcula alcance real quando possível; fallback usa o valor do payload
    try:
        fat = pd.to_numeric(df.get("faturamento"), errors="coerce")
        meta = pd.to_numeric(df.get("meta_faturamento"), errors="coerce")
        mask_ok = fat.notna() & meta.notna() & (meta > 0)
        df.loc[mask_ok, "alcance_real_pct"] = (fat[mask_ok] / meta[mask_ok]) * 100.0

        # fallback para quem não tem meta/faturamento (mantém o campo vindo do payload)
        mask_missing = df["alcance_real_pct"].isna()
        if mask_missing.any():
            df.loc[mask_missing, "alcance_real_pct"] = df.loc[mask_missing, "nome"].apply(
                lambda n: getattr(raw_map.get(n), "alcance_pct", None)
            )
    except Exception:
        pass
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
    conn = connect(settings.db_path, settings.database_url)
    init_db(conn)
    # Bootstrap: garante admin no DB e atribui ownership às análises antigas
    admin_user = (settings.admin_username or "").strip()
    admin_pass = (settings.admin_password or "").strip()
    if not admin_user or not admin_pass:
        raise RuntimeError(
            "Configuração obrigatória ausente: defina ADMIN_USERNAME e ADMIN_PASSWORD "
            "via Streamlit Secrets (.streamlit/secrets.toml) ou .env antes de usar o app."
        )
    admin_id = ensure_admin_user(
        conn,
        username=admin_user,
        password_hash=hash_password(admin_pass),
        name="Administrador",
    )
    backfill_owner_user_id(conn, admin_user_id=admin_id)
    return settings, conn


def _maybe_login(settings) -> None:
    if isinstance(st.session_state.get("user"), dict) and st.session_state["user"].get("id"):
        return
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
        "Acesso restrito — entre com seu usuário ou crie conta via convite.",
        right="Multiusuário",
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
        tab_login, tab_signup = st.tabs(["Entrar", "Criar conta (convite)"])

        with tab_login:
            u = st.text_input("Usuário", placeholder="ex.: gerson", key="login_user")
            p = st.text_input("Senha", type="password", key="login_pass")
            if st.button("Entrar", use_container_width=True, key="btn_login"):
                try:
                    _, conn = _ensure_db()
                except Exception as e:
                    st.error(str(e))
                    st.stop()
                rec = get_user_by_username(conn, (u or "").strip())
                if not rec or int(rec.get("active") or 0) != 1:
                    st.error("Usuário inválido ou inativo.")
                else:
                    if verify_password((p or ""), str(rec.get("password_hash") or "")):
                        st.session_state["user"] = {
                            "id": int(rec["id"]),
                            "username": str(rec["username"]),
                            "name": str(rec["name"]),
                            "role": str(rec["role"]),
                        }
                        st.rerun()
                    else:
                        st.error("Usuário ou senha inválidos.")

        with tab_signup:
            code = st.text_input("Convite", placeholder="cole o código", key="signup_invite")
            u2 = st.text_input("Usuário", placeholder="ex.: yago.silva", key="signup_user")
            name2 = st.text_input("Nome", placeholder="Nome para exibição", key="signup_name")
            p1 = st.text_input("Senha", type="password", key="signup_pass1")
            p2 = st.text_input("Confirmar senha", type="password", key="signup_pass2")
            if st.button("Criar conta", use_container_width=True, key="btn_signup"):
                if not code.strip() or not u2.strip() or not name2.strip():
                    st.error("Preencha convite, usuário e nome.")
                elif p1 != p2 or len(p1 or "") < 6:
                    st.error("Senha inválida (mínimo 6 caracteres) ou confirmação não confere.")
                else:
                    try:
                        _, conn = _ensure_db()
                    except Exception as e:
                        st.error(str(e))
                        st.stop()
                    try:
                        uid, role = create_user_from_invite(
                            conn,
                            invite_code=code.strip(),
                            username=u2.strip(),
                            name=name2.strip(),
                            password_hash=hash_password(p1 or ""),
                        )
                    except Exception as e:
                        st.error(str(e))
                    else:
                        st.session_state["user"] = {
                            "id": int(uid),
                            "username": u2.strip(),
                            "name": name2.strip(),
                            "role": str(role),
                        }
                        st.success("Conta criada. Entrando…")
                        st.rerun()
    st.stop()


def _uploads_dir(settings) -> Path:
    data_dir = resolve_data_dir(
        db_path=settings.db_path,
        database_url=settings.database_url,
        data_dir=settings.data_dir,
    )
    p = data_dir / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _load_upload_bytes(settings, conn, upload_row: dict) -> bytes | None:
    # 1) tenta do banco (Postgres ou SQLite com blob)
    try:
        uid = int(upload_row.get("id") or 0)
        if uid:
            b = get_upload_blob_bytes(conn, uid)
            if b:
                return b
    except Exception:
        pass
    # 2) fallback disco
    rel = str(upload_row.get("rel_path") or "")
    if not rel:
        return None
    base = Path(settings.data_dir)
    p = base / Path(rel)
    try:
        return p.read_bytes()
    except Exception:
        return None


def page_upload(settings, conn, *, embedded: bool = False) -> None:
    if not embedded:
        render_header(
            "Upload e extração",
            "Envie prints (ou Excel) → extrai JSON → você revisa → salva no histórico.",
            right="Fallback Gemini ↔ OpenAI",
        )
    else:
        st.markdown(
            "<div class='dp-card' style='padding:12px 14px;margin: 6px 0 10px 0;'>"
            "<div style='display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;'>"
            "<div style='color:#E5E7EB;font-weight:900'>+ Nova análise</div>"
            "<div style='color:#94A3B8;font-size:.86rem'>Upload dos arquivos (até 9) → validar → salvar no histórico</div>"
            "</div></div>",
            unsafe_allow_html=True,
        )

    def default_provider_index() -> int:
        # Preferir Auto quando as 2 chaves existem; senão, cair para a que existir.
        if settings.google_api_key and settings.openai_api_key:
            return 0  # auto
        if settings.google_api_key:
            return 1  # gemini
        return 2  # openai (ou última opção)

    # IA/OCR é opcional — fica recolhido para não ocupar a tela
    provider: Provider = "auto"
    images: list[tuple[str, bytes, str | None]] = []
    run_ia = False
    run_ocr = False
    ocr_debug = False
    use_manual = False
    with st.expander("Extração com IA/OCR (opcional) — expandir/minimizar", expanded=False):
        provider = st.selectbox(
            "Provedor de IA",
            options=["auto", "gemini", "openai"],
            format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
            index=default_provider_index(),
            key="upload_provider",
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

        b1i, b2i, b3i = st.columns([1, 1, 1])
        with b1i:
            run_ia = st.button("🤖 Extrair com IA", use_container_width=True, disabled=not images)
        with b2i:
            use_manual = st.button("✍️ Usar JSON manual", use_container_width=True)
        with b3i:
            run_ocr = st.button("🧾 Extrair sem IA (OCR)", use_container_width=True, disabled=not images)
        ocr_debug = st.toggle("Debug OCR (mostrar diagnóstico)", value=False, disabled=not images)

    st.markdown("---")
    # `periodo` é usado por fluxos de import (Excel/OCR/IA), então precisa existir antes.
    left, right = st.columns([1, 1])
    with left:
        periodo = st.text_input("Período", value="")
    with right:
        st.caption("Dica: algo como `Abril/2026` ou `Abril (até 15/04)`.")

    # Controle explícito de acúmulo diário (aplica no momento de salvar).
    has_day_in_periodo = _text_has_date_token(periodo)
    active_id_for_acc = st.session_state.get("active_analysis_id")
    if has_day_in_periodo and active_id_for_acc is not None:
        st.checkbox(
            "Somar este período no acumulado da análise ativa ao salvar (delta do dia)",
            value=False,
            key="upload_do_accumulate_on_save",
            help="Marque quando você está importando apenas o resultado do dia (ex.: 27/04/2026) e quer somar no acumulado já existente no histórico.",
        )

    st.markdown("### 📄 Importar Excel (mais confiável que OCR)")
    excel_files = st.file_uploader(
        "Envie os **6 arquivos de performance** e, se quiser, **+2 de orçamentos** (pendentes e finalizados) **no mesmo lote** — até **8** arquivos. "
        "Aceita .xlsx / .xls (incluindo export HTML). O app reconhece: Performance (prints 1–5), **Departamentos** e **Orçamentos** (por colunas ou por nomes com *pendente* / *finalizado*).",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        key="excel_upload",
    )
    if excel_files:
        if st.button("📥 Importar arquivos (Excel/HTML + Orçamentos)", use_container_width=True):
            try:
                with st.spinner("Importando arquivos..."):
                    files_bytes = [(f.name, f.read()) for f in excel_files]
                    # 1) Classifica arquivos para não misturar "Departamentos" com "Vendedores"
                    perf_files: list[tuple[str, bytes]] = []
                    dept_files: list[tuple[str, bytes]] = []
                    orc_files: list[tuple[str, bytes]] = []

                    for fname, b in files_bytes:
                        # tenta identificar "Departamentos" (por conteúdo)
                        try:
                            dpt1 = import_departamentos([(fname, b)])
                            dept_rows = (dpt1.payload or {}).get("departamentos") if isinstance(dpt1.payload, dict) else None
                            if isinstance(dept_rows, list) and len(dept_rows) > 0:
                                dept_files.append((fname, b))
                                continue  # não entra no import de performance
                        except Exception:
                            pass

                        # Orçamentos (pendentes + finalizados): não passa pelo import dos prints
                        if is_orcamento_workbook(b, file_name=fname):
                            orc_files.append((fname, b))
                            continue

                        perf_files.append((fname, b))

                    # 2) Importa Performance SOMENTE com arquivos de vendedores (prints 1–5)
                    res = import_5_files_to_payload(perf_files)

                    # 2.1) Se o usuário estiver importando apenas o resultado de um dia (ex.: 27/04/2026),
                    # some no acumulado da análise ativa para atualizar KPIs e médias.
                    try:
                        import re as _re

                        ptxt = str(periodo or "").strip()
                        has_day = bool(_re.search(r"(?<!\\d)\\d{2}/\\d{2}/\\d{4}(?!\\d)", ptxt))
                        user = st.session_state.get("user") or {}
                        owner_id = int(user.get("id") or 0) or None
                        is_admin = str(user.get("role") or "").lower() == "admin"
                        active_id = st.session_state.get("active_analysis_id")
                        if has_day and active_id is not None and isinstance(res.payload, dict):
                            # Controle explícito: quando o período tem data, é comum importar "resultado do dia" para somar
                            # no acumulado já salvo. Deixa o usuário decidir (default: somar).
                            do_acc = st.checkbox(
                                "Somar este upload no acumulado da análise ativa (delta do dia)",
                                value=False,
                                help="Use quando você está importando apenas o resultado de um dia (ex.: 27/04/2026) e quer somar no acumulado já existente.",
                                key="upload_do_accumulate",
                            )
                            base_row = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
                            if base_row:
                                try:
                                    base_payload = json.loads(base_row.payload_json)
                                except Exception:
                                    base_payload = None
                                if isinstance(base_payload, dict):
                                    # Diagnóstico (antes)
                                    bt0 = base_payload.get("totais") if isinstance(base_payload.get("totais"), dict) else {}
                                    dt0 = res.payload.get("totais") if isinstance(res.payload.get("totais"), dict) else {}
                                    base_f0 = _as_float(bt0.get("faturamento_total"))
                                    delta_f0 = _as_float(dt0.get("faturamento_total"))

                                    if do_acc:
                                        res.payload = _accumulate_payload(base_payload, res.payload)

                                    # Diagnóstico (depois)
                                    rt0 = res.payload.get("totais") if isinstance(res.payload.get("totais"), dict) else {}
                                    res_f0 = _as_float(rt0.get("faturamento_total"))
                                    st.session_state["_upload_acc_diag"] = {
                                        "aplicou": bool(do_acc),
                                        "active_id": int(active_id),
                                        "periodo": ptxt,
                                        "base_faturamento_total": base_f0,
                                        "delta_faturamento_total": delta_f0,
                                        "resultado_faturamento_total": res_f0,
                                    }
                    except Exception:
                        pass

                    # 3) Cache: departamentos (apenas arquivos classificados como dept)
                    if dept_files:
                        try:
                            dpt = import_departamentos(dept_files)
                            dept_rows2 = (dpt.payload or {}).get("departamentos") if isinstance(dpt.payload, dict) else None
                            if isinstance(dept_rows2, list) and len(dept_rows2) > 0:
                                st.session_state["dept_payload"] = dpt.payload
                                st.session_state["dept_meta"] = dpt.meta
                                st.session_state["dept_source_names"] = [n for (n, _) in dept_files]
                            if dpt.warnings:
                                st.session_state["dept_warnings"] = dpt.warnings
                        except Exception:
                            pass
                    else:
                        # diagnóstico: nenhum arquivo foi classificado como "Departamentos"
                        st.session_state["dept_warnings"] = [
                            "Não reconheci nenhum arquivo como base de Departamentos neste lote. "
                            "Confirme se o export contém colunas como Departamento/Categoria/Grupo + Meta/Faturamento/Participação/Alcance/Margem."
                        ]

                    st.session_state["upload_files_cache"] = {n: b for (n, b) in files_bytes}
                if periodo and isinstance(res.payload, dict):
                    res.payload["periodo"] = periodo
                st.session_state["payload"] = res.payload
                st.session_state["extraction_meta"] = res.meta
                # Avisos consolidados
                combined_warnings: list[str] = []
                orc_saved_msg: str | None = None
                try:
                    combined_warnings.extend(list(res.warnings or []))
                except Exception:
                    pass

                if len(orc_files) == 1:
                    combined_warnings.append(
                        "Há **1** planilha de orçamentos neste lote; para gravar conversão são necessárias **duas** (pendentes e finalizados)."
                    )
                elif len(orc_files) > 2:
                    combined_warnings.append(
                        f"Foram reconhecidas **{len(orc_files)}** planilhas de orçamentos; envie no máximo **2** (pendentes e finalizados) no mesmo lote. "
                        "Orçamentos **não foram gravados** — ajuste os arquivos e importe de novo."
                    )
                elif len(orc_files) == 2:
                    try:
                        pend_b, fin_b = resolve_orcamentos_pend_fin_bytes(orc_files)
                        parsed = parse_orcamentos(pend_b, fin_b)
                        payload_o = {
                            "_kind": "orcamentos",
                            "pendentes": {
                                "rows": parsed.pendentes_df.fillna("").to_dict(orient="records"),
                            },
                            "finalizados": {
                                "rows": parsed.finalizados_df.fillna("").to_dict(orient="records"),
                            },
                            "meta": parsed.meta,
                        }
                        import datetime as _dt

                        periodo_orc = _dt.date.today().strftime("%d/%m/%Y") + " - Orçamentos"
                        user = st.session_state.get("user") or {}
                        owner_id = int(user.get("id") or 0) or None
                        analysis_id = save_analysis(
                            conn,
                            periodo=periodo_orc,
                            provider_used="orcamentos",
                            model_used="pandas",
                            parent_analysis_id=None,
                            owner_user_id=owner_id,
                            payload=payload_o,
                            total_bonus=0.0,
                        )
                        st.session_state["active_orcamentos_analysis_id"] = int(analysis_id)
                        orc_saved_msg = f"Orçamentos gravados no histórico como análise **#{analysis_id}**."
                    except Exception as e_orc:
                        combined_warnings.append(f"Orçamentos não gravados: {e_orc}")

                if combined_warnings:
                    st.warning("Importação concluída com avisos.")
                    for w in combined_warnings:
                        st.caption(w)
                    if orc_saved_msg:
                        st.success(orc_saved_msg)
                else:
                    if orc_saved_msg:
                        st.success("Importação concluída. " + orc_saved_msg)
                    else:
                        st.success("Importação concluída.")

                # Diagnóstico do acúmulo (quando aplicável)
                diag = st.session_state.get("_upload_acc_diag")
                if isinstance(diag, dict) and diag.get("periodo"):
                    try:
                        st.markdown("### Diagnóstico do acúmulo (importação)")
                        st.caption(
                            f"Período: **{diag.get('periodo')}** · análise ativa: **#{diag.get('active_id')}** · acumulou: **{('SIM' if diag.get('aplicou') else 'NÃO')}**"
                        )
                        bf = diag.get("base_faturamento_total")
                        df0 = diag.get("delta_faturamento_total")
                        rf = diag.get("resultado_faturamento_total")
                        st.caption(
                            f"Faturamento total — base: **{('R$ ' + format(float(bf), ',.2f')) if bf is not None else '—'}** · "
                            f"delta: **{('R$ ' + format(float(df0), ',.2f')) if df0 is not None else '—'}** · "
                            f"resultado: **{('R$ ' + format(float(rf), ',.2f')) if rf is not None else '—'}**"
                        )
                    except Exception:
                        pass
            except Exception as e:
                st.error("Falha ao importar Excel/HTML.")
                st.caption(str(e))

    b1, b2, b3 = st.columns([1, 1, 1])
    with b3:
        clear = st.button("🧹 Limpar dados", use_container_width=True)

    if clear:
        st.session_state.pop("payload", None)
        st.session_state.pop("extraction_meta", None)
        st.session_state.pop("insights", None)
        st.session_state.pop("sg_daily_df", None)
        st.session_state.pop("sg_daily_meta", None)
        st.session_state.pop("sg_daily_source_name", None)
        st.session_state.pop("sg_daily_scope_id", None)
        st.session_state.pop("dept_payload", None)
        st.session_state.pop("dept_meta", None)
        st.session_state.pop("dept_source_names", None)
        st.session_state.pop("dept_warnings", None)
        st.session_state.pop("upload_files_cache", None)
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
        st.warning(
            "**Atenção — ainda não foi salva:** a prévia abaixo fica **só nesta sessão** do navegador "
            "até você clicar em **Salvar análise**. Sem isso, ao **reiniciar o Streamlit** "
            "essa extração some (o que está no banco, no Histórico, continua).",
        )

        sellers = parse_sellers(payload)
        results, total = calcular_time(sellers) if sellers else ([], 0.0)

        if sellers:
            df_prev = pd.DataFrame([r.__dict__ for r in results])
            # tenta enriquecer validação com faturamento/meta/ticket quando existirem no payload
            raw_map = {s.nome: s for s in sellers}
            df_prev["chamadas"] = df_prev["nome"].apply(lambda n: raw_map.get(n).chamadas if raw_map.get(n) else None)
            df_prev["faturamento"] = df_prev["nome"].apply(lambda n: raw_map.get(n).faturamento if raw_map.get(n) else None)
            df_prev["meta_faturamento"] = df_prev["nome"].apply(lambda n: raw_map.get(n).meta_faturamento if raw_map.get(n) else None)
            # Campo manual (não vem do cálculo de bônus): permite preencher por consultor e consolidar no total
            df_prev["clientes_atendidos"] = df_prev["nome"].apply(
                lambda n: (payload.get("vendedores") or [])
            )
            try:
                # busca do payload original por vendedor
                raw_v0 = payload.get("vendedores") if isinstance(payload.get("vendedores"), list) else []
                vmap0 = {str(v.get("nome") or "").strip(): v for v in raw_v0 if isinstance(v, dict) and v.get("nome")}
                df_prev["clientes_atendidos"] = df_prev["nome"].apply(lambda n: vmap0.get(str(n).strip(), {}).get("clientes_atendidos"))
            except Exception:
                pass
            df_prev["ticket_medio"] = df_prev.apply(
                lambda r: (float(r["faturamento"]) / float(r["qtd_faturadas"])) if (pd.notna(r.get("faturamento")) and (r.get("qtd_faturadas") or 0) > 0) else None,
                axis=1,
            )
            st.markdown("### Ajuste rápido (edite aqui antes de salvar)")
            st.caption("Edite os valores que estiverem errados e clique em **Aplicar ajustes na prévia**.")

            cols_edit = [
                "nome",
                "clientes_atendidos",
                "alcance_pct",
                "margem_pct",
                "prazo_medio",
                "conversao_pct",
                "tme_minutos",
                "interacoes",
                "chamadas",
                "qtd_faturadas",
                "faturamento",
                "meta_faturamento",
            ]
            cols_edit = [c for c in cols_edit if c in df_prev.columns]
            df_edit_base = df_prev[cols_edit].copy()

            edited = st.data_editor(
                df_edit_base,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="preview_editor_vendedores",
                column_config={
                    "nome": st.column_config.TextColumn(
                        "Vendedor",
                        disabled=True,
                        help="Nome vindo do import. Ajuste os números na mesma linha; não renomeie aqui.",
                    ),
                },
            )

            def _to_num(v: object) -> float | None:
                if v is None:
                    return None
                try:
                    if isinstance(v, str):
                        s = v.strip().replace("R$", "").replace("%", "").strip()
                        s = s.replace(".", "").replace(",", ".") if s.count(",") == 1 and s.count(".") >= 1 else s.replace(",", ".")
                        if s == "":
                            return None
                        x = float(s)
                        return None if pd.isna(x) else x
                    x = float(v)  # type: ignore[arg-type]
                    return None if pd.isna(x) else x
                except Exception:
                    return None

            if st.button("✅ Aplicar ajustes na prévia", use_container_width=True, key="btn_apply_preview_edits"):
                try:
                    # Atualiza payload["vendedores"] com o que foi editado
                    raw_v = payload.get("vendedores") if isinstance(payload.get("vendedores"), list) else []

                    edited_rows = edited.to_dict(orient="records") if hasattr(edited, "to_dict") else []
                    for r in edited_rows:
                        nome = str(r.get("nome") or "").strip()
                        if not nome:
                            continue
                        targets = _quick_adjust_find_vendedores(raw_v, nome)
                        if not targets:
                            tgt = {"nome": nome}
                            raw_v.append(tgt)
                            targets = [tgt]

                        # campos numéricos (mantém None quando vazio)
                        for tgt in targets:
                            for k in [
                                "clientes_atendidos",
                                "alcance_pct",
                                "margem_pct",
                                "prazo_medio",
                                "conversao_pct",
                                "tme_minutos",
                                "interacoes",
                                "chamadas",
                                "qtd_faturadas",
                                "faturamento",
                                "meta_faturamento",
                            ]:
                                if k not in r:
                                    continue
                                nv = _to_num(r.get(k))
                                if nv is None:
                                    # não apaga campos existentes se usuário deixou vazio sem querer
                                    # (só seta None se o campo não existia)
                                    if k not in tgt:
                                        tgt[k] = None
                                    continue
                                # ints para campos naturalmente inteiros
                                if k in {"prazo_medio", "interacoes", "chamadas", "qtd_faturadas", "clientes_atendidos"}:
                                    tgt[k] = int(round(float(nv)))
                                else:
                                    tgt[k] = float(nv)
                                # Bônus e elegibilidade usam alcance_projetado_pct; o editor expõe alcance_pct (snapshot do resultado).
                                if k == "alcance_pct":
                                    tgt["alcance_projetado_pct"] = float(nv)

                    payload["vendedores"] = raw_v
                    try:
                        refresh_payload_totais_from_vendedores(payload)
                    except Exception:
                        pass
                    # Consolidação do total (time)
                    try:
                        tot = payload.get("totais") if isinstance(payload.get("totais"), dict) else {}
                        tot = dict(tot) if isinstance(tot, dict) else {}
                        cli_sum = 0
                        for it in raw_v:
                            if not isinstance(it, dict):
                                continue
                            v = it.get("clientes_atendidos")
                            if v is None:
                                continue
                            try:
                                cli_sum += int(float(v))
                            except Exception:
                                continue
                        tot["clientes_atendidos_total"] = int(cli_sum)
                        payload["totais"] = tot
                    except Exception:
                        pass
                    st.session_state["payload"] = payload
                    st.success("Ajustes aplicados na prévia. Os cálculos e o bônus serão recalculados automaticamente.")
                    st.rerun()
                except Exception as e:
                    st.error("Não consegui aplicar os ajustes.")
                    st.caption(str(e))

            # Colunas somente leitura para revisão
            st.markdown("### Resultado recalculado (somente leitura)")
            st.dataframe(
                df_prev[
                    [
                        "nome",
                        "bonus_total",
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
            user = st.session_state.get("user") or {}
            owner_id = int(user.get("id") or 0) or None
            # Vincular bases auxiliares (Sala de Gestão) à análise salva:
            # evita ficar dependente do "cache da sessão" ao trocar a análise ativa.
            payload_to_save = dict(payload)
            # Por padrão, cada análise salva é um "snapshot" do dia (não acumulado).
            # O modo acumulativo (ledger/delta do dia) só roda se o usuário marcar explicitamente.
            ref_date = _extract_ref_date_iso_from_periodo(periodo_final)
            is_admin = str(user.get("role") or "").lower() == "admin"

            # Regra obrigatória: análises históricas só funcionam com data correta no Período.
            if not ref_date:
                st.error("Para salvar a análise no histórico, o campo **Período** deve conter uma data no formato **dd/mm/aaaa** (ex.: 02/05/2026).")
                st.stop()

            # Regra obrigatória: sequência crescente (dia atual deve ser maior que o último salvo).
            try:
                last_saved = _latest_saved_perf_date_key_iso(conn, owner_user_id=owner_id, include_all=is_admin)
            except Exception:
                last_saved = None
            if last_saved and str(ref_date) <= str(last_saved):
                st.error(
                    f"Data fora de sequência. Última análise de performance salva está em **{_iso_to_br(str(last_saved))}**. "
                    f"Você tentou salvar **{_iso_to_br(str(ref_date))}**. Ajuste o **Período** para o próximo dia correto."
                )
                st.stop()

            do_ledger_acc = bool(st.session_state.get("upload_do_accumulate_on_save"))
            if ref_date and do_ledger_acc:
                try:
                    # 1) Salvar/atualizar DELTA do dia (idempotente por data)
                    rows_scan = list_analyses(conn, limit=2000, owner_user_id=owner_id, include_all=is_admin)
                    for rr in rows_scan:
                        try:
                            p0 = json.loads(getattr(rr, "payload_json", "") or "")
                        except Exception:
                            continue
                        if not isinstance(p0, dict):
                            continue
                        if str(p0.get("_kind") or "") != "daily_delta":
                            continue
                        if str(p0.get("ref_date") or "") != str(ref_date):
                            continue
                        # remove delta antigo do mesmo dia (substituição)
                        try:
                            delete_analysis(conn, int(getattr(rr, "id", 0) or 0), owner_user_id=owner_id, include_all=is_admin)
                        except Exception:
                            pass

                    delta_payload = dict(payload_to_save)
                    delta_payload["_kind"] = "daily_delta"
                    delta_payload["ref_date"] = str(ref_date)
                    delta_payload["periodo"] = str(periodo_final or _iso_to_br(ref_date))

                    delta_id = save_analysis(
                        conn,
                        periodo=str(periodo_final or _iso_to_br(ref_date)),
                        provider_used=str(meta.get("provider", "unknown")),
                        model_used=str(meta.get("model", "unknown")),
                        parent_analysis_id=None,
                        owner_user_id=owner_id,
                        payload=delta_payload,
                        total_bonus=0.0,
                    )

                    # 2) Recalcular ACUMULADO até ref_date somando deltas <= ref_date
                    rows_all = list_analyses(conn, limit=4000, owner_user_id=owner_id, include_all=is_admin)
                    deltas: list[tuple[str, dict]] = []
                    for rr in rows_all:
                        try:
                            p0 = json.loads(getattr(rr, "payload_json", "") or "")
                        except Exception:
                            continue
                        if not isinstance(p0, dict):
                            continue
                        if str(p0.get("_kind") or "") != "daily_delta":
                            continue
                        rk = str(p0.get("ref_date") or "")
                        if not rk or rk > str(ref_date):
                            continue
                        deltas.append((rk, p0))
                    deltas.sort(key=lambda x: x[0])

                    acc: dict = {"vendedores": [], "totais": {}}
                    for _, dp in deltas:
                        acc = _accumulate_payload(acc, dp)
                    # Garante KPIs consistentes mesmo se deltas não trouxerem `totais`:
                    # recalcula `totais.faturamento_total` e `totais.meta_total` a partir dos vendedores.
                    try:
                        refresh_payload_totais_from_vendedores(acc)
                    except Exception:
                        pass
                    acc["periodo"] = f"Acumulado até {_iso_to_br(ref_date)}"
                    acc["_ledger"] = {"through": str(ref_date), "n_deltas": int(len(deltas))}

                    analysis_id = save_analysis(
                        conn,
                        periodo=str(acc.get("periodo") or f"Acumulado até {_iso_to_br(ref_date)}"),
                        provider_used="ledger",
                        model_used="ledger",
                        parent_analysis_id=int(delta_id),
                        owner_user_id=owner_id,
                        payload=acc,
                        total_bonus=0.0,
                    )

                    st.success(f"Delta do dia salvo (**#{delta_id}**) e acumulado materializado (**#{analysis_id}**).")
                    st.session_state["active_analysis_id"] = analysis_id
                    st.session_state["show_upload"] = False
                    st.rerun()
                except Exception as e:
                    st.error("Não consegui salvar no modo acumulativo (ledger).")
                    st.caption(str(e))
                    return
            try:
                daily_df = st.session_state.get("sg_daily_df")
                daily_meta = st.session_state.get("sg_daily_meta") if isinstance(st.session_state.get("sg_daily_meta"), dict) else None
                if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
                    cols = [c for c in ["dia", "faturamento", "nfs_emitidas", "clientes_atendidos"] if c in daily_df.columns]
                    rows_daily = daily_df[cols].copy().to_dict(orient="records") if cols else daily_df.to_dict(orient="records")
                    payload_to_save["_sg_daily"] = {
                        "rows": rows_daily,
                        "meta": daily_meta or {},
                        "source": st.session_state.get("sg_daily_source_name") or st.session_state.get("sg_kpi_source_name"),
                    }
            except Exception:
                pass
            # Departamentos (Sala de Gestão) — salva junto da análise ativa
            try:
                dp = st.session_state.get("dept_payload")
                if isinstance(dp, dict) and isinstance(dp.get("departamentos"), list) and dp.get("departamentos"):
                    payload_to_save["_sg_dept"] = {
                        "departamentos": dp.get("departamentos"),
                        "meta": st.session_state.get("dept_meta") if isinstance(st.session_state.get("dept_meta"), dict) else {},
                        "source": st.session_state.get("dept_source_name"),
                    }
            except Exception:
                pass
            analysis_id = save_analysis(
                conn,
                periodo=periodo_final,
                provider_used=str(meta.get("provider", "unknown")),
                model_used=str(meta.get("model", "unknown")),
                parent_analysis_id=None,
                owner_user_id=owner_id,
                payload=payload_to_save,
                total_bonus=float(total),
            )

            # Persistir uploads para auditoria (se houver)
            up_dir = _uploads_dir(settings) / str(owner_id or "anon") / str(analysis_id)
            up_dir.mkdir(parents=True, exist_ok=True)
            for n, b, ctype in images:
                digest = sha256_hex(b)
                safe_name = "".join(ch for ch in n if ch.isalnum() or ch in (" ", "-", "_")).strip().replace(" ", "_")
                filename = f"{safe_name}_{digest[:10]}.png"
                rel_path = str(Path("uploads") / str(owner_id or "anon") / str(analysis_id) / filename)
                # Em Cloud, o disco pode ser efêmero; ainda tentamos salvar localmente por compat,
                # mas o binário também é guardado no banco para migração/persistência.
                try:
                    (up_dir / filename).write_bytes(b)
                except Exception:
                    pass
                save_upload_file(
                    conn,
                    analysis_id=analysis_id,
                    filename=filename,
                    content_type=ctype,
                    sha256=digest,
                    rel_path=rel_path,
                    blob_bytes=b,
                )

            st.success(f"Análise salva com ID **{analysis_id}**.")
            diag2 = st.session_state.get("_save_acc_diag")
            if isinstance(diag2, dict) and diag2.get("aplicou"):
                try:
                    bf = diag2.get("base_faturamento_total")
                    df0 = diag2.get("delta_faturamento_total")
                    rf = diag2.get("resultado_faturamento_total")
                    st.caption(
                        f"Acúmulo aplicado ao salvar — base: {('R$ ' + format(float(bf), ',.2f')) if bf is not None else '—'} · "
                        f"delta: {('R$ ' + format(float(df0), ',.2f')) if df0 is not None else '—'} · "
                        f"resultado: {('R$ ' + format(float(rf), ',.2f')) if rf is not None else '—'}"
                    )
                except Exception:
                    pass
            st.session_state["active_analysis_id"] = analysis_id
            # Se o upload estiver embutido no topo, fecha o expander para voltar à visão
            st.session_state["show_upload"] = False
            st.rerun()


def page_dashboard(settings, conn) -> None:
    render_header("Dashboard", "Visualize a análise ativa (ou carregue do histórico).")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Vá em **Upload e extração** ou carregue no **Histórico**.")
        return

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
    if not row:
        st.warning("Análise ativa não encontrada (talvez tenha sido apagada).")
        return

    payload = json.loads(row.payload_json)
    sellers = parse_sellers(payload)
    results, total = calcular_time(sellers) if sellers else ([], 0.0)

    top = results[0].nome if results else "—"
    totais = payload.get("totais") if isinstance(payload, dict) else {}
    if not isinstance(totais, dict):
        totais = {}
    fat_total = float(totais.get("faturamento_total") or 0.0) if totais.get("faturamento_total") is not None else None
    meta_total = float(totais.get("meta_total") or 0.0) if totais.get("meta_total") is not None else None
    perc_meta = (fat_total / meta_total * 100.0) if (fat_total is not None and meta_total and meta_total > 0) else None

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

    # Cards do Dashboard de Bônus (foco em métricas operacionais)
    stats = _team_stats(df)
    tot_inter = int(pd.to_numeric(df.get("interacoes"), errors="coerce").fillna(0).sum()) if "interacoes" in df.columns else 0

    import html as _html

    def _parse_dt(s: object):
        try:
            from datetime import datetime

            txt = str(s or "")
            if not txt:
                return None
            txt = txt.replace("Z", "+00:00")
            return datetime.fromisoformat(txt)
        except Exception:
            return None

    def _get_prev_perf_payload() -> dict | None:
        rows = list_analyses(conn, limit=200, owner_user_id=owner_id, include_all=is_admin)
        if not rows:
            return None
        cur_id = int(getattr(row, "id", 0) or 0)
        cur_dt = _parse_dt(getattr(row, "created_at", None))
        try:
            cur_date_key, _ = _extract_date_label_from_periodo(
                str(getattr(row, "periodo", "") or ""),
                str(getattr(row, "created_at", "") or ""),
            )
        except Exception:
            cur_date_key = "0000-00-00"

        # Preferir "dia anterior" baseado na data presente no `periodo` (dd/mm/aaaa),
        # para não depender de UTC/created_at quando o usuário carrega uma base de um dia específico.
        if cur_date_key and cur_date_key != "0000-00-00":
            best = None
            best_key = None
            best_dt = None
            for rr in rows:
                rid = int(getattr(rr, "id", 0) or 0)
                if rid == cur_id:
                    continue
                try:
                    p = json.loads(getattr(rr, "payload_json", "") or "")
                except Exception:
                    continue
                if not isinstance(p, dict):
                    continue
                kind = str(p.get("_kind") or "")
                if kind.startswith("sala_gestao_"):
                    continue
                if not parse_sellers(p):
                    continue
                try:
                    rk, _ = _extract_date_label_from_periodo(
                        str(getattr(rr, "periodo", "") or ""),
                        str(getattr(rr, "created_at", "") or ""),
                    )
                except Exception:
                    rk = "0000-00-00"
                if not rk or rk == "0000-00-00" or rk >= cur_date_key:
                    continue
                rdt = _parse_dt(getattr(rr, "created_at", None))
                if best_key is None or rk > best_key:
                    best, best_key, best_dt = p, rk, rdt
                elif rk == best_key:
                    # desempate: maior created_at (ou maior id se não parsear)
                    if best_dt is None and rdt is not None:
                        best, best_dt = p, rdt
                    elif best_dt is not None and rdt is not None and rdt > best_dt:
                        best, best_dt = p, rdt
                    elif best_dt is None and rdt is None:
                        bid = int(getattr(best, "id", 0) or 0) if best is not None else 0
                        if rid > bid:
                            best = p
            if best is not None:
                return best

        best = None
        best_dt = None
        for rr in rows:
            rid = int(getattr(rr, "id", 0) or 0)
            if rid == cur_id:
                continue
            try:
                p = json.loads(getattr(rr, "payload_json", "") or "")
            except Exception:
                continue
            if not isinstance(p, dict):
                continue
            kind = str(p.get("_kind") or "")
            if kind.startswith("sala_gestao_"):
                continue
            if not parse_sellers(p):
                continue
            rdt = _parse_dt(getattr(rr, "created_at", None))
            if cur_dt is not None and rdt is not None and rdt >= cur_dt:
                continue
            if best_dt is None or (rdt is not None and rdt > best_dt):
                best = p
                best_dt = rdt
        return best

    prev_stats = None
    prev_payload = None
    try:
        prev_payload = _get_prev_perf_payload()
        if isinstance(prev_payload, dict):
            prev_sellers = parse_sellers(prev_payload)
            prev_results, _ = calcular_time(prev_sellers) if prev_sellers else ([], 0.0)
            if prev_results:
                prev_df = pd.DataFrame([r.__dict__ for r in prev_results])
                prev_stats = _team_stats(prev_df)
                prev_tot_inter = int(pd.to_numeric(prev_df.get("interacoes"), errors="coerce").fillna(0).sum()) if "interacoes" in prev_df.columns else 0
                prev_stats["total_interacoes"] = float(prev_tot_inter)
    except Exception:
        prev_stats = None
        prev_payload = None

    # "Dia anterior" (delta) = diferença entre a análise ativa e a análise anterior salva.
    # Aqui segue o conceito de snapshot acumulado do mês (05/05 - 04/05).
    def _clients_total_from_payload(p: dict) -> int | None:
        try:
            t = p.get("totais") if isinstance(p, dict) else None
            if isinstance(t, dict) and t.get("clientes_atendidos_total") is not None:
                return int(float(t.get("clientes_atendidos_total") or 0))
        except Exception:
            pass
        try:
            vs = p.get("vendedores") if isinstance(p, dict) else None
            if not isinstance(vs, list):
                return None
            s = 0
            any_v = False
            for it in vs:
                if not isinstance(it, dict):
                    continue
                v = it.get("clientes_atendidos")
                if v is None:
                    continue
                try:
                    s += int(float(v))
                    any_v = True
                except Exception:
                    continue
            return int(s) if any_v else None
        except Exception:
            return None

    cur_nf_total = int(pd.to_numeric(df.get("qtd_faturadas"), errors="coerce").fillna(0).sum()) if "qtd_faturadas" in df.columns else 0
    prev_nf_total = None
    if isinstance(prev_payload, dict):
        try:
            ps = parse_sellers(prev_payload)
            pr, _ = calcular_time(ps) if ps else ([], 0.0)
            if pr:
                p_df = pd.DataFrame([r.__dict__ for r in pr])
                prev_nf_total = int(pd.to_numeric(p_df.get("qtd_faturadas"), errors="coerce").fillna(0).sum()) if "qtd_faturadas" in p_df.columns else 0
        except Exception:
            prev_nf_total = None

    cur_cli_total = _clients_total_from_payload(payload)
    prev_cli_total = _clients_total_from_payload(prev_payload) if isinstance(prev_payload, dict) else None

    d_fat_total = None
    d_nf_total = None
    d_cli_total = None
    try:
        if isinstance(fat_total, (int, float)) and isinstance(prev_payload, dict):
            pt = prev_payload.get("totais") if isinstance(prev_payload.get("totais"), dict) else {}
            prev_fat_total = float(pt.get("faturamento_total") or 0.0) if pt.get("faturamento_total") is not None else None
            if prev_fat_total is not None:
                d_fat_total = float(fat_total) - float(prev_fat_total)
    except Exception:
        d_fat_total = None
    if prev_nf_total is not None:
        d_nf_total = int(cur_nf_total) - int(prev_nf_total)
    if cur_cli_total is not None and prev_cli_total is not None:
        d_cli_total = int(cur_cli_total) - int(prev_cli_total)

    def _fmt_delta_int(v: int | None) -> str:
        return "—" if v is None else f"{int(v):+d}"

    def _fmt_delta_money(v: float | None) -> str:
        return "—" if v is None else f"R$ {float(v):+,.2f}"

    def _delta_qty_and_pct(cur: object, ref: object, *, digits: int = 1) -> str:
        try:
            c = float(cur)  # type: ignore[arg-type]
            r = float(ref)  # type: ignore[arg-type]
        except Exception:
            return "—"
        if pd.isna(c) or pd.isna(r):
            return "—"
        diff = c - r
        arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
        if abs(r) < 1e-9:
            return f"{arrow} {diff:+.{digits}f}"
        pct = (diff / abs(r)) * 100.0
        return f"{arrow} {diff:+.{digits}f} ({pct:+.1f}%)"

    def _delta_vs_ideal(cur: object, ideal: float, *, direction: str, digits: int = 1) -> str:
        """
        direction:
          - '>=': maior é melhor (ex.: margem, conversão, interações)
          - '<=': menor é melhor (ex.: prazo, tme)
        """
        try:
            c = float(cur)  # type: ignore[arg-type]
        except Exception:
            return "—"
        if pd.isna(c):
            return "—"
        # diff_pos: positivo = bom (acima do ideal para >=, abaixo do ideal para <=)
        diff_pos = (c - ideal) if direction == ">=" else (ideal - c)
        arrow = "▲" if diff_pos > 0 else ("▼" if diff_pos < 0 else "→")
        if abs(ideal) < 1e-9:
            return f"{arrow} {diff_pos:+.{digits}f}"
        pct = (diff_pos / abs(ideal)) * 100.0
        return f"{arrow} {diff_pos:+.{digits}f} ({pct:+.1f}%)"

    def _delta_color(val: str) -> str:
        if val.startswith("▲"):
            return "color:#22c55e;font-weight:800;"
        if val.startswith("▼"):
            return "color:#fb7185;font-weight:800;"
        if val.startswith("→"):
            return "color:#94a3b8;font-weight:650;"
        return "color:#94a3b8;"

    def _kpi_card(title: str, value: str, *, icon: str, accent: str, d_prev: str | None = None, d_ideal: str | None = None) -> None:
        d1 = d_prev or "—"
        d2 = d_ideal or "—"
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:12px 12px;
  min-height: 158px;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
  <div style="margin-top:8px;display:flex;flex-direction:column;gap:6px;">
    <div style="font-size:0.84rem;{_delta_color(d1)}">{_html.escape(d1)} <span style="color:#94a3b8;font-weight:600">(vs anterior)</span></div>
    <div style="font-size:0.84rem;{_delta_color(d2)}">{_html.escape(d2)} <span style="color:#94a3b8;font-weight:600">(vs ideal)</span></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        cur = float(stats.get("media_margem") or 0.0)
        ref = float(prev_stats.get("media_margem") or 0.0) if isinstance(prev_stats, dict) else None
        _kpi_card(
            "% Margem",
            f"{cur:.1f}%",
            icon="📊",
            accent="#A7F3D0",
            d_prev=(_delta_qty_and_pct(cur, ref, digits=1) if ref is not None else "—"),
            d_ideal=_delta_vs_ideal(cur, 26.0, direction=">=", digits=1),
        )
    with c2:
        cur = float(stats.get("media_prazo") or 0.0)
        ref = float(prev_stats.get("media_prazo") or 0.0) if isinstance(prev_stats, dict) else None
        _kpi_card(
            "Prazo médio",
            f"{cur:.0f}",
            icon="⏱",
            accent="#FBBF24",
            d_prev=(_delta_qty_and_pct(cur, ref, digits=0) if ref is not None else "—"),
            d_ideal=_delta_vs_ideal(cur, META_PRAZO_MEDIO_DIAS, direction="<=", digits=0),
        )
    with c3:
        cur = float(stats.get("media_conversao") or 0.0)
        ref = float(prev_stats.get("media_conversao") or 0.0) if isinstance(prev_stats, dict) else None
        _kpi_card(
            "Conversão (%)",
            f"{cur:.1f}%",
            icon="🔁",
            accent="#C4B5FD",
            d_prev=(_delta_qty_and_pct(cur, ref, digits=1) if ref is not None else "—"),
            d_ideal=_delta_vs_ideal(cur, 12.0, direction=">=", digits=1),
        )
    with c4:
        cur = float(stats.get("media_tme") or 0.0)
        ref = float(prev_stats.get("media_tme") or 0.0) if isinstance(prev_stats, dict) else None
        _kpi_card(
            "TME (min)",
            f"{cur:.1f}",
            icon="⏳",
            accent="#6EE7B7",
            d_prev=(_delta_qty_and_pct(cur, ref, digits=1) if ref is not None else "—"),
            d_ideal=_delta_vs_ideal(cur, 5.0, direction="<=", digits=1),
        )
    with c5:
        cur = float(tot_inter)
        ref = float(prev_stats.get("total_interacoes") or 0.0) if isinstance(prev_stats, dict) and prev_stats.get("total_interacoes") is not None else None
        _kpi_card(
            "Interações",
            f"{int(cur):d}",
            icon="☎",
            accent="#93c5fd",
            d_prev=(_delta_qty_and_pct(cur, ref, digits=0) if ref is not None else "—"),
            d_ideal=_delta_vs_ideal(cur, 200.0, direction=">=", digits=0),
        )

    # Delta do acumulado vs análise anterior (conceito de "dia anterior" do seu processo).
    d1, d2, d3 = st.columns(3)
    with d1:
        _kpi_card("Faturamento (dia anterior Δ)", _fmt_delta_money(d_fat_total), icon="💸", accent="#6EE7B7", d_prev=None, d_ideal=None)
    with d2:
        _kpi_card("NFs (dia anterior Δ)", _fmt_delta_int(d_nf_total), icon="📦", accent="#93c5fd", d_prev=None, d_ideal=None)
    with d3:
        _kpi_card("Clientes (dia anterior Δ)", _fmt_delta_int(d_cli_total), icon="👥", accent="#C4B5FD", d_prev=None, d_ideal=None)

    def _render_sdr_mayara_section(*, margin_top_panel: int = 0) -> float:
        """Retorna o total SDR (R$). Painel escuro com mesmo CSS da Central."""
        st.caption(
            "Conversão / TME / Margem = média do **time**. **Participação em vendas** = manual (campo abaixo)."
        )

        def _mean_col_team(dfx: pd.DataFrame, col: str) -> float | None:
            if col not in dfx.columns:
                return None
            s = pd.to_numeric(dfx[col], errors="coerce").dropna()
            if s.empty:
                return None
            return float(s.mean())

        sdr_name = "Mayara Barros"
        sdr_role = "Assistente Comercial SDR — responde por no momento"
        tc = _mean_col_team(df, "conversao_pct")
        tt = _mean_col_team(df, "tme_minutos")
        tm = _mean_col_team(df, "margem_pct")
        part_key = "bonus_sdr_participacao_pct"
        part_default = float(st.session_state.get(part_key, 0.0) or 0.0)
        part_pct = st.number_input(
            "% Participação em vendas (Mayara — preenchimento manual)",
            min_value=0.0,
            max_value=100.0,
            step=0.1,
            format="%.1f",
            value=part_default,
            key=part_key,
            help="Indicador não calculado pela ferramenta; informe o percentual para a meta de 20% (R$ 100).",
        )

        b_conv: bool | None = None if tc is None else bool(tc >= 17.0)
        b_tme: bool | None = None if tt is None else bool(tt <= 5.0)
        b_marg: bool | None = None if tm is None else bool(tm >= 26.0)
        b_part: bool | None = bool(part_pct >= 20.0)

        v_conv = 150.0 if b_conv is True else 0.0
        v_tme = 150.0 if b_tme is True else 0.0
        v_marg = 150.0 if b_marg is True else 0.0
        v_part = 100.0 if b_part else 0.0

        sdr_indicadores: list[dict[str, object]] = [
            {
                "indicador": "Conversão geral (time)",
                "origem": "Média do time (ferramenta)",
                "entrega": f"{tc:.1f}%" if tc is not None else "—",
                "meta": "≥ 17%",
                "ok": b_conv,
                "bonus": v_conv,
            },
            {
                "indicador": "TME (time)",
                "origem": "Média do time (ferramenta)",
                "entrega": f"{tt:.1f} min" if tt is not None else "—",
                "meta": "≤ 5 min",
                "ok": b_tme,
                "bonus": v_tme,
            },
            {
                "indicador": "Participação em vendas",
                "origem": "Manual",
                "entrega": f"{part_pct:.1f}%",
                "meta": "≥ 20%",
                "ok": b_part,
                "bonus": v_part,
            },
            {
                "indicador": "Margem (time)",
                "origem": "Média do time (ferramenta)",
                "entrega": f"{tm:.1f}%" if tm is not None else "—",
                "meta": "≥ 26%",
                "ok": b_marg,
                "bonus": v_marg,
            },
        ]
        sdr_total = float(v_conv + v_tme + v_part + v_marg)
        st.markdown(
            render_bonus_sdr_panel_html(
                periodo=row.periodo,
                nome=sdr_name,
                cargo=sdr_role,
                indicadores=sdr_indicadores,
                total_sdr=sdr_total,
                margin_top_px=margin_top_panel,
            ),
            unsafe_allow_html=True,
        )
        return sdr_total

    tab_resumo, tab_bonus = st.tabs(["Resumo completo", "Central de Vendas | Bônus"])

    with tab_resumo:
        st.caption(
            "Bônus SDR (Mayara) e consolidado: na aba **Central de Vendas | Bônus**, **acima** da tabela do time."
        )
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
            """
<div style="
  padding:14px 16px;
  margin:0 0 14px 0;
  border-radius:16px;
  border:2px solid rgba(110,231,183,.55);
  background:linear-gradient(135deg, rgba(110,231,183,.14), rgba(59,130,246,.08));
  box-shadow:0 0 0 1px rgba(255,255,255,.06) inset;
">
  <div style="font-weight:900;font-size:1.08rem;color:#E5E7EB;letter-spacing:.2px;">
    1º — Bônus SDR · Mayara Barros
  </div>
  <div style="margin-top:6px;color:#CBD5E1;font-size:0.92rem;line-height:1.45;">
    Critérios da assistente comercial <strong>antes</strong> do resultado do time.
    Mesmo visual escuro da Central (cards + tabela). Preencha <strong>% Participação</strong> no campo abaixo.
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
        sdr_total_tab = _render_sdr_mayara_section(margin_top_panel=0)
        st.markdown(
            '<p style="margin:18px 0 10px 0;font-weight:900;font-size:1.12rem;color:#E5E7EB;">2º — Resultado do time · Central de Vendas</p>',
            unsafe_allow_html=True,
        )
        st.caption("Vendedores — mesma Central de sempre (abaixo do bloco SDR).")
        st.markdown(
            render_bonus_central_panel_html(df, periodo=row.periodo, total=float(total)),
            unsafe_allow_html=True,
        )
        st.caption(
            "Detalhamento por coluna de R$ (margem, prazo, etc.) permanece na aba **Resumo completo**."
        )
        st.markdown(
            render_bonus_consolidated_footer_html(total_vendedores=float(total), total_sdr=float(sdr_total_tab)),
            unsafe_allow_html=True,
        )


def page_evolution(settings, conn) -> None:
    render_header("Evolução", "Acompanhe a evolução do bônus ao longo do tempo.")
    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    rows = _perf_analysis_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=600)
    if len(rows) < 2:
        st.info("Você precisa de pelo menos 2 análises salvas para ver a evolução.")
        return

    # Filtra somente análises de performance/bônus (evita misturar Sala de Gestão e registros sem vendedores).
    hist: list[dict] = []
    for r in rows:  # cronológico (antigo -> novo) por data do período
        try:
            p = json.loads(getattr(r, "payload_json", "") or "")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        hist.append(
            {
                "id": int(r.id),
                "created_at": _fmt_created_at_local(getattr(r, "created_at", None)),
                "periodo": str(r.periodo),
                "total_bonus": float(getattr(r, "total_bonus", 0.0) or 0.0),
            }
        )

    df = pd.DataFrame(hist)
    if df.empty or len(df) < 2:
        st.info("Você precisa de pelo menos 2 análises válidas (com vendedores) para ver a evolução.")
        return

    # Eixo X contínuo (evita “buracos” quando há períodos apagados/ignorados).
    # Já está em ordem cronológica; apenas garante ordem estável por id.
    df = df.sort_values(["id"], ascending=True).reset_index(drop=True)
    df["seq"] = list(range(1, len(df) + 1))

    c1, c2 = st.columns(2)
    c1.metric("Análises", f"{len(df)}")
    c2.metric("Último bônus", f"R$ {df.iloc[-1]['total_bonus']:,.2f}")

    try:
        import plotly.express as px

        fig = px.line(
            df,
            x="seq",
            y="total_bonus",
            markers=True,
            title="Evolução do bônus total",
            hover_data={"periodo": True, "created_at": True, "seq": False},
            labels={"seq": "Snapshot", "total_bonus": "Bônus total (R$)"},
        )
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

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
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

    def _discount_summary(sellers_list: list) -> dict[str, object]:
        vals = [getattr(x, "desconto_valor", None) for x in sellers_list]
        qtys = [getattr(x, "qtd_desconto", None) for x in sellers_list]
        pcts = [getattr(x, "desconto_pct", None) for x in sellers_list]
        qpcts = [getattr(x, "qtd_desconto_pct", None) for x in sellers_list]
        dsum = sum(float(v) for v in vals if v is not None and not pd.isna(v))
        qsum = sum(int(v) for v in qtys if v is not None)
        dp = [float(v) for v in pcts if v is not None and not pd.isna(v)]
        qp = [float(v) for v in qpcts if v is not None and not pd.isna(v)]
        return {
            "desconto_valor": dsum if dsum else None,
            "qtd_desconto": qsum if qsum else None,
            "desconto_pct": (sum(dp) / len(dp)) if dp else None,
            "qtd_desconto_pct": (sum(qp) / len(qp)) if qp else None,
        }

    disc = _discount_summary(sellers)

    totais = payload.get("totais") if isinstance(payload, dict) else {}
    if not isinstance(totais, dict):
        totais = {}
    fat_total = float(totais.get("faturamento_total") or 0.0) if totais.get("faturamento_total") is not None else None
    meta_total = float(totais.get("meta_total") or 0.0) if totais.get("meta_total") is not None else None
    perc_meta = (fat_total / meta_total * 100.0) if (fat_total is not None and meta_total and meta_total > 0) else None

    import html as _html

    def _kpi_card(title: str, value: str, *, icon: str, accent: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:12px 12px;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    # KPIs (Visão Geral): inclui projeção de faturamento (time)
    proj_fat_txt = "—"
    if key_prefix == "perf_overview":
        try:
            from src.app.domain import Seller as SellerDC
            from src.app.projection import projetar_resultados

            cal2 = st.session_state.get("calendar_info")
            dt_total = int(st.session_state.get("proj_dias_uteis_total") or (cal2.get("dias_uteis_total") if isinstance(cal2, dict) else 22))
            dt_rest = int(
                st.session_state.get("proj_dias_uteis_restantes")
                if st.session_state.get("proj_dias_uteis_restantes") is not None
                else max(0, int(dt_total) - int(cal2.get("dias_uteis_trabalhados") or 0))
            )
            dt_trab = max(1, int(dt_total) - int(dt_rest))

            qtd_sum = int(sum(int(x.qtd_faturadas or 0) for x in sellers))
            ini_sum = int(sum(int(x.iniciados or 0) for x in sellers))
            rec_sum = int(sum(int(x.recebidos or 0) for x in sellers))
            ch_sum = int(sum(int(x.chamadas or 0) for x in sellers))

            soma = SellerDC(
                nome="Time",
                qtd_faturadas=qtd_sum,
                iniciados=ini_sum,
                recebidos=rec_sum,
                chamadas=ch_sum,
                faturamento=float(fat_total) if fat_total is not None else None,
                meta_faturamento=float(meta_total) if meta_total is not None else None,
            )
            ticket_auto = (float(fat_total) / float(qtd_sum)) if (fat_total is not None and qtd_sum > 0) else None
            meta_eff = float(meta_total) if (meta_total is not None and float(meta_total) > 0) else soma.meta_faturamento
            proj0 = projetar_resultados(
                soma,
                dias_uteis_total=int(dt_total),
                dias_uteis_trabalhados=int(dt_trab),
                meta_faturamento=float(meta_eff) if meta_eff is not None else None,
                ticket_medio_override=float(ticket_auto) if ticket_auto is not None else None,
            )
            if proj0.projecao_faturamento is not None and meta_eff is not None and float(meta_eff) > 0:
                pct_proj = (float(proj0.projecao_faturamento) / float(meta_eff)) * 100.0
                proj_fat_txt = f"R$ {float(proj0.projecao_faturamento):,.2f} ({pct_proj:.1f}%)"
            elif proj0.projecao_faturamento is not None:
                proj_fat_txt = f"R$ {float(proj0.projecao_faturamento):,.2f}"
        except Exception:
            proj_fat_txt = "—"

    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    with r1c1:
        _kpi_card("Faturamento (time)", (f"R$ {fat_total:,.2f}" if fat_total is not None else "—"), icon="💰", accent="#6EE7B7")
    with r1c2:
        _kpi_card("Meta (time)", (f"R$ {meta_total:,.2f}" if meta_total is not None else "—"), icon="🎯", accent="#93c5fd")
    with r1c3:
        _kpi_card("% da meta", (f"{perc_meta:.1f}%" if perc_meta is not None else "—"), icon="📈", accent="#FBBF24")
    with r1c4:
        if key_prefix == "perf_overview":
            _kpi_card("Projeção faturamento", proj_fat_txt, icon="🔮", accent="#FDE68A")
        else:
            _kpi_card("Projeção faturamento", "—", icon="🔮", accent="#FDE68A")

    r2c1, r2c2, r2c3 = st.columns(3)
    with r2c1:
        _kpi_card("Margem média", f"{stats['media_margem']:.1f}%", icon="📊", accent="#A7F3D0")
    with r2c2:
        _kpi_card("Conversão média", f"{stats['media_conversao']:.1f}%", icon="🔁", accent="#C4B5FD")
    with r2c3:
        d_pct = disc.get("desconto_pct")
        pct_txt = f"{float(d_pct):.2f}%" if d_pct is not None and not pd.isna(d_pct) else "—"
        _kpi_card("Desconto", pct_txt, icon="🏷", accent="#93c5fd")

    # "Dia anterior" (delta) = diferença entre a análise ativa e a análise anterior do mês.
    # Importante: aqui "dia anterior" segue o seu conceito de ACUMULADO+DELTA (snapshot do mês).
    def _clients_total_from_payload(p: dict) -> int | None:
        try:
            t = p.get("totais") if isinstance(p, dict) else None
            if isinstance(t, dict) and t.get("clientes_atendidos_total") is not None:
                return int(float(t.get("clientes_atendidos_total") or 0))
        except Exception:
            pass
        try:
            vs = p.get("vendedores") if isinstance(p, dict) else None
            if not isinstance(vs, list):
                return None
            s = 0
            any_v = False
            for it in vs:
                if not isinstance(it, dict):
                    continue
                v = it.get("clientes_atendidos")
                if v is None:
                    continue
                try:
                    s += int(float(v))
                    any_v = True
                except Exception:
                    continue
            return int(s) if any_v else None
        except Exception:
            return None

    def _pick_prev_perf_payload_for_row(current_row) -> dict | None:
        try:
            cur_key, _ = _extract_date_label_from_periodo(str(getattr(current_row, "periodo", "") or ""), str(getattr(current_row, "created_at", "") or ""))
        except Exception:
            cur_key = "0000-00-00"
        if not cur_key or cur_key == "0000-00-00":
            return None

        rows_all = _perf_analysis_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=800)
        # encontra a posição do current_row por id; se falhar, usa date_key
        cur_id = int(getattr(current_row, "id", 0) or 0)
        prev_row = None
        prev_key = None
        for rr in rows_all:
            rid = int(getattr(rr, "id", 0) or 0)
            if rid == cur_id:
                break
            try:
                rk, _ = _extract_date_label_from_periodo(str(getattr(rr, "periodo", "") or ""), str(getattr(rr, "created_at", "") or ""))
            except Exception:
                rk = "0000-00-00"
            if not rk or rk == "0000-00-00" or rk >= cur_key:
                continue
            if prev_key is None or str(rk) > str(prev_key):
                prev_row, prev_key = rr, rk

        if prev_row is None:
            return None
        try:
            p = json.loads(getattr(prev_row, "payload_json", "") or "")
        except Exception:
            return None
        return p if isinstance(p, dict) else None

    prev_payload_for_delta = _pick_prev_perf_payload_for_row(row)
    try:
        cur_sum = _extract_perf_summary_from_payload(str(row.periodo), payload)
        prev_sum = _extract_perf_summary_from_payload(str(getattr(row, "periodo", "") or ""), prev_payload_for_delta) if isinstance(prev_payload_for_delta, dict) else None
    except Exception:
        cur_sum, prev_sum = None, None

    d_fat = None
    d_nf = None
    d_cli = None
    if isinstance(cur_sum, dict) and isinstance(prev_sum, dict):
        try:
            d_fat = float(cur_sum.get("tot_faturamento") or 0.0) - float(prev_sum.get("tot_faturamento") or 0.0)
        except Exception:
            d_fat = None
        try:
            d_nf = int(float(cur_sum.get("tot_nfs") or 0.0) - float(prev_sum.get("tot_nfs") or 0.0))
        except Exception:
            d_nf = None
        try:
            cur_cli = _clients_total_from_payload(payload)
            prev_cli = _clients_total_from_payload(prev_payload_for_delta) if isinstance(prev_payload_for_delta, dict) else None
            if cur_cli is not None and prev_cli is not None:
                d_cli = int(cur_cli) - int(prev_cli)
        except Exception:
            d_cli = None

    def _fmt_delta_int(v: int | None) -> str:
        if v is None:
            return "—"
        return f"{v:+d}"

    def _fmt_delta_money(v: float | None) -> str:
        if v is None:
            return "—"
        return f"R$ {v:+,.2f}"

    d3c1, d3c2, d3c3 = st.columns(3)
    with d3c1:
        _kpi_card("Faturamento (dia anterior Δ)", _fmt_delta_money(d_fat), icon="💸", accent="#6EE7B7")
    with d3c2:
        _kpi_card("NFs (dia anterior Δ)", _fmt_delta_int(d_nf), icon="📦", accent="#93c5fd")
    with d3c3:
        _kpi_card("Clientes (dia anterior Δ)", _fmt_delta_int(d_cli), icon="👥", accent="#C4B5FD")

    # Evolução de conversão por período (últimas análises salvas) — ordem cronológica por data no Período
    st.markdown("### Conversão x Interações (comparativo por análise salva)")
    rows = _perf_analysis_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=60)
    if len(rows) >= 2:
        # Filtro por mês/ano para não misturar meses no mesmo gráfico (ex.: abr + mai).
        def _month_key_from_periodo(periodo: str, created_at: str | None) -> str | None:
            try:
                dk, _ = _extract_date_label_from_periodo(str(periodo or ""), str(created_at or ""))
            except Exception:
                dk = "0000-00-00"
            if not dk or dk == "0000-00-00" or len(dk) < 7:
                return None
            return str(dk)[:7]  # YYYY-MM

        def _month_label(yyyy_mm: str) -> str:
            try:
                yy, mm = yyyy_mm.split("-")
                names = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
                mi = int(mm)
                return f"{names[mi-1]}/{yy}"
            except Exception:
                return str(yyyy_mm)

        months: list[str] = []
        for rr in rows:
            mk = _month_key_from_periodo(str(getattr(rr, "periodo", "") or ""), str(getattr(rr, "created_at", "") or ""))
            if mk and mk not in months:
                months.append(mk)
        months = sorted(months)

        cur_mk = _month_key_from_periodo(str(getattr(row, "periodo", "") or ""), str(getattr(row, "created_at", "") or ""))
        default_idx = (months.index(cur_mk) if cur_mk in months else (len(months) - 1 if months else 0))
        selected_mk = st.selectbox(
            "Mês do histórico (gráfico)",
            options=months,
            index=max(0, int(default_idx)),
            format_func=_month_label,
            key=f"{key_prefix}_hist_month_pick",
        )

        rows_month = [
            rr
            for rr in rows
            if _month_key_from_periodo(str(getattr(rr, "periodo", "") or ""), str(getattr(rr, "created_at", "") or "")) == selected_mk
        ]
        rows_month = rows_month[-12:]  # limite visual

        hist: list[dict] = []
        for r in rows_month:  # mantém apenas as últimas N análises (do mês selecionado), mas sempre cronológico
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
            # Eixo X contínuo (evita "buracos" quando análises são deletadas).
            hdf["seq"] = list(range(1, len(hdf) + 1))
            last = hdf.iloc[-1]
            prev = hdf.iloc[-2] if len(hdf) >= 2 else None

            def _fmt_conv(v) -> str:
                return f"{float(v):.1f}%" if v is not None and not pd.isna(v) else "—"

            def _delta_qty_and_pct(cur: object, ref: object) -> str | None:
                try:
                    c = float(cur)  # type: ignore[arg-type]
                    r = float(ref)  # type: ignore[arg-type]
                except Exception:
                    return None
                if pd.isna(c) or pd.isna(r):
                    return None
                diff = c - r
                if r == 0:
                    # sem base pra percentual
                    arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
                    return f"{arrow} {diff:+.0f}"
                pct = (diff / abs(r)) * 100.0
                arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
                return f"{arrow} {diff:+.0f} ({pct:+.1f}%)"

            def _delta_pp_and_pct(cur_pct: object, ref_pct: object) -> str | None:
                try:
                    c = float(cur_pct)  # type: ignore[arg-type]
                    r = float(ref_pct)  # type: ignore[arg-type]
                except Exception:
                    return None
                if pd.isna(c) or pd.isna(r):
                    return None
                diff_pp = c - r
                if r == 0:
                    arrow = "▲" if diff_pp > 0 else ("▼" if diff_pp < 0 else "→")
                    return f"{arrow} {diff_pp:+.1f} pp"
                pct = (diff_pp / abs(r)) * 100.0
                arrow = "▲" if diff_pp > 0 else ("▼" if diff_pp < 0 else "→")
                return f"{arrow} {diff_pp:+.1f} pp ({pct:+.1f}%)"

            m1, m2, m3 = st.columns(3)
            if prev is not None:
                m1.metric("Interações (time)", f"{int(last['interacoes'])}", delta=_delta_qty_and_pct(last["interacoes"], prev["interacoes"]))
                m2.metric("NFs (time)", f"{int(last['nfs'])}", delta=_delta_qty_and_pct(last["nfs"], prev["nfs"]))
                if pd.notna(last.get("conversao_total_pct")) and pd.notna(prev.get("conversao_total_pct")):
                    m3.metric(
                        "Conversão (NFs/Interações)",
                        _fmt_conv(last["conversao_total_pct"]),
                        delta=_delta_pp_and_pct(last["conversao_total_pct"], prev["conversao_total_pct"]),
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
                    go.Bar(x=hdf["seq"], y=hdf["interacoes"], name="Interações", marker_color="rgba(59,130,246,0.55)"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Bar(x=hdf["seq"], y=hdf["nfs"], name="NFs", marker_color="rgba(110,231,183,0.75)"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Scatter(
                        x=hdf["seq"],
                        y=hdf["conversao_total_pct"],
                        name="Conversão (%)",
                        mode="lines+markers",
                        line=dict(color="rgba(251,191,36,0.95)", width=3),
                    ),
                    secondary_y=True,
                )
                if best_idx is not None:
                    best_seq = hdf.loc[best_idx, "seq"]
                    fig.add_trace(
                        go.Scatter(
                            x=[best_seq],
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
                fig.update_xaxes(
                    title_text="Análises (ordem cronológica)",
                    tickmode="array",
                    tickvals=hdf["seq"].tolist(),
                    ticktext=hdf["periodo"].astype(str).tolist(),
                )
                fig.update_yaxes(title_text="Volume", secondary_y=False)
                fig.update_yaxes(title_text="Conversão (%)", secondary_y=True, rangemode="tozero")
                st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_conv_history_combo")
            except Exception as e:
                st.caption(f"Gráfico combinado indisponível: {e}")
    else:
        st.caption("Salve pelo menos 2 análises para comparar conversão vs interações ao longo do tempo.")

    st.markdown("### Indicadores (ranking)")
    # Mapa de "temperatura": quantos indicadores cada vendedor entrega
    try:
        if isinstance(df, pd.DataFrame) and (not df.empty):
            raw_map = {getattr(s, "nome", None): s for s in (sellers or [])}

            def _num(x: object) -> float | None:
                try:
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return None
                    v = float(x)
                    return None if pd.isna(v) else v
                except Exception:
                    return None

            def _int(x: object) -> int | None:
                v = _num(x)
                return int(v) if v is not None else None

            # Limites (já usados na regra de bônus/ideal):
            # Margem >= 26 | Conversão >= 12 | Prazo <= META_PRAZO_MEDIO_DIAS | TME <= 5 | Interações >= 200
            # Meta Faturamento entregue: (Faturamento / Meta) * 100 >= 100
            # Desconto: menor é melhor — usa referência dinâmica (média do time no período)
            disc_ref = None
            try:
                dp = [float(getattr(x, "desconto_pct", 0.0)) for x in (sellers or []) if getattr(x, "desconto_pct", None) is not None]
                disc_ref = (sum(dp) / len(dp)) if dp else None
            except Exception:
                disc_ref = None

            dfx = df[["nome", "margem_pct", "conversao_pct", "prazo_medio", "tme_minutos", "interacoes"]].copy()
            dfx["faturamento"] = dfx["nome"].apply(lambda n: _num(getattr(raw_map.get(n), "faturamento", None)))
            dfx["meta_faturamento"] = dfx["nome"].apply(lambda n: _num(getattr(raw_map.get(n), "meta_faturamento", None)))
            dfx["desconto_pct"] = dfx["nome"].apply(lambda n: _num(getattr(raw_map.get(n), "desconto_pct", None)))

            fat = pd.to_numeric(dfx["faturamento"], errors="coerce")
            meta = pd.to_numeric(dfx["meta_faturamento"], errors="coerce")
            alcance_real = (fat / meta) * 100.0

            dfx["entregue_meta_faturamento"] = (meta.notna()) & (meta > 0) & (fat.notna()) & (alcance_real >= 100.0)
            dfx["entregue_margem"] = pd.to_numeric(dfx["margem_pct"], errors="coerce") >= 26.0
            dfx["entregue_conversao"] = pd.to_numeric(dfx["conversao_pct"], errors="coerce") >= 12.0
            dfx["entregue_prazo"] = pd.to_numeric(dfx["prazo_medio"], errors="coerce") <= META_PRAZO_MEDIO_DIAS
            dfx["entregue_tme"] = pd.to_numeric(dfx["tme_minutos"], errors="coerce") <= 5.0
            dfx["entregue_interacoes"] = pd.to_numeric(dfx["interacoes"], errors="coerce") >= 200.0
            if disc_ref is not None:
                dfx["entregue_desconto"] = pd.to_numeric(dfx["desconto_pct"], errors="coerce") <= float(disc_ref)
            else:
                dfx["entregue_desconto"] = False

            entregas_cols = [
                "entregue_meta_faturamento",
                "entregue_margem",
                "entregue_conversao",
                "entregue_prazo",
                "entregue_tme",
                "entregue_interacoes",
                "entregue_desconto",
            ]
            dfx["indicadores_entregues"] = dfx[entregas_cols].fillna(False).sum(axis=1).astype(int)
            dfx = dfx.sort_values(["indicadores_entregues", "nome"], ascending=[False, True]).reset_index(drop=True)

            best = dfx.iloc[0]
            worst = dfx.iloc[-1]
            st.markdown("#### Temperatura — indicadores entregues (por vendedor)")
            s1, s2 = st.columns(2)
            s1.markdown(
                f"**Maior entrega**: **{best['nome']}** — **{int(best['indicadores_entregues'])}** indicador(es)"
            )
            s2.markdown(
                f"**Menor entrega**: **{worst['nome']}** — **{int(worst['indicadores_entregues'])}** indicador(es)"
            )

            try:
                import plotly.graph_objects as go

                # Quais indicadores (para hover moderno)
                ind_labels = {
                    "entregue_meta_faturamento": "Meta faturamento entregue",
                    "entregue_margem": "Margem",
                    "entregue_conversao": "Conversão",
                    "entregue_prazo": "Prazo médio",
                    "entregue_tme": "TME",
                    "entregue_interacoes": "Interações",
                    "entregue_desconto": "Desconto",
                }
                quais: list[str] = []
                for _, r in dfx.iterrows():
                    delivered = [ind_labels[c] for c in entregas_cols if bool(r.get(c))]
                    if not delivered:
                        quais.append("—")
                    else:
                        # um por linha para ficar legível
                        quais.append("<br>".join([f"• {x}" for x in delivered]))

                fig_h = go.Figure(
                    data=go.Heatmap(
                        z=[dfx["indicadores_entregues"].tolist()],
                        x=dfx["nome"].tolist(),
                        y=["Indicadores entregues"],
                        customdata=[quais],
                        colorscale=[
                            [0.0, "rgba(251,113,133,0.35)"],
                            [0.4, "rgba(251,191,36,0.35)"],
                            [0.7, "rgba(110,231,183,0.45)"],
                            [1.0, "rgba(34,197,94,0.55)"],
                        ],
                        zmin=0,
                        zmax=max(1, int(dfx["indicadores_entregues"].max())),
                        hovertemplate=(
                            "<b>%{x}</b>"
                            "<br><span style='color:#94a3b8'>Indicadores entregues:</span> <b>%{z}</b>"
                            "<br><span style='color:#94a3b8'>Quais:</span><br>%{customdata}"
                            "<extra></extra>"
                        ),
                        showscale=True,
                        colorbar=dict(title="Qtd", thickness=12),
                    )
                )
                fig_h.update_layout(
                    height=220,
                    margin=dict(l=10, r=10, t=10, b=10),
                    xaxis=dict(tickangle=-35),
                    yaxis=dict(tickfont=dict(size=12)),
                    hoverlabel=dict(
                        bgcolor="rgba(11,18,32,0.96)",
                        bordercolor="rgba(148,163,184,0.28)",
                        font=dict(color="#E5E7EB", size=12),
                    ),
                )
                st.plotly_chart(fig_h, use_container_width=True, key=f"{key_prefix}_heat_indicadores_entregues")
                if disc_ref is not None:
                    st.caption(f"Regra do indicador **Desconto**: entregue quando % aplicado ≤ média do time (**{float(disc_ref):.2f}%**).")
            except Exception as e:
                st.caption(f"Mapa de calor indisponível: {e}")
    except Exception:
        pass

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

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
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

    nome_historico = st.text_input(
        "Nome no histórico",
        value=str(row.periodo or ""),
        key=f"manual_edit_nome_{int(analysis_id)}",
        help="Texto que aparece na aba Histórico e nos cards. Ajuste antes de salvar para identificar esta versão (ex.: “Março 2026 — correção margens”).",
        placeholder="ex.: Abril 2026 (edição manual)",
    )

    if st.button("💾 Salvar nova versão", use_container_width=True):
        novos = edited.to_dict(orient="records")
        new_payload = dict(payload)
        new_payload["vendedores"] = novos

        label = (nome_historico or "").strip()
        if not label:
            label = str(new_payload.get("periodo") or row.periodo or "Edição manual")

        sellers2 = parse_sellers(new_payload)
        results2, total2 = calcular_time(sellers2) if sellers2 else ([], 0.0)
        new_payload["periodo"] = label
        new_id = save_analysis(
            conn,
            periodo=label,
            provider_used="manual_edit",
            model_used="manual_edit",
            parent_analysis_id=int(analysis_id),
            owner_user_id=int(row.owner_user_id) if row.owner_user_id is not None else owner_id,
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

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
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
    default_rest = max(0, int(default_total) - int(default_trab))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        dias_total = st.number_input(
            "Total de dias úteis no mês",
            min_value=1,
            max_value=31,
            value=int(default_total),
            key="proj_dias_uteis_total",
        )
    with col2:
        dias_rest = st.number_input(
            "Dias úteis restantes no mês",
            min_value=0,
            max_value=int(dias_total),
            value=min(int(default_rest), int(dias_total)),
            help="O app recalcula automaticamente os dias trabalhados = total - restantes.",
            key="proj_dias_uteis_restantes",
        )
        dias_trab = max(1, int(dias_total) - int(dias_rest))
    with col3:
        meta_faturamento = st.number_input("Meta de faturamento (R$)", min_value=0.0, max_value=1e9, value=0.0, step=1000.0, format="%.2f")
    with col4:
        modo = st.selectbox("Modo", options=["Por vendedor", "Time (somado)"], key="proj_mode")

    def _parse_dt(s: object):
        try:
            from datetime import datetime

            txt = str(s or "")
            if not txt:
                return None
            # ex: 2026-04-24T23:50:00.123Z
            txt = txt.replace("Z", "+00:00")
            return datetime.fromisoformat(txt)
        except Exception:
            return None

    def _get_prev_analysis_row(current_row):
        # Pega a análise anterior.
        # Prioridade:
        # - Se o `periodo` tiver dd/mm/aaaa, usa essa data para comparar (corrige casos em que
        #   o servidor salva `created_at` em UTC e muda o "dia" da análise).
        # - Senão, usa ordem por created_at como fallback.
        rows = list_analyses(conn, limit=200, owner_user_id=owner_id, include_all=is_admin)
        if not rows:
            return None

        cur_id = int(getattr(current_row, "id", 0) or 0)
        cur_dt = _parse_dt(getattr(current_row, "created_at", None))
        try:
            cur_date_key, _ = _extract_date_label_from_periodo(
                str(getattr(current_row, "periodo", "") or ""),
                str(getattr(current_row, "created_at", "") or ""),
            )
        except Exception:
            cur_date_key = "0000-00-00"

        def _is_perf_row(rr) -> bool:
            try:
                payload_r = json.loads(getattr(rr, "payload_json", "") or "")
            except Exception:
                return False
            if not isinstance(payload_r, dict):
                return False
            kind = str(payload_r.get("_kind") or "")
            if kind.startswith("sala_gestao_"):
                return False
            return bool(parse_sellers(payload_r))

        # Se tiver uma data "real" no período (dd/mm/aaaa), pega a anterior por data_key.
        if cur_date_key and cur_date_key != "0000-00-00":
            best = None
            best_key = None
            best_dt = None
            for rr in rows:
                rid = int(getattr(rr, "id", 0) or 0)
                if rid == cur_id:
                    continue
                if not _is_perf_row(rr):
                    continue
                try:
                    rk, _ = _extract_date_label_from_periodo(
                        str(getattr(rr, "periodo", "") or ""),
                        str(getattr(rr, "created_at", "") or ""),
                    )
                except Exception:
                    rk = "0000-00-00"
                if not rk or rk == "0000-00-00" or rk >= cur_date_key:
                    continue
                rdt = _parse_dt(getattr(rr, "created_at", None))
                if best_key is None or rk > best_key:
                    best, best_key, best_dt = rr, rk, rdt
                elif rk == best_key:
                    # desempate: maior created_at (ou maior id se não parsear)
                    if best_dt is None and rdt is not None:
                        best, best_dt = rr, rdt
                    elif best_dt is not None and rdt is not None and rdt > best_dt:
                        best, best_dt = rr, rdt
                    elif best_dt is None and rdt is None:
                        bid = int(getattr(best, "id", 0) or 0) if best is not None else 0
                        if rid > bid:
                            best = rr
            if best is not None:
                return best

        if cur_dt is not None:
            best = None
            best_dt = None
            for rr in rows:
                rid = int(getattr(rr, "id", 0) or 0)
                if rid == cur_id:
                    continue
                rdt = _parse_dt(getattr(rr, "created_at", None))
                if rdt is None or rdt >= cur_dt:
                    continue
                if not _is_perf_row(rr):
                    continue
                if best_dt is None or rdt > best_dt:
                    best = rr
                    best_dt = rdt
            if best is not None:
                return best

        # Fallback: usa maior ID menor que o atual (quando created_at não é confiável)
        best = None
        best_id = None
        for rr in rows:
            rid = int(getattr(rr, "id", 0) or 0)
            if rid >= cur_id:
                continue
            if not _is_perf_row(rr):
                continue
            if best_id is None or rid > best_id:
                best = rr
                best_id = rid
        if best is not None:
            return best

        # Último fallback: pega a primeira análise "performance" diferente da atual
        for rr in rows:
            rid = int(getattr(rr, "id", 0) or 0)
            if rid == cur_id:
                continue
            if _is_perf_row(rr):
                return rr
        return None

    def _pct_delta(cur: object, ref: object) -> float | None:
        try:
            c = float(cur)  # type: ignore[arg-type]
            r = float(ref)  # type: ignore[arg-type]
        except Exception:
            return None
        if r == 0 or pd.isna(c) or pd.isna(r):
            return None
        return (c - r) / abs(r) * 100.0

    def _norm_nome(x: object) -> str:
        return " ".join(str(x or "").strip().lower().split())

    def _delta_qty_and_pct(cur: object, ref: object) -> str | None:
        try:
            c = float(cur)  # type: ignore[arg-type]
            r = float(ref)  # type: ignore[arg-type]
        except Exception:
            return None
        if pd.isna(c) or pd.isna(r):
            return None
        diff = c - r
        arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
        if r == 0:
            return f"{arrow} {diff:+.0f}"
        pct = (diff / abs(r)) * 100.0
        return f"{arrow} {diff:+.0f} ({pct:+.1f}%)"

    def _delta_money_and_pct(cur: object, ref: object) -> str | None:
        try:
            c = float(cur)  # type: ignore[arg-type]
            r = float(ref)  # type: ignore[arg-type]
        except Exception:
            return None
        if pd.isna(c) or pd.isna(r):
            return None
        diff = c - r
        arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
        if r == 0:
            return f"{arrow} R$ {diff:+,.2f}"
        pct = (diff / abs(r)) * 100.0
        return f"{arrow} R$ {diff:+,.2f} ({pct:+.1f}%)"

    def _delta_float_and_pct(cur: object, ref: object, *, digits: int = 2, unit: str = "") -> str | None:
        try:
            c = float(cur)  # type: ignore[arg-type]
            r = float(ref)  # type: ignore[arg-type]
        except Exception:
            return None
        if pd.isna(c) or pd.isna(r):
            return None
        diff = c - r
        arrow = "▲" if diff > 0 else ("▼" if diff < 0 else "→")
        if r == 0:
            return f"{arrow} {diff:+.{digits}f}{unit}"
        pct = (diff / abs(r)) * 100.0
        return f"{arrow} {diff:+.{digits}f}{unit} ({pct:+.1f}%)"

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
        meta_eff_for_ideal = meta_faturamento_eff
        proj = projetar_resultados(
            s,
            dias_uteis_total=int(dias_total),
            dias_uteis_trabalhados=int(dias_trab),
            meta_faturamento=meta_faturamento_eff,
            ticket_medio_override=float(ticket_override) if ticket_override > 0 else None,
        )
        titulo = f"Projeção — {s.nome}"

        # Comparativo com a análise anterior (mesmo vendedor)
        prev_row = _get_prev_analysis_row(row)
        prev_proj = None
        prev_expect = None
        if prev_row:
            try:
                prev_payload = json.loads(prev_row.payload_json)
                prev_sellers = parse_sellers(prev_payload)
                prev_s = next((x for x in prev_sellers if _norm_nome(x.nome) == _norm_nome(nome)), None)
                if prev_s is not None:
                    prev_meta_auto = float(prev_s.meta_faturamento) if (prev_s.meta_faturamento is not None and prev_s.meta_faturamento > 0) else 0.0
                    prev_meta_eff = float(meta_faturamento) if meta_faturamento > 0 else (prev_meta_auto if prev_meta_auto > 0 else None)
                    prev_proj = projetar_resultados(
                        prev_s,
                        dias_uteis_total=int(dias_total),
                        dias_uteis_trabalhados=int(dias_trab),
                        meta_faturamento=prev_meta_eff,
                        # Comparativo deve refletir o dado do documento anterior (sem override atual)
                        ticket_medio_override=None,
                    )
                    prev_expect = prev_proj
            except Exception:
                prev_proj = None
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
        qtd_time = int(soma.qtd_faturadas or 0)
        fat_time = soma.faturamento
        ticket_auto_time = (float(fat_time) / float(qtd_time)) if (fat_time is not None and qtd_time > 0) else 0.0
        ticket_override_time = st.number_input(
            "Ticket médio (R$) — opcional (time)",
            min_value=0.0,
            max_value=1e8,
            value=float(ticket_auto_time),
            step=50.0,
            format="%.2f",
            help="Média do faturamento total do time ÷ NFs, quando houver. Ajuste se o total do print não refletir o real.",
            key="proj_team_ticket",
        )
        meta_faturamento_eff = float(meta_faturamento) if meta_faturamento > 0 else soma.meta_faturamento
        meta_eff_for_ideal = meta_faturamento_eff
        proj = projetar_resultados(
            soma,
            dias_uteis_total=int(dias_total),
            dias_uteis_trabalhados=int(dias_trab),
            meta_faturamento=meta_faturamento_eff,
            ticket_medio_override=float(ticket_override_time) if ticket_override_time > 0 else None,
        )
        titulo = "Projeção — Time"

        # Comparativo com a análise anterior (time)
        prev_row = _get_prev_analysis_row(row)
        prev_proj = None
        prev_expect = None
        if prev_row:
            try:
                prev_payload = json.loads(prev_row.payload_json)
                prev_sellers = parse_sellers(prev_payload)
                prev_totais = prev_payload.get("totais") if isinstance(prev_payload, dict) else {}
                if not isinstance(prev_totais, dict):
                    prev_totais = {}
                prev_fat_total = prev_totais.get("faturamento_total")
                prev_meta_total = prev_totais.get("meta_total")
                prev_soma = SellerDC(
                    nome="Time",
                    qtd_faturadas=sum(int(x.qtd_faturadas or 0) for x in prev_sellers),
                    iniciados=sum(int(x.iniciados or 0) for x in prev_sellers),
                    recebidos=sum(int(x.recebidos or 0) for x in prev_sellers),
                    chamadas=sum(int(x.chamadas or 0) for x in prev_sellers),
                    faturamento=float(prev_fat_total) if isinstance(prev_fat_total, (int, float)) else None,
                    meta_faturamento=float(prev_meta_total) if isinstance(prev_meta_total, (int, float)) else None,
                )
                prev_meta_eff = float(meta_faturamento) if meta_faturamento > 0 else prev_soma.meta_faturamento
                prev_proj = projetar_resultados(
                    prev_soma,
                    dias_uteis_total=int(dias_total),
                    dias_uteis_trabalhados=int(dias_trab),
                    meta_faturamento=prev_meta_eff,
                    # Comparativo deve refletir o dado do documento anterior (sem override atual)
                    ticket_medio_override=None,
                )
                prev_expect = prev_proj
            except Exception:
                prev_proj = None

    def _section_header(title: str, subtitle: str, *, pill: str, accent: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:14px 16px;
  border-color: rgba(59,130,246,.18);
  background:
    radial-gradient(900px 220px at 12% 0%, rgba(59,130,246,.18), transparent 60%),
    radial-gradient(900px 220px at 88% 12%, rgba(110,231,183,.10), transparent 55%),
    linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <div>
      <div style="color:#94A3B8;font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;font-weight:800;">
        {html.escape(subtitle)}
      </div>
      <div style="color:#E5E7EB;font-size:1.22rem;font-weight:950;margin-top:6px;line-height:1.2;">
        {html.escape(title)}
      </div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;">
      <span class="dp-pill" style="
        border-color: rgba(255,255,255,.12);
        background: rgba(255,255,255,.03);
        color: {accent};
        font-weight:850;
      ">{html.escape(pill)}</span>
    </div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    _section_header(str(titulo), "Simulação / Projeção", pill="Time", accent="#93c5fd")

    import html as _html

    def _fmt_int(v: object) -> str:
        try:
            return f"{int(round(float(v))):d}"
        except Exception:
            return "—"

    def _fmt_float(v: object, digits: int = 2) -> str:
        try:
            return f"{float(v):.{digits}f}"
        except Exception:
            return "—"

    def _fmt_money(v: object) -> str:
        try:
            return f"R$ {float(v):,.2f}"
        except Exception:
            return "—"

    def _fmt_money_with_meta_pct(v: object, meta_v: object) -> str:
        try:
            vv = float(v)
        except Exception:
            return "—"
        try:
            mm = float(meta_v)
        except Exception:
            mm = 0.0
        if mm and mm > 0:
            return f"R$ {vv:,.2f} ({(vv/mm)*100.0:.1f}%)"
        return f"R$ {vv:,.2f}"

    def _fmt_pct(v: object, digits: int = 2) -> str:
        try:
            return f"{float(v):.{digits}f}%"
        except Exception:
            return "—"

    def _delta_vs(ref: float | None, cur: float | None, *, kind: str, digits: int = 2) -> str:
        if ref is None or cur is None:
            return "—"
        if pd.isna(ref) or pd.isna(cur):
            return "—"
        diff = float(cur) - float(ref)
        if abs(diff) < 1e-9:
            arrow = "→"
        else:
            arrow = "▲" if diff > 0 else "▼"
        if abs(ref) < 1e-9:
            pct = None
        else:
            pct = (diff / abs(ref)) * 100.0

        if kind == "money":
            base = f"{arrow} R$ {diff:+,.2f}"
        elif kind == "int":
            base = f"{arrow} {diff:+.0f}"
        elif kind == "float":
            base = f"{arrow} {diff:+.{digits}f}"
        elif kind == "pct":
            base = f"{arrow} {diff:+.{digits}f} pp"
        else:
            base = f"{arrow} {diff:+.{digits}f}"
        return f"{base} ({pct:+.1f}%)" if pct is not None else base

    def _delta_color(val: str) -> str:
        if val.startswith("▲"):
            return "color:#22c55e;font-weight:800;"
        if val.startswith("▼"):
            return "color:#fb7185;font-weight:800;"
        if val.startswith("→"):
            return "color:#94a3b8;font-weight:650;"
        return "color:#94a3b8;"

    _PROJ_KPI_MIN_H = 176

    def _render_dual_kpi(title: str, value: str, d_prev: str, d_ideal: str, *, help_prev: str, help_ideal: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:14px 14px;
  min-height: {_PROJ_KPI_MIN_H}px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
">
  <div class="dp-kpi-label">{_html.escape(title)}</div>
  <div class="dp-kpi-value">{_html.escape(value)}</div>
  <div style="margin-top:8px;display:flex;flex-direction:column;gap:6px;">
    <div style="font-size:0.84rem;{_delta_color(d_prev)}">{_html.escape(d_prev)} <span style="color:#94a3b8;font-weight:600">({ _html.escape(help_prev) })</span></div>
    <div style="font-size:0.84rem;{_delta_color(d_ideal)}">{_html.escape(d_ideal)} <span style="color:#94a3b8;font-weight:600">({ _html.escape(help_ideal) })</span></div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    # Ideal (pacing linear) — só quando existe meta de faturamento
    ideal_fat_hoje = None
    ideal_nf_hoje = None
    ideal_inter_hoje = None
    ideal_fat_dia = None
    ideal_nf_dia = None
    ideal_inter_dia = None
    ideal_conv_pct = None
    ideal_ticket = None
    try:
        if meta_eff_for_ideal is not None and float(meta_eff_for_ideal) > 0 and proj.dias_uteis_total > 0:
            meta_eff = float(meta_eff_for_ideal)
            ratio = float(proj.dias_uteis_trabalhados) / float(proj.dias_uteis_total)
            ratio = min(1.0, max(0.0, ratio))
            ideal_fat_hoje = meta_eff * ratio
            # ticket ideal = ticket atual (é parâmetro de produtividade; comparar ticket com ideal faz sentido só vs anterior)
            ideal_ticket = None
            # nfs ideal pela meta e ticket atual (se existir)
            if proj.ticket_medio and float(proj.ticket_medio) > 0:
                ideal_nf_hoje = ideal_fat_hoje / float(proj.ticket_medio)
                ideal_nf_dia = meta_eff / float(proj.dias_uteis_total) / float(proj.ticket_medio)
            # interações ideal pela conversão atual (se existir)
            if ideal_nf_hoje is not None and proj.conversao_atual_pct and float(proj.conversao_atual_pct) > 0:
                ideal_inter_hoje = ideal_nf_hoje / (float(proj.conversao_atual_pct) / 100.0)
                ideal_inter_dia = (ideal_nf_dia / (float(proj.conversao_atual_pct) / 100.0)) if ideal_nf_dia is not None else None
            ideal_fat_dia = meta_eff / float(proj.dias_uteis_total)
            ideal_conv_pct = None
    except Exception:
        pass

    # Move para o topo: "O que falta para bater a meta"
    if proj.meta_faturamento is not None and proj.meta_faturamento > 0:
        _section_header("O que falta para bater a meta", "Foco do dia", pill="Meta & gap", accent="#FBBF24")
        def _render_modern_kpi(title: str, value: str, *, icon: str, accent: str, subtitle: str | None = None) -> None:
            sub = subtitle or ""
            st.markdown(
                f"""
<div class="dp-card" style="
  padding:14px 14px;
  min-height: 142px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
  <div style="margin-top:8px;color:#94a3b8;font-weight:650;font-size:0.84rem;">{_html.escape(sub) if sub else ""}</div>
</div>
""",
                unsafe_allow_html=True,
            )

        x1, x2, x3, x4 = st.columns(4)
        with x1:
            _render_modern_kpi("Meta faturamento", f"R$ {proj.meta_faturamento:,.2f}", icon="🎯", accent="#93c5fd")
        with x2:
            _render_modern_kpi(
                "Falta (R$)",
                f"R$ {proj.faturamento_faltando:,.2f}" if proj.faturamento_faltando is not None else "—",
                icon="🧾",
                accent="#fb7185",
            )
        with x3:
            _render_modern_kpi(
                "NFs/dia necessárias",
                f"{proj.nfs_por_dia_necessarias:.2f}" if proj.nfs_por_dia_necessarias is not None else "—",
                icon="📦",
                accent="#C4B5FD",
                subtitle="mesmo ticket",
            )
        with x4:
            status_txt = str(proj.status or "—")
            status_lower = status_txt.lower()
            if "ating" in status_lower or "meta" in status_lower and "próximo" not in status_lower and "proximo" not in status_lower:
                status_accent = "#6EE7B7"
                status_icon = "✅"
            elif "próximo" in status_lower or "proximo" in status_lower:
                status_accent = "#FBBF24"
                status_icon = "⚠️"
            else:
                status_accent = "#fb7185"
                status_icon = "⛔"
            _render_modern_kpi("Status", status_txt, icon=status_icon, accent=status_accent)
        if proj.ticket_necessario_com_mesmo_ritmo is not None:
            st.caption(f"Se mantiver o mesmo ritmo de NFs/dia, o ticket médio necessário seria ~ **R$ {proj.ticket_necessario_com_mesmo_ritmo:,.2f}**.")

    c1, c2, c3 = st.columns(3)
    # Comparativos:
    # - Atual (faturadas/interações): vs "meta de amanhã" calculada na análise anterior.
    #   Ex.: ontem tinha 30 faturadas e precisava +8/dia → hoje deveria estar em 38.
    # - Projeções: vs projeção da análise anterior.
    prev_nf_esperado_hoje = None
    prev_inter_esperado_hoje = None
    if prev_proj is not None:
        # NFs esperadas hoje: ontem + NFs/dia necessárias (se houver meta), senão ontem + ritmo atual.
        if prev_proj.nfs_por_dia_necessarias is not None:
            prev_nf_esperado_hoje = float(prev_proj.qtd_faturadas_atual) + float(prev_proj.nfs_por_dia_necessarias)
        else:
            prev_nf_esperado_hoje = float(prev_proj.qtd_faturadas_atual) + float(prev_proj.media_diaria_faturas)

        # Interações esperadas hoje:
        # - se houver NFs/dia necessárias + conversão atual: deriva interações/dia necessárias
        # - senão: usa ritmo de interações/dia
        conv = prev_proj.conversao_atual_pct
        if prev_proj.nfs_por_dia_necessarias is not None and conv is not None and float(conv) > 0:
            inter_por_dia_nec = float(prev_proj.nfs_por_dia_necessarias) / (float(conv) / 100.0)
            prev_inter_esperado_hoje = float(prev_proj.interacoes_atual) + inter_por_dia_nec
        else:
            prev_inter_esperado_hoje = float(prev_proj.interacoes_atual) + float(prev_proj.media_diaria_interacoes)

    d_fat = _delta_qty_and_pct(proj.qtd_faturadas_atual, prev_nf_esperado_hoje) if prev_nf_esperado_hoje is not None else None
    d_int = _delta_qty_and_pct(proj.interacoes_atual, prev_inter_esperado_hoje) if prev_inter_esperado_hoje is not None else None
    d_proj_fat = _delta_float_and_pct(proj.projecao_faturas, prev_proj.projecao_faturas, digits=1) if prev_proj is not None else None
    with c1:
        _render_dual_kpi(
            "Faturadas (atual)",
            _fmt_int(proj.qtd_faturadas_atual),
            d_fat or "→ 0 (0.0%)",
            _delta_vs(ideal_nf_hoje, float(proj.qtd_faturadas_atual), kind="float", digits=0) if ideal_nf_hoje is not None else "—",
            help_prev="vs esperado (análise anterior)",
            help_ideal="vs ideal p/ meta",
        )
    with c2:
        _render_dual_kpi(
            "Interações (atual)",
            _fmt_int(proj.interacoes_atual),
            d_int or "→ 0 (0.0%)",
            _delta_vs(ideal_inter_hoje, float(proj.interacoes_atual), kind="float", digits=0) if ideal_inter_hoje is not None else "—",
            help_prev="vs esperado (análise anterior)",
            help_ideal="vs ideal p/ meta",
        )
    with c3:
        _render_dual_kpi(
            "Projeção faturadas",
            _fmt_float(proj.projecao_faturas, 1),
            d_proj_fat or "→ +0.0 (0.0%)",
            _delta_vs(ideal_nf_hoje, float(proj.projecao_faturas), kind="float", digits=1) if ideal_nf_hoje is not None else "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )

    _section_header("Ritmo diário", "Cadência operacional", pill="Dia útil", accent="#6EE7B7")
    k1, k2, k3, k4 = st.columns(4)
    d_mfat = _pct_delta(proj.media_diaria_faturas, prev_proj.media_diaria_faturas) if prev_proj is not None else None
    d_mint = _pct_delta(proj.media_diaria_interacoes, prev_proj.media_diaria_interacoes) if prev_proj is not None else None
    d_pconv = _pct_delta(proj.projecao_conversao_pct, prev_proj.projecao_conversao_pct) if prev_proj is not None else None
    with k1:
        _render_dual_kpi(
            "Média faturas/dia",
            _fmt_float(proj.media_diaria_faturas, 2),
            (_delta_float_and_pct(proj.media_diaria_faturas, prev_proj.media_diaria_faturas, digits=2) if prev_proj is not None else "→ +0.00 (0.0%)"),
            _delta_vs(ideal_nf_dia, float(proj.media_diaria_faturas), kind="float", digits=2) if ideal_nf_dia is not None else "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )
    with k2:
        _render_dual_kpi(
            "Média interações/dia",
            _fmt_float(proj.media_diaria_interacoes, 2),
            (_delta_float_and_pct(proj.media_diaria_interacoes, prev_proj.media_diaria_interacoes, digits=2) if prev_proj is not None else "→ +0.00 (0.0%)"),
            _delta_vs(ideal_inter_dia, float(proj.media_diaria_interacoes), kind="float", digits=2) if ideal_inter_dia is not None else "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )
    # Card de desconto (puxa do arquivo "Qtd Faturadas")
    def _render_discount_card(*, pct_aplicado: float | None, valor: float | None, qtd: int | None, pct_qtd: float | None) -> None:
        pct_txt = f"{float(pct_aplicado):.2f}%" if pct_aplicado is not None and not pd.isna(pct_aplicado) else "—"
        val_txt = f"R$ {float(valor):,.2f}" if valor is not None and not pd.isna(valor) else "—"
        qtd_txt = f"{int(qtd):d}" if qtd is not None else "—"
        pctq_txt = f"{float(pct_qtd):.2f}%" if pct_qtd is not None and not pd.isna(pct_qtd) else "—"
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:14px 14px;
  min-height: {_PROJ_KPI_MIN_H}px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">Desconto (% aplicado)</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: #93c5fd;
    ">🏷</div>
  </div>
  <div class="dp-kpi-value">{pct_txt}</div>
  <div style="margin-top:8px;display:flex;gap:10px;flex-wrap:wrap;">
    <span class="dp-pill" style="background:rgba(255,255,255,.02);">Valor: <b>{val_txt}</b></span>
    <span class="dp-pill" style="background:rgba(255,255,255,.02);">Qtd desc.: <b>{qtd_txt}</b></span>
    <span class="dp-pill" style="background:rgba(255,255,255,.02);">% qtd: <b>{pctq_txt}</b></span>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    desc_pct = None
    desc_val = None
    qdesc = None
    qdesc_pct = None
    try:
        if modo == "Por vendedor":
            desc_pct = getattr(s, "desconto_pct", None)
            desc_val = getattr(s, "desconto_valor", None)
            qdesc = getattr(s, "qtd_desconto", None)
            qdesc_pct = getattr(s, "qtd_desconto_pct", None)
        else:
            # time: soma valores/quantidades; % usa média simples quando disponível
            desc_vals = [getattr(x, "desconto_valor", None) for x in sellers]
            qdesc_vals = [getattr(x, "qtd_desconto", None) for x in sellers]
            desc_pcts = [getattr(x, "desconto_pct", None) for x in sellers]
            qdesc_pcts = [getattr(x, "qtd_desconto_pct", None) for x in sellers]
            dsum = sum(float(v) for v in desc_vals if v is not None and not pd.isna(v))
            qsum = sum(int(v) for v in qdesc_vals if v is not None)
            desc_val = dsum if dsum != 0 else None
            qdesc = qsum if qsum != 0 else None
            dp = [float(v) for v in desc_pcts if v is not None and not pd.isna(v)]
            qp = [float(v) for v in qdesc_pcts if v is not None and not pd.isna(v)]
            desc_pct = (sum(dp) / len(dp)) if dp else None
            qdesc_pct = (sum(qp) / len(qp)) if qp else None
    except Exception:
        pass

    with k3:
        _render_discount_card(pct_aplicado=desc_pct, valor=desc_val, qtd=qdesc, pct_qtd=qdesc_pct)
    with k4:
        _render_dual_kpi(
            "Conversão proj.",
            f"{proj.projecao_conversao_pct:.2f}%" if proj.projecao_conversao_pct is not None else "—",
            (_delta_float_and_pct(proj.projecao_conversao_pct, prev_proj.projecao_conversao_pct, digits=2, unit="%") if prev_proj is not None else "→ +0.00% (0.0%)"),
            "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )

    _section_header("Meta em faturamento", "Mantendo o ritmo / ticket", pill="R$ & %", accent="#C4B5FD")
    m1, m2, m3, m4 = st.columns(4)
    d_ticket = _pct_delta(proj.ticket_medio, prev_proj.ticket_medio) if prev_proj is not None else None
    d_fat_atual = _pct_delta(proj.faturamento_atual, prev_proj.faturamento_atual) if prev_proj is not None else None
    d_fat_dia = _pct_delta(proj.faturamento_dia_atual, prev_proj.faturamento_dia_atual) if prev_proj is not None else None
    d_proj_fat_r = _pct_delta(proj.projecao_faturamento, prev_proj.projecao_faturamento) if prev_proj is not None else None
    with m1:
        _render_dual_kpi(
            "Ticket médio",
            _fmt_money(proj.ticket_medio) if proj.ticket_medio is not None else "—",
            (_delta_money_and_pct(proj.ticket_medio, prev_proj.ticket_medio) if prev_proj is not None else "→ R$ +0.00 (0.0%)"),
            "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )
    with m2:
        _render_dual_kpi(
            "Faturamento atual",
            _fmt_money(proj.faturamento_atual) if proj.faturamento_atual is not None else "—",
            (_delta_money_and_pct(proj.faturamento_atual, prev_proj.faturamento_atual) if prev_proj is not None else "→ R$ +0.00 (0.0%)"),
            _delta_vs(ideal_fat_hoje, float(proj.faturamento_atual), kind="money") if (ideal_fat_hoje is not None and proj.faturamento_atual is not None) else "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )
    with m3:
        _render_dual_kpi(
            "Faturamento/dia (atual)",
            _fmt_money(proj.faturamento_dia_atual) if proj.faturamento_dia_atual is not None else "—",
            (_delta_money_and_pct(proj.faturamento_dia_atual, prev_proj.faturamento_dia_atual) if prev_proj is not None else "→ R$ +0.00 (0.0%)"),
            _delta_vs(ideal_fat_dia, float(proj.faturamento_dia_atual), kind="money") if (ideal_fat_dia is not None and proj.faturamento_dia_atual is not None) else "—",
            help_prev="vs análise anterior",
            help_ideal="vs ideal p/ meta",
        )
    with m4:
        _render_dual_kpi(
            "Projeção faturamento",
            _fmt_money_with_meta_pct(proj.projecao_faturamento, meta_eff_for_ideal) if proj.projecao_faturamento is not None else "—",
            (_delta_money_and_pct(proj.projecao_faturamento, prev_proj.projecao_faturamento) if prev_proj is not None else "→ R$ +0.00 (0.0%)"),
            _delta_vs(meta_eff_for_ideal, float(proj.projecao_faturamento), kind="money") if (meta_eff_for_ideal is not None and proj.projecao_faturamento is not None) else "—",
            help_prev="vs análise anterior",
            help_ideal="vs meta (ideal)",
        )


def page_star(settings, conn) -> None:
    render_header("Feedback STAR", "Gere feedback individual e baixe em PDF.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
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
    import html as _html

    def _kpi_card(title: str, value: str, *, icon: str, accent: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:12px 12px;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi_card("Bônus", f"R$ {r.bonus_total:,.2f}", icon="💵", accent="#6EE7B7")
    with c2:
        _kpi_card("Margem", f"{float(r.margem_pct):.2f}%" if r.margem_pct is not None else "—", icon="📊", accent="#A7F3D0")
    with c3:
        _kpi_card("Conversão", f"{float(r.conversao_pct):.2f}%" if r.conversao_pct is not None else "—", icon="🔁", accent="#C4B5FD")
    with c4:
        _kpi_card("Interações", f"{int(r.interacoes):d}" if r.interacoes is not None else "—", icon="☎️", accent="#93c5fd")

    prior = get_last_feedback_for_seller(
        conn, r.nome, owner_user_id=owner_id, include_all=is_admin
    )
    if prior:
        reg = _fmt_created_at_local(prior.get("created_at"))
        st.caption(
            f"Evolução: último feedback **salvo** deste vendedor — análise **{prior.get('analysis_id') or '—'}**, "
            f"período **{prior.get('periodo_analise') or '—'}** (registrado {reg or '—'}). "
            "Será usado como base comparativa na geração (texto completo). "
            "Regerar na mesma análise compara com o registro imediatamente anterior."
        )
    else:
        st.caption(
            "Evolução: não existe feedback **anterior** deste vendedor no histórico. "
            "A IA informará que o comparativo com feedback passado não se aplica ainda."
        )

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
        base_prev = get_last_feedback_for_seller(
            conn, r.nome, owner_user_id=owner_id, include_all=is_admin
        )
        prompt = build_prompt_star(
            star_in,
            feedback_anterior_texto=base_prev.get("feedback_text") if base_prev else None,
            periodo_analise_anterior=base_prev.get("periodo_analise") if base_prev else None,
            feedback_anterior_registrado_em=base_prev.get("created_at") if base_prev else None,
        )
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
                texto = append_secao_simulacao_capacidade_venda(
                    star_in,
                    texto,
                )
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
            fbd = pd.DataFrame(fb)
            if not fbd.empty and "created_at" in fbd.columns:
                fbd["created_at"] = fbd["created_at"].apply(_fmt_created_at_local)
            st.dataframe(
                fbd[["created_at", "seller_name", "provider_used", "model_used"]],
                use_container_width=True,
                hide_index=True,
            )


def page_history(settings, conn) -> None:
    render_header("Histórico", "Carregue análises anteriores sem perder informação.")

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    rows_all = list_analyses(conn, limit=150, owner_user_id=owner_id, include_all=is_admin)
    # Separar histórico por tipo (sem misturar Sala de Gestão com Vendedores ou Orçamentos)
    rows_base: list = []
    rows_orc: list = []
    for r in rows_all:
        try:
            p = json.loads(r.payload_json)
        except Exception:
            p = None
        kind = p.get("_kind") if isinstance(p, dict) else None
        if not kind:
            rows_base.append(r)  # prints/excel de vendedores (bônus/performance)
        elif str(kind) == "orcamentos":
            rows_orc.append(r)

    tab_base, tab_orc = st.tabs(["Vendedores (Bônus/Performance)", "Orçamentos x Conversão"])

    with tab_base:
        if not rows_base:
            st.info("Sem histórico de vendedores neste usuário.")
        else:
            options = {
                f"#{r.id} · {r.periodo} · {_fmt_created_at_local(getattr(r, 'created_at', None))} · R$ {r.total_bonus:,.2f}": r.id
                for r in rows_base
            }
            selected = st.selectbox("Selecione uma análise", options=list(options.keys()), key="hist_base_pick")
            selected_id = int(options[selected])

            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                if st.button("📌 Tornar ativa", use_container_width=True, key="hist_base_activate"):
                    st.session_state["active_analysis_id"] = selected_id
                    st.success("Análise ativa atualizada.")
                    st.rerun()
            with c2:
                if st.button("🗑️ Apagar", use_container_width=True, key="hist_base_delete"):
                    delete_analysis(conn, selected_id, owner_user_id=owner_id, include_all=is_admin)
                    if st.session_state.get("active_analysis_id") == selected_id:
                        st.session_state.pop("active_analysis_id", None)
                    st.success("Análise apagada.")
                    st.rerun()
            with c3:
                st.caption("Dica: apagar remove o registro e os uploads vinculados (por cascata).")

            _bp = next((str(r.periodo) for r in rows_base if int(r.id) == selected_id), "")
            new_period_hist = st.text_input(
                "Renomear período / descrição",
                value=_bp,
                key=f"hist_page_rename_vend_{selected_id}",
                placeholder="ex.: Abril/2026 · Até 27/04/2026",
            )
            if st.button("💾 Salvar novo nome", use_container_width=True, key=f"hist_page_rename_save_vend_{selected_id}"):
                try:
                    if update_analysis_periodo(
                        conn,
                        selected_id,
                        new_periodo=new_period_hist,
                        owner_user_id=owner_id,
                        include_all=is_admin,
                    ):
                        st.success("Nome atualizado.")
                        st.rerun()
                    else:
                        st.error("Não foi possível salvar (texto vazio ou sem permissão).")
                except Exception as e:
                    st.error(str(e))

            row = get_analysis(conn, selected_id, owner_user_id=owner_id, include_all=is_admin)
            if row:
                st.markdown("---")
                st.subheader("Detalhe")
                st.write("**Período:**", row.periodo)
                st.write("**IA:**", f"{row.provider_used} / {row.model_used}")
                st.write("**Total bônus:**", f"R$ {row.total_bonus:,.2f}")
                st.json(json.loads(row.payload_json))

    with tab_orc:
        if not rows_orc:
            n_db = count_all_analyses(conn)
            if n_db > 0 and not is_admin:
                st.info("Sem histórico de **Orçamentos** neste usuário (pode ser permissão/owner).")
            else:
                st.info("Ainda não existe análise de **Orçamentos** salva.")
        else:
            options2 = {
                f"#{r.id} · {r.periodo} · {_fmt_created_at_local(getattr(r, 'created_at', None))}": r.id
                for r in rows_orc
            }
            selected2 = st.selectbox("Selecione uma análise (Orçamentos)", options=list(options2.keys()), key="hist_orc_pick")
            selected_id2 = int(options2[selected2])

            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                if st.button("📌 Tornar ativa (Orçamentos)", use_container_width=True, key="hist_orc_activate"):
                    st.session_state["active_orcamentos_analysis_id"] = selected_id2
                    st.session_state["dash_selector"] = "Orçamento x Conversão"
                    st.success("Análise de orçamentos ativa atualizada.")
                    st.rerun()
            with c2:
                if st.button("🗑️ Apagar (Orçamentos)", use_container_width=True, key="hist_orc_delete"):
                    delete_analysis(conn, selected_id2, owner_user_id=owner_id, include_all=is_admin)
                    if st.session_state.get("active_orcamentos_analysis_id") == selected_id2:
                        st.session_state.pop("active_orcamentos_analysis_id", None)
                    st.success("Análise apagada.")
                    st.rerun()
            with c3:
                st.caption("Dica: apagar remove o registro e os uploads vinculados (por cascata).")

            _op = next((str(r.periodo) for r in rows_orc if int(r.id) == selected_id2), "")
            new_period_orc = st.text_input(
                "Renomear período / descrição",
                value=_op,
                key=f"hist_page_rename_orc_{selected_id2}",
                placeholder="ex.: Semana 15–21/04 · Orçamentos",
            )
            if st.button("💾 Salvar novo nome", use_container_width=True, key=f"hist_page_rename_save_orc_{selected_id2}"):
                try:
                    if update_analysis_periodo(
                        conn,
                        selected_id2,
                        new_periodo=new_period_orc,
                        owner_user_id=owner_id,
                        include_all=is_admin,
                    ):
                        st.success("Nome atualizado.")
                        st.rerun()
                    else:
                        st.error("Não foi possível salvar (texto vazio ou sem permissão).")
                except Exception as e:
                    st.error(str(e))

            row2 = get_analysis(conn, selected_id2, owner_user_id=owner_id, include_all=is_admin)
            if row2:
                st.markdown("---")
                st.subheader("Detalhe (Orçamentos)")
                st.write("**Período:**", row2.periodo)
                st.write("**IA:**", f"{row2.provider_used} / {row2.model_used}")
                st.json(json.loads(row2.payload_json))


def page_insights(settings, conn) -> None:
    render_header("Insights", "IA gera recomendações e pontos de atenção do time.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico** ou crie em **Upload**.")
        return

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
    if not row:
        st.warning("Análise não encontrada.")
        return

    def default_provider_index() -> int:
        if settings.google_api_key and settings.openai_api_key:
            return 0
        if settings.google_api_key:
            return 1
        return 2

    with st.expander("Análise com IA (expandir/minimizar)", expanded=False):
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
        totais = payload.get("totais") if isinstance(payload, dict) else None
        if not isinstance(totais, dict):
            totais = {}
        dados_json = json.dumps(
            {
                "periodo": row.periodo,
                "total_bonus": total,
                # Importante: `totais.meta_total` pode refletir a linha TOTAL do print (time completo).
                # sem que eles apareçam em vendedores/dashboards.
                "totais": totais,
                "vendedores": df.to_dict(orient="records") if not df.empty else [],
            },
            ensure_ascii=False,
            indent=2,
        )
        prompt = PROMPT_INSIGHTS.format(dados_json=dados_json)

        import html as _html

        def _kpi_card(title: str, value: str, *, icon: str, accent: str) -> None:
            st.markdown(
                f"""
<div class="dp-card" style="
  padding:12px 12px;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
</div>
""",
                unsafe_allow_html=True,
            )

        # Visual rápido (antes de gerar IA)
        if not df.empty:
            st.markdown("### Visão rápida (performance)")
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            with k1:
                _kpi_card(
                    "NFs (time)",
                    f"{int(pd.to_numeric(df.get('qtd_faturadas'), errors='coerce').fillna(0).sum())}",
                    icon="📦",
                    accent="#93c5fd",
                )
            with k2:
                _kpi_card(
                    "Faturamento (time)",
                    f"R$ {float(pd.to_numeric(df.get('faturamento'), errors='coerce').fillna(0).sum()):,.2f}",
                    icon="💰",
                    accent="#6EE7B7",
                )
            with k3:
                _kpi_card(
                    "Interações (time)",
                    f"{int(pd.to_numeric(df.get('interacoes'), errors='coerce').fillna(0).sum())}",
                    icon="☎️",
                    accent="#C4B5FD",
                )
            conv = pd.to_numeric(df.get("conversao_pct"), errors="coerce").dropna()
            with k4:
                _kpi_card(
                    "Conversão (média)",
                    f"{float(conv.mean()):.2f}%" if len(conv) else "—",
                    icon="🔁",
                    accent="#FBBF24",
                )
            marg = pd.to_numeric(df.get("margem_pct"), errors="coerce").dropna()
            with k5:
                _kpi_card(
                    "Margem (média)",
                    f"{float(marg.mean()):.2f}%" if len(marg) else "—",
                    icon="📊",
                    accent="#A7F3D0",
                )
            # Desconto (do arquivo Qtd Faturadas)
            dp = [float(getattr(s, "desconto_pct", 0.0)) for s in sellers if getattr(s, "desconto_pct", None) is not None]
            with k6:
                _kpi_card(
                    "Desconto (médio)",
                    f"{(sum(dp)/len(dp)):.2f}%" if dp else "—",
                    icon="🏷",
                    accent="#93c5fd",
                )

            try:
                import plotly.express as px

                st.markdown("### Gráficos")
                c1, c2 = st.columns(2)
                with c1:
                    if "faturamento" in df.columns:
                        fig = px.bar(df, x="nome", y="faturamento", title="Faturamento por vendedor")
                        fig.update_traces(marker_color="rgba(110,231,183,0.75)", marker_line_color="rgba(255,255,255,0.12)", marker_line_width=1)
                        fig.update_layout(
                            height=340,
                            template="plotly_dark",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=10, r=10, t=60, b=10),
                            font=dict(color="#E5E7EB"),
                            title_font=dict(size=18),
                            bargap=0.25,
                        )
                        fig.update_xaxes(showgrid=False, tickangle=-30)
                        fig.update_yaxes(showgrid=True, gridcolor="rgba(148,163,184,0.12)", zeroline=False)
                        st.plotly_chart(fig, use_container_width=True, key="ins_perf_faturamento_bar")
                with c2:
                    if "qtd_faturadas" in df.columns:
                        fig = px.bar(df, x="nome", y="qtd_faturadas", title="NFs (Qtd. faturadas) por vendedor")
                        fig.update_traces(marker_color="rgba(59,130,246,0.70)", marker_line_color="rgba(255,255,255,0.12)", marker_line_width=1)
                        fig.update_layout(
                            height=340,
                            template="plotly_dark",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=10, r=10, t=60, b=10),
                            font=dict(color="#E5E7EB"),
                            title_font=dict(size=18),
                            bargap=0.25,
                        )
                        fig.update_xaxes(showgrid=False, tickangle=-30)
                        fig.update_yaxes(showgrid=True, gridcolor="rgba(148,163,184,0.12)", zeroline=False)
                        st.plotly_chart(fig, use_container_width=True, key="ins_perf_nfs_bar")

                c3, c4 = st.columns(2)
                with c3:
                    if "ticket_medio" in df.columns:
                        fig = px.bar(df, x="nome", y="ticket_medio", title="Ticket médio por vendedor")
                        fig.update_traces(marker_color="rgba(251,191,36,0.75)", marker_line_color="rgba(255,255,255,0.12)", marker_line_width=1)
                        fig.update_layout(
                            height=340,
                            template="plotly_dark",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=10, r=10, t=60, b=10),
                            font=dict(color="#E5E7EB"),
                            title_font=dict(size=18),
                            bargap=0.25,
                        )
                        fig.update_xaxes(showgrid=False, tickangle=-30)
                        fig.update_yaxes(showgrid=True, gridcolor="rgba(148,163,184,0.12)", zeroline=False)
                        st.plotly_chart(fig, use_container_width=True, key="ins_perf_ticket_bar")
                with c4:
                    if "conversao_pct" in df.columns and "interacoes" in df.columns:
                        # px.scatter(size=...) exige numérico >= 0; garantir conversão antes do plot.
                        df_plot = df.copy()
                        if "qtd_faturadas" in df_plot.columns:
                            df_plot["qtd_faturadas"] = (
                                pd.to_numeric(df_plot["qtd_faturadas"], errors="coerce")
                                .fillna(0)
                                .clip(lower=0)
                                .astype(float)
                            )
                        fig = px.scatter(
                            df_plot,
                            x="interacoes",
                            y="conversao_pct",
                            size="qtd_faturadas" if "qtd_faturadas" in df_plot.columns else None,
                            color="elegivel_margem" if "elegivel_margem" in df_plot.columns else None,
                            hover_name="nome",
                            title="Interações x Conversão (bolha = NFs)",
                        )
                        fig.update_traces(marker=dict(line=dict(width=1, color="rgba(255,255,255,0.16)")))
                        fig.update_layout(
                            height=340,
                            template="plotly_dark",
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=10, r=10, t=60, b=10),
                            font=dict(color="#E5E7EB"),
                            title_font=dict(size=18),
                        )
                        fig.update_xaxes(showgrid=True, gridcolor="rgba(148,163,184,0.12)", zeroline=False)
                        fig.update_yaxes(showgrid=True, gridcolor="rgba(148,163,184,0.12)", zeroline=False)
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
                def _style_priority(s: pd.Series) -> list[str]:
                    out: list[str] = []
                    for v in s.astype(str).fillna("").tolist():
                        vv = v.strip().lower()
                        if vv.startswith("alta"):
                            out.append("background-color: rgba(251,113,133,.14); color:#fecdd3; font-weight:900;")
                        elif vv.startswith("m"):
                            out.append("background-color: rgba(251,191,36,.14); color:#fde68a; font-weight:900;")
                        else:
                            out.append("background-color: rgba(110,231,183,.12); color:#bbf7d0; font-weight:850;")
                    return out

                def _style_motivos(s: pd.Series) -> list[str]:
                    out: list[str] = []
                    for v in s.astype(str).fillna("").tolist():
                        out.append("color:#E5E7EB; font-weight:650;")
                    return out

                pr_show = pr.copy()
                # deixa a coluna de motivos mais amigável: quebra por " | "
                if "Motivos (curto)" in pr_show.columns:
                    pr_show["Motivos (curto)"] = pr_show["Motivos (curto)"].astype(str).str.replace(" | ", "\n", regex=False)

                styled = (
                    pr_show.style.apply(_style_priority, subset=["Prioridade"])
                    .apply(_style_motivos, subset=["Motivos (curto)"])
                    .set_properties(subset=["Vendedor"], **{"font-weight": "800", "color": "#E5E7EB"})
                )
                st.dataframe(styled, use_container_width=True, hide_index=True)

            # PDF (IA)
            try:
                txt = json.dumps(ins.get("data") or {}, ensure_ascii=False, indent=2)
                pdf_bytes = _build_text_pdf_bytes(
                    title=f"Análise com IA — {row.periodo}",
                    text=txt,
                )
                st.download_button(
                    "⬇️ Baixar análise (PDF)",
                    data=pdf_bytes,
                    file_name="analise_ia_insights.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="insights_pdf_btn",
                )
            except Exception as e:
                st.caption(f"PDF indisponível: {e}")
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
    if not df.empty and "desconto_pct" in df.columns:
        disc = pd.to_numeric(df["desconto_pct"], errors="coerce").dropna()
    else:
        disc = pd.Series([], dtype=float)

    return {
        "periodo": periodo,
        "n_vendedores": int(len(df)) if not df.empty else 0,
        "tot_faturamento": tot_fat,
        "tot_nfs": tot_nf,
        "tot_interacoes": tot_inter,
        "media_ticket": float(ticket.mean()) if len(ticket) else None,
        "media_conversao": float(conv.mean()) if len(conv) else None,
        "media_margem": float(marg.mean()) if len(marg) else None,
        "media_desconto": float(disc.mean()) if len(disc) else None,
        "total_bonus": float(total_bonus),
        "vendedores": df.to_dict(orient="records") if not df.empty else [],
    }


def _extract_date_label_from_periodo(periodo: str, created_at: str | None) -> tuple[str, str]:
    """
    Retorna (date_key_iso, date_label_br) para ordenar e exibir no eixo X.
    - Prioriza dd/mm/aaaa no texto do `periodo` (ex.: "23/04/2026 - Atualizado")
    - Fallback: usa `created_at` (YYYY-mm-ddTHH:MM:SS...)
    """
    import re

    p = str(periodo or "").strip()
    m = re.search(r"(?<!\d)(\d{2})/(\d{2})/(\d{4})(?!\d)", p)
    if m:
        dd, mm, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f"{yy:04d}-{mm:02d}-{dd:02d}", f"{dd:02d}/{mm:02d}/{yy:04d}"
    s = str(created_at or "").strip()
    m2 = re.search(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)", s)
    if m2:
        yy, mm, dd = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        return f"{yy:04d}-{mm:02d}-{dd:02d}", f"{dd:02d}/{mm:02d}/{yy:04d}"
    return "0000-00-00", "—"


def page_highlights(settings, conn) -> None:
    render_header("Highlight semanal e mensal", "Leitura profunda e gráficos do período e do histórico.")

    analysis_id = st.session_state.get("active_analysis_id")
    if analysis_id is None:
        st.info("Nenhuma análise ativa. Carregue uma no **Histórico**.")
        return

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    row = get_analysis(conn, int(analysis_id), owner_user_id=owner_id, include_all=is_admin)
    if not row:
        st.warning("Análise não encontrada.")
        return

    payload = json.loads(row.payload_json)
    base = _extract_perf_summary_from_payload(row.periodo, payload)
    sellers = parse_sellers(payload)
    df = pd.DataFrame(base.get("vendedores") or [])

    # Cards do período atual
    st.markdown("### Período atual (análise ativa)")
    import html as _html

    def _kpi_card(title: str, value: str, *, icon: str, accent: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:12px 12px;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    dp = [float(getattr(s, "desconto_pct", 0.0)) for s in sellers if getattr(s, "desconto_pct", None) is not None]
    disc_txt = f"{(sum(dp)/len(dp)):.2f}%" if dp else "—"

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        _kpi_card("Faturamento (time)", f"R$ {float(base['tot_faturamento']):,.2f}", icon="💰", accent="#6EE7B7")
    with c2:
        _kpi_card("NFs (time)", f"{int(base['tot_nfs'])}", icon="📦", accent="#93c5fd")
    with c3:
        _kpi_card("Interações (time)", f"{int(base['tot_interacoes'])}", icon="☎️", accent="#C4B5FD")
    with c4:
        _kpi_card("Ticket médio (média)", f"R$ {base['media_ticket']:,.2f}" if base.get("media_ticket") else "—", icon="🧾", accent="#FBBF24")
    with c5:
        _kpi_card("Desconto (médio)", disc_txt, icon="🏷", accent="#93c5fd")

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
    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"
    # Usar o mesmo conjunto de "análises base" que aparece no Histórico rápido:
    # filtra registros auxiliares com `_kind` (KPIs/Dept/Radar/etc.)
    rows_all = list_analyses(conn, limit=200, owner_user_id=owner_id, include_all=is_admin)
    base_rows = []
    for r in rows_all:
        try:
            p0 = json.loads(r.payload_json)
        except Exception:
            p0 = None
        kind0 = p0.get("_kind") if isinstance(p0, dict) else None
        if not kind0:
            base_rows.append(r)

    if len(base_rows) < 2:
        st.info("Salve mais análises para habilitar tendência semanal/mensal.")
        return

    hist: list[dict] = []
    for r in reversed(base_rows):
        try:
            p = json.loads(r.payload_json)
        except Exception:
            continue
        date_key, date_label = _extract_date_label_from_periodo(str(r.periodo), str(r.created_at))
        hist.append(
            _extract_perf_summary_from_payload(r.periodo, p)
            | {
                "id": r.id,
                "created_at": r.created_at,
                "date_key": date_key,
                "date_label": date_label,
            }
        )
    hdf = pd.DataFrame(hist)
    if hdf.empty:
        st.info("Não consegui montar o histórico.")
        return
    try:
        hdf = hdf.sort_values(["date_key", "id"]).reset_index(drop=True)
    except Exception:
        pass

    # “Semanal” = últimas N análises disponíveis (até 4), “Mensal” = até 12
    n_week = int(min(4, len(hdf)))
    n_month = int(min(12, len(hdf)))
    lastw = hdf.tail(n_week)
    lastm = hdf.tail(n_month)

    tab_w, tab_m, tab_ai = st.tabs(
        [f"Highlight semanal (últimas {n_week})", f"Highlight mensal (últimas {n_month})", "Análise profunda (IA)"]
    )

    def _render_trend(sub: pd.DataFrame, title: str) -> None:
        st.markdown(f"### {title}")
        try:
            import plotly.express as px

            c1, c2 = st.columns(2)
            with c1:
                fig = px.line(sub, x="date_label", y="media_desconto", markers=True, title="Desconto médio (time)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_faturamento_{title}")
            with c2:
                fig = px.line(sub, x="date_label", y="tot_nfs", markers=True, title="NFs do time")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_nfs_{title}")

            c3, c4 = st.columns(2)
            with c3:
                fig = px.line(sub, x="date_label", y="media_ticket", markers=True, title="Ticket médio (média)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key=f"hl_trend_ticket_{title}")
            with c4:
                fig = px.line(sub, x="date_label", y="media_conversao", markers=True, title="Conversão (média)")
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
                    "date_label",
                    "media_desconto",
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
        _render_trend(lastw, f"Últimas {n_week} análises (aprox. semanal)")
    with tab_m:
        _render_trend(lastm, f"Últimas {n_month} análises (aprox. mensal)")

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
            try:
                pdf_bytes = _build_text_pdf_bytes(
                    title=f"Análise profunda (IA) — {str(base.get('periodo') or row.periodo)}",
                    text=str(t.get("t") or ""),
                )
                st.download_button(
                    "⬇️ Baixar análise (PDF)",
                    data=pdf_bytes,
                    file_name="analise_ia_highlight.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="hl_ai_pdf_btn",
                )
            except Exception as e:
                st.caption(f"PDF indisponível: {e}")
        else:
            st.info("Clique em **Gerar análise profunda**.")


def page_sala_gestao(settings, conn, *, show_header: bool = True) -> None:
    if show_header:
        render_header("Sala de Gestão", "Reunião diária: projeção, evolução, vendedores e departamentos.")

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"

    # --- Departamentos (Sala de Gestão): helpers compartilhados entre abas ---
    def _dept_norm(name: object) -> str:
        s = str(name or "").strip().lower()
        s = (
            s.replace("á", "a")
            .replace("ã", "a")
            .replace("â", "a")
            .replace("à", "a")
            .replace("é", "e")
            .replace("ê", "e")
            .replace("è", "e")
            .replace("í", "i")
            .replace("ì", "i")
            .replace("ó", "o")
            .replace("ô", "o")
            .replace("õ", "o")
            .replace("ò", "o")
            .replace("ú", "u")
            .replace("ù", "u")
            .replace("ç", "c")
        )
        s = " ".join(s.split())
        return s

    def _dept_ok(name: object) -> bool:
        s = _dept_norm(name)
        if not s:
            return False
        if s.startswith("filtros aplicados") or "filtros aplicados" in s:
            return False
        if s == "nan":
            return False
        if s == "outros":
            return False
        if s.startswith("paineis eletric") or s.startswith("painel eletric"):
            return False
        return True

    def _ensure_participacao_pct(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        if "participacao_pct" in df.columns and pd.to_numeric(df["participacao_pct"], errors="coerce").notna().any():
            return df
        if "faturamento" not in df.columns:
            return df
        fat_s = pd.to_numeric(df.get("faturamento"), errors="coerce")
        total = float(fat_s.dropna().sum() or 0.0)
        if total <= 0:
            return df
        out = df.copy()
        out["participacao_pct"] = (fat_s / total) * 100.0
        return out

    def _dept_fingerprint(p: dict) -> tuple[int, float]:
        try:
            rows = p.get("departamentos") or []
            dfp = pd.DataFrame([d for d in rows if _dept_ok((d or {}).get("departamento"))])
            if dfp.empty:
                return (0, 0.0)
            fat = pd.to_numeric(dfp.get("faturamento"), errors="coerce")
            return (int(len(dfp)), float(fat.dropna().sum() or 0.0))
        except Exception:
            return (0, 0.0)

    def _pick_prev_dept_payload(current: dict | None) -> dict | None:
        if not isinstance(current, dict):
            return None
        cur_fp = _dept_fingerprint(current)
        rows = list_analyses(conn, limit=140, owner_user_id=owner_id, include_all=is_admin)
        for r in rows:
            try:
                p = json.loads(r.payload_json)
            except Exception:
                continue
            if not (isinstance(p, dict) and p.get("_kind") == "sala_gestao_departamentos"):
                continue
            if _dept_fingerprint(p) == cur_fp:
                continue
            return p
        return None

    cal = st.session_state.get("calendar_info") or {}
    dias_restantes = int(cal.get("dias_uteis_restantes") or 0)

    tab_consol, tab_kpis, tab_evol, tab_rel, tab_vend, tab_dept, tab_radar = st.tabs(
        ["Consolidado", "Projeção e KPIs", "Evolução dia a dia", "Relatório executivo", "Vendedores", "Departamentos", "Radar (manual)"]
    )

    def _last_payload_of_kind(kind: str) -> dict | None:
        rows = list_analyses(conn, limit=80, owner_user_id=owner_id, include_all=is_admin)
        for r in rows:
            try:
                p = json.loads(r.payload_json)
            except Exception:
                continue
            if isinstance(p, dict) and p.get("_kind") == kind:
                return p
        return None

    def _last_payloads_of_kind(kind: str, n: int) -> list[dict]:
        rows = list_analyses(conn, limit=120, owner_user_id=owner_id, include_all=is_admin)
        out: list[dict] = []
        for r in rows:
            try:
                p = json.loads(r.payload_json)
            except Exception:
                continue
            if isinstance(p, dict) and p.get("_kind") == kind:
                out.append(p)
                if len(out) >= n:
                    break
        return out

    with tab_consol:
        st.markdown("### Visão consolidada (tudo em um só lugar)")
        st.caption("Use esta visão para a reunião — KPIs, vendedores, departamentos e radar no mesmo painel.")

        active_id = st.session_state.get("active_analysis_id")
        payload_base: dict | None = None
        if active_id is not None:
            r0 = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
            if r0:
                try:
                    payload_base = json.loads(r0.payload_json)
                except Exception:
                    payload_base = None

        # Se a análise ativa tiver base diária salva, usa ela (evita "travamento" ao trocar análise).
        # Se não tiver, tenta herdar automaticamente da análise anterior (quando existir), para
        # manter "dia anterior" e séries diárias funcionando ao alternar o histórico.
        def _try_load_prev_sg_daily_into_session(current_row) -> None:
            try:
                cur_key, cur_label = _extract_date_label_from_periodo(
                    str(getattr(current_row, "periodo", "") or ""),
                    str(getattr(current_row, "created_at", "") or ""),
                )
            except Exception:
                cur_key, cur_label = "0000-00-00", "—"

            if not cur_key or cur_key == "0000-00-00":
                return

            try:
                rows = list_analyses(conn, limit=500, owner_user_id=owner_id, include_all=is_admin)
            except Exception:
                return

            best_payload = None
            best_row = None
            best_key = None

            cur_id = int(getattr(current_row, "id", 0) or 0)
            for rr in rows:
                rid = int(getattr(rr, "id", 0) or 0)
                if rid == cur_id:
                    continue
                try:
                    p = json.loads(getattr(rr, "payload_json", "") or "")
                except Exception:
                    continue
                if not isinstance(p, dict):
                    continue
                if str(p.get("_kind") or ""):
                    continue
                # Só herdar de análises "base" com vendedores.
                if not parse_sellers(p):
                    continue
                sd = p.get("_sg_daily")
                if not (isinstance(sd, dict) and isinstance(sd.get("rows"), list) and len(sd.get("rows") or []) > 0):
                    continue
                try:
                    rk, rlabel = _extract_date_label_from_periodo(
                        str(getattr(rr, "periodo", "") or ""),
                        str(getattr(rr, "created_at", "") or ""),
                    )
                except Exception:
                    rk, rlabel = "0000-00-00", "—"
                if not rk or rk == "0000-00-00" or rk >= cur_key:
                    continue

                if best_key is None or str(rk) > str(best_key):
                    best_payload, best_row, best_key = p, rr, rk

            if not (isinstance(best_payload, dict) and isinstance(best_payload.get("_sg_daily"), dict)):
                return

            sd = best_payload.get("_sg_daily") or {}
            rows_daily = sd.get("rows")
            if not (isinstance(rows_daily, list) and rows_daily):
                return

            st.session_state["sg_daily_df"] = pd.DataFrame(rows_daily)
            st.session_state["sg_daily_meta"] = sd.get("meta") if isinstance(sd.get("meta"), dict) else {}
            src = sd.get("source")
            src_txt = str(src) if src else "Base diária (histórico)"
            try:
                _, best_label = _extract_date_label_from_periodo(
                    str(getattr(best_row, "periodo", "") or ""),
                    str(getattr(best_row, "created_at", "") or ""),
                )
            except Exception:
                best_label = "—"
            st.session_state["sg_daily_source_name"] = f"{src_txt} (herdado de {best_label})"
            st.session_state["sg_daily_scope_id"] = int(getattr(best_row, "id", 0) or 0)
            st.session_state["sg_daily_inherited_from"] = {"date_key": str(best_key), "date_label": str(best_label), "from_id": int(getattr(best_row, "id", 0) or 0), "to": str(cur_label)}

        try:
            if isinstance(payload_base, dict) and isinstance(payload_base.get("_sg_daily"), dict):
                sd = payload_base.get("_sg_daily") or {}
                rows = sd.get("rows")
                if isinstance(rows, list) and rows:
                    st.session_state["sg_daily_df"] = pd.DataFrame(rows)
                    st.session_state["sg_daily_meta"] = sd.get("meta") if isinstance(sd.get("meta"), dict) else {}
                    if sd.get("source"):
                        st.session_state["sg_daily_source_name"] = sd.get("source")
                    if active_id is not None:
                        try:
                            st.session_state["sg_daily_scope_id"] = int(active_id)
                        except Exception:
                            pass
            elif active_id is not None:
                st.session_state.pop("sg_daily_df", None)
                st.session_state.pop("sg_daily_meta", None)
                st.session_state.pop("sg_daily_source_name", None)
                st.session_state.pop("sg_daily_scope_id", None)
                st.session_state.pop("sg_daily_inherited_from", None)
                if r0 is not None:
                    _try_load_prev_sg_daily_into_session(r0)
        except Exception:
            pass

        # Se a análise ativa tiver departamentos salvos, usa eles (evita pedir upload ao trocar histórico)
        try:
            if isinstance(payload_base, dict) and isinstance(payload_base.get("_sg_dept"), dict):
                dd = payload_base.get("_sg_dept") or {}
                deps = dd.get("departamentos")
                if isinstance(deps, list) and deps:
                    st.session_state["dept_payload"] = {"departamentos": deps}
                    if isinstance(dd.get("meta"), dict):
                        st.session_state["dept_meta"] = dd.get("meta")
                    if dd.get("source"):
                        st.session_state["dept_source_name"] = dd.get("source")
        except Exception:
            pass

        # Fallback (históricos antigos): buscar base de deptos salva com parent_analysis_id = análise ativa
        try:
            if st.session_state.get("dept_payload") is None and active_id is not None:
                r = conn.execute(
                    "SELECT payload_json FROM analyses WHERE parent_analysis_id = ? ORDER BY id DESC LIMIT 1",
                    (int(active_id),),
                ).fetchone()
                if r and r[0]:
                    try:
                        p2 = json.loads(str(r[0]))
                    except Exception:
                        p2 = None
                    if isinstance(p2, dict) and p2.get("_kind") == "sala_gestao_departamentos":
                        deps2 = p2.get("departamentos")
                        if isinstance(deps2, list) and deps2:
                            st.session_state["dept_payload"] = {"departamentos": deps2}
        except Exception:
            pass

        totais = (payload_base or {}).get("totais") if isinstance(payload_base, dict) else {}
        if not isinstance(totais, dict):
            totais = {}

        cal = st.session_state.get("calendar_info") or {}
        dias_restantes = int(cal.get("dias_uteis_restantes") or 0)

        # Meta geral do time deve vir do payload/totais (linha TOTAL / consolidado).
        # O Excel "Faturamento e Atendidos" serve para KPIs diários (dia anterior), não para meta geral.
        fat_atual = float(totais.get("faturamento_total") or 0.0)
        meta_total = float(totais.get("meta_total") or 0.0)
        # Regra: acumulado soma todos os dias da base diária (inclui sábado, se existir).
        # "Dia anterior" = último dia com movimento na ordem cronológica do arquivo; se não houver
        # linha com fat/NFS/clientes > 0, usa o último dia do arquivo (ex.: fechamento em 28/04).
        sat_note = None
        daily_roll = None  # {"acc_fat","acc_nf","acc_cli","last_bus":{...},"prev_bus":{...},"sat":{...},"df"}
        try:
            daily = st.session_state.get("sg_daily_df")
            meta_d = st.session_state.get("sg_daily_meta") if isinstance(st.session_state.get("sg_daily_meta"), dict) else {}
            if isinstance(daily, pd.DataFrame) and not daily.empty and "dia" in daily.columns:
                import datetime as _dt
                import re as _re

                cal_now = st.session_state.get("calendar_info") if isinstance(st.session_state.get("calendar_info"), dict) else {}
                expected_yy = int(cal_now.get("ano") or _dt.date.today().year)
                expected_mm = int(cal_now.get("mes") or _dt.date.today().month)

                periodo_txt = ""
                if isinstance(payload_base, dict):
                    periodo_txt = str(payload_base.get("periodo") or "").strip()
                if not periodo_txt and active_id is not None:
                    try:
                        ar0 = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
                        if ar0:
                            periodo_txt = str(getattr(ar0, "periodo", "") or "").strip()
                    except Exception:
                        pass
                iso_anal = _extract_ref_date_iso_from_periodo(periodo_txt)
                anal_yy = int(iso_anal[:4]) if iso_anal else None
                anal_mm = int(iso_anal[5:7]) if iso_anal else None

                has_embedded_daily = bool(
                    isinstance(payload_base, dict)
                    and isinstance(payload_base.get("_sg_daily"), dict)
                    and isinstance((payload_base.get("_sg_daily") or {}).get("rows"), list)
                    and len((payload_base.get("_sg_daily") or {}).get("rows") or []) > 0
                )

                mes_ref = str(meta_d.get("mes_referencia") or "").strip().lower()
                today = _dt.date.today()
                yy, mm = today.year, today.month
                m = _re.search(r"(?<!\\d)(\\d{1,2})\\s*[/\\-]\\s*(\\d{2,4})(?!\\d)", mes_ref)
                if m:
                    mm = int(m.group(1))
                    yy = int(m.group(2))
                    if yy < 100:
                        yy += 2000
                else:
                    m2 = _re.search(r"(?<!\\d)(\\d{4})\\s*[/\\-]\\s*(\\d{1,2})(?!\\d)", mes_ref)
                    if m2:
                        yy = int(m2.group(1))
                        mm = int(m2.group(2))

                month_matches_cal = int(yy) == int(expected_yy) and int(mm) == int(expected_mm)
                month_matches_analysis = (
                    anal_yy is not None
                    and anal_mm is not None
                    and int(yy) == int(anal_yy)
                    and int(mm) == int(anal_mm)
                )
                try:
                    scope_id = int(st.session_state.get("sg_daily_scope_id")) if st.session_state.get("sg_daily_scope_id") is not None else None
                except Exception:
                    scope_id = None
                daily_scope_ok = scope_id is not None and active_id is not None and scope_id == int(active_id)
                use_daily = month_matches_cal or month_matches_analysis or has_embedded_daily or daily_scope_ok

                if not use_daily:
                    daily_roll = None
                    sat_note = None
                else:
                    d0 = daily.copy().sort_values("dia").reset_index(drop=True)
                    d0["dia"] = pd.to_numeric(d0["dia"], errors="coerce")
                    d0 = d0[d0["dia"].notna()].copy()
                    d0["dia"] = d0["dia"].astype(int)
                    d0["_weekday"] = d0["dia"].apply(lambda dd: _dt.date(int(yy), int(mm), int(dd)).weekday())

                    acc_fat = float(pd.to_numeric(d0.get("faturamento"), errors="coerce").fillna(0.0).sum())
                    acc_nf = int(pd.to_numeric(d0.get("nfs_emitidas"), errors="coerce").fillna(0).sum())
                    acc_cli = int(pd.to_numeric(d0.get("clientes_atendidos"), errors="coerce").fillna(0).sum())

                    sat = d0[d0["_weekday"] == 5].copy()
                    if not sat.empty:
                        sat_move = sat[
                            (pd.to_numeric(sat.get("faturamento"), errors="coerce").fillna(0.0) > 0.0)
                            | (pd.to_numeric(sat.get("nfs_emitidas"), errors="coerce").fillna(0) > 0)
                            | (pd.to_numeric(sat.get("clientes_atendidos"), errors="coerce").fillna(0) > 0)
                        ]
                        if not sat_move.empty:
                            sat_last = sat_move.sort_values("dia").iloc[-1]
                            sat_note = {
                                "dia": int(sat_last.get("dia") or 0),
                                "fat": float(pd.to_numeric(sat_last.get("faturamento"), errors="coerce") or 0.0),
                                "nf": int(pd.to_numeric(sat_last.get("nfs_emitidas"), errors="coerce") or 0),
                                "cli": int(pd.to_numeric(sat_last.get("clientes_atendidos"), errors="coerce") or 0),
                            }

                    fat_s = pd.to_numeric(d0.get("faturamento"), errors="coerce").fillna(0.0)
                    nf_s = pd.to_numeric(d0.get("nfs_emitidas"), errors="coerce").fillna(0)
                    cli_s = pd.to_numeric(d0.get("clientes_atendidos"), errors="coerce").fillna(0)
                    move = (fat_s > 0) | (nf_s > 0) | (cli_s > 0)
                    d_mov = d0[move].copy() if bool(move.any()) else d0
                    bb = d_mov.iloc[-1]
                    pb = d_mov.iloc[-2] if len(d_mov) >= 2 else None
                    daily_roll = {
                        "acc_fat": acc_fat,
                        "acc_nf": acc_nf,
                        "acc_cli": acc_cli,
                        "last_bus": {
                            "dia": int(bb.get("dia") or 0),
                            "fat": float(pd.to_numeric(bb.get("faturamento"), errors="coerce") or 0.0),
                            "nf": int(pd.to_numeric(bb.get("nfs_emitidas"), errors="coerce") or 0),
                            "cli": int(pd.to_numeric(bb.get("clientes_atendidos"), errors="coerce") or 0),
                        },
                        "prev_bus": (
                            {
                                "dia": int(pb.get("dia") or 0),
                                "fat": float(pd.to_numeric(pb.get("faturamento"), errors="coerce") or 0.0),
                                "nf": int(pd.to_numeric(pb.get("nfs_emitidas"), errors="coerce") or 0),
                                "cli": int(pd.to_numeric(pb.get("clientes_atendidos"), errors="coerce") or 0),
                            }
                            if pb is not None
                            else None
                        ),
                        "df": d0,
                    }
                    if sat_note:
                        daily_roll["sat"] = dict(sat_note)
        except Exception:
            sat_note = sat_note

        if sat_note:
            st.markdown(
                f"""
<div class="dp-pill" style="
  background:rgba(251,191,36,.12);
  border-color:rgba(251,191,36,.35);
  color:#FBBF24;
  font-weight:850;
  margin-top:6px;
  margin-bottom:8px;
">
  Houve resultado no <b>sábado (dia {int(sat_note['dia']):02d})</b>: <b>R$ {float(sat_note['fat']):,.2f}</b> • NFS <b>{int(sat_note['nf'])}</b> • Atendidos <b>{int(sat_note['cli'])}</b>.
</div>
""",
                unsafe_allow_html=True,
            )
            # pills com valores do sábado + setas vs última sexta (dia útil)
            try:
                if isinstance(daily_roll, dict) and isinstance(daily_roll.get("last_bus"), dict):
                    lb = daily_roll["last_bus"]
                    sat_f = float(sat_note.get("fat") or 0.0)
                    sat_nf = int(sat_note.get("nf") or 0)
                    sat_cli = int(sat_note.get("cli") or 0)
                    fri_f = float(lb.get("fat") or 0.0)
                    fri_nf = int(lb.get("nf") or 0)
                    fri_cli = int(lb.get("cli") or 0)

                    def _arrow_delta(cur: float, ref: float, *, kind: str) -> str:
                        diff = float(cur) - float(ref)
                        if abs(diff) < 1e-9:
                            return "→ 0"
                        arrow = "▲" if diff > 0 else "▼"
                        if kind == "money":
                            return f"{arrow} R$ {abs(diff):,.2f}"
                        return f"{arrow} {abs(diff):,.0f}"

                    d_f = _arrow_delta(sat_f, fri_f, kind="money")
                    d_nf = _arrow_delta(float(sat_nf), float(fri_nf), kind="int")
                    d_cli = _arrow_delta(float(sat_cli), float(fri_cli), kind="int")
                    st.markdown(
                        f"""
<div style="margin-top:-2px;margin-bottom:8px;display:flex;gap:8px;flex-wrap:wrap;">
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    Sáb: <b>R$ {sat_f:,.2f}</b> <span style="color:#94a3b8;font-weight:700">({d_f})</span>
  </span>
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    NFS <b>{sat_nf}</b> <span style="color:#94a3b8;font-weight:700">({d_nf})</span>
  </span>
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    Atend. <b>{sat_cli}</b> <span style="color:#94a3b8;font-weight:700">({d_cli})</span>
  </span>
</div>
""",
                        unsafe_allow_html=True,
                    )
            except Exception:
                pass

        falta_meta = max(0.0, meta_total - fat_atual) if meta_total > 0 else 0.0
        falta_por_dia = (falta_meta / dias_restantes) if dias_restantes > 0 else None
        perc_meta = (fat_atual / meta_total * 100.0) if meta_total > 0 else None

        # Comparativos (setas) — vs último registro salvo de KPIs (que carrega um snapshot de totais)
        prevs_kpis = _last_payloads_of_kind("sala_gestao_kpis", 2)
        prev0 = prevs_kpis[0] if len(prevs_kpis) >= 1 else {}
        prev1 = prevs_kpis[1] if len(prevs_kpis) >= 2 else {}
        prev0_totais = prev0.get("totais") if isinstance(prev0, dict) else {}
        prev1_k = prev1.get("kpis") if isinstance(prev1, dict) else {}
        prev0_k = prev0.get("kpis") if isinstance(prev0, dict) else {}
        if not isinstance(prev0_totais, dict):
            prev0_totais = {}
        if not isinstance(prev0_k, dict):
            prev0_k = {}
        if not isinstance(prev1_k, dict):
            prev1_k = {}
        # compat: usado no bloco de insights (IA) abaixo
        prev_k = prev0_k

        prev_fat_total = float(prev0_totais.get("faturamento_total") or 0.0) if prev0_totais else None
        prev_meta_total = float(prev0_totais.get("meta_total") or 0.0) if prev0_totais else None
        prev_falta_meta = (
            max(0.0, float(prev_meta_total or 0.0) - float(prev_fat_total or 0.0))
            if prev_meta_total is not None and prev_meta_total > 0 and prev_fat_total is not None
            else None
        )
        prev_falta_por_dia = (
            (prev_falta_meta / dias_restantes) if (prev_falta_meta is not None and dias_restantes > 0) else None
        )
        prev_perc_meta = (
            (float(prev_fat_total) / float(prev_meta_total) * 100.0)
            if (prev_fat_total is not None and prev_meta_total is not None and prev_meta_total > 0)
            else None
        )

        def _fmt_delta_with_pct(delta: float | None, prev_val: float | None, is_money: bool) -> str | None:
            if delta is None or prev_val is None:
                return None
            try:
                d = float(delta)
                p = float(prev_val)
            except Exception:
                return None
            if abs(p) < 1e-9:
                return f"R$ {d:,.2f}" if is_money else f"{d:,.0f}"
            pct = (d / p) * 100.0
            if is_money:
                return f"R$ {d:,.2f} ({pct:+.1f}%)"
            return f"{d:,.0f} ({pct:+.1f}%)"

        import html as _html
        import re as _re

        def _sg_delta_style(delta_text: str | None, *, inverse: bool) -> str:
            if not delta_text or str(delta_text).strip() in {"—", "-"}:
                return "color:#94a3b8;font-weight:650;"
            s = str(delta_text).strip()
            m = _re.search(r"([+-])\s*\d", s)
            sign = m.group(1) if m else None
            if sign is None:
                return "color:#94a3b8;font-weight:650;"
            is_pos = sign == "+"
            good = (not inverse and is_pos) or (inverse and not is_pos)
            return ("color:#22c55e;font-weight:800;" if good else "color:#fb7185;font-weight:800;")

        def _sg_kpi_card(title: str, value: str, *, icon: str, accent: str, delta: str | None = None, inverse: bool = False) -> None:
            # Se delta=None, não renderiza a linha (evita "—" desnecessário nos acumulados)
            delta_html = ""
            if delta is not None:
                d = str(delta or "—")
                delta_html = f'<div style="margin-top:8px;font-size:0.84rem;{_sg_delta_style(d, inverse=inverse)}">{_html.escape(d)}</div>'
            st.markdown(
                f"""
<div class="dp-card" style="
  padding:12px 12px;
  min-height: 156px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
  {delta_html}
</div>
""",
                unsafe_allow_html=True,
            )

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            _sg_kpi_card(
                "Faturamento (até agora)",
                f"R$ {fat_atual:,.2f}",
                icon="💰",
                accent="#6EE7B7",
                delta=_fmt_delta_with_pct(
                    (fat_atual - prev_fat_total) if isinstance(prev_fat_total, (int, float)) else None,
                    prev_fat_total if isinstance(prev_fat_total, (int, float)) else None,
                    is_money=True,
                ),
            )
        with c2:
            _sg_kpi_card("Meta (time)", f"R$ {meta_total:,.2f}" if meta_total > 0 else "—", icon="🎯", accent="#93c5fd", delta=None)
        with c3:
            _sg_kpi_card("% da meta", f"{perc_meta:.1f}%" if perc_meta is not None else "—", icon="📈", accent="#FBBF24", delta=None)
        with c4:
            _sg_kpi_card("Falta p/ meta", f"R$ {falta_meta:,.2f}" if meta_total > 0 else "—", icon="🧾", accent="#fb7185", delta=None)

        k0, k1, k2, k3 = st.columns(4)
        with k0:
            _sg_kpi_card(
                "Meta por dia útil (necessária)",
                f"R$ {falta_por_dia:,.2f}" if falta_por_dia is not None else "—",
                icon="🗓",
                accent="#93c5fd",
                delta=_fmt_delta_with_pct(
                    (falta_por_dia - prev_falta_por_dia)
                    if (falta_por_dia is not None and isinstance(prev_falta_por_dia, (int, float)))
                    else None,
                    prev_falta_por_dia if isinstance(prev_falta_por_dia, (int, float)) else None,
                    is_money=True,
                ),
                inverse=True,
            )
        # "Dia anterior" (delta) no seu processo = diferença do acumulado do mês entre
        # a análise ativa e a análise anterior salva (snapshot acumulado).
        def _clients_total_from_payload(p: dict) -> int | None:
            try:
                t = p.get("totais") if isinstance(p, dict) else None
                if isinstance(t, dict) and t.get("clientes_atendidos_total") is not None:
                    return int(float(t.get("clientes_atendidos_total") or 0))
            except Exception:
                pass
            try:
                vs = p.get("vendedores") if isinstance(p, dict) else None
                if not isinstance(vs, list):
                    return None
                s = 0
                any_v = False
                for it in vs:
                    if not isinstance(it, dict):
                        continue
                    v = it.get("clientes_atendidos")
                    if v is None:
                        continue
                    try:
                        s += int(float(v))
                        any_v = True
                    except Exception:
                        continue
                return int(s) if any_v else None
            except Exception:
                return None

        def _pick_prev_perf_payload_for_row(current_row) -> dict | None:
            try:
                cur_key, _ = _extract_date_label_from_periodo(
                    str(getattr(current_row, "periodo", "") or ""),
                    str(getattr(current_row, "created_at", "") or ""),
                )
            except Exception:
                cur_key = "0000-00-00"
            if not cur_key or cur_key == "0000-00-00":
                return None
            rows_all = _perf_analysis_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=800)
            cur_id = int(getattr(current_row, "id", 0) or 0)
            prev_row = None
            prev_key = None
            for rr in rows_all:
                rid = int(getattr(rr, "id", 0) or 0)
                if rid == cur_id:
                    break
                try:
                    rk, _ = _extract_date_label_from_periodo(
                        str(getattr(rr, "periodo", "") or ""),
                        str(getattr(rr, "created_at", "") or ""),
                    )
                except Exception:
                    rk = "0000-00-00"
                if not rk or rk == "0000-00-00" or rk >= cur_key:
                    continue
                if prev_key is None or str(rk) > str(prev_key):
                    prev_row, prev_key = rr, rk
            if prev_row is None:
                return None
            try:
                p = json.loads(getattr(prev_row, "payload_json", "") or "")
            except Exception:
                return None
            return p if isinstance(p, dict) else None

        prev_perf_payload = _pick_prev_perf_payload_for_row(r0) if r0 is not None else None
        cur_sum = None
        prev_sum = None
        try:
            if isinstance(payload_base, dict):
                cur_sum = _extract_perf_summary_from_payload(str((payload_base or {}).get("periodo") or ""), payload_base)
            if isinstance(prev_perf_payload, dict):
                prev_sum = _extract_perf_summary_from_payload(str(prev_perf_payload.get("periodo") or ""), prev_perf_payload)
        except Exception:
            cur_sum, prev_sum = None, None

        fat_dia_ant = None
        nf_dia_ant = None
        cli_dia_ant = None
        if isinstance(cur_sum, dict) and isinstance(prev_sum, dict):
            try:
                fat_dia_ant = float(cur_sum.get("tot_faturamento") or 0.0) - float(prev_sum.get("tot_faturamento") or 0.0)
            except Exception:
                fat_dia_ant = None
            try:
                nf_dia_ant = int(float(cur_sum.get("tot_nfs") or 0.0) - float(prev_sum.get("tot_nfs") or 0.0))
            except Exception:
                nf_dia_ant = None
            try:
                cur_cli = _clients_total_from_payload(payload_base) if isinstance(payload_base, dict) else None
                prev_cli = _clients_total_from_payload(prev_perf_payload) if isinstance(prev_perf_payload, dict) else None
                if cur_cli is not None and prev_cli is not None:
                    cli_dia_ant = int(cur_cli) - int(prev_cli)
            except Exception:
                cli_dia_ant = None

        # Mantemos as variáveis de margem/inputs manuais para o bloco de KPIs (se usados),
        # mas o "dia anterior" agora é delta vs análise anterior.
        marg_hoje_pct = prev0_k.get("margem_hoje_pct")
        marg_ontem_pct = prev0_k.get("margem_dia_anterior_pct")
        prev_marg_hoje_pct = prev1_k.get("margem_hoje_pct")

        # Melhor dia do mês (pela base diária "Faturamento e Atendidos")
        best_nf_day: int | None = None
        best_nf_val: int | None = None
        best_cli_day: int | None = None
        best_cli_val: int | None = None
        last_bus = None
        if isinstance(daily_roll, dict) and isinstance(daily_roll.get("last_bus"), dict):
            last_bus = daily_roll.get("last_bus")
        try:
            if isinstance(daily_roll, dict) and isinstance(daily_roll.get("df"), pd.DataFrame):
                d0 = daily_roll["df"]
            else:
                d0 = None
            if isinstance(d0, pd.DataFrame) and not d0.empty and "dia" in d0.columns:
                nf_s = pd.to_numeric(d0.get("nfs_emitidas"), errors="coerce").fillna(0)
                cli_s = pd.to_numeric(d0.get("clientes_atendidos"), errors="coerce").fillna(0)
                dia_s = pd.to_numeric(d0.get("dia"), errors="coerce")
                # Melhor dia = maior valor (ignora linhas sem dia numérico)
                ok_mask = dia_s.notna()
                if ok_mask.any():
                    d2 = d0.loc[ok_mask].copy()
                    d2["_dia"] = pd.to_numeric(d2.get("dia"), errors="coerce")
                    d2["_nf"] = pd.to_numeric(d2.get("nfs_emitidas"), errors="coerce").fillna(0)
                    d2["_cli"] = pd.to_numeric(d2.get("clientes_atendidos"), errors="coerce").fillna(0)
                    if d2["_nf"].notna().any():
                        i_nf = int(d2["_nf"].idxmax())
                        best_nf_day = int(d2.loc[i_nf, "_dia"] or 0) if pd.notna(d2.loc[i_nf, "_dia"]) else None
                        best_nf_val = int(d2.loc[i_nf, "_nf"] or 0)
                    if d2["_cli"].notna().any():
                        i_cli = int(d2["_cli"].idxmax())
                        best_cli_day = int(d2.loc[i_cli, "_dia"] or 0) if pd.notna(d2.loc[i_cli, "_dia"]) else None
                        best_cli_val = int(d2.loc[i_cli, "_cli"] or 0)
        except Exception:
            pass

        with k1:
            _sg_kpi_card(
                "Faturamento (dia anterior Δ)",
                (f"R$ {float(fat_dia_ant or 0.0):+,.2f}" if fat_dia_ant is not None else "—"),
                icon="🧾",
                accent="#6EE7B7",
                delta=None,
            )
        with k2:
            best_nf_txt = None
            try:
                ref_day = int(last_bus.get("dia") or 0) if isinstance(daily_roll, dict) and isinstance(daily_roll.get("last_bus"), dict) else None
            except Exception:
                ref_day = None
            if best_nf_day is not None and best_nf_val is not None:
                if ref_day is not None and int(best_nf_day) == int(ref_day):
                    best_nf_txt = f"Melhor dia do mês: **dia {int(best_nf_day):02d}** (este) • **{int(best_nf_val)}** NFs"
                else:
                    best_nf_txt = f"Melhor dia do mês: **dia {int(best_nf_day):02d}** • **{int(best_nf_val)}** NFs"
            _sg_kpi_card(
                "NFs (dia anterior Δ)",
                (f"{int(nf_dia_ant):+d}" if nf_dia_ant is not None else "—"),
                icon="🧾",
                accent="#93c5fd",
                delta=None,
            )
            if best_nf_txt:
                st.caption(best_nf_txt)
        with k3:
            best_cli_txt = None
            try:
                ref_day2 = int(last_bus.get("dia") or 0) if isinstance(daily_roll, dict) and isinstance(daily_roll.get("last_bus"), dict) else None
            except Exception:
                ref_day2 = None
            if best_cli_day is not None and best_cli_val is not None:
                if ref_day2 is not None and int(best_cli_day) == int(ref_day2):
                    best_cli_txt = f"Melhor dia do mês: **dia {int(best_cli_day):02d}** (este) • **{int(best_cli_val)}** atendidos"
                else:
                    best_cli_txt = f"Melhor dia do mês: **dia {int(best_cli_day):02d}** • **{int(best_cli_val)}** atendidos"
            _sg_kpi_card(
                "Clientes (dia anterior Δ)",
                (f"{int(cli_dia_ant):+d}" if cli_dia_ant is not None else "—"),
                icon="👥",
                accent="#C4B5FD",
                delta=None,
            )
            if best_cli_txt:
                st.caption(best_cli_txt)
        # Margem média (replica o "Performance > Visão Geral")
        margem_media = None
        try:
            if isinstance(payload_base, dict):
                sellers_tmp = parse_sellers(payload_base)
                results_tmp, _ = calcular_time(sellers_tmp) if sellers_tmp else ([], 0.0)
                if results_tmp:
                    df_tmp = pd.DataFrame([r.__dict__ for r in results_tmp])
                    stats_tmp = _team_stats(df_tmp)
                    margem_media = float(stats_tmp.get("media_margem")) if stats_tmp.get("media_margem") is not None else None
        except Exception:
            margem_media = None

        # Margem média — comparar com a última análise anterior (penúltimo dia) do histórico de performance
        prev_margem_media = None
        try:
            rows_perf = list_analyses(conn, limit=120, owner_user_id=owner_id, include_all=is_admin)
            for r in rows_perf:
                if active_id is not None and int(r.id) == int(active_id):
                    continue
                try:
                    p = json.loads(r.payload_json)
                except Exception:
                    continue
                if not isinstance(p, dict):
                    continue
                kind = str(p.get("_kind") or "")
                if kind.startswith("sala_gestao_"):
                    continue
                sellers_p = parse_sellers(p)
                if not sellers_p:
                    continue
                results_p, _ = calcular_time(sellers_p)
                if not results_p:
                    continue
                df_p = pd.DataFrame([x.__dict__ for x in results_p])
                stats_p = _team_stats(df_p)
                if stats_p.get("media_margem") is None:
                    continue
                prev_margem_media = float(stats_p.get("media_margem"))
                break
        except Exception:
            prev_margem_media = None

        def _fmt_delta_pp_and_pct(cur_pct: float | None, ref_pct: float | None) -> str | None:
            if cur_pct is None or ref_pct is None:
                return None
            try:
                c = float(cur_pct)
                r = float(ref_pct)
            except Exception:
                return None
            diff_pp = c - r
            if abs(r) < 1e-9:
                return f"{diff_pp:+.1f} pp"
            rel = (diff_pp / abs(r)) * 100.0
            return f"{diff_pp:+.1f} pp ({rel:+.1f}%)"

        k4, k5, k6, k7 = st.columns(4)
        with k4:
            nf_acc = None
            try:
                if isinstance(cur_sum, dict):
                    nf_acc = int(float(cur_sum.get("tot_nfs") or 0.0))
            except Exception:
                nf_acc = None
            _sg_kpi_card("Acumulado NFs", (str(int(nf_acc)) if nf_acc is not None else "—"), icon="📦", accent="#93c5fd", delta=None)
        with k5:
            cli_acc = _clients_total_from_payload(payload_base) if isinstance(payload_base, dict) else None
            _sg_kpi_card("Acumulado Clientes", (str(int(cli_acc)) if cli_acc is not None else "—"), icon="👥", accent="#C4B5FD", delta=None)
        with k6:
            _sg_kpi_card(
                "Margem média (time)",
                f"{margem_media:.1f}%" if margem_media is not None else "—",
                icon="📊",
                accent="#A7F3D0",
                delta=_fmt_delta_pp_and_pct(margem_media, prev_margem_media),
            )
        k7.empty()

        # Vendedores (alcance)
        st.markdown("### Vendedores — faixas de % Alcance projetado")
        vend_df = pd.DataFrame()
        if isinstance(payload_base, dict):
            sellers = parse_sellers(payload_base)
            results, _ = calcular_time(sellers) if sellers else ([], 0.0)
            vend_df = pd.DataFrame([r.__dict__ for r in results]) if results else pd.DataFrame()
            vend_df = _enrich_results_df_for_performance(vend_df, sellers)
        if vend_df.empty:
            st.caption("Sem vendedores (defina uma análise ativa).")
            meta_batida = acima_80 = abaixo_80 = 0
        else:
            def _bucket(v):
                # Regra da sala (dashboards acima): usar % Alcance PROJETADO para as faixas
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return "Sem dado"
                x = float(v)
                if x >= 100.0:
                    return "Meta batida (>=100%)"
                if x >= 80.0:
                    return "Alcance >= 80%"
                return "Alcance < 80%"

            vend_df["faixa_alcance"] = vend_df["alcance_pct"].apply(_bucket)
            meta_batida = int((vend_df["faixa_alcance"] == "Meta batida (>=100%)").sum())
            acima_80 = int((vend_df["faixa_alcance"] == "Alcance >= 80%").sum())
            abaixo_80 = int((vend_df["faixa_alcance"] == "Alcance < 80%").sum())

            show = vend_df[
                ["nome", "alcance_real_pct", "alcance_pct", "margem_pct", "conversao_pct", "interacoes", "faixa_alcance"]
            ].rename(columns={"alcance_real_pct": "% Alcance", "alcance_pct": "% Alcance Projetado", "nome": "Vendedor"})

            # Formatação e estilo (mais moderno)
            def _faixa_style(s: pd.Series) -> list[str]:
                out = []
                for v in s.astype(str).fillna("").tolist():
                    if "Meta batida" in v:
                        out.append("background-color: rgba(34,197,94,.16); color:#bbf7d0; font-weight:800;")
                    elif ">= 80" in v:
                        out.append("background-color: rgba(251,191,36,.14); color:#fde68a; font-weight:800;")
                    elif "< 80" in v:
                        out.append("background-color: rgba(251,113,133,.14); color:#fecdd3; font-weight:800;")
                    else:
                        out.append("color:#94a3b8;")
                return out

            styled = (
                show.style.format(
                    {
                        "% Alcance": lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—",
                        "% Alcance Projetado": lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—",
                        "margem_pct": lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—",
                        "conversao_pct": lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—",
                        "interacoes": lambda x: f"{int(float(x))}" if pd.notna(x) else "—",
                    },
                    na_rep="—",
                )
                .apply(_faixa_style, subset=["faixa_alcance"])
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

        vc1, vc2, vc3 = st.columns(3)
        with vc1:
            _sg_kpi_card("Meta batida (>=100%)", str(meta_batida), icon="✅", accent="#6EE7B7", delta=None)
        with vc2:
            _sg_kpi_card("Alcance >=80%", str(acima_80), icon="🟡", accent="#FBBF24", delta=None)
        with vc3:
            _sg_kpi_card("Alcance <80%", str(abaixo_80), icon="🔴", accent="#fb7185", delta=None)

        # Departamentos (sempre a partir da análise ativa / sessão; comparativo Δ só se houver snapshot anterior)
        st.markdown("### Departamentos (análise ativa)")
        st.caption(
            "Os números abaixo vêm dos departamentos ligados a esta análise. "
            "Colunas **Δ** só aparecem quando existe outro salvamento de departamentos no histórico para comparar."
        )
        dept_payload = st.session_state.get("dept_payload")
        if not (isinstance(dept_payload, dict) and isinstance(dept_payload.get("departamentos"), list)):
            st.caption("Nenhuma base de departamentos na análise ativa. Inclua o Excel em **Nova análise** e salve.")
        else:
            df_today = pd.DataFrame([d for d in (dept_payload.get("departamentos") or []) if _dept_ok((d or {}).get("departamento"))])
            df_today = _ensure_participacao_pct(df_today) if not df_today.empty else df_today

            prev_payload = _pick_prev_dept_payload(dept_payload)
            df_yday = pd.DataFrame(
                [d for d in ((prev_payload or {}).get("departamentos") or []) if _dept_ok((d or {}).get("departamento"))]
            )
            df_yday = _ensure_participacao_pct(df_yday) if not df_yday.empty else df_yday

            if df_today.empty:
                st.caption("Base de departamentos vazia.")
            else:
                cal2 = st.session_state.get("calendar_info") if isinstance(st.session_state.get("calendar_info"), dict) else {}
                du_total = int(cal2.get("dias_uteis_total") or 0)
                du_trab = int(cal2.get("dias_uteis_trabalhados") or 0)
                ratio = (float(du_trab) / float(du_total)) if (du_total > 0 and du_trab >= 0) else 0.0
                ratio = min(1.0, max(0.0, ratio))

                def _add_falta_meta_to_date(df_in: pd.DataFrame) -> pd.DataFrame:
                    if df_in is None or df_in.empty:
                        return df_in
                    if "meta_faturamento" in df_in.columns and "faturamento" in df_in.columns:
                        mm = pd.to_numeric(df_in.get("meta_faturamento"), errors="coerce")
                        ff = pd.to_numeric(df_in.get("faturamento"), errors="coerce")
                        out = df_in.copy()
                        out["falta_meta"] = (mm * ratio) - ff
                        return out
                    return df_in

                def _add_alcance_real(df_in: pd.DataFrame) -> pd.DataFrame:
                    """% Alcançado Real = (Faturamento / Meta) * 100."""
                    if df_in is None or df_in.empty:
                        return df_in
                    if "meta_faturamento" not in df_in.columns or "faturamento" not in df_in.columns:
                        return df_in
                    mm = pd.to_numeric(df_in.get("meta_faturamento"), errors="coerce")
                    ff = pd.to_numeric(df_in.get("faturamento"), errors="coerce")
                    out = df_in.copy()
                    out["alcance_real_pct"] = None
                    mask = mm.notna() & (mm > 0) & ff.notna()
                    out.loc[mask, "alcance_real_pct"] = (ff[mask] / mm[mask]) * 100.0
                    return out

                def _mini_card(title: str, value: str, subtitle: str, *, icon: str, accent: str) -> None:
                    st.markdown(
                        f"""
<div class="dp-card" style="
  padding:12px 12px;
  min-height: 156px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{html.escape(str(title))}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{html.escape(str(icon))}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{html.escape(str(value))}</div>
  <div style="margin-top:8px;color:#CBD5E1;font-size:0.84rem;line-height:1.35;white-space:normal;">
    {html.escape(str(subtitle)) if subtitle else ""}
  </div>
</div>
""",
                        unsafe_allow_html=True,
                    )

                df_today2 = _add_alcance_real(_add_falta_meta_to_date(df_today))
                df_yday2: pd.DataFrame | None = None
                if not df_yday.empty:
                    df_yday2 = _add_alcance_real(_add_falta_meta_to_date(df_yday))
                if df_yday2 is None or df_yday2.empty:
                    st.caption("Não há outro snapshot de departamentos no histórico para comparar — abaixo, só a situação desta análise.")

                key = "departamento"
                keep_cols = [
                    "faturamento",
                    "participacao_pct",
                    "margem_pct",
                    "meta_margem_pct",
                    "alcance_real_pct",
                    "alcance_projetado_pct",
                    "falta_meta",
                ]
                merged: pd.DataFrame | None = None
                if df_yday2 is not None and not df_yday2.empty:
                    keep_cols_m = [c for c in keep_cols if c in df_today2.columns and c in df_yday2.columns]
                    t = df_today2[[key] + keep_cols_m].copy()
                    y = df_yday2[[key] + keep_cols_m].copy()
                    for c in keep_cols_m:
                        t[c] = pd.to_numeric(t[c], errors="coerce")
                        y[c] = pd.to_numeric(y[c], errors="coerce")
                    merged = t.merge(y, on=key, how="outer", suffixes=("_hoje", "_ontem"))
                    for c in keep_cols_m:
                        merged[f"Δ {c}"] = merged[f"{c}_hoje"] - merged[f"{c}_ontem"]

                # Cards por faixas de Alcance Real
                try:
                    ar = pd.to_numeric(df_today2.get("alcance_real_pct"), errors="coerce")
                    dept_names = df_today2.get(key).astype(str).fillna("").tolist()

                    ge100 = [
                        dept_names[i]
                        for i, v in enumerate(ar.tolist())
                        if pd.notna(v) and float(v) >= 100.0 and str(dept_names[i]).strip().lower() not in {"", "nan"}
                    ]
                    ge80 = [
                        dept_names[i]
                        for i, v in enumerate(ar.tolist())
                        if pd.notna(v) and 80.0 <= float(v) < 100.0 and str(dept_names[i]).strip().lower() not in {"", "nan"}
                    ]
                    lt80 = [
                        dept_names[i]
                        for i, v in enumerate(ar.tolist())
                        if pd.notna(v) and float(v) < 80.0 and str(dept_names[i]).strip().lower() not in {"", "nan"}
                    ]

                    def _join_names(xs: list[str], max_n: int = 6) -> str:
                        xs2 = [str(x).strip() for x in xs if str(x).strip() and str(x).strip().lower() != "nan"]
                        if not xs2:
                            return "—"
                        head = xs2[:max_n]
                        tail = len(xs2) - len(head)
                        return ", ".join(head) + (f" (+{tail})" if tail > 0 else "")

                    s1, s2, s3 = st.columns(3)
                    with s1:
                        _mini_card("Alcance Real ≥ 100%", str(len(ge100)), _join_names(ge100), icon="✅", accent="#6EE7B7")
                    with s2:
                        _mini_card("Alcance Real ≥ 80%", str(len(ge80)), _join_names(ge80), icon="🟡", accent="#FBBF24")
                    with s3:
                        _mini_card("Alcance Real < 80%", str(len(lt80)), _join_names(lt80), icon="🔴", accent="#fb7185")
                except Exception:
                    pass

                # Participação + Margem (entregue vs não entregue)
                try:
                    part_df = df_today2.copy()
                    part_df["participacao_pct"] = pd.to_numeric(part_df.get("participacao_pct"), errors="coerce")
                    part_df["margem_pct"] = pd.to_numeric(part_df.get("margem_pct"), errors="coerce")
                    part_df[key] = part_df[key].astype(str)

                    part_ok = part_df[part_df["participacao_pct"].notna() & (part_df[key].str.strip() != "")].copy()
                    top_part = part_ok.sort_values("participacao_pct", ascending=False).head(6) if not part_ok.empty else pd.DataFrame()

                    def _join_part_rows(df_in: pd.DataFrame, max_n: int = 4) -> str:
                        if df_in is None or df_in.empty:
                            return "—"
                        rows2: list[str] = []
                        for _, rr in df_in.head(max_n).iterrows():
                            nm = str(rr.get(key) or "").strip()
                            pv = rr.get("participacao_pct")
                            if not nm or nm.lower() == "nan" or pv is None or (isinstance(pv, float) and pd.isna(pv)):
                                continue
                            rows2.append(f"{nm} ({float(pv):.2f}%)")
                        if not rows2:
                            return "—"
                        tail = max(0, int(len(df_in)) - len(rows2))
                        return ", ".join(rows2) + (f" (+{tail})" if tail > 0 else "")

                    # Margem entregue: comparar com a meta individual do depto (meta_margem_pct), se existir.
                    part_df["meta_margem_pct"] = pd.to_numeric(part_df.get("meta_margem_pct"), errors="coerce")
                    has_meta = part_df["meta_margem_pct"].notna() & (part_df[key].str.strip() != "")
                    m_ok = part_df[has_meta & part_df["margem_pct"].notna() & (part_df["margem_pct"] >= part_df["meta_margem_pct"])]
                    m_bad = part_df[has_meta & part_df["margem_pct"].notna() & (part_df["margem_pct"] < part_df["meta_margem_pct"])]
                    m_na = part_df[(~has_meta) & part_df["margem_pct"].notna() & (part_df[key].str.strip() != "")]
                    n_ok = int(len(m_ok))
                    n_bad = int(len(m_bad))
                    n_na = int(len(m_na))
                    ok_names = [str(x).strip() for x in m_ok[key].astype(str).tolist() if str(x).strip() and str(x).strip().lower() != "nan"]
                    bad_names = [str(x).strip() for x in m_bad[key].astype(str).tolist() if str(x).strip() and str(x).strip().lower() != "nan"]
                    na_names = [str(x).strip() for x in m_na[key].astype(str).tolist() if str(x).strip() and str(x).strip().lower() != "nan"]

                    def _join_names2(xs: list[str], max_n: int = 6) -> str:
                        xs2 = [str(x).strip() for x in xs if str(x).strip() and str(x).strip().lower() != "nan"]
                        if not xs2:
                            return "—"
                        head = xs2[:max_n]
                        tail = len(xs2) - len(head)
                        return ", ".join(head) + (f" (+{tail})" if tail > 0 else "")

                    p1, p2 = st.columns(2)
                    with p1:
                        _mini_card(
                            "% Participação — TOP",
                            (f"{float(top_part['participacao_pct'].max()):.2f}%" if (not top_part.empty and top_part["participacao_pct"].notna().any()) else "—"),
                            _join_part_rows(top_part, max_n=4),
                            icon="🥇",
                            accent="#FBBF24",
                        )
                    with p2:
                        c_ok, c_bad = st.columns(2)
                        with c_ok:
                            _mini_card(
                                "Margem — metas batidas",
                                f"{n_ok}",
                                f"OK (meta depto): { _join_names2(ok_names, max_n=4) }"
                                + (f" • Sem meta: { _join_names2(na_names, max_n=4) }" if n_na > 0 else ""),
                                icon="✅",
                                accent="#6EE7B7",
                            )
                        with c_bad:
                            _mini_card(
                                "Margem — abaixo da meta",
                                f"{n_bad}",
                                f"Abaixo (meta depto): { _join_names2(bad_names, max_n=4) }"
                                + (f" • Sem meta: { _join_names2(na_names, max_n=4) }" if n_na > 0 else ""),
                                icon="⚠️",
                                accent="#fb7185" if n_bad > 0 else "#93c5fd",
                            )

                    # Diagnóstico rápido (quando meta margem não veio)
                    if int(n_ok + n_bad) == 0 and int(n_na) > 0:
                        cols_here = ", ".join([c for c in ["meta_margem_pct", "margem_pct", "meta_faturamento", "faturamento"] if c in part_df.columns])
                        st.warning(
                            "Meta de margem **não foi detectada** nesta base de Departamentos. "
                            "Confirme que você carregou o Excel **Faturamento por departamento** (onde a **coluna G** é % Meta Margem e a **H** é % Margem). "
                            f"Campos presentes agora: `{cols_here or '—'}`. "
                            "Depois de recarregar, salve a análise novamente."
                        )
                except Exception:
                    pass

                def _arrow(v: object, kind: str) -> str:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return "—"
                    x = float(v)
                    if abs(x) < 1e-9:
                        return "→ 0"
                    arrow = "▲" if x > 0 else "▼"
                    if kind == "money":
                        return f"{arrow} R$ {abs(x):,.2f}"
                    if kind == "pct":
                        return f"{arrow} {abs(x):.2f} pp"
                    return f"{arrow} {abs(x):.0f}"

                def _delta_color_series(s: pd.Series) -> list[str]:
                    out2 = []
                    for v in s.astype(str).fillna("—").tolist():
                        if v.startswith("▲"):
                            out2.append("color:#22c55e; font-weight:800;")
                        elif v.startswith("▼"):
                            out2.append("color:#fb7185; font-weight:800;")
                        elif v.startswith("→"):
                            out2.append("color:#94a3b8; font-weight:650;")
                        else:
                            out2.append("color:#94a3b8;")
                    return out2

                def _alc_bucket_style(s: pd.Series) -> list[str]:
                    out3: list[str] = []
                    for v in s.tolist():
                        try:
                            x = float(v)
                        except Exception:
                            out3.append("color:#94a3b8;")
                            continue
                        if x >= 100:
                            out3.append("background-color: rgba(34,197,94,.14); color:#bbf7d0; font-weight:900;")
                        elif x >= 80:
                            out3.append("background-color: rgba(251,191,36,.14); color:#fde68a; font-weight:900;")
                        else:
                            out3.append("background-color: rgba(251,113,133,.14); color:#fecdd3; font-weight:900;")
                    return out3

                if merged is not None and not merged.empty:
                    show = pd.DataFrame()
                    show["Departamento"] = merged[key].astype(str)
                    show["Departamento"] = show["Departamento"].astype(str).str.strip()
                    show = show[(show["Departamento"] != "") & (show["Departamento"].str.lower() != "nan")].copy()
                    if "faturamento_hoje" in merged.columns:
                        show["Faturamento"] = merged["faturamento_hoje"]
                        show["Δ Faturamento"] = merged["Δ faturamento"].apply(lambda x: _arrow(x, "money"))
                    if "falta_meta_hoje" in merged.columns:
                        show["Falta meta"] = merged["falta_meta_hoje"]
                        show["Δ Falta meta"] = merged["Δ falta_meta"].apply(lambda x: _arrow(x, "money"))
                    if "alcance_real_pct_hoje" in merged.columns:
                        show["Alc. Real"] = merged["alcance_real_pct_hoje"]
                        show["Δ Alc. Real"] = merged["Δ alcance_real_pct"].apply(lambda x: _arrow(x, "pct"))
                    if "alcance_projetado_pct_hoje" in merged.columns:
                        show["Alc. Proj."] = merged["alcance_projetado_pct_hoje"]
                        show["Δ Alc. Proj."] = merged["Δ alcance_projetado_pct"].apply(lambda x: _arrow(x, "pct"))
                    if "participacao_pct_hoje" in merged.columns:
                        show["% Part."] = merged["participacao_pct_hoje"]
                        show["Δ % Part."] = merged["Δ participacao_pct"].apply(lambda x: _arrow(x, "pct"))
                    if "margem_pct_hoje" in merged.columns:
                        show["% Margem"] = merged["margem_pct_hoje"]
                        show["Δ % Margem"] = merged["Δ margem_pct"].apply(lambda x: _arrow(x, "pct"))
                    if "meta_margem_pct_hoje" in merged.columns:
                        show["Meta Margem"] = merged["meta_margem_pct_hoje"]
                        show["Δ Meta Margem"] = merged["Δ meta_margem_pct"].apply(lambda x: _arrow(x, "pct"))

                    preferred = [
                        "Departamento",
                        "Faturamento",
                        "Δ Faturamento",
                        "Falta meta",
                        "Δ Falta meta",
                        "Alc. Real",
                        "Δ Alc. Real",
                        "Alc. Proj.",
                        "Δ Alc. Proj.",
                        "% Part.",
                        "Δ % Part.",
                        "% Margem",
                        "Δ % Margem",
                        "Meta Margem",
                        "Δ Meta Margem",
                    ]
                    show = show[[c for c in preferred if c in show.columns]].copy()

                    fmt: dict[str, object] = {}
                    if "Faturamento" in show.columns:
                        fmt["Faturamento"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"
                    if "% Part." in show.columns:
                        fmt["% Part."] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                    if "% Margem" in show.columns:
                        fmt["% Margem"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                    if "Meta Margem" in show.columns:
                        fmt["Meta Margem"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                    if "Alc. Proj." in show.columns:
                        fmt["Alc. Proj."] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                    if "Alc. Real" in show.columns:
                        fmt["Alc. Real"] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                    if "Falta meta" in show.columns:
                        fmt["Falta meta"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"

                    styled = show.style.format(fmt, na_rep="—")
                    if "Alc. Proj." in show.columns:
                        styled = styled.apply(_alc_bucket_style, subset=["Alc. Proj."])
                    if "Alc. Real" in show.columns:
                        styled = styled.apply(_alc_bucket_style, subset=["Alc. Real"])
                    for c in [c for c in show.columns if c.startswith("Δ ")]:
                        styled = styled.apply(_delta_color_series, subset=[c])
                    st.dataframe(styled, use_container_width=True, hide_index=True)
                else:
                    snap = df_today2.copy()
                    if key in snap.columns:
                        snap = snap.rename(columns={key: "Departamento"})
                    snap_cols = [
                        c
                        for c in [
                            "Departamento",
                            "faturamento",
                            "falta_meta",
                            "alcance_real_pct",
                            "alcance_projetado_pct",
                            "participacao_pct",
                            "margem_pct",
                            "meta_margem_pct",
                        ]
                        if c in snap.columns
                    ]
                    if snap_cols:
                        fmt2: dict[str, object] = {}
                        if "faturamento" in snap.columns:
                            fmt2["faturamento"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"
                        if "falta_meta" in snap.columns:
                            fmt2["falta_meta"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"
                        if "participacao_pct" in snap.columns:
                            fmt2["participacao_pct"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                        if "margem_pct" in snap.columns:
                            fmt2["margem_pct"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                        if "meta_margem_pct" in snap.columns:
                            fmt2["meta_margem_pct"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                        if "alcance_projetado_pct" in snap.columns:
                            fmt2["alcance_projetado_pct"] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                        if "alcance_real_pct" in snap.columns:
                            fmt2["alcance_real_pct"] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                        st.dataframe(snap[snap_cols].style.format(fmt2, na_rep="—"), use_container_width=True, hide_index=True)
                    else:
                        st.dataframe(snap, use_container_width=True, hide_index=True)

        st.markdown("### Radar (manual)")
        radar = st.session_state.get("radar") or []
        if radar:
            st.dataframe(pd.DataFrame(radar), use_container_width=True, hide_index=True)
        else:
            st.caption("Sem itens no radar.")

        st.markdown("### Insights (IA) — Sala de Gestão (consolidado)")
        provider: Provider = st.selectbox(
            "Provedor de IA (Sala de Gestão)",
            options=["auto", "gemini", "openai"],
            format_func=lambda x: {"auto": "Auto (Gemini → OpenAI)", "gemini": "Gemini", "openai": "OpenAI"}[x],
            key="sg_provider",
        )
        # Estrutura de dados que a IA deve seguir
        depts_for_ai = []
        try:
            if isinstance(st.session_state.get("dept_payload"), dict):
                _tmp = pd.DataFrame([d for d in (st.session_state["dept_payload"].get("departamentos") or []) if _dept_ok((d or {}).get("departamento"))])
                _tmp = _ensure_participacao_pct(_tmp)
                depts_for_ai = _tmp.to_dict(orient="records") if not _tmp.empty else []
        except Exception:
            depts_for_ai = []

        dados_json = json.dumps(
            {
                "meta_faturamento_total": meta_total,
                "faturamento_total_ate_agora": fat_atual,
                "falta_para_meta": falta_meta,
                "dias_uteis_restantes": dias_restantes,
                "meta_por_dia_util_necessaria": falta_por_dia,
                "faturamento_dia_anterior": prev_k.get("faturamento_dia_anterior"),
                "nfs_dia_anterior": prev_k.get("nf_dia_anterior"),
                "nfs_acumulado": prev_k.get("nf_acumulado"),
                "clientes_dia_anterior": prev_k.get("clientes_dia_anterior"),
                "clientes_acumulado": prev_k.get("clientes_acumulado"),
                "margem_dia_anterior_pct": prev_k.get("margem_dia_anterior_pct"),
                "margem_hoje_pct": prev_k.get("margem_hoje_pct"),
                "departamentos": depts_for_ai,
            },
            ensure_ascii=False,
            indent=2,
        )

        prompt = (
            "Você está preparando a leitura da reunião 'Sala de Gestão'.\n"
            "Retorne APENAS um JSON no formato EXATO: {\"texto\":\"...\"}.\n"
            "Sem markdown.\n\n"
            "A resposta deve vir em blocos com títulos:\n"
            "1) Meta Faturamento\n"
            "2) Dia anterior\n"
            "3) Volume (NFs e Clientes)\n"
            "4) Departamentos\n\n"
            "Regras:\n"
            "- Use os números fornecidos em DADOS (não invente).\n"
            "- Em 'Meta Faturamento', cite: Meta total, Faturamento até agora, Falta pra meta, Meta por dia útil.\n"
            "- Em 'Dia anterior', cite o Faturamento dia anterior e Margem (hoje vs ontem) se houver.\n"
            "- Em 'Departamentos', listar quantos deptos estão com Alcance Projetado >=100, >=80 e <80; "
            "apontar melhor margem e pior margem; trazer TOP 3 em % participação; e oportunidades (falta para meta baixa).\n"
            "- IGNORE completamente departamentos 'Outros' e 'Paineis Eletricos'.\n\n"
            "DADOS:\n"
            + dados_json
        )
        if st.button("🧠 Gerar insights (consolidado)", use_container_width=True, key="btn_sg_insights"):
            try:
                with st.spinner("Gerando insights..."):
                    resp, prov_used, model_used = json_from_text(settings=settings, provider=provider, prompt=prompt)
                st.session_state["sg_insights"] = {"t": str(resp.get("texto") or "").strip(), "p": prov_used, "m": model_used}
            except Exception as e:
                st.error("Falha ao gerar insights.")
                st.caption(str(e))

        t = st.session_state.get("sg_insights")
        if isinstance(t, dict) and t.get("t"):
            st.caption(f"Gerado por **{t.get('p')}** (`{t.get('m')}`).")
            st.text_area("Insights — Sala de Gestão", value=str(t.get("t")), height=300)
            try:
                pdf_bytes = _build_text_pdf_bytes(
                    title=f"Insights (IA) — Sala de Gestão — {str((payload_base or {}).get('periodo') or '—')}",
                    text=str(t.get("t") or ""),
                )
                st.download_button(
                    "⬇️ Baixar insights (PDF)",
                    data=pdf_bytes,
                    file_name="insights_ia_sala_gestao.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                    key="sg_insights_pdf_btn",
                )
            except Exception as e:
                st.caption(f"PDF indisponível: {e}")

    with tab_kpis:
        st.markdown("### Projeção de faturamento / alcance")

        src_name = st.session_state.get("sg_kpi_source_name") or st.session_state.get("sg_daily_source_name")
        if src_name:
            st.caption(f"KPIs diários carregados automaticamente a partir de **{src_name}** (Nova análise).")
        else:
            st.info("Para preencher os KPIs diários automaticamente, envie o arquivo **Faturamento e Atendidos.xlsx** em **Nova análise**.")

        active_id = st.session_state.get("active_analysis_id")
        payload_base: dict | None = None
        if active_id is not None:
            r0 = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
            if r0:
                try:
                    payload_base = json.loads(r0.payload_json)
                except Exception:
                    payload_base = None

        totais = (payload_base or {}).get("totais") if isinstance(payload_base, dict) else {}
        if not isinstance(totais, dict):
            totais = {}

        fat_atual = float(totais.get("faturamento_total") or 0.0)
        meta_total = float(totais.get("meta_total") or 0.0)
        falta_meta = max(0.0, meta_total - fat_atual) if meta_total > 0 else 0.0
        falta_por_dia = (falta_meta / dias_restantes) if dias_restantes > 0 else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Faturamento até o momento", f"R$ {fat_atual:,.2f}")
        c2.metric("Meta (time)", f"R$ {meta_total:,.2f}" if meta_total > 0 else "—")
        c3.metric("Falta para meta", f"R$ {falta_meta:,.2f}" if meta_total > 0 else "—")
        c4.metric("Falta por dia útil restante", f"R$ {falta_por_dia:,.2f}" if falta_por_dia is not None else "—")

        st.markdown("### KPIs do dia anterior (digitação)")
        prev = _last_payload_of_kind("sala_gestao_kpis") or {}
        prev_k = prev.get("kpis") if isinstance(prev, dict) else {}
        if not isinstance(prev_k, dict):
            prev_k = {}

        k1, k2, k3 = st.columns(3)
        nf_dia = k1.number_input(
            "NFs feitas no dia anterior",
            min_value=0,
            value=int(st.session_state.get("sg_nf_dia") or prev_k.get("nf_dia_anterior") or 0),
        )
        nf_acum = k2.number_input(
            "Acumulado de NFs (total)",
            min_value=0,
            value=int(st.session_state.get("sg_nf_acum") or prev_k.get("nf_acumulado") or 0),
        )
        cli_dia = k3.number_input(
            "Clientes atendidos (dia anterior)",
            min_value=0,
            value=int(st.session_state.get("sg_cli_dia") or prev_k.get("clientes_dia_anterior") or 0),
        )
        k4, k5, k6 = st.columns(3)
        cli_acum = k4.number_input(
            "Clientes atendidos (acumulado)",
            min_value=0,
            value=int(st.session_state.get("sg_cli_acum") or prev_k.get("clientes_acumulado") or 0),
        )
        marg_ontem = k5.number_input(
            "Margem dia anterior (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(prev_k.get("margem_dia_anterior_pct") or 0.0),
        )
        marg_hoje = k6.number_input(
            "Margem hoje (%)",
            min_value=0.0,
            max_value=100.0,
            value=float(prev_k.get("margem_hoje_pct") or 0.0),
        )

        st.metric(
            "Margem (pp) — hoje vs ontem",
            f"{marg_hoje:.1f}%",
            delta=round(float(marg_hoje) - float(marg_ontem), 1),
            help="Delta em pontos percentuais (pp).",
        )

        if st.button("💾 Salvar KPIs (Sala de Gestão)", use_container_width=True):
            payload = {
                "_kind": "sala_gestao_kpis",
                "periodo": str((payload_base or {}).get("periodo") or "Sala de Gestão"),
                "totais": totais,
                "kpis": {
                    "faturamento_dia_anterior": float(st.session_state.get("sg_fat_dia_anterior") or 0.0) if st.session_state.get("sg_fat_dia_anterior") is not None else None,
                    "nf_dia_anterior": int(nf_dia),
                    "nf_acumulado": int(nf_acum),
                    "clientes_dia_anterior": int(cli_dia),
                    "clientes_acumulado": int(cli_acum),
                    "margem_dia_anterior_pct": float(marg_ontem),
                    "margem_hoje_pct": float(marg_hoje),
                },
            }
            analysis_id = save_analysis(
                conn,
                periodo=str(payload.get("periodo") or "Sala de Gestão"),
                provider_used="manual_kpis",
                model_used="manual_kpis",
                parent_analysis_id=None,
                owner_user_id=owner_id,
                payload=payload,
                total_bonus=0.0,
            )
            st.success(f"KPIs salvos como análise **#{analysis_id}**.")

    with tab_evol:
        st.markdown("### Evolução dia a dia — NFS, Atendidos e Faturamento")
        st.caption("Gráficos separados para evitar confusão de escala (R$ vs contagens).")

        daily = st.session_state.get("sg_daily_df")
        daily_meta = st.session_state.get("sg_daily_meta")
        if not isinstance(daily, pd.DataFrame) or daily.empty:
            st.info("Envie o arquivo **Faturamento e Atendidos.xlsx** em **Nova análise** para habilitar esta aba.")
        else:
            try:
                df = daily
                res_meta = daily_meta if isinstance(daily_meta, dict) else {}
                import plotly.express as px

                title_suffix = ""
                if isinstance(res_meta, dict) and res_meta.get("mes_referencia"):
                    title_suffix = f" — {res_meta.get('mes_referencia')}"

                df_plot = df

                # Resumo diário (inclui sábado) + seletor de dia
                try:
                    import datetime as _dt
                    import re as _re

                    mes_ref = (res_meta or {}).get("mes_referencia") if isinstance(res_meta, dict) else None
                    today = _dt.date.today()
                    yy, mm = today.year, today.month
                    if mes_ref:
                        s = str(mes_ref).strip().lower()
                        m = _re.search(r"(?<!\d)(\d{1,2})\s*[/\\-]\s*(\d{2,4})(?!\d)", s)
                        if m:
                            mm = int(m.group(1))
                            yy = int(m.group(2))
                            if yy < 100:
                                yy += 2000
                        else:
                            m2 = _re.search(r"(?<!\d)(\d{4})\s*[/\\-]\s*(\d{1,2})(?!\d)", s)
                            if m2:
                                yy = int(m2.group(1))
                                mm = int(m2.group(2))

                    d0 = df.copy().sort_values("dia").reset_index(drop=True)
                    fat_s = pd.to_numeric(d0.get("faturamento"), errors="coerce").fillna(0.0)
                    nf_s = pd.to_numeric(d0.get("nfs_emitidas"), errors="coerce").fillna(0.0)
                    cli_s = pd.to_numeric(d0.get("clientes_atendidos"), errors="coerce").fillna(0.0)

                    move = (fat_s > 0) | (nf_s > 0) | (cli_s > 0)
                    last_move_day = int(d0.loc[move, "dia"].iloc[-1]) if move.any() else int(d0["dia"].max())
                    default_day = last_move_day if today.weekday() == 0 else int(d0["dia"].max())

                    wd_short = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
                    labels: list[str] = []
                    for dd in d0["dia"].astype(int).tolist():
                        try:
                            wd = _dt.date(int(yy), int(mm), int(dd)).weekday()
                            labels.append(f"Dia {dd:02d} ({wd_short[wd]})")
                        except Exception:
                            labels.append(f"Dia {dd:02d}")
                    label_to_day = {labels[i]: int(d0["dia"].iloc[i]) for i in range(len(labels))}

                    st.markdown("#### Fechamento diário (inclui sábado)")
                    cL, cR = st.columns([1.35, 1])
                    with cL:
                        sel_label = st.selectbox(
                            "Selecione o dia para análise",
                            options=labels,
                            index=max(0, next((i for i, lab in enumerate(labels) if label_to_day.get(lab) == default_day), len(labels) - 1)),
                            help="Dica: na segunda-feira o app sugere automaticamente o último dia com movimento (geralmente sábado).",
                            key="sg_daily_selected_label",
                        )
                    with cR:
                        st.caption(f"**Último dia com movimento**: {last_move_day:02d}")
                        # sinalização rápida de sábado com movimento (fica ao lado/abaixo do indicador)
                        try:
                            d0["_weekday"] = d0["dia"].apply(lambda dd: _dt.date(int(yy), int(mm), int(dd)).weekday())
                            sat = d0[d0["_weekday"] == 5].copy()
                            if not sat.empty:
                                sat_move = sat[
                                    (pd.to_numeric(sat.get("faturamento"), errors="coerce").fillna(0.0) > 0.0)
                                    | (pd.to_numeric(sat.get("nfs_emitidas"), errors="coerce").fillna(0) > 0)
                                    | (pd.to_numeric(sat.get("clientes_atendidos"), errors="coerce").fillna(0) > 0)
                                ]
                                if not sat_move.empty:
                                    sat_last = sat_move.sort_values("dia").iloc[-1]
                                    # sexta imediatamente anterior (se existir) para comparar com setas
                                    fri = d0[(d0["_weekday"] == 4) & (d0["dia"] < int(sat_last["dia"]))].sort_values("dia")
                                    fri_last = fri.iloc[-1] if not fri.empty else None

                                    def _arrow_delta(cur: float, ref: float, *, kind: str) -> str:
                                        diff = float(cur) - float(ref)
                                        if abs(diff) < 1e-9:
                                            return "→ 0"
                                        arrow = "▲" if diff > 0 else "▼"
                                        if kind == "money":
                                            return f"{arrow} R$ {abs(diff):,.2f}"
                                        return f"{arrow} {abs(diff):,.0f}"

                                    st.markdown(
                                        f"""
<div class="dp-pill" style="
  margin-top:6px;
  background:rgba(251,191,36,.12);
  border-color:rgba(251,191,36,.35);
  color:#FBBF24;
  font-weight:850;
">
  Sábado com faturamento (dia {int(sat_last["dia"]):02d})
</div>
""",
                                        unsafe_allow_html=True,
                                    )

                                    # detalhe compacto com valores + setas vs sexta
                                    try:
                                        sat_f = float(pd.to_numeric(sat_last.get("faturamento"), errors="coerce") or 0.0)
                                        sat_nf = int(pd.to_numeric(sat_last.get("nfs_emitidas"), errors="coerce") or 0)
                                        sat_cli = int(pd.to_numeric(sat_last.get("clientes_atendidos"), errors="coerce") or 0)
                                        fri_f = float(pd.to_numeric(fri_last.get("faturamento"), errors="coerce") or 0.0) if fri_last is not None else 0.0
                                        fri_nf = int(pd.to_numeric(fri_last.get("nfs_emitidas"), errors="coerce") or 0) if fri_last is not None else 0
                                        fri_cli = int(pd.to_numeric(fri_last.get("clientes_atendidos"), errors="coerce") or 0) if fri_last is not None else 0

                                        d_f = _arrow_delta(sat_f, fri_f, kind="money") if fri_last is not None else "—"
                                        d_nf = _arrow_delta(float(sat_nf), float(fri_nf), kind="int") if fri_last is not None else "—"
                                        d_cli = _arrow_delta(float(sat_cli), float(fri_cli), kind="int") if fri_last is not None else "—"

                                        st.markdown(
                                            f"""
<div style="margin-top:6px;display:flex;gap:8px;flex-wrap:wrap;">
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    Sáb: <b>R$ {sat_f:,.2f}</b> <span style="color:#94a3b8;font-weight:700">({d_f})</span>
  </span>
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    NFS <b>{sat_nf}</b> <span style="color:#94a3b8;font-weight:700">({d_nf})</span>
  </span>
  <span class="dp-pill" style="background:rgba(255,255,255,.02);border-color:rgba(255,255,255,.10);color:#E5E7EB;">
    Atend. <b>{sat_cli}</b> <span style="color:#94a3b8;font-weight:700">({d_cli})</span>
  </span>
</div>
""",
                                            unsafe_allow_html=True,
                                        )
                                    except Exception:
                                        pass
                        except Exception:
                            pass

                    sel_day = int(label_to_day.get(sel_label) or default_day)
                    row = d0[d0["dia"] == sel_day].head(1)
                    fat_v = float(pd.to_numeric(row.get("faturamento"), errors="coerce").fillna(0.0).iloc[0]) if not row.empty else 0.0
                    nf_v = int(pd.to_numeric(row.get("nfs_emitidas"), errors="coerce").fillna(0).iloc[0]) if not row.empty else 0
                    cli_v = int(pd.to_numeric(row.get("clientes_atendidos"), errors="coerce").fillna(0).iloc[0]) if not row.empty else 0

                    a1, a2, a3, a4 = st.columns(4)
                    with a1:
                        _sg_kpi_card("Dia selecionado", f"{sel_day:02d}", icon="🗓️", accent="#93c5fd", delta=None)
                    with a2:
                        _sg_kpi_card("Faturamento (dia)", f"R$ {fat_v:,.2f}", icon="💰", accent="#6EE7B7", delta=None)
                    with a3:
                        _sg_kpi_card("NFS (dia)", f"{nf_v:d}", icon="📦", accent="#C4B5FD", delta=None)
                    with a4:
                        _sg_kpi_card("Atendidos (dia)", f"{cli_v:d}", icon="👥", accent="#FBBF24", delta=None)

                    # Remove sábado dos gráficos/tabela (mas mantém análise via seletor)
                    try:
                        d0["_weekday"] = d0["dia"].apply(lambda dd: _dt.date(int(yy), int(mm), int(dd)).weekday())
                        df_plot = d0[d0["_weekday"] != 5].drop(columns=["_weekday"], errors="ignore").copy()
                    except Exception:
                        df_plot = d0.copy()

                except Exception:
                    pass

                    avg_fat = float(pd.to_numeric(df_plot.get("faturamento"), errors="coerce").fillna(0).mean())
                    avg_nf = float(pd.to_numeric(df_plot.get("nfs_emitidas"), errors="coerce").fillna(0).mean())
                    avg_cli = float(pd.to_numeric(df_plot.get("clientes_atendidos"), errors="coerce").fillna(0).mean())

                    fig_fat = px.line(
                        df_plot,
                        x="dia",
                        y="faturamento",
                        markers=True,
                        title=f"Faturamento por dia{title_suffix} (média: R$ {avg_fat:,.2f})",
                        labels={"dia": "Dia do mês", "faturamento": "Faturamento (R$)"},
                    )
                    fig_fat.update_traces(line_width=3)
                    st.plotly_chart(fig_fat, use_container_width=True, key="sg_evol_faturamento_line")

                    df_counts = df_plot.melt(
                        id_vars=["dia"],
                        value_vars=["nfs_emitidas", "clientes_atendidos"],
                        var_name="metric",
                        value_name="valor",
                    )
                    df_counts["metric"] = df_counts["metric"].map(
                        {"nfs_emitidas": "NFS emitidas", "clientes_atendidos": "Clientes atendidos"}
                    )
                    fig_counts = px.line(
                        df_counts,
                        x="dia",
                        y="valor",
                        color="metric",
                        markers=True,
                        title=f"NFS e Atendidos por dia{title_suffix} (médias: NFS {avg_nf:.1f} | Atendidos {avg_cli:.1f})",
                        labels={"dia": "Dia do mês", "valor": "Quantidade", "metric": ""},
                    )
                    fig_counts.update_traces(line_width=3)
                    st.plotly_chart(fig_counts, use_container_width=True, key="sg_evol_counts_line")

                    with st.expander("Ver base diária (tabela)"):
                        df_show = df_plot.copy()
                        df_show = df_show.sort_values("dia").reset_index(drop=True)

                        def _delta_str(v: float, is_money: bool) -> str:
                            if v is None or (isinstance(v, float) and pd.isna(v)):
                                return "—"
                            x = float(v)
                            if abs(x) < 1e-9:
                                return "→ 0"
                            arrow = "▲" if x > 0 else "▼"
                            if is_money:
                                return f"{arrow} R$ {abs(x):,.2f}"
                            return f"{arrow} {abs(x):,.0f}"

                        # deltas vs dia anterior (para deixar a tabela mais interativa)
                        df_show["Δ faturamento"] = pd.to_numeric(df_show["faturamento"], errors="coerce").diff().apply(
                            lambda x: _delta_str(x, is_money=True)
                        )
                        df_show["Δ clientes"] = pd.to_numeric(df_show["clientes_atendidos"], errors="coerce").diff().apply(
                            lambda x: _delta_str(x, is_money=False)
                        )
                        df_show["Δ NFS"] = pd.to_numeric(df_show["nfs_emitidas"], errors="coerce").diff().apply(
                            lambda x: _delta_str(x, is_money=False)
                        )

                        def _delta_color(s: pd.Series) -> list[str]:
                            out = []
                            for v in s.astype(str).fillna("—").tolist():
                                if v.startswith("▲"):
                                    out.append("color:#22c55e; font-weight:700;")
                                elif v.startswith("▼"):
                                    out.append("color:#fb7185; font-weight:700;")
                                elif v.startswith("→"):
                                    out.append("color:#94a3b8; font-weight:600;")
                                else:
                                    out.append("color:#94a3b8;")
                            return out

                        styled = (
                            df_show.rename(
                                columns={
                                    "dia": "Dia",
                                    "faturamento": "Faturamento",
                                    "clientes_atendidos": "Atendidos",
                                    "nfs_emitidas": "NFS",
                                }
                            )
                            .style.format(
                                {
                                    "Faturamento": lambda x: f"R$ {float(x):,.2f}",
                                    "Atendidos": lambda x: f"{int(x):d}",
                                    "NFS": lambda x: f"{int(x):d}",
                                },
                                na_rep="—",
                            )
                            .apply(_delta_color, subset=["Δ faturamento", "Δ clientes", "Δ NFS"])
                        )
                        st.dataframe(styled, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error("Falha ao ler o Excel para evolução diária.")
                st.caption(str(e))

    with tab_rel:
        st.markdown("### Relatório executivo (auto)")
        st.caption("Gera um texto no padrão de highlights, usando os dados do seu dashboard (sem precisar IA).")

        # Base ativa (vendedores / totais)
        active_id = st.session_state.get("active_analysis_id")
        payload_base: dict | None = None
        if active_id is not None:
            r0 = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
            if r0:
                try:
                    payload_base = json.loads(r0.payload_json)
                except Exception:
                    payload_base = None

        totais = (payload_base or {}).get("totais") if isinstance(payload_base, dict) else {}
        if not isinstance(totais, dict):
            totais = {}

        fat_atual = float(totais.get("faturamento_total") or 0.0)
        meta_total = float(totais.get("meta_total") or 0.0)
        perc_meta = (fat_atual / meta_total * 100.0) if meta_total > 0 else None
        falta_meta = max(0.0, meta_total - fat_atual) if meta_total > 0 else 0.0
        falta_por_dia = (falta_meta / dias_restantes) if dias_restantes > 0 else None

        # KPIs diários (últimos salvos)
        prev = _last_payload_of_kind("sala_gestao_kpis") or {}
        prev_k = prev.get("kpis") if isinstance(prev, dict) else {}
        if not isinstance(prev_k, dict):
            prev_k = {}

        # Vendedores (faixas de alcance)
        vend_counts = {"meta_batida": 0, "acima_80": 0, "abaixo_80": 0}
        try:
            if isinstance(payload_base, dict):
                sellers = parse_sellers(payload_base)
                results, _ = calcular_time(sellers) if sellers else ([], 0.0)
                vend_df = pd.DataFrame([r.__dict__ for r in results]) if results else pd.DataFrame()
                vend_df = _enrich_results_df_for_performance(vend_df, sellers)
                if not vend_df.empty and "alcance_real_pct" in vend_df.columns:
                    s = pd.to_numeric(vend_df["alcance_real_pct"], errors="coerce")
                    vend_counts["meta_batida"] = int((s >= 100).sum())
                    vend_counts["acima_80"] = int(((s >= 80) & (s < 100)).sum())
                    vend_counts["abaixo_80"] = int((s < 80).sum())
        except Exception:
            pass

        # Departamentos: análise ativa (`_sg_dept`); comparativo só se houver snapshot anterior no histórico
        dept_today_df = None
        dept_yday_df = None
        try:
            if isinstance(payload_base, dict) and isinstance(payload_base.get("_sg_dept"), dict):
                dept_today_df = pd.DataFrame((payload_base.get("_sg_dept") or {}).get("departamentos") or [])
            if isinstance(dept_today_df, pd.DataFrame) and not dept_today_df.empty:
                prev_d = _pick_prev_dept_payload(
                    {"departamentos": dept_today_df.to_dict(orient="records")}
                )
                if isinstance(prev_d, dict) and prev_d.get("departamentos"):
                    dept_yday_df = pd.DataFrame(prev_d.get("departamentos") or [])
        except Exception:
            dept_today_df = None
            dept_yday_df = None

        def _fmt_rs(v: object) -> str:
            try:
                return f"R$ {float(v):,.2f}"
            except Exception:
                return "—"

        def _fmt_pct(v: object, digits: int = 1) -> str:
            try:
                return f"{float(v):.{digits}f}%"
            except Exception:
                return "—"

        # Evolução diária (se já carregou o Excel nessa sessão)
        daily = st.session_state.get("sg_daily_df")
        daily_meta = st.session_state.get("sg_daily_meta") if isinstance(st.session_state.get("sg_daily_meta"), dict) else {}

        daily_line = "—"
        try:
            if isinstance(daily, pd.DataFrame) and not daily.empty:
                d = daily.copy()
                fat_s = pd.to_numeric(d.get("faturamento"), errors="coerce").fillna(0.0)
                nf_s = pd.to_numeric(d.get("nfs_emitidas"), errors="coerce").fillna(0.0)
                cli_s = pd.to_numeric(d.get("clientes_atendidos"), errors="coerce").fillna(0.0)
                best_day = int(d.loc[fat_s.idxmax(), "dia"]) if len(d) else None
                worst_day = int(d.loc[fat_s.idxmin(), "dia"]) if len(d) else None
                daily_line = (
                    f"Evolução diária: média faturamento {_fmt_rs(float(fat_s.mean()))}, "
                    f"média NFs {float(nf_s.mean()):.1f}, média atendidos {float(cli_s.mean()):.1f}. "
                    f"Melhor dia: {best_day} | Pior dia: {worst_day}."
                )
        except Exception:
            pass

        mes_ref = str(daily_meta.get("mes_referencia") or "").strip()
        head = f"## Relatório executivo — Sala de Gestão\n\n**Período**: {mes_ref or str((payload_base or {}).get('periodo') or '—')}\n"

        sec1 = (
            "### 1) Resultado vs Meta\n"
            f"- **Faturamento (até agora)**: {_fmt_rs(fat_atual)}\n"
            f"- **Meta do time**: {_fmt_rs(meta_total) if meta_total > 0 else '—'}\n"
            f"- **% da meta**: {_fmt_pct(perc_meta) if perc_meta is not None else '—'}\n"
            f"- **Gap p/ meta**: {_fmt_rs(falta_meta) if meta_total > 0 else '—'}\n"
            f"- **Meta por dia útil (necessária)**: {_fmt_rs(falta_por_dia) if falta_por_dia is not None else '—'}\n"
        )

        sec2 = (
            "### 2) Dia anterior (KPIs operacionais)\n"
            f"- **Faturamento (dia anterior)**: {_fmt_rs(prev_k.get('faturamento_dia_anterior'))}\n"
            f"- **NFs (dia anterior)**: {int(prev_k.get('nf_dia_anterior') or 0)}\n"
            f"- **Clientes (dia anterior)**: {int(prev_k.get('clientes_dia_anterior') or 0)}\n"
        )

        sec3 = (
            "### 3) Cadência (dia a dia)\n"
            f"- {daily_line}\n"
        )

        sec4 = (
            "### 4) Vendedores (execução)\n"
            f"- **Meta batida (>=100%)**: {vend_counts.get('meta_batida', 0)}\n"
            f"- **Alcance >=80%**: {vend_counts.get('acima_80', 0)}\n"
            f"- **Alcance <80%**: {vend_counts.get('abaixo_80', 0)}\n"
        )

        sec5 = "### 5) Departamentos (análise ativa; Δ se houver snapshot anterior)\n"
        try:
            if isinstance(dept_today_df, pd.DataFrame) and not dept_today_df.empty and isinstance(dept_yday_df, pd.DataFrame) and not dept_yday_df.empty:
                a = dept_today_df.copy()
                b = dept_yday_df.copy()
                a["departamento"] = a.get("departamento").astype(str)
                b["departamento"] = b.get("departamento").astype(str)
                a = a.set_index("departamento")
                b = b.set_index("departamento")
                # delta de faturamento se existir
                if "faturamento" in a.columns and "faturamento" in b.columns:
                    da = pd.to_numeric(a["faturamento"], errors="coerce")
                    db = pd.to_numeric(b["faturamento"], errors="coerce")
                    d = (da - db).dropna().sort_values(ascending=False)
                    top_up = d.head(3)
                    top_down = d.tail(3)
                    sec5 += "- **Top 3 alta (faturamento)**: " + ", ".join([f"{k} ({_fmt_rs(v)})" for k, v in top_up.items()]) + "\n"
                    sec5 += "- **Top 3 queda (faturamento)**: " + ", ".join([f"{k} ({_fmt_rs(v)})" for k, v in top_down.items()]) + "\n"
                else:
                    sec5 += "- Sem coluna de faturamento para comparar.\n"
            elif isinstance(dept_today_df, pd.DataFrame) and not dept_today_df.empty:
                sec5 += "- Situação atual por departamento disponível na análise ativa (sem segundo snapshot para Δ).\n"
            else:
                sec5 += "- Sem base de departamentos nesta análise (importe e salve com Excel de departamentos).\n"
        except Exception:
            sec5 += "- Não consegui montar o comparativo de departamentos.\n"

        sec6 = (
            "### 6) Próximos passos (direto ao ponto)\n"
            "- **Cadência**: estabilizar clientes atendidos/dia para sustentar NFs e faturamento.\n"
            "- **Qualidade**: recuperar ticket/mix se houver queda com base estável de clientes.\n"
            "- **Foco**: atacar o maior gargalo do dia (volume, conversão, ou mix por departamento).\n"
        )

        report_md = "\n\n".join([head, sec1, sec2, sec3, sec4, sec5, sec6]).strip() + "\n"

        # Gráficos (na tela) — para enriquecer leitura
        figs: dict[str, object] = {}
        try:
            import plotly.express as px

            if isinstance(daily, pd.DataFrame) and not daily.empty:
                df_line = daily.copy()
                fig1 = px.line(
                    df_line,
                    x="dia",
                    y="faturamento",
                    markers=True,
                    title="Evolução diária — Faturamento",
                    labels={"dia": "Dia do mês", "faturamento": "Faturamento (R$)"},
                )
                fig1.update_traces(line_width=3)
                figs["evolucao_faturamento"] = fig1

                df_counts = df_line.melt(
                    id_vars=["dia"],
                    value_vars=["nfs_emitidas", "clientes_atendidos"],
                    var_name="metric",
                    value_name="valor",
                )
                df_counts["metric"] = df_counts["metric"].map(
                    {"nfs_emitidas": "NFS emitidas", "clientes_atendidos": "Clientes atendidos"}
                )
                fig2 = px.line(
                    df_counts,
                    x="dia",
                    y="valor",
                    color="metric",
                    markers=True,
                    title="Evolução diária — NFS e Atendidos",
                    labels={"dia": "Dia do mês", "valor": "Quantidade", "metric": ""},
                )
                fig2.update_traces(line_width=3)
                figs["evolucao_volumes"] = fig2

            if isinstance(dept_today_df, pd.DataFrame) and not dept_today_df.empty and isinstance(dept_yday_df, pd.DataFrame) and not dept_yday_df.empty:
                a = dept_today_df.copy()
                b = dept_yday_df.copy()
                if "departamento" in a.columns and "departamento" in b.columns and "faturamento" in a.columns and "faturamento" in b.columns:
                    a = a.set_index("departamento")
                    b = b.set_index("departamento")
                    da = pd.to_numeric(a["faturamento"], errors="coerce")
                    db = pd.to_numeric(b["faturamento"], errors="coerce")
                    d = (da - db).dropna().sort_values(ascending=False)
                    dd = pd.DataFrame({"departamento": d.index.astype(str), "delta": d.values})
                    # top 10 por impacto absoluto
                    dd["abs"] = dd["delta"].abs()
                    dd = dd.sort_values("abs", ascending=False).head(10)
                    fig3 = px.bar(
                        dd.sort_values("delta", ascending=False),
                        x="delta",
                        y="departamento",
                        orientation="h",
                        title="Departamentos — Delta de faturamento vs dia anterior (Top 10)",
                        labels={"delta": "Δ Faturamento (R$)", "departamento": ""},
                    )
                    figs["dept_delta"] = fig3
        except Exception:
            figs = figs

        if figs:
            st.markdown("### Gráficos (resumo)")
            for k, fig in figs.items():
                st.plotly_chart(fig, use_container_width=True, key=f"rel_fig_{k}")

        st.markdown("### Texto do relatório")
        st.text_area("Relatório (markdown)", value=report_md, height=520)

        cdl1, cdl2 = st.columns(2)
        with cdl1:
            st.download_button(
                "⬇️ Baixar relatório (.md)",
                data=report_md.encode("utf-8"),
                file_name="relatorio_sala_gestao.md",
                mime="text/markdown",
                use_container_width=True,
            )

        # PDF (texto + gráficos)
        def _strip_md(s: str) -> str:
            # simples e seguro: remove markdown básico
            out = s.replace("**", "")
            out = out.replace("## ", "").replace("### ", "")
            return out

        def _pdf_safe_text(s: str) -> str:
            """
            Helvetica no FPDF não suporta alguns caracteres unicode (ex.: '—', '•', setas).
            Normaliza para ASCII/Latin-1 seguro.
            """
            if s is None:
                return ""
            out = str(s)
            out = out.replace("—", "-")
            out = out.replace("•", "-")
            out = out.replace("▲", "^").replace("▼", "v").replace("→", ">")
            out = out.replace("\u00A0", " ")  # nbsp
            # remove qualquer coisa fora de latin-1
            try:
                out = out.encode("latin-1", errors="ignore").decode("latin-1")
            except Exception:
                pass
            return out

        def _break_long_words(s: str, max_len: int = 60) -> str:
            """
            Evita erro do FPDF quando existe "palavra" maior que a largura útil.
            Insere quebras suaves em sequências longas sem espaços.
            """
            import re

            def _chunk(m: re.Match) -> str:
                w = m.group(0)
                return " ".join(w[i : i + max_len] for i in range(0, len(w), max_len))

            return re.sub(r"\S{" + str(max_len + 1) + r",}", _chunk, s)

        def _fig_to_png_bytes(fig_obj: object) -> bytes | None:
            try:
                # plotly Figure
                return fig_obj.to_image(format="png", scale=2)  # type: ignore[attr-defined]
            except Exception:
                return None

        def _build_pdf_bytes(text_md: str, fig_map: dict[str, object]) -> bytes:
            from fpdf import FPDF
            import tempfile
            from pathlib import Path

            pdf = FPDF(format="A4")
            pdf.set_auto_page_break(auto=True, margin=12)
            pdf.add_page()
            pdf.set_font("Helvetica", size=11)
            epw = float(pdf.w - pdf.l_margin - pdf.r_margin)
            for line in _strip_md(text_md).splitlines():
                if not line.strip():
                    pdf.ln(2)
                    continue
                safe = _break_long_words(_pdf_safe_text(line))
                # força cursor no início da linha para evitar "width < 1 char"
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(epw, 6, safe)

            # insere gráficos em páginas separadas (quando existirem)
            for title, fig_obj in fig_map.items():
                png = _fig_to_png_bytes(fig_obj)
                if not png:
                    continue
                pdf.add_page()
                pdf.set_font("Helvetica", size=11)
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(epw, 7, _break_long_words(_pdf_safe_text(f"Gráfico: {title}")))
                pdf.ln(2)
                with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                    tmp.write(png)
                    tmp_path = tmp.name
                try:
                    # largura útil A4 = 210mm - 2*margem (12) = 186mm
                    pdf.image(tmp_path, x=12, w=186)
                finally:
                    try:
                        Path(tmp_path).unlink(missing_ok=True)
                    except Exception:
                        pass

            return bytes(pdf.output(dest="S"))  # type: ignore[arg-type]

        with cdl2:
            try:
                pdf_bytes = _build_pdf_bytes(report_md, figs)
                st.download_button(
                    "⬇️ Baixar relatório (PDF)",
                    data=pdf_bytes,
                    file_name="relatorio_sala_gestao.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            except Exception as e:
                st.caption(f"PDF indisponível: {e}")

    with tab_vend:
        st.markdown("### Análise de vendedores")
        active_id = st.session_state.get("active_analysis_id")
        if active_id is None:
            st.info("Defina uma análise ativa no **Histórico** (vendedores) para visualizar aqui.")
        else:
            row = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
            if not row:
                st.warning("Análise ativa não encontrada.")
            else:
                payload = json.loads(row.payload_json)
                sellers = parse_sellers(payload)
                results, _ = calcular_time(sellers) if sellers else ([], 0.0)
                df = pd.DataFrame([r.__dict__ for r in results]) if results else pd.DataFrame()
                df = _enrich_results_df_for_performance(df, sellers)
                if df.empty:
                    st.caption("Sem vendedores.")
                else:
                    def _bucket(v):
                        # Regra da sala: faixas por % Alcance PROJETADO (para ficar alinhado ao consolidado)
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return "Sem dado"
                        x = float(v)
                        if x >= 100.0:
                            return "Meta batida (>=100%)"
                        if x >= 80.0:
                            return "Alcance >= 80%"
                        return "Alcance < 80%"

                    df["faixa_alcance"] = df["alcance_pct"].apply(_bucket)
                    st.dataframe(
                        df[["nome", "alcance_real_pct", "alcance_pct", "margem_pct", "conversao_pct", "interacoes", "faixa_alcance"]]
                        .rename(columns={"alcance_real_pct": "% Alcance", "alcance_pct": "% Alcance Projetado"}),
                        use_container_width=True,
                        hide_index=True,
                    )
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Meta batida", str(int((df["faixa_alcance"] == "Meta batida (>=100%)").sum())))
                    c2.metric(">= 80%", str(int((df["faixa_alcance"] == "Alcance >= 80%").sum())))
                    c3.metric("< 80%", str(int((df["faixa_alcance"] == "Alcance < 80%").sum())))

    with tab_dept:
        st.markdown("### Performance por departamentos")
        if st.session_state.get("dept_source_names"):
            st.caption("Base de departamentos carregada via **Nova análise** (upload único).")
        else:
            st.info("Para habilitar esta aba, envie os arquivos de **Departamentos** em **Nova análise**.")
            dw = st.session_state.get("dept_warnings")
            if isinstance(dw, list) and dw:
                with st.expander("Ver diagnóstico do import (Departamentos)", expanded=False):
                    for w in dw:
                        st.caption(str(w))
            cache_names = st.session_state.get("upload_files_cache")
            if isinstance(cache_names, dict) and cache_names:
                with st.expander("Arquivos recebidos no upload único", expanded=False):
                    for n in sorted(cache_names.keys()):
                        st.caption(str(n))

        dept_payload = st.session_state.get("dept_payload")
        if isinstance(dept_payload, dict) and isinstance(dept_payload.get("departamentos"), list):
            # aplicar as mesmas exclusões do consolidado/insights
            ddf = pd.DataFrame([d for d in dept_payload["departamentos"] if _dept_ok((d or {}).get("departamento"))])
            ddf = _ensure_participacao_pct(ddf)
            if not ddf.empty:
                # Cards (fora de tabela)
                alc_proj = pd.to_numeric(ddf.get("alcance_projetado_pct"), errors="coerce") if "alcance_projetado_pct" in ddf.columns else pd.Series([], dtype="float64")
                marg = pd.to_numeric(ddf.get("margem_pct"), errors="coerce") if "margem_pct" in ddf.columns else pd.Series([], dtype="float64")
                fat = pd.to_numeric(ddf.get("faturamento"), errors="coerce") if "faturamento" in ddf.columns else pd.Series([], dtype="float64")
                meta = pd.to_numeric(ddf.get("meta_faturamento"), errors="coerce") if "meta_faturamento" in ddf.columns else pd.Series([], dtype="float64")

                ddf2 = ddf.copy()
                if "meta_faturamento" in ddf2.columns and "faturamento" in ddf2.columns:
                    # Falta p/ meta (até hoje): meta proporcional aos dias úteis trabalhados - faturamento atual
                    # Regra pedida: somar metas "já trabalhadas no momento" e subtrair do faturamento.
                    cal = st.session_state.get("calendar_info") if isinstance(st.session_state.get("calendar_info"), dict) else {}
                    du_total = int(cal.get("dias_uteis_total") or 0)
                    du_trab = int(cal.get("dias_uteis_trabalhados") or 0)
                    ratio = (float(du_trab) / float(du_total)) if (du_total > 0 and du_trab >= 0) else 0.0
                    ratio = min(1.0, max(0.0, ratio))
                    meta_to_date = meta * ratio
                    ddf2["falta_meta"] = (meta_to_date - fat)
                    # soma só o que falta (não deixa negativo "puxar" o total)
                    ddf2["falta_meta"] = pd.to_numeric(ddf2["falta_meta"], errors="coerce").clip(lower=0)
                    # Alcance Real (%): Faturamento/Meta*100
                    mm = pd.to_numeric(ddf2.get("meta_faturamento"), errors="coerce")
                    ff = pd.to_numeric(ddf2.get("faturamento"), errors="coerce")
                    ddf2["alcance_real_pct"] = None
                    mask = mm.notna() & (mm > 0) & ff.notna()
                    ddf2.loc[mask, "alcance_real_pct"] = (ff[mask] / mm[mask]) * 100.0

                alc_real = pd.to_numeric(ddf2.get("alcance_real_pct"), errors="coerce") if "alcance_real_pct" in ddf2.columns else pd.Series([], dtype="float64")

                # Contagem baseada no ALCANÇADO REAL (não projetado)
                n100 = int((alc_real >= 100).sum()) if len(alc_real) else 0
                n80_90 = int(((alc_real >= 80) & (alc_real < 100)).sum()) if len(alc_real) else 0
                nlt = int((alc_real < 80).sum()) if len(alc_real) else 0

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Deptos Alcance Real >= 100%", str(n100))
                c2.metric("Deptos Alcance Real 80% a 99,9%", str(n80_90))
                c3.metric("Deptos Alcance Real < 80%", str(nlt))
                if "falta_meta" in ddf2.columns:
                    low = pd.to_numeric(ddf2["falta_meta"], errors="coerce")
                    c4.metric("Falta meta (até hoje) — Deptos", f"R$ {float(low.dropna().sum()):,.2f}" if low.notna().any() else "—")
                else:
                    c4.metric("Falta meta (até hoje) — Deptos", "—")

                st.markdown("### Departamentos: melhor margem vs detrator")
                if marg.notna().any():
                    best_i = int(marg.idxmax())
                    worst_i = int(marg.idxmin())
                    best = ddf2.loc[best_i]
                    worst = ddf2.loc[worst_i]
                    a1, a2 = st.columns(2)
                    a1.markdown(
                        f"<div class='dp-card'><div class='dp-kpi-label'>Melhor margem</div>"
                        f"<div class='dp-kpi-value' style='font-size:1.02rem'>{best.get('departamento')}</div>"
                        f"<div class='dp-sub' style='margin-top:6px'>% Margem: <b>{float(best.get('margem_pct') or 0)*1:.2f}%</b></div></div>",
                        unsafe_allow_html=True,
                    )
                    a2.markdown(
                        f"<div class='dp-card'><div class='dp-kpi-label'>Detrator (pior margem)</div>"
                        f"<div class='dp-kpi-value' style='font-size:1.02rem'>{worst.get('departamento')}</div>"
                        f"<div class='dp-sub' style='margin-top:6px'>% Margem: <b>{float(worst.get('margem_pct') or 0)*1:.2f}%</b></div></div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Sem coluna de margem para destacar melhor/detrator.")

                st.markdown("### Oportunidades (falta para meta baixa)")
                if "falta_meta" in ddf2.columns and "meta_faturamento" in ddf2.columns:
                    # Heurística: falta <= 5% da meta (e meta>0)
                    mm = pd.to_numeric(ddf2["meta_faturamento"], errors="coerce")
                    fm = pd.to_numeric(ddf2["falta_meta"], errors="coerce")
                    opp = ddf2[(mm > 0) & (fm.notna())].copy()
                    opp["falta_pct_meta"] = (fm / mm) * 100.0
                    opp = opp.sort_values(["falta_pct_meta", "falta_meta"], ascending=[True, True])
                    opp = opp.head(6)
                    if not opp.empty:
                        for _, r in opp.iterrows():
                            dept = r.get("departamento")
                            falta = r.get("falta_meta")
                            alc_p = r.get("alcance_projetado_pct")
                            part = r.get("participacao_pct") if "participacao_pct" in opp.columns else None
                            part_html = (
                                f"<span class='dp-pill'>% Part.: {float(part):.2f}%</span>"
                                if part is not None and pd.notna(part)
                                else ""
                            )
                            st.markdown(
                                f"<div class='dp-card' style='padding:14px 16px;margin-bottom:10px'>"
                                f"<div style='display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap'>"
                                f"<div style='color:#E5E7EB;font-weight:850'>{dept}</div>"
                                f"<span class='dp-pill'>Alc. Proj: {float(alc_p or 0):.1f}%</span>"
                                f"{part_html}"
                                f"</div>"
                                f"<div style='margin-top:8px;color:#CBD5E1'>Falta para meta: <b>R$ {float(falta or 0):,.2f}</b></div>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                    else:
                        st.caption("Não encontrei oportunidades (falta_meta calculável).")

                st.markdown("### Tabela (opcional)")
                if "meta_faturamento" in ddf.columns and "faturamento" in ddf.columns:
                    ddf["falta_meta"] = ddf.apply(
                        lambda r: (float(r["meta_faturamento"]) - float(r["faturamento"]))
                        if pd.notna(r.get("meta_faturamento")) and pd.notna(r.get("faturamento"))
                        else None,
                        axis=1,
                    )
                st.dataframe(ddf, use_container_width=True, hide_index=True)

                st.markdown("### Comparativo vs dia anterior (por departamento)")
                prev_pl = _pick_prev_dept_payload(dept_payload)
                if prev_pl is None or not prev_pl.get("departamentos"):
                    st.caption(
                        "A tabela principal desta aba já reflete a **análise ativa**. "
                        "O comparativo com Δ aparece quando existir outro snapshot de departamentos salvo no histórico (botão **Salvar Departamentos**)."
                    )
                else:
                    p_today = dept_payload
                    p_yday = prev_pl
                    try:
                        df_today = pd.DataFrame([d for d in (p_today.get("departamentos") or []) if _dept_ok((d or {}).get("departamento"))])
                        df_yday = pd.DataFrame([d for d in (p_yday.get("departamentos") or []) if _dept_ok((d or {}).get("departamento"))])
                    except Exception:
                        df_today = pd.DataFrame()
                        df_yday = pd.DataFrame()

                    df_today = _ensure_participacao_pct(df_today) if not df_today.empty else df_today
                    df_yday = _ensure_participacao_pct(df_yday) if not df_yday.empty else df_yday

                    if df_today.empty or df_yday.empty:
                        st.caption("Não consegui montar o comparativo (base vazia em algum dos dias).")
                    else:
                        def _add_falta_meta(df_in: pd.DataFrame) -> pd.DataFrame:
                            if df_in is None or df_in.empty:
                                return df_in
                            if "meta_faturamento" in df_in.columns and "faturamento" in df_in.columns:
                                mm = pd.to_numeric(df_in.get("meta_faturamento"), errors="coerce")
                                ff = pd.to_numeric(df_in.get("faturamento"), errors="coerce")
                                out = df_in.copy()
                                out["falta_meta"] = mm - ff
                                return out
                            return df_in

                        def _add_alcance_real(df_in: pd.DataFrame) -> pd.DataFrame:
                            """% Alcançado Real = (Faturamento / Meta) * 100."""
                            if df_in is None or df_in.empty:
                                return df_in
                            if "meta_faturamento" not in df_in.columns or "faturamento" not in df_in.columns:
                                return df_in
                            mm = pd.to_numeric(df_in.get("meta_faturamento"), errors="coerce")
                            ff = pd.to_numeric(df_in.get("faturamento"), errors="coerce")
                            out = df_in.copy()
                            out["alcance_real_pct"] = None
                            mask = mm.notna() & (mm > 0) & ff.notna()
                            out.loc[mask, "alcance_real_pct"] = (ff[mask] / mm[mask]) * 100.0
                            return out

                        df_today = _add_falta_meta(df_today)
                        df_yday = _add_falta_meta(df_yday)
                        df_today = _add_alcance_real(df_today)
                        df_yday = _add_alcance_real(df_yday)

                        key = "departamento"
                        keep_cols = [
                            "faturamento",
                            "participacao_pct",
                            "margem_pct",
                            "alcance_real_pct",
                            "alcance_projetado_pct",
                            "falta_meta",
                        ]
                        keep_cols = [c for c in keep_cols if c in df_today.columns and c in df_yday.columns]

                        t = df_today[[key] + keep_cols].copy()
                        y = df_yday[[key] + keep_cols].copy()
                        for c in keep_cols:
                            t[c] = pd.to_numeric(t[c], errors="coerce")
                            y[c] = pd.to_numeric(y[c], errors="coerce")

                        merged = t.merge(y, on=key, how="outer", suffixes=("_hoje", "_ontem"))
                        for c in keep_cols:
                            merged[f"Δ {c}"] = merged[f"{c}_hoje"] - merged[f"{c}_ontem"]

                        # resumo rápido (faturamento e margem)
                        if "Δ faturamento" in merged.columns:
                            d = pd.to_numeric(merged["Δ faturamento"], errors="coerce")
                            up = int((d > 0).sum())
                            down = int((d < 0).sum())
                            zero = int((d == 0).sum())
                            try:
                                import html as _html

                                def _mini_card(title: str, value: str, subtitle: str, *, icon: str, accent: str) -> None:
                                    st.markdown(
                                        f"""
<div class="dp-card" style="
  padding:12px 12px;
  min-height: 156px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
  border-color: rgba(59,130,246,.18);
  background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.18), transparent 60%),
              radial-gradient(900px 220px at 85% 10%, rgba(110,231,183,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{_html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{_html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{_html.escape(value)}</div>
  <div style="margin-top:8px;color:#CBD5E1;font-size:0.84rem;line-height:1.35;white-space:normal;">
    {_html.escape(subtitle) if subtitle else ""}
  </div>
</div>
""",
                                        unsafe_allow_html=True,
                                    )

                                depts = merged[key].astype(str).fillna("").tolist()
                                up_names = [depts[i] for i, v in enumerate(d.tolist()) if pd.notna(v) and float(v) > 0 and str(depts[i]).strip().lower() not in {"", "nan"}]
                                down_names = [depts[i] for i, v in enumerate(d.tolist()) if pd.notna(v) and float(v) < 0 and str(depts[i]).strip().lower() not in {"", "nan"}]
                                zero_names = [depts[i] for i, v in enumerate(d.tolist()) if pd.notna(v) and float(v) == 0 and str(depts[i]).strip().lower() not in {"", "nan"}]

                                def _join_names(xs: list[str], max_n: int = 4) -> str:
                                    xs2 = [str(x).strip() for x in xs if str(x).strip()]
                                    if not xs2:
                                        return "—"
                                    head = xs2[:max_n]
                                    tail = len(xs2) - len(head)
                                    return ", ".join(head) + (f" (+{tail})" if tail > 0 else "")

                                s1, s2, s3 = st.columns(3)
                                with s1:
                                    _mini_card("↑ Evolução (Fat.)", str(up), _join_names(up_names), icon="📈", accent="#6EE7B7")
                                with s2:
                                    _mini_card("↓ Queda (Fat.)", str(down), _join_names(down_names), icon="📉", accent="#fb7185")
                                with s3:
                                    _mini_card("→ Sem mudança (Fat.)", str(zero), _join_names(zero_names), icon="➖", accent="#94a3b8")
                            except Exception:
                                s1, s2, s3 = st.columns(3)
                                s1.metric("↑ Deptos com evolução (Fat.)", str(up))
                                s2.metric("↓ Deptos com queda (Fat.)", str(down))
                                s3.metric("→ Sem mudança (Fat.)", str(zero))

                        def _arrow(v: object, kind: str) -> str:
                            if v is None or (isinstance(v, float) and pd.isna(v)):
                                return "—"
                            x = float(v)
                            if abs(x) < 1e-9:
                                return "→ 0"
                            arrow = "▲" if x > 0 else "▼"
                            if kind == "money":
                                return f"{arrow} R$ {abs(x):,.2f}"
                            if kind == "pct":
                                return f"{arrow} {abs(x):.2f} pp"
                            return f"{arrow} {abs(x):.0f}"

                        show = pd.DataFrame()
                        show["Departamento"] = merged[key].astype(str)
                        # limpa linhas inválidas ("nan", vazio)
                        show["Departamento"] = show["Departamento"].astype(str).str.strip()
                        show = show[(show["Departamento"] != "") & (show["Departamento"].str.lower() != "nan")].copy()
                        if "faturamento_hoje" in merged.columns:
                            show["Faturamento"] = merged["faturamento_hoje"]
                            show["Δ Faturamento"] = merged["Δ faturamento"].apply(lambda x: _arrow(x, "money"))
                        if "participacao_pct_hoje" in merged.columns:
                            show["% Part."] = merged["participacao_pct_hoje"]
                            show["Δ % Part."] = merged["Δ participacao_pct"].apply(lambda x: _arrow(x, "pct"))
                        if "margem_pct_hoje" in merged.columns:
                            show["% Margem"] = merged["margem_pct_hoje"]
                            show["Δ % Margem"] = merged["Δ margem_pct"].apply(lambda x: _arrow(x, "pct"))
                        if "alcance_projetado_pct_hoje" in merged.columns:
                            show["Alc. Proj."] = merged["alcance_projetado_pct_hoje"]
                            show["Δ Alc. Proj."] = merged["Δ alcance_projetado_pct"].apply(lambda x: _arrow(x, "pct"))
                        if "alcance_real_pct_hoje" in merged.columns:
                            show["Alc. Real"] = merged["alcance_real_pct_hoje"]
                            show["Δ Alc. Real"] = merged["Δ alcance_real_pct"].apply(lambda x: _arrow(x, "pct"))
                        if "falta_meta_hoje" in merged.columns:
                            show["Falta meta"] = merged["falta_meta_hoje"]
                            show["Δ Falta meta"] = merged["Δ falta_meta"].apply(lambda x: _arrow(x, "money"))

                        # reordena colunas (leitura mais moderna: valor → delta)
                        preferred = [
                            "Departamento",
                            "Faturamento",
                            "Δ Faturamento",
                            "Falta meta",
                            "Δ Falta meta",
                            "Alc. Real",
                            "Δ Alc. Real",
                            "Alc. Proj.",
                            "Δ Alc. Proj.",
                            "% Part.",
                            "Δ % Part.",
                            "% Margem",
                            "Δ % Margem",
                        ]
                        show = show[[c for c in preferred if c in show.columns]].copy()

                        def _delta_color_series(s: pd.Series) -> list[str]:
                            out2 = []
                            for v in s.astype(str).fillna("—").tolist():
                                if v.startswith("▲"):
                                    out2.append("color:#22c55e; font-weight:800;")
                                elif v.startswith("▼"):
                                    out2.append("color:#fb7185; font-weight:800;")
                                elif v.startswith("→"):
                                    out2.append("color:#94a3b8; font-weight:650;")
                                else:
                                    out2.append("color:#94a3b8;")
                            return out2

                        def _alc_bucket_style(s: pd.Series) -> list[str]:
                            out3: list[str] = []
                            for v in s.tolist():
                                try:
                                    x = float(v)
                                except Exception:
                                    out3.append("color:#94a3b8;")
                                    continue
                                if x >= 100:
                                    out3.append("background-color: rgba(34,197,94,.14); color:#bbf7d0; font-weight:900;")
                                elif x >= 80:
                                    out3.append("background-color: rgba(251,191,36,.14); color:#fde68a; font-weight:900;")
                                else:
                                    out3.append("background-color: rgba(251,113,133,.14); color:#fecdd3; font-weight:900;")
                            return out3

                        fmt: dict[str, object] = {}
                        if "Faturamento" in show.columns:
                            fmt["Faturamento"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"
                        if "% Part." in show.columns:
                            fmt["% Part."] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                        if "% Margem" in show.columns:
                            fmt["% Margem"] = lambda x: f"{float(x):.2f}%" if pd.notna(x) else "—"
                        if "Alc. Proj." in show.columns:
                            fmt["Alc. Proj."] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                        if "Alc. Real" in show.columns:
                            fmt["Alc. Real"] = lambda x: f"{float(x):.1f}%" if pd.notna(x) else "—"
                        if "Falta meta" in show.columns:
                            fmt["Falta meta"] = lambda x: f"R$ {float(x):,.2f}" if pd.notna(x) else "—"

                        styled = show.style.format(fmt, na_rep="—")
                        if "Alc. Proj." in show.columns:
                            styled = styled.apply(_alc_bucket_style, subset=["Alc. Proj."])
                        if "Alc. Real" in show.columns:
                            styled = styled.apply(_alc_bucket_style, subset=["Alc. Real"])
                        for c in [c for c in show.columns if c.startswith("Δ ")]:
                            styled = styled.apply(_delta_color_series, subset=[c])
                        st.dataframe(styled, use_container_width=True, hide_index=True)

                st.markdown("### Potenciais oportunidades (Alcance Projetado ≥ 80%)")
                st.caption(
                    "Lista baseada na **última base carregada**. Considera **Alcance Projetado** e, quando existir, "
                    "**Fat. Projetado Acumulado / Meta** para calcular a falta para 100%."
                )
                if "alcance_projetado_pct" in ddf2.columns:
                    alc_s = pd.to_numeric(ddf2.get("alcance_projetado_pct"), errors="coerce")
                    base = ddf2.copy()
                    base["alcance_projetado_pct"] = alc_s
                    base = base[base["alcance_projetado_pct"].notna() & (base["alcance_projetado_pct"] >= 80)].copy()

                    def _fmt_rs(x: object) -> str:
                        try:
                            v = float(x)  # type: ignore[arg-type]
                        except Exception:
                            return "—"
                        return f"R$ {v:,.2f}"

                    def _fmt_pct(x: object) -> str:
                        try:
                            v = float(x)  # type: ignore[arg-type]
                        except Exception:
                            return "—"
                        return f"{v:,.2f}%"

                    # falta para 100%: prioriza Fat.Proj.Acum / Meta; fallback no faturamento
                    mm = pd.to_numeric(base.get("meta_faturamento"), errors="coerce") if "meta_faturamento" in base.columns else None
                    proj = (
                        pd.to_numeric(base.get("faturamento_projetado_acumulado"), errors="coerce")
                        if "faturamento_projetado_acumulado" in base.columns
                        else None
                    )
                    fat = pd.to_numeric(base.get("faturamento"), errors="coerce") if "faturamento" in base.columns else None

                    if mm is not None and mm.notna().any():
                        if proj is not None and proj.notna().any():
                            base["falta_para_100"] = (mm - proj)
                            base["base_calc"] = "proj"
                        elif fat is not None and fat.notna().any():
                            base["falta_para_100"] = (mm - fat)
                            base["base_calc"] = "real"

                    # Subconjuntos: perto de bater (80-100) e já passou de 100
                    near = base[(base["alcance_projetado_pct"] < 100)].copy()
                    over = base[(base["alcance_projetado_pct"] >= 100)].copy()

                    if not near.empty:
                        # Ordenar por menor falta (se existir), senão por maior alcance
                        if "falta_para_100" in near.columns:
                            near["_ord"] = pd.to_numeric(near["falta_para_100"], errors="coerce")
                            near = near.sort_values(["_ord", "alcance_projetado_pct"], ascending=[True, False])
                        else:
                            near = near.sort_values("alcance_projetado_pct", ascending=False)
                        st.markdown("**🟡 Quase lá (80% a 99,99%)**")
                        for _, r in near.head(12).iterrows():
                            dept = r.get("departamento") or "—"
                            alc = r.get("alcance_projetado_pct")
                            meta = r.get("meta_faturamento")
                            proj_v = r.get("faturamento_projetado_acumulado") if "faturamento_projetado_acumulado" in near.columns else None
                            fat_v = r.get("faturamento") if "faturamento" in near.columns else None
                            falta = r.get("falta_para_100") if "falta_para_100" in near.columns else None
                            part = r.get("participacao_pct") if "participacao_pct" in near.columns else None
                            base_calc = str(r.get("base_calc") or "")
                            pill = "Proj." if base_calc == "proj" else ("Real" if base_calc == "real" else "—")

                            st.markdown(
                                f"""\n<div class="dp-card" style="padding:14px 16px;margin-bottom:10px;">
  <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:flex-start;">
    <div style="color:#E5E7EB;font-weight:900;font-size:1.02rem;">{html.escape(str(dept))}</div>
    <span class="dp-pill" style="border-color:rgba(255,255,255,.12);">Alc. Proj: <b>{html.escape(_fmt_pct(alc))}</b></span>
  </div>
  <div style="margin-top:8px;display:flex;gap:10px;flex-wrap:wrap;color:#CBD5E1;">
    <div class="dp-pill" style="background:rgba(255,255,255,.02);">Meta: <b>{html.escape(_fmt_rs(meta))}</b></div>
    {('<div class="dp-pill" style="background:rgba(255,255,255,.02);">% Part.: <b>' + html.escape(_fmt_pct(part)) + '</b></div>') if part is not None else ''}
    {('<div class="dp-pill" style="background:rgba(255,255,255,.02);">Fat. Proj. Acum: <b>' + html.escape(_fmt_rs(proj_v)) + '</b></div>') if proj_v is not None else ''}
    {('<div class="dp-pill" style="background:rgba(255,255,255,.02);">Faturamento: <b>' + html.escape(_fmt_rs(fat_v)) + '</b></div>') if (proj_v is None and fat_v is not None) else ''}
    {('<div class="dp-pill" style="background:rgba(251,191,36,.12);border-color:rgba(251,191,36,.35);color:#FBBF24;">Falta p/ 100% (' + html.escape(pill) + '): <b>' + html.escape(_fmt_rs(falta)) + '</b></div>') if falta is not None else ''}
  </div>
</div>
""",
                                unsafe_allow_html=True,
                            )
                    else:
                        st.caption("Nenhum departamento entre 80% e 100% de alcance projetado.")

                    if not over.empty:
                        over = over.sort_values("alcance_projetado_pct", ascending=False)
                        st.markdown("**🟢 Meta já batida (≥ 100%)**")
                        for _, r in over.head(8).iterrows():
                            dept = r.get("departamento") or "—"
                            alc = r.get("alcance_projetado_pct")
                            part = r.get("participacao_pct") if "participacao_pct" in over.columns else None
                            st.markdown(
                                f"""\n<div class="dp-card" style="padding:12px 14px;margin-bottom:10px;">
  <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:flex-start;">
    <div style="color:#E5E7EB;font-weight:850;">{html.escape(str(dept))}</div>
    <span class="dp-pill" style="background:rgba(34,197,94,.14);border-color:rgba(34,197,94,.35);color:#6EE7B7;">Alc. Proj: <b>{html.escape(_fmt_pct(alc))}</b></span>
  </div>
  {('<div style="margin-top:8px;color:#CBD5E1"><span class="dp-pill" style="background:rgba(255,255,255,.02);">% Part.: <b>' + html.escape(_fmt_pct(part)) + '</b></span></div>') if part is not None else ''}
</div>
""",
                                unsafe_allow_html=True,
                            )
                    if base.empty:
                        st.caption("Nenhum departamento com alcance projetado ≥ 80%.")

                if st.button("💾 Salvar Departamentos (Sala de Gestão)", use_container_width=True):
                    payload = dict(dept_payload)
                    payload["_kind"] = "sala_gestao_departamentos"
                    analysis_id = save_analysis(
                        conn,
                        periodo=str((payload_base or {}).get("periodo") or "Sala de Gestão (Dept)"),
                        provider_used=str((st.session_state.get("dept_meta") or {}).get("provider") or "dept"),
                        model_used=str((st.session_state.get("dept_meta") or {}).get("model") or "dept"),
                        # Linka esta base ao histórico ativo (permite recarregar sem reupload)
                        parent_analysis_id=int(active_id) if active_id is not None else None,
                        owner_user_id=owner_id,
                        payload=payload,
                        total_bonus=0.0,
                    )
                    st.success(f"Departamentos salvos como análise **#{analysis_id}**.")
        else:
            st.caption("Nenhuma base de departamentos carregada ainda.")

    with tab_radar:
        st.markdown("### Radar de oportunidades (manual)")
        radar = st.session_state.get("radar") or [{"oportunidade": "", "responsavel": "", "impacto": "", "prazo": ""}]
        rdf = pd.DataFrame(radar)
        edited = st.data_editor(rdf, num_rows="dynamic", use_container_width=True, hide_index=True)
        st.session_state["radar"] = edited.to_dict(orient="records")
        if st.button("💾 Salvar Radar (Sala de Gestão)", use_container_width=True):
            payload = {"_kind": "sala_gestao_radar", "radar": st.session_state.get("radar") or []}
            analysis_id = save_analysis(
                conn,
                periodo="Sala de Gestão (Radar)",
                provider_used="manual_radar",
                model_used="manual_radar",
                parent_analysis_id=None,
                owner_user_id=owner_id,
                payload=payload,
                total_bonus=0.0,
            )
            st.success(f"Radar salvo como análise **#{analysis_id}**.")


def page_orcamentos(settings, conn) -> None:
    render_header(
        "Orçamento x Conversão",
        "Visão do time ou por consultor — cards no estilo Projeção/Simulação, com faixas de valor e tipo F/J.",
    )

    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"

    # Descobrir análise ativa / última análise do tipo
    active_id = st.session_state.get("active_orcamentos_analysis_id")
    if active_id is None:
        rows = list_analyses(conn, limit=200, owner_user_id=owner_id, include_all=is_admin)
        for r in rows:
            try:
                p = json.loads(r.payload_json)
            except Exception:
                continue
            if isinstance(p, dict) and p.get("_kind") == "orcamentos":
                active_id = int(r.id)
                st.session_state["active_orcamentos_analysis_id"] = active_id
                break

    if active_id is None:
        st.info("Nenhuma análise de orçamentos ainda. Vá em **Nova análise** e salve os 2 arquivos (Pendentes + Finalizados).")
        return

    row = get_analysis(conn, int(active_id), owner_user_id=owner_id, include_all=is_admin)
    if not row:
        st.warning("Análise de orçamentos não encontrada.")
        return

    payload = json.loads(row.payload_json)
    pend_rows = ((payload.get("pendentes") or {}).get("rows")) if isinstance(payload, dict) else None
    fin_rows = ((payload.get("finalizados") or {}).get("rows")) if isinstance(payload, dict) else None
    if not isinstance(pend_rows, list) or not isinstance(fin_rows, list):
        st.warning("Payload de orçamentos inválido (sem rows).")
        return

    df_p = pd.DataFrame(pend_rows)
    df_f = pd.DataFrame(fin_rows)

    _FAIXA_ORDER = [
        "0,00–500",
        "500,01–1000",
        "1001,01–2000",
        "2000,01–5000",
        "5000,01–10000",
        "10000,01–30000",
        "30000,01+",
    ]

    def _faixa(v: float) -> str:
        if v <= 500:
            return "0,00–500"
        if v <= 1000:
            return "500,01–1000"
        if v <= 2000:
            return "1001,01–2000"
        if v <= 5000:
            return "2000,01–5000"
        if v <= 10000:
            return "5000,01–10000"
        if v <= 30000:
            return "10000,01–30000"
        return "30000,01+"

    def _faixa_rank(label: str) -> int:
        try:
            return _FAIXA_ORDER.index(str(label))
        except ValueError:
            return 999

    def _orc_section_header(title: str, subtitle: str, *, pill: str, accent: str) -> None:
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:14px 16px;
  border-color: rgba(59,130,246,.18);
  background:
    radial-gradient(900px 220px at 12% 0%, rgba(59,130,246,.18), transparent 60%),
    radial-gradient(900px 220px at 88% 12%, rgba(110,231,183,.10), transparent 55%),
    linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <div>
      <div style="color:#94A3B8;font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;font-weight:800;">
        {html.escape(subtitle)}
      </div>
      <div style="color:#E5E7EB;font-size:1.22rem;font-weight:950;margin-top:6px;line-height:1.2;">
        {html.escape(title)}
      </div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;">
      <span class="dp-pill" style="
        border-color: rgba(255,255,255,.12);
        background: rgba(255,255,255,.03);
        color: {accent};
        font-weight:850;
      ">{html.escape(pill)}</span>
    </div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    def _orc_modern_kpi(
        title: str,
        value: str,
        *,
        icon: str,
        accent: str,
        subtitle: str | None = None,
        subtitle_extra: str | None = None,
    ) -> None:
        sub = subtitle or ""
        sub2 = subtitle_extra or ""
        extra_block = ""
        if sub2:
            extra_block = (
                f'<div style="margin-top:6px;color:#CBD5E1;font-weight:600;font-size:0.78rem;line-height:1.45;">'
                f"{html.escape(sub2)}</div>"
            )
        st.markdown(
            f"""
<div class="dp-card" style="
  padding:14px 14px;
  min-height: 158px;
  display:flex;
  flex-direction:column;
  justify-content:space-between;
">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;">
    <div class="dp-kpi-label">{html.escape(title)}</div>
    <div style="
      width:28px;height:28px;border-radius:10px;
      display:flex;align-items:center;justify-content:center;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.10);
      font-size: 0.95rem;
      color: {accent};
    ">{html.escape(icon)}</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.35rem;color:{accent};text-shadow:0 0 24px rgba(59,130,246,.18);">{html.escape(value)}</div>
  <div style="margin-top:8px;color:#94a3b8;font-weight:650;font-size:0.84rem;">{html.escape(sub) if sub else ""}</div>
  {extra_block}
</div>
""",
            unsafe_allow_html=True,
        )

    def _render_faixa_card(
        fx: str,
        tot_q: int,
        tot_v: float,
        *,
        pct_q_scope: float,
        pct_v_scope: float,
        by_tipo: dict[str, tuple[int, float, float, float]],
    ) -> None:
        """by_tipo: tipo -> (qtd, valor, %q global, %v global)."""
        pills = []
        for tk in ("F", "J", ""):
            if tk not in by_tipo:
                continue
            qq, vv, pq, pv = by_tipo[tk]
            lab = tk if tk else "—"
            pills.append(
                f'<span class="dp-pill" style="background:rgba(255,255,255,.03);border-color:rgba(255,255,255,.10);">'
                f"<b>{html.escape(lab)}</b> · {qq} orç. ({pq:.1f}% qtd) · "
                f"<b>R$ {vv:,.2f}</b> ({pv:.1f}% valor)</span>"
            )
        pills_html = " ".join(pills) if pills else '<span style="color:#64748b;font-size:0.82rem;">Sem tipo</span>'
        st.markdown(
            f"""
<div class="dp-card" style="padding:14px 14px;min-height:188px;display:flex;flex-direction:column;justify-content:space-between;">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px;">
    <div>
      <div style="color:#94A3B8;font-size:.72rem;letter-spacing:.12em;text-transform:uppercase;font-weight:800;">Faixa de valor</div>
      <div style="color:#E5E7EB;font-size:1.05rem;font-weight:900;margin-top:6px;">{html.escape(fx)}</div>
    </div>
    <div style="width:28px;height:28px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.10);font-size:0.95rem;color:#C4B5FD;">◫</div>
  </div>
  <div class="dp-kpi-value" style="font-size:1.25rem;color:#93c5fd;">R$ {tot_v:,.2f} <span style="font-size:0.82rem;color:#94a3b8;font-weight:700;">({pct_v_scope:.1f}% do valor total)</span></div>
  <div style="margin-top:6px;color:#94a3b8;font-size:0.84rem;font-weight:650;">{tot_q} orç. · <b>{pct_q_scope:.1f}%</b> das quantidades no escopo</div>
  <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">{pills_html}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    st.markdown("### Filtros")
    f1, f2, f3 = st.columns([1.2, 1.4, 1.0])
    with f1:
        filiais = []
        if "_filial" in df_p.columns:
            filiais = sorted([x for x in df_p["_filial"].astype(str).unique().tolist() if x and x.lower() != "nan"])
        filial_pick = st.multiselect("Filial", options=filiais, default=filiais)
    with f2:
        ems = pd.to_datetime(df_p.get("_emissao"), errors="coerce") if "_emissao" in df_p.columns else pd.Series([], dtype="datetime64[ns]")
        dmin = ems.min().date() if len(ems.dropna()) else None
        dmax = ems.max().date() if len(ems.dropna()) else None
        if dmin and dmax:
            dr = st.date_input("Emissão (intervalo)", value=(dmin, dmax))
        else:
            dr = None
            st.caption("Sem coluna Emissão detectada.")
    with f3:
        tipo_pick = st.selectbox("CNPJ? (Tipo)", options=["Todos", "F", "J"], index=0)

    search = st.text_input("Buscar nº Orçamento", value="", placeholder="ex.: 458585")

    consultores_opts: list[str] = []
    has_consultor = "_consultor" in df_p.columns
    if has_consultor:
        raw_c = (
            df_p["_consultor"].astype(str).str.strip()
            .replace({"nan": "", "None": ""})
        )
        consultores_opts = sorted({x for x in raw_c.unique().tolist() if x})

    rmodo1, rmodo2 = st.columns([1.1, 1.4])
    with rmodo1:
        modo_orc = st.selectbox(
            "Escopo da análise",
            options=["Time (consolidado)", "Por consultor"],
            key="orc_escopo",
            help="Igual à Simulação / Projeções: primeiro o time; depois detalhe por consultor.",
        )
    with rmodo2:
        consultor_sel = None
        if modo_orc == "Por consultor":
            if consultores_opts:
                consultor_sel = st.selectbox("Consultor", options=consultores_opts, key="orc_consultor_pick")
            else:
                st.warning("Coluna de consultor não encontrada nos dados — use **Time (consolidado)**.")

    def _apply_filters(df_in: pd.DataFrame) -> pd.DataFrame:
        df = df_in.copy()
        if filial_pick and "_filial" in df.columns:
            df = df[df["_filial"].astype(str).isin(set(filial_pick))]
        if tipo_pick in {"F", "J"} and "_tipo_cliente" in df.columns:
            df = df[df["_tipo_cliente"].astype(str).str.upper() == tipo_pick]
        if search.strip() and "_orcamento" in df.columns:
            s = search.strip()
            df = df[df["_orcamento"].astype(str).str.contains(s, na=False)]
        if dr and isinstance(dr, tuple) and len(dr) == 2 and "_emissao" in df.columns:
            a, b = dr
            em = pd.to_datetime(df["_emissao"], errors="coerce")
            df = df[em.notna() & (em.dt.date >= a) & (em.dt.date <= b)]
        return df

    def _scope_cons(df_in: pd.DataFrame) -> pd.DataFrame:
        df = df_in.copy()
        if modo_orc == "Por consultor" and consultor_sel and "_consultor" in df.columns:
            m = df["_consultor"].astype(str).str.strip().str.lower() == str(consultor_sel).strip().lower()
            df = df[m]
        return df

    dfp = _scope_cons(_apply_filters(df_p))
    dff = _scope_cons(_apply_filters(df_f))

    def _sum_val(df: pd.DataFrame) -> float:
        v = pd.to_numeric(df.get("_valor"), errors="coerce").fillna(0.0) if "_valor" in df.columns else pd.Series([0.0])
        return float(v.sum())

    def _count_orc(df: pd.DataFrame) -> int:
        if "_orcamento" not in df.columns:
            return 0
        s = df["_orcamento"].astype(str).str.strip()
        s = s.replace("nan", "")
        return int(s.ne("").sum())

    def _norm_orc_df(df: pd.DataFrame) -> pd.DataFrame:
        """Prepara colunas de faixa e tipo para agrupamentos."""
        if df is None or len(df) == 0:
            return pd.DataFrame()
        out = df.copy()
        out["_valor_num"] = pd.to_numeric(out.get("_valor"), errors="coerce").fillna(0.0)
        out["faixa"] = out["_valor_num"].apply(lambda x: _faixa(float(x or 0.0)))
        if "_tipo_cliente" in out.columns:
            out["tipo"] = out["_tipo_cliente"].astype(str).str.upper().replace({"PF": "F", "PJ": "J"})
        else:
            out["tipo"] = ""
        return out

    def _orcamento_ids(df: pd.DataFrame) -> set[str]:
        if "_orcamento" not in df.columns or len(df) == 0:
            return set()
        return {
            str(x).strip()
            for x in df["_orcamento"].tolist()
            if str(x).strip() and str(x).strip().lower() not in {"nan", "none"}
        }

    def _conversion_rates_per_faixa(
        *,
        prev_p_df: pd.DataFrame,
        conv_ids_set: set[str],
        dfp_cur: pd.DataFrame,
        dff_cur: pd.DataFrame,
    ) -> tuple[list[tuple[str, float, int, int]], str]:
        rows: list[tuple[str, float, int, int]] = []
        note = ""
        if len(prev_p_df) and conv_ids_set and "_orcamento" in prev_p_df.columns:
            pn = _norm_orc_df(prev_p_df)
            if len(pn):
                for fx in sorted(pn["faixa"].unique().tolist(), key=_faixa_rank):
                    sub = pn[pn["faixa"].astype(str) == str(fx)]
                    ids = _orcamento_ids(sub)
                    if not ids:
                        continue
                    cnv = sum(1 for x in ids if x in conv_ids_set)
                    rows.append((str(fx), 100.0 * cnv / len(ids), int(cnv), len(ids)))
            note = (
                "Taxa por faixa = convertidos no cruzamento com a análise anterior ÷ "
                "pendentes da mesma faixa na análise anterior."
            )
        elif len(dfp_cur) and len(dff_cur) and "_orcamento" in dfp_cur.columns and "_orcamento" in dff_cur.columns:
            pn = _norm_orc_df(dfp_cur)
            fin_ids = _orcamento_ids(dff_cur)
            if len(pn):
                for fx in sorted(pn["faixa"].unique().tolist(), key=_faixa_rank):
                    sub = pn[pn["faixa"].astype(str) == str(fx)]
                    ids = _orcamento_ids(sub)
                    if not ids:
                        continue
                    cnv = sum(1 for x in ids if x in fin_ids)
                    rows.append((str(fx), 100.0 * cnv / len(ids), int(cnv), len(ids)))
            note = (
                "Taxa por faixa = pendentes da faixa que também constam em finalizados (import atual; sem análise anterior)."
            )
        else:
            note = "Sem dados para taxa por faixa."
        return rows, note

    def _best_worst_conv(rates: list[tuple[str, float, int, int]]) -> tuple[
        str | None,
        float | None,
        int | None,
        int | None,
        str | None,
        float | None,
        int | None,
        int | None,
    ]:
        if not rates:
            return None, None, None, None, None, None, None, None
        best = max(rates, key=lambda t: (t[1], t[2]))
        worst = min(rates, key=lambda t: (t[1], t[3]))
        return (
            best[0],
            float(best[1]),
            int(best[2]),
            int(best[3]),
            worst[0],
            float(worst[1]),
            int(worst[2]),
            int(worst[3]),
        )

    pend_q = _count_orc(dfp)
    pend_v = _sum_val(dfp)
    fin_q = _count_orc(dff)
    fin_v = _sum_val(dff)
    tot_q_scope = int(pend_q + fin_q)
    tot_v_scope = float(pend_v + fin_v)

    def _tipo_stats(df: pd.DataFrame) -> tuple[int, float, int, float]:
        """Retorna (q_pf, v_pf, q_pj, v_pj) no escopo já filtrado."""
        if df is None or len(df) == 0:
            return 0, 0.0, 0, 0.0
        t = df.get("_tipo_cliente")
        if t is None:
            return 0, 0.0, 0, 0.0
        tipo = t.astype(str).str.strip().str.upper().replace({"PF": "F", "PJ": "J"})
        v = pd.to_numeric(df.get("_valor"), errors="coerce").fillna(0.0) if "_valor" in df.columns else pd.Series([0.0] * len(df))
        m_pf = tipo == "F"
        m_pj = tipo == "J"
        return int(m_pf.sum()), float(v[m_pf].sum()), int(m_pj.sum()), float(v[m_pj].sum())

    def _pct_part(n: float, d: float) -> str:
        if d and d > 0:
            return f"{100.0 * float(n) / float(d):.1f}%"
        return "—"

    fin_q_pf, fin_v_pf, fin_q_pj, fin_v_pj = _tipo_stats(dff)
    fin_extra_q = (
        f"PF {_pct_part(fin_q_pf, fin_q)} · {fin_q_pf} orç. · "
        f"PJ {_pct_part(fin_q_pj, fin_q)} · {fin_q_pj} orç. "
        f"(sobre finalizados filtrados)"
    )
    fin_extra_v = (
        f"PF {_pct_part(fin_v_pf, fin_v)} · R$ {fin_v_pf:,.2f} · "
        f"PJ {_pct_part(fin_v_pj, fin_v)} · R$ {fin_v_pj:,.2f} "
        f"(sobre valor finalizado)"
    )

    periodo_lbl = str(getattr(row, "periodo", "") or "").strip()
    if periodo_lbl:
        st.caption(f"Período da análise ativa: **{periodo_lbl}** · ID **{int(active_id)}**")

    _orc_section_header("Resumo executivo", "Orçamentos no escopo atual", pill="Pend × Fin", accent="#93c5fd")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _orc_modern_kpi(
            "Pendentes (qtd)",
            str(pend_q),
            icon="📋",
            accent="#93c5fd",
            subtitle=f"{_pct_part(pend_q, tot_q_scope)} do total de orçamentos · linhas filtradas",
        )
    with k2:
        _orc_modern_kpi(
            "Pendentes (valor)",
            f"R$ {pend_v:,.2f}",
            icon="💰",
            accent="#FBBF24",
            subtitle=f"{_pct_part(pend_v, tot_v_scope)} do valor total (pend+fin)",
        )
    with k3:
        _orc_modern_kpi(
            "Finalizados (qtd)",
            str(fin_q),
            icon="✅",
            accent="#6EE7B7",
            subtitle=f"{_pct_part(fin_q, tot_q_scope)} do total de orçamentos · linhas filtradas",
            subtitle_extra=fin_extra_q,
        )
    with k4:
        _orc_modern_kpi(
            "Finalizados (valor)",
            f"R$ {fin_v:,.2f}",
            icon="📈",
            accent="#C4B5FD",
            subtitle=f"{_pct_part(fin_v, tot_v_scope)} do valor total (pend+fin)",
            subtitle_extra=fin_extra_v,
        )

    _orc_section_header("Volume total (filtro atual)", "Soma pendentes + finalizados", pill="Σ", accent="#a78bfa")
    df_all_scope = pd.concat([x for x in (dfp, dff) if x is not None and len(x)], ignore_index=True) if (len(dfp) or len(dff)) else pd.DataFrame()
    q_pf, v_pf, q_pj, v_pj = _tipo_stats(df_all_scope)
    ticket_pf = (v_pf / float(q_pf)) if q_pf else 0.0
    ticket_pj = (v_pj / float(q_pj)) if q_pj else 0.0
    gt1, gt2, gt3, gt4 = st.columns(4)
    with gt1:
        _orc_modern_kpi(
            "Total orçamentos (qtd)",
            str(tot_q_scope),
            icon="➕",
            accent="#e9d5ff",
            subtitle="pendentes + finalizados no escopo (100% da base deste bloco)",
        )
    with gt2:
        _orc_modern_kpi(
            "Total valores (R$)",
            f"R$ {tot_v_scope:,.2f}",
            icon="💎",
            accent="#fcd34d",
            subtitle="soma dos valores nos dois arquivos (mesmos filtros)",
        )
    with gt3:
        _orc_modern_kpi(
            "PF (clientes)",
            f"{q_pf} ({_pct_part(q_pf, tot_q_scope)})",
            icon="👤",
            accent="#93c5fd",
            subtitle=f"R$ {v_pf:,.2f} ({_pct_part(v_pf, tot_v_scope)}) · Ticket médio: R$ {ticket_pf:,.2f}",
        )
    with gt4:
        _orc_modern_kpi(
            "PJ (clientes)",
            f"{q_pj} ({_pct_part(q_pj, tot_q_scope)})",
            icon="🏢",
            accent="#6EE7B7",
            subtitle=f"R$ {v_pj:,.2f} ({_pct_part(v_pj, tot_v_scope)}) · Ticket médio: R$ {ticket_pj:,.2f}",
        )

    _orc_section_header("Conversão", "Pendente → finalizado (por cruzamento do nº Orçamento)", pill="Histórico", accent="#FBBF24")
    st.caption(
        "Validação pelo campo **Orçamento** entre esta análise e a **anterior** do mesmo tipo. "
        "No modo **consultor**, o cruzamento usa apenas linhas desse consultor nas duas bases."
    )

    prev_payload = None
    try:
        rows_prev = list_analyses(conn, limit=300, owner_user_id=owner_id, include_all=is_admin)
        seen_current = False
        for r in rows_prev:
            if int(r.id) == int(active_id):
                seen_current = True
                continue
            if not seen_current:
                continue
            try:
                p2 = json.loads(r.payload_json)
            except Exception:
                continue
            if isinstance(p2, dict) and p2.get("_kind") == "orcamentos":
                prev_payload = p2
                break
    except Exception:
        prev_payload = None

    conv_ids: set[str] = set()
    conv_val = 0.0
    prev_pend_q_scope = 0
    prev_p_scoped = pd.DataFrame()
    if isinstance(prev_payload, dict):
        prev_p = pd.DataFrame(((prev_payload.get("pendentes") or {}).get("rows")) or [])
        now_f = pd.DataFrame(((payload.get("finalizados") or {}).get("rows")) or [])
        if modo_orc == "Por consultor" and consultor_sel and "_consultor" in prev_p.columns:
            prev_p = prev_p[prev_p["_consultor"].astype(str).str.strip().str.lower() == str(consultor_sel).strip().lower()]
        if modo_orc == "Por consultor" and consultor_sel and "_consultor" in now_f.columns:
            now_f = now_f[now_f["_consultor"].astype(str).str.strip().str.lower() == str(consultor_sel).strip().lower()]
        prev_p_scoped = prev_p
        if "_orcamento" in prev_p.columns and "_orcamento" in now_f.columns:
            prev_pending = set(prev_p["_orcamento"].astype(str).str.strip().tolist())
            now_final = set(now_f["_orcamento"].astype(str).str.strip().tolist())
            conv_ids = {x for x in prev_pending.intersection(now_final) if x and x.lower() != "nan"}
            prev_pend_q_scope = _count_orc(prev_p)
            if conv_ids and "_valor" in prev_p.columns:
                vv = pd.to_numeric(prev_p["_valor"], errors="coerce").fillna(0.0)
                conv_val = float(vv[prev_p["_orcamento"].astype(str).str.strip().isin(conv_ids)].sum())

    pct_conv = None
    try:
        if prev_pend_q_scope > 0:
            pct_conv = (len(conv_ids) / float(prev_pend_q_scope)) * 100.0
    except Exception:
        pct_conv = None

    cnv1, cnv2, cnv3 = st.columns(3)
    conv_val_hint = (
        "Somatório na base de pendentes (análise anterior), só nos nº convertidos."
        if conv_ids
        else "Sem cruzamento pendente→finalizado com a análise anterior."
    )
    with cnv1:
        sub_cq = (
            f"{100.0 * len(conv_ids) / float(prev_pend_q_scope):.1f}% dos {prev_pend_q_scope} pendente(s) na análise anterior"
            if prev_pend_q_scope and conv_ids
            else ("Sem base anterior para %" if not prev_pend_q_scope else "Nenhum convertido neste cruzamento")
        )
        _orc_modern_kpi(
            "Convertidos (qtd)",
            str(len(conv_ids)),
            icon="✅",
            accent="#34d399",
            subtitle=sub_cq,
        )
    with cnv2:
        _orc_modern_kpi(
            "Convertidos (valor)",
            f"R$ {conv_val:,.2f}",
            icon="🏷",
            accent="#FBBF24",
            subtitle=conv_val_hint[:140] + ("…" if len(conv_val_hint) > 140 else ""),
        )
    with cnv3:
        pct_txt = f"{pct_conv:.1f}%" if pct_conv is not None else "—"
        sub_pct = (
            f"sobre {prev_pend_q_scope} pendente(s) na análise anterior (mesmo escopo)"
            if prev_pend_q_scope
            else "sem pendências na análise anterior para calcular %"
        )
        _orc_modern_kpi("Taxa de conversão", pct_txt, icon="🔁", accent="#C4B5FD", subtitle=sub_pct)

    faixa_fonte = st.radio(
        "Base das faixas",
        options=[
            "Todos (pendentes + finalizados)",
            "Apenas pendentes",
            "Apenas finalizados",
        ],
        horizontal=True,
        key="orc_faixa_fonte",
        help="Altera o conjunto de linhas usado nos cards de faixa e nos percentuais de representatividade.",
    )

    if str(faixa_fonte).startswith("Todos"):
        _fx_parts: list[pd.DataFrame] = []
        if len(dfp):
            _fx_parts.append(dfp)
        if len(dff):
            _fx_parts.append(dff)
        df_faixas = _norm_orc_df(pd.concat(_fx_parts, ignore_index=True)) if _fx_parts else pd.DataFrame()
        faixa_caption = (
            "Faixas sobre **pendentes e finalizados** somados (linhas dos dois arquivos no filtro). "
            "Os % são sobre o total deste conjunto."
        )
    elif str(faixa_fonte).startswith("Apenas pend"):
        df_faixas = _norm_orc_df(dfp)
        faixa_caption = "Faixas só no arquivo de **pendentes**. Percentuais sobre o total de pendentes filtrados."
    else:
        df_faixas = _norm_orc_df(dff)
        faixa_caption = "Faixas só no arquivo de **finalizados**. Percentuais sobre o total de finalizados filtrados."

    _orc_section_header("Faixas de valor", "Distribuição por faixa e tipo F/J", pill="Cards", accent="#6EE7B7")
    st.caption(faixa_caption)

    g_fx = (
        df_faixas.groupby(["faixa", "tipo"], as_index=False)
        .agg(qtd=("_orcamento", "count"), valor=("_valor_num", "sum"))
        if len(df_faixas)
        else pd.DataFrame(columns=["faixa", "tipo", "qtd", "valor"])
    )
    grand_q_fx = int(g_fx["qtd"].sum()) if len(g_fx) else 0
    grand_v_fx = float(g_fx["valor"].sum()) if len(g_fx) else 0.0
    faixas_sorted = sorted({str(x) for x in g_fx["faixa"].unique()}, key=_faixa_rank) if len(g_fx) else []

    fx_sum = (
        g_fx.groupby("faixa", as_index=False).agg(qtd=("qtd", "sum"), valor=("valor", "sum"))
        if len(g_fx)
        else pd.DataFrame(columns=["faixa", "qtd", "valor"])
    )
    conv_rates_list, conv_faixa_note = _conversion_rates_per_faixa(
        prev_p_df=prev_p_scoped,
        conv_ids_set=conv_ids,
        dfp_cur=dfp,
        dff_cur=dff,
    )
    bf, br, bc, bd, wf, wr, wc, wd = _best_worst_conv(conv_rates_list)

    if len(fx_sum):
        iq = int(fx_sum["qtd"].idxmax())
        iv = int(fx_sum["valor"].idxmax())
        fq = str(fx_sum.iloc[iq]["faixa"])
        fv_lbl = str(fx_sum.iloc[iv]["faixa"])
        q_q = int(fx_sum.iloc[iq]["qtd"])
        q_v = float(fx_sum.iloc[iv]["valor"])
        pq = (100.0 * q_q / grand_q_fx) if grand_q_fx else 0.0
        pv = (100.0 * q_v / grand_v_fx) if grand_v_fx else 0.0
        ins1, ins2, ins3 = st.columns(3)
        with ins1:
            _orc_modern_kpi(
                "Maior concentração · qtd",
                fq,
                icon="📊",
                accent="#93c5fd",
                subtitle=f"{q_q} orçamentos · {pq:.1f}% de todas as quantidades (escopo das faixas)",
            )
        with ins2:
            _orc_modern_kpi(
                "Maior concentração · valor",
                fv_lbl,
                icon="💎",
                accent="#fcd34d",
                subtitle=f"R$ {q_v:,.2f} · {pv:.1f}% de todo o valor (escopo das faixas)",
            )
        with ins3:
            if bf is not None and wf is not None:
                _orc_modern_kpi(
                    "Conversão por faixa (mais vs menos)",
                    f"↑ {bf} / ↓ {wf}",
                    icon="🔁",
                    accent="#6EE7B7",
                    subtitle=(
                        f"Melhor taxa: {br:.1f}% ({bc}/{bd} na base da faixa) · "
                        f"pior: {wr:.1f}% ({wc}/{wd}). {conv_faixa_note[:100]}"
                        + ("…" if len(conv_faixa_note) > 100 else "")
                    ),
                )
            else:
                _orc_modern_kpi(
                    "Conversão por faixa",
                    "—",
                    icon="🔁",
                    accent="#64748b",
                    subtitle=conv_faixa_note[:180] + ("…" if len(conv_faixa_note) > 180 else ""),
                )
        st.caption(conv_faixa_note)

    if not faixas_sorted:
        st.info("Sem dados para montar faixas neste escopo — ajuste filtros ou troque a base acima (pendentes/finalizados).")
    else:
        cols_per_row = 3
        for i in range(0, len(faixas_sorted), cols_per_row):
            chunk = faixas_sorted[i : i + cols_per_row]
            cols = st.columns(len(chunk))
            for j, fx in enumerate(chunk):
                sub = g_fx[g_fx["faixa"].astype(str) == fx]
                tot_q_f = int(sub["qtd"].sum())
                tot_v_f = float(sub["valor"].sum())
                pq_scope = (100.0 * tot_q_f / grand_q_fx) if grand_q_fx else 0.0
                pv_scope = (100.0 * tot_v_f / grand_v_fx) if grand_v_fx else 0.0
                by_tipo: dict[str, tuple[int, float, float, float]] = {}
                for _, rr in sub.iterrows():
                    tk = str(rr.get("tipo") or "").strip().upper()
                    if tk not in {"F", "J"}:
                        tk = ""
                    qq = int(rr["qtd"])
                    vv = float(rr["valor"])
                    pq = (100.0 * qq / grand_q_fx) if grand_q_fx else 0.0
                    pv = (100.0 * vv / grand_v_fx) if grand_v_fx else 0.0
                    by_tipo[tk] = (qq, vv, pq, pv)
                with cols[j]:
                    _render_faixa_card(
                        fx,
                        tot_q_f,
                        tot_v_f,
                        pct_q_scope=pq_scope,
                        pct_v_scope=pv_scope,
                        by_tipo=by_tipo,
                    )

    with st.expander("Tabelas detalhadas (setor / consultor / busca)", expanded=False):
        tab1, tab2, tab3 = st.tabs(["Setor", "Consultor", "Busca"])
        with tab1:
            st.markdown("### Setor (por faixa e tipo)")
            if len(g_fx):
                st.dataframe(g_fx.sort_values(["faixa", "tipo"]), use_container_width=True, hide_index=True)
            else:
                st.caption("Sem linhas.")
        with tab2:
            st.markdown("### Consultor (por faixa e tipo)")
            if "_consultor" in df_faixas.columns:
                g2 = df_faixas.groupby(["_consultor", "faixa", "tipo"], as_index=False).agg(
                    qtd=("_orcamento", "count"), valor=("_valor_num", "sum")
                )
                st.dataframe(g2.sort_values(["_consultor", "faixa", "tipo"]), use_container_width=True, hide_index=True)
            else:
                st.info("Coluna de consultor/vendedor não detectada.")
        with tab3:
            st.markdown("### Detalhe por nº do orçamento")
            if not search.strip():
                st.caption("Digite um número (ou parte dele) em **Buscar nº Orçamento**.")
            else:
                st.write("Pendentes (filtrados)")
                st.dataframe(dfp, use_container_width=True, hide_index=True)
                st.write("Finalizados (filtrados)")
                st.dataframe(dff, use_container_width=True, hide_index=True)


def _parse_month_periodo_to_key(periodo: object) -> tuple[str | None, str | None]:
    """
    Aceita `MM/AAAA` (ou `MM-AAAA`) e retorna:
    - month_key: `YYYY-MM` (para ordenar)
    - month_label: `MM/AAAA` (para exibir)
    """
    import re

    s = str(periodo or "").strip()
    if not s:
        return None, None
    m = re.search(r"(?<!\d)(\d{2})\s*[/\-]\s*(\d{4})(?!\d)", s)
    if not m:
        return None, None
    mm = int(m.group(1))
    yy = int(m.group(2))
    if not (1 <= mm <= 12 and yy >= 2000):
        return None, None
    return f"{yy:04d}-{mm:02d}", f"{mm:02d}/{yy:04d}"


def _monthly_rows_chronological(conn: object, *, owner_user_id: int | None, include_all: bool, limit: int = 240) -> list[object]:
    try:
        rows = list_analyses(conn, limit=int(limit), owner_user_id=owner_user_id, include_all=include_all)
    except Exception:
        return []
    tmp: list[tuple[str, int, object]] = []
    for r in rows:
        try:
            p = json.loads(getattr(r, "payload_json", "") or "")
        except Exception:
            continue
        if not isinstance(p, dict):
            continue
        if str(p.get("_kind") or "") != "monthly_snapshot":
            continue
        mk, _ml = _parse_month_periodo_to_key(getattr(r, "periodo", ""))
        if not mk:
            continue
        tmp.append((str(mk), int(getattr(r, "id", 0) or 0), r))
    tmp.sort(key=lambda x: (x[0], x[1]))
    return [x[2] for x in tmp]


def page_analise_historica(settings, conn) -> None:
    render_header("Análise Histórica", "Comparação mensal (meses fechados) — separado do diário.")
    user = st.session_state.get("user") or {}
    owner_id = int(user.get("id") or 0) or None
    is_admin = str(user.get("role") or "").lower() == "admin"

    tab_up, tab_dash = st.tabs(["Upload mensal", "Dashboard mensal"])

    with tab_up:
        st.markdown("### Salvar mês fechado (MM/AAAA)")
        periodo_mes = st.text_input("Mês/Ano", value="", placeholder="ex.: 01/2026", key="hist_month_periodo")
        month_key, month_label = _parse_month_periodo_to_key(periodo_mes)
        if not month_key:
            st.info("Informe o mês no formato **MM/AAAA** (ex.: `01/2026`).")

        files = st.file_uploader(
            "Envie os **6 arquivos de performance** e, se quiser, **+2 de orçamentos** (pendentes e finalizados) — até **8** arquivos.",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="hist_month_upload_files",
        )

        if files and st.button("📥 Importar (mensal)", use_container_width=True, key="hist_month_import_btn"):
            try:
                with st.spinner("Importando arquivos do mês..."):
                    files_bytes = [(f.name, f.read()) for f in files]
                    perf_files: list[tuple[str, bytes]] = []
                    dept_files: list[tuple[str, bytes]] = []
                    orc_files: list[tuple[str, bytes]] = []

                    for fname, b in files_bytes:
                        # Departamentos
                        try:
                            dpt1 = import_departamentos([(fname, b)])
                            dept_rows = (dpt1.payload or {}).get("departamentos") if isinstance(dpt1.payload, dict) else None
                            if isinstance(dept_rows, list) and len(dept_rows) > 0:
                                dept_files.append((fname, b))
                                continue
                        except Exception:
                            pass
                        # Orçamentos
                        if is_orcamento_workbook(b, file_name=fname):
                            orc_files.append((fname, b))
                            continue
                        perf_files.append((fname, b))

                    res = import_5_files_to_payload(perf_files)
                    payload = dict(res.payload or {})
                    payload["_kind"] = "monthly_snapshot"
                    payload["periodo"] = str(month_label or periodo_mes)
                    payload["_month_key"] = str(month_key or "")

                    # vincular deptos, se houver
                    try:
                        if dept_files:
                            dpt = import_departamentos(dept_files)
                            if isinstance(dpt.payload, dict) and isinstance(dpt.payload.get("departamentos"), list) and dpt.payload.get("departamentos"):
                                payload["_sg_dept"] = {
                                    "departamentos": dpt.payload.get("departamentos"),
                                    "meta": dpt.meta if isinstance(dpt.meta, dict) else {},
                                    "source": [n for (n, _) in dept_files],
                                }
                    except Exception:
                        pass

                    # orçamentos (opcional)
                    try:
                        if len(orc_files) == 2:
                            pend_b, fin_b = resolve_orcamentos_pend_fin_bytes(orc_files)
                            parsed = parse_orcamentos(pend_b, fin_b)
                            payload["_orcamentos"] = {
                                "pendentes": {"rows": parsed.pendentes_df.fillna("").to_dict(orient="records")},
                                "finalizados": {"rows": parsed.finalizados_df.fillna("").to_dict(orient="records")},
                                "meta": parsed.meta,
                            }
                    except Exception:
                        pass

                    st.session_state["hist_month_payload_preview"] = payload
                    st.session_state["hist_month_meta"] = res.meta
                    st.session_state["hist_month_warnings"] = res.warnings

                if res.warnings:
                    st.warning("Importação concluída com avisos.")
                    for w in res.warnings:
                        st.caption(w)
                else:
                    st.success("Importação concluída.")
            except Exception as e:
                st.error("Falha ao importar o mês.")
                st.caption(str(e))

        prev = st.session_state.get("hist_month_payload_preview")
        if isinstance(prev, dict) and prev.get("vendedores"):
            st.markdown("### Prévia (mês fechado)")
            try:
                sellers = parse_sellers(prev)
                results, _total_bonus = calcular_time(sellers) if sellers else ([], 0.0)
                st.dataframe(pd.DataFrame([r.__dict__ for r in results]), use_container_width=True, hide_index=True)
            except Exception:
                pass

            if st.button("💾 Salvar mês no histórico", use_container_width=True, key="hist_month_save_btn"):
                if not month_key:
                    st.error("Informe `MM/AAAA` válido antes de salvar.")
                    st.stop()
                rows = _monthly_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=240)
                last_key = None
                last_label = None
                if rows:
                    last_label = str(getattr(rows[-1], "periodo", "") or "")
                    try:
                        last_key, _ = _parse_month_periodo_to_key(last_label)
                    except Exception:
                        last_key = None
                if last_key and str(month_key) <= str(last_key):
                    st.error(f"Mês fora de sequência. Último mês salvo: **{last_label or '—'}**.")
                    st.stop()
                aid = save_analysis(
                    conn,
                    periodo=str(month_label or periodo_mes),
                    provider_used="monthly_excel",
                    model_used="pandas",
                    parent_analysis_id=None,
                    owner_user_id=owner_id,
                    payload=prev,
                    total_bonus=0.0,
                )
                st.success(f"Mês salvo como análise **#{aid}**.")
                st.session_state.pop("hist_month_payload_preview", None)

    with tab_dash:
        st.markdown("### Filtro de meses")
        rows = _monthly_rows_chronological(conn, owner_user_id=owner_id, include_all=is_admin, limit=240)
        if not rows:
            st.info("Nenhum mês salvo ainda. Use a aba **Upload mensal** para salvar `01/2026`, `02/2026`, etc.")
            return
        months = [str(getattr(r, "periodo", "") or "") for r in rows]
        pick = st.multiselect("Selecione 1+ meses (vazio = todos)", options=months, default=[], key="hist_month_pick")
        use_rows = [r for r in rows if (not pick or str(getattr(r, 'periodo', '') or '') in set(pick))]

        hist: list[dict] = []
        for r in use_rows:
            try:
                p = json.loads(getattr(r, "payload_json", "") or "")
            except Exception:
                continue
            if not isinstance(p, dict):
                continue
            base = _extract_perf_summary_from_payload(str(getattr(r, "periodo", "") or ""), p)
            mk, ml = _parse_month_periodo_to_key(str(getattr(r, "periodo", "") or ""))
            hist.append(
                base
                | {
                    "id": int(getattr(r, "id", 0) or 0),
                    "month_key": mk or "",
                    "month_label": ml or str(getattr(r, "periodo", "") or ""),
                }
            )
        hdf = pd.DataFrame(hist)
        if hdf.empty:
            st.info("Não consegui montar o consolidado mensal.")
            return
        hdf = hdf.sort_values(["month_key", "id"]).reset_index(drop=True)

        st.markdown("### KPIs do mês (consolidado)")
        last = hdf.iloc[-1]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Mês", str(last.get("month_label") or "—"))
        c2.metric("Faturamento (time)", f"R$ {float(last.get('fat_total') or 0.0):,.2f}")
        c3.metric("Meta (time)", f"R$ {float(last.get('meta_total') or 0.0):,.2f}" if float(last.get("meta_total") or 0.0) > 0 else "—")
        c4.metric("Margem média", f"{float(last.get('media_margem') or 0.0):.1f}%")

        st.markdown("### Tendência mensal")
        try:
            import plotly.express as px

            g1, g2 = st.columns(2)
            with g1:
                fig = px.line(hdf, x="month_label", y="fat_total", markers=True, title="Faturamento (time)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key="hist_month_trend_fat")
            with g2:
                fig = px.line(hdf, x="month_label", y="media_margem", markers=True, title="Margem média (%)")
                fig.update_layout(height=320)
                st.plotly_chart(fig, use_container_width=True, key="hist_month_trend_margem")
        except Exception as e:
            st.caption(f"Gráficos indisponíveis: {e}")

        st.markdown("### Tabela consolidada (meses)")
        st.dataframe(hdf, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Dashboard Performance", page_icon="📊", layout="wide")
    # Perfil de layout (Mobile / Tablet / Desktop)
    ui_profile = st.session_state.get("ui_profile") or "desktop"
    inject_styles(profile=str(ui_profile))

    settings, conn = _ensure_db()
    _maybe_login(settings)
    # Calendário é usado em várias telas (meta por dia útil, projeções, etc.).
    # Se o usuário não abrir o card "Calendário", ainda assim precisamos de um default (mês atual).
    try:
        if not isinstance(st.session_state.get("calendar_info"), dict) or not st.session_state["calendar_info"].get("ano"):
            import datetime as _dt

            today = _dt.date.today()
            info0 = compute_calendar_info(ano=int(today.year), mes=int(today.month), subdiv=None)
            st.session_state["calendar_info"] = {
                "ano": info0.ano,
                "mes": info0.mes,
                "hoje": info0.hoje.isoformat(),
                "dias_uteis_total": info0.dias_uteis_total,
                "dias_uteis_trabalhados": info0.dias_uteis_trabalhados,
                "dias_uteis_restantes": info0.dias_uteis_restantes,
            }
    except Exception:
        pass
    try:
        purge_excluded_sellers_from_all_analyses(conn)
    except Exception:
        pass

    # Após reinício do Streamlit, `active_analysis_id` some: reativar a última análise
    # salva (bônus/vendedores), se existir, para o usuário não achar que "perdeu" o histórico.
    u = st.session_state.get("user")
    if (
        isinstance(u, dict)
        and u.get("id")
        and st.session_state.get("active_analysis_id") is None
        and st.session_state.get("_restored_active_on_load") is not True
    ):
        oid = int(u.get("id") or 0) or None
        adm = str(u.get("role") or "").lower() == "admin"
        last = get_latest_base_analysis_id(
            conn, owner_user_id=oid, include_all=adm
        )
        st.session_state["_restored_active_on_load"] = True
        if last is not None:
            st.session_state["active_analysis_id"] = int(last)
            st.toast("Análise ativa restaurada: última análise salva no histórico.", icon="📌")

    with st.sidebar:
        st.markdown("### 🖥️ Layout / Dispositivo")
        prof = st.selectbox(
            "Perfil",
            options=["desktop", "tablet", "mobile"],
            format_func=lambda x: {"desktop": "Notebook / PC", "tablet": "iPad / Tablet", "mobile": "Smartphone"}[x],
            key="ui_profile",
        )
        st.caption(
            "No **iPad**, use o perfil **Tablet** e, em tabelas largas, deslize horizontalmente. "
            "Paisagem costuma mostrar mais colunas sem cortar números."
        )
        c_refresh1, c_refresh2 = st.columns([1, 1])
        with c_refresh1:
            if st.button("🔄 Atualizar (Refresh)", use_container_width=True, key="btn_refresh"):
                st.rerun()
        with c_refresh2:
            if st.button("🧹 Limpar filtros UI", use_container_width=True, key="btn_clear_ui"):
                # Mantém sessão/login e análise ativa; limpa só escolhas de interface.
                for k in ("ui_profile", "proj_mode", "tab_main", "sg_provider"):
                    if k in st.session_state:
                        st.session_state.pop(k, None)
                st.rerun()
        st.markdown("---")
        st.markdown("### 📌 Sessão")
        user = st.session_state.get("user") or {}
        owner_id = int(user.get("id") or 0) or None
        is_admin = str(user.get("role") or "").lower() == "admin"
        uname = str(user.get("name") or user.get("username") or "—")
        role = str(user.get("role") or "user")
        st.caption(f"Logado: **{uname}** ({role})")
        aid = st.session_state.get("active_analysis_id")
        if aid is not None:
            st.success(f"Análise ativa: **#{aid}**")
        else:
            st.caption("Nenhuma análise ativa — use Upload ou Histórico.")
        if st.button("Sair da sessão", use_container_width=True):
            st.session_state.pop("user", None)
            st.session_state.pop("active_analysis_id", None)
            st.rerun()
        # Diagnóstico rápido: versão do app + fonte do banco
        try:
            import subprocess
            from pathlib import Path as _P

            repo_root = str(_P(__file__).resolve().parent)
            rev = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root).decode().strip()
        except Exception:
            rev = "unknown"
        try:
            db_mode = "Postgres (DATABASE_URL)" if getattr(settings, "uses_postgres", False) else "SQLite (arquivo local)"
        except Exception:
            db_mode = "unknown"
        st.caption(f"Versão do app: `{rev}`")
        st.caption(f"Banco em uso: **{db_mode}**")
        st.markdown("---")
        # Admin: geração de convites
        if str((st.session_state.get("user") or {}).get("role") or "").lower() == "admin":
            st.markdown("### 🛡️ Admin")
            with st.expander("🗄️ Banco de dados (histórico)", expanded=False):
                if settings.uses_postgres:
                    st.markdown("**Armazenamento (PostgreSQL / ex.: Neon):**")
                    st.caption("O histórico (análises, usuários, feedbacks) está no servidor definido em `DATABASE_URL` — **não** no arquivo `app.db`.")
                    st.caption("**Arquivos locais:** anexos e prints vão em `DATA_DIR` (padrão: pasta `data/` do projeto) — isso fica fora do Git.")
                else:
                    db_abs = str(Path(settings.db_path).resolve())
                    st.markdown("**Arquivo em uso (fonte do histórico):**")
                    st.code(db_abs, language="text")
                    p = Path(db_abs)
                    if p.is_file():
                        st.caption(f"Existe: sim — tamanho **{p.stat().st_size:,}** bytes")
                    else:
                        st.error("Arquivo ainda **não existe** — a primeira análise criará o banco neste caminho.")
                n_a = count_all_analyses(conn)
                n_u = int(
                    conn.execute("SELECT COUNT(*) AS c FROM uploads")
                    .fetchone()["c"]  # type: ignore[index]
                )
                n_f = int(
                    conn.execute("SELECT COUNT(*) AS c FROM feedbacks")
                    .fetchone()["c"]  # type: ignore[index]
                )
                st.caption(
                    f"**Conteúdo:** {n_a} análise(s) · {n_u} upload(s) · {n_f} feedback(s) "
                    "(números têm de bater com o que você espera; se forem 0, é outro `.db` ou banco vazio)."
                )
                if settings.uses_postgres:
                    st.caption(
                        "Se o histórico 'sumiu': 1) Confirme se `DATABASE_URL` aponta para o **mesmo** projeto (Neon) de antes. "
                        "2) Regras de dono: usuário comum vê só o próprio; **admin** vê tudo. 3) Backup: use o painel do Neon (dump / restore)."
                    )
                else:
                    st.caption(
                        "**Solução** se o histórico 'sumiu': 1) Confira se este é o **mesmo caminho** de antes "
                        "(subir o Streamlit sempre **nesta** pasta, ou defina `DB_PATH` no `.env`). 2) Restaure uma cópia de "
                        "`app.db` de backup. 3) Usuário comum: ver só análises do **seu dono**; use **admin** para ver tudo. "
                        "4) Não conte com o Git — a pasta `data/` está fora do repositório."
                    )
                st.markdown("**Backup (recomendado na empresa)**")
                if settings.uses_postgres:
                    st.caption("Com Neon/Postgres, faça backup do banco no **console do Neon** (ou `pg_dump`). A pasta local `data/` ainda contém anexos — faça backup dela com o app **fechado** se quiser cópia off-line completa dos arquivos.")
                else:
                    st.caption(
                        "Baixe o banco com frequência e guarde no **OneDrive / Google Drive / rede** da empresa. "
                        "Os **prints** ficam em `data/uploads/` — para cópia completa, compacte a pasta `data/` com o app **fechado**, "
                        "ou copie `data/` inteira para o backup de rede."
                    )
                if not settings.uses_postgres:
                    p = Path(settings.db_path).resolve()
                    if p.is_file():
                        import datetime as _dt

                        snap = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                        try:
                            blob = backup_database_to_bytes(conn)
                            st.download_button(
                                label="⬇️ Baixar cópia do banco (app.db seguro)",
                                data=blob,
                                file_name=f"app_backup_{snap}.db",
                                mime="application/x-sqlite3",
                                use_container_width=True,
                                key="dl_db_backup",
                            )
                        except Exception as e:
                            st.caption(f"Não foi possível gerar o backup agora: {e}")

            with st.expander("🧾 Histórico completo (listar/apagar)", expanded=False):
                st.caption("Mostra tudo que existe no banco atual (inclui registros auxiliares com `_kind`).")
                user = st.session_state.get("user") or {}
                owner_id = int(user.get("id") or 0) or None
                is_admin = str(user.get("role") or "").lower() == "admin"

                show_all = st.checkbox("Mostrar tudo (todos os usuários)", value=True, key="adm_hist_show_all")
                include_aux = st.checkbox("Incluir auxiliares (_kind)", value=True, key="adm_hist_include_aux")
                limit = st.number_input("Limite", min_value=50, max_value=5000, value=500, step=50, key="adm_hist_limit")

                rows = list_analyses(conn, limit=int(limit), owner_user_id=(None if show_all else owner_id), include_all=bool(show_all))
                out_rows: list[dict] = []
                for r in rows:
                    try:
                        p0 = json.loads(getattr(r, "payload_json", "") or "")
                    except Exception:
                        p0 = None
                    kind = p0.get("_kind") if isinstance(p0, dict) else None
                    if (not include_aux) and kind:
                        continue
                    out_rows.append(
                        {
                            "id": int(getattr(r, "id", 0) or 0),
                            "created_at": _fmt_created_at_local(getattr(r, "created_at", None)),
                            "periodo": str(getattr(r, "periodo", "")),
                            "kind": str(kind or ""),
                            "owner_user_id": int(getattr(r, "owner_user_id", 0) or 0) if getattr(r, "owner_user_id", None) is not None else None,
                            "provider": str(getattr(r, "provider_used", "")),
                            "model": str(getattr(r, "model_used", "")),
                            "total_bonus": float(getattr(r, "total_bonus", 0.0) or 0.0),
                        }
                    )
                df_hist_all = pd.DataFrame(out_rows)
                st.dataframe(df_hist_all, use_container_width=True, hide_index=True)

                st.markdown("**Apagar por ID (permanente)**")
                ids_txt = st.text_input("IDs (separados por vírgula)", value="", key="adm_hist_delete_ids")
                confirm = st.text_input("Digite APAGAR para confirmar", value="", key="adm_hist_delete_confirm")
                if st.button("🗑️ Apagar IDs", use_container_width=True, key="adm_hist_delete_btn"):
                    if str(confirm).strip().upper() != "APAGAR":
                        st.error("Confirmação inválida. Digite APAGAR.")
                    else:
                        ids: list[int] = []
                        for part in str(ids_txt).replace(";", ",").split(","):
                            part = part.strip()
                            if not part:
                                continue
                            try:
                                ids.append(int(part))
                            except Exception:
                                pass
                        if not ids:
                            st.error("Nenhum ID válido.")
                        else:
                            ok = 0
                            for i in ids:
                                try:
                                    delete_analysis(conn, int(i), owner_user_id=None if show_all else owner_id, include_all=bool(show_all))
                                    ok += 1
                                except Exception:
                                    pass
                            st.success(f"Apagadas: {ok} / {len(ids)}. Recarregue para atualizar a lista.")
                            st.rerun()

            with st.expander("✅ Teste de acúmulo (ledger por data)", expanded=False):
                st.caption("Valida que deltas diários acumulam corretamente e que KPIs batem no acumulado materializado.")
                user = st.session_state.get("user") or {}
                owner_id = int(user.get("id") or 0) or None
                is_admin = str(user.get("role") or "").lower() == "admin"

                d1 = st.text_input("Data 1 (dd/mm/aaaa)", value="26/04/2026", key="ledger_test_d1")
                d2 = st.text_input("Data 2 (dd/mm/aaaa)", value="27/04/2026", key="ledger_test_d2")
                if st.button("Rodar teste", use_container_width=True, key="ledger_test_run"):
                    rd1 = _extract_ref_date_iso_from_periodo(d1)
                    rd2 = _extract_ref_date_iso_from_periodo(d2)
                    if not rd1 or not rd2:
                        st.error("Não consegui interpretar as datas. Use dd/mm/aaaa.")
                    else:
                        # carrega deltas
                        rows_all = list_analyses(conn, limit=8000, owner_user_id=owner_id, include_all=is_admin)
                        deltas_map: dict[str, dict] = {}
                        for rr in rows_all:
                            try:
                                p0 = json.loads(getattr(rr, "payload_json", "") or "")
                            except Exception:
                                continue
                            if not isinstance(p0, dict):
                                continue
                            if str(p0.get("_kind") or "") != "daily_delta":
                                continue
                            rk = str(p0.get("ref_date") or "")
                            if rk:
                                deltas_map[rk] = p0

                        if rd1 not in deltas_map:
                            st.error(f"Não achei delta do dia {rd1} (Período {d1}). Salve uma análise com essa data primeiro.")
                        elif rd2 not in deltas_map:
                            st.error(f"Não achei delta do dia {rd2} (Período {d2}). Salve uma análise com essa data primeiro.")
                        else:
                            base = {"vendedores": [], "totais": {}}
                            acc_1 = _accumulate_payload(base, deltas_map[rd1])
                            acc_2 = _accumulate_payload(acc_1, deltas_map[rd2])
                            try:
                                refresh_payload_totais_from_vendedores(acc_2)
                            except Exception:
                                pass

                            # KPIs a validar (time)
                            t = acc_2.get("totais") if isinstance(acc_2.get("totais"), dict) else {}
                            fat = float(t.get("faturamento_total") or 0.0)
                            meta = float(t.get("meta_total") or 0.0)
                            st.success("OK: acumulado calculado (delta 1 + delta 2).")
                            st.caption(f"Faturamento total acumulado: **R$ {fat:,.2f}**")
                            st.caption(f"Meta total: **R$ {meta:,.2f}**")

                            # Confere se existe uma análise materializada "Acumulado até d2"
                            want_periodo = f"Acumulado até {_iso_to_br(rd2)}"
                            mat_row = None
                            for rr in rows_all:
                                if str(getattr(rr, "periodo", "")) == want_periodo:
                                    mat_row = rr
                                    break
                            if not mat_row:
                                st.warning(f"Não encontrei análise materializada com período '{want_periodo}'.")
                            else:
                                try:
                                    mp = json.loads(getattr(mat_row, "payload_json", "") or "")
                                except Exception:
                                    mp = None
                                if isinstance(mp, dict):
                                    try:
                                        refresh_payload_totais_from_vendedores(mp)
                                    except Exception:
                                        pass
                                    mt = mp.get("totais") if isinstance(mp.get("totais"), dict) else {}
                                    m_fat = float(mt.get("faturamento_total") or 0.0)
                                    m_meta = float(mt.get("meta_total") or 0.0)
                                    ok1 = abs(m_fat - fat) < 0.01
                                    ok2 = abs(m_meta - meta) < 0.01
                                    if ok1 and ok2:
                                        st.success(f"PASS: materializado (#{int(getattr(mat_row,'id',0))}) bate com acumulado calculado.")
                                    else:
                                        st.error(
                                            f"FAIL: materializado (#{int(getattr(mat_row,'id',0))}) difere. "
                                            f"fat_calc={fat:,.2f} vs fat_mat={m_fat:,.2f} | meta_calc={meta:,.2f} vs meta_mat={m_meta:,.2f}"
                                        )
            with st.expander("Convites (cadastro)", expanded=False):
                c1, c2 = st.columns([1, 1])
                with c1:
                    inv_role = st.selectbox("Papel", options=["user", "admin"], index=0, key="inv_role")
                with c2:
                    inv_exp = st.selectbox("Expira em", options=["Nunca", "7 dias", "30 dias"], index=1, key="inv_exp")
                if st.button("Gerar convite", use_container_width=True, key="btn_invite"):
                    code = new_invite_code()
                    expires_at = None
                    if inv_exp == "7 dias":
                        import datetime as _dt

                        expires_at = (_dt.datetime.now() + _dt.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
                    elif inv_exp == "30 dias":
                        import datetime as _dt

                        expires_at = (_dt.datetime.now() + _dt.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
                    create_invite(
                        conn,
                        code=code,
                        role=str(inv_role),
                        created_by_user_id=int((st.session_state.get("user") or {}).get("id") or 0) or None,
                        expires_at=expires_at,
                    )
                    st.success("Convite gerado.")
                    st.code(code)

                invs = list_invites(conn, limit=20)
                if invs:
                    st.caption("Últimos convites:")
                    for it in invs[:10]:
                        status = "usado" if it.get("used_at") else "ativo"
                        st.write(f"`{it.get('code')}` · {it.get('role')} · {status}")
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
        rows_all = list_analyses(conn, limit=20, owner_user_id=owner_id, include_all=is_admin)
        base_rows = []
        for r in rows_all:
            try:
                p = json.loads(r.payload_json)
            except Exception:
                p = None
            kind = p.get("_kind") if isinstance(p, dict) else None
            if not kind:
                base_rows.append(r)
        if base_rows:
            options = {f"#{r.id} · {r.periodo}": r.id for r in base_rows}
            pick = st.selectbox("Carregar análise", options=list(options.keys()), key="sidebar_hist_quick_pick")
            sel_hist_id = int(options[pick])
            sel_hist_period = next((str(r.periodo) for r in base_rows if int(r.id) == sel_hist_id), "")
            if st.button("📌 Tornar ativa", use_container_width=True, key="sidebar_hist_activate"):
                st.session_state["active_analysis_id"] = sel_hist_id
                st.rerun()
            st.caption("Esqueceu o período ao salvar? **Renomeie** o rótulo abaixo (atualiza o histórico e o JSON da análise).")
            new_period_label = st.text_input(
                "Renomear período desta análise",
                value=sel_hist_period,
                key=f"sidebar_hist_rename_field_{sel_hist_id}",
                placeholder="ex.: Abril/2026 ou Até 27/04/2026",
            )
            if st.button("💾 Salvar novo nome", use_container_width=True, key="sidebar_hist_rename_btn"):
                try:
                    ok = update_analysis_periodo(
                        conn,
                        sel_hist_id,
                        new_periodo=new_period_label,
                        owner_user_id=owner_id,
                        include_all=is_admin,
                    )
                    if ok:
                        st.success("Nome atualizado no histórico.")
                        st.rerun()
                    else:
                        st.error("Não foi possível salvar (texto vazio ou sem permissão).")
                except Exception as e:
                    st.error(str(e))
        else:
            n_db = count_all_analyses(conn)
            if n_db > 0 and not is_admin:
                st.caption(
                    f"Histórico rápido vazio, mas o banco tem {n_db} análise(s) — outro dono. "
                    "Entre como **admin** para listar tudo."
                )
            else:
                st.caption("Nenhuma análise salva ainda.")
        st.markdown("---")
        with st.expander("ℹ️ Navegação", expanded=False):
            st.caption(
                "Use as **abas** no painel principal. Fluxo típico: Nova análise → "
                "revisar dados → salvar → abrir **Dashboard** ou **Histórico**."
            )

    # Título do painel (topo)
    st.markdown(
        """
<div class="dp-card" style="
  padding:18px 18px;
  margin: 8px 0 10px 0;
  border-color: rgba(110,231,183,.22);
  background: radial-gradient(1200px 500px at 20% 0%, rgba(110,231,183,.16), transparent 45%),
              radial-gradient(900px 420px at 80% 20%, rgba(59,130,246,.12), transparent 55%),
              linear-gradient(180deg, rgba(17,26,46,.96), rgba(11,18,32,.94));
">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div style="
        width:40px;height:40px;border-radius:14px;
        display:flex;align-items:center;justify-content:center;
        background: rgba(255,255,255,.04);
        border: 1px solid rgba(255,255,255,.10);
        font-size: 1.15rem;
        color: #6EE7B7;
      ">🏛️</div>
      <div>
        <div style="color:#E5E7EB;font-weight:950;font-size:1.35rem;letter-spacing:.2px;line-height:1.1;">
          Central de Vendas Resultado &amp; Performance
        </div>
        <div style="margin-top:6px;color:#94A3B8;font-size:.92rem;line-height:1.45;">
          Visão única para acompanhar resultado, performance, projeção e evolução — com histórico e upload centralizado.
        </div>
      </div>
    </div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    # Zona do topo: ações rápidas (padrão de cards clicáveis)
    st.markdown(
        """
<style>
  .dp-action-select{ margin: 6px 0 10px 0; }
  .dp-action-select [data-testid="stButton"] > button{
    width: 100% !important;
    text-align: left !important;
    border-radius: 16px !important;
    border: 1px solid rgba(255,255,255,.12) !important;
    /* Base neutra (topo) */
    border-color: rgba(255,255,255,.12) !important;
    background: linear-gradient(180deg, rgba(17,26,46,.90), rgba(11,18,32,.92)) !important;
    padding: 12px 12px !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18) !important;
    transition: transform .12s ease, border-color .12s ease, box-shadow .12s ease, background .12s ease !important;
    min-height: 74px !important;
    white-space: pre-line !important;
    color: rgba(229,231,235,.96) !important;
    font-weight: 850 !important;
  }
  .dp-action-select [data-testid="stButton"] > button:hover{
    transform: translateY(-2px) !important;
    border-color: rgba(59,130,246,.34) !important;
    box-shadow: 0 18px 40px rgba(0,0,0,.28) !important;
    background: radial-gradient(900px 220px at 15% 0%, rgba(59,130,246,.14), transparent 60%),
                linear-gradient(180deg, rgba(17,26,46,.92), rgba(11,18,32,.94)) !important;
  }
  .dp-action-select .dp-action-selected [data-testid="stButton"] > button{
    border-color: rgba(110,231,183,.75) !important;
    background: radial-gradient(900px 260px at 15% 0%, rgba(110,231,183,.30), transparent 62%),
                radial-gradient(900px 260px at 88% 12%, rgba(59,130,246,.20), transparent 58%),
                linear-gradient(180deg, rgba(17,26,46,.96), rgba(11,18,32,.98)) !important;
    box-shadow: inset 0 0 0 1px rgba(110,231,183,.35), 0 26px 56px rgba(0,0,0,.40) !important;
  }
</style>
""",
        unsafe_allow_html=True,
    )

    # Linha: Calendário (esq) + Nova análise (dir)
    if st.session_state.get("show_calendar") is None:
        st.session_state["show_calendar"] = False
    if st.session_state.get("show_upload") is None:
        st.session_state["show_upload"] = False

    is_cal_open = bool(st.session_state.get("show_calendar"))
    is_open = bool(st.session_state.get("show_upload"))

    top1, top2 = st.columns([1, 1])
    with top1:
        st.markdown("<div class='dp-action-select'>", unsafe_allow_html=True)
        st.markdown("<div class='dp-action-selected'>" if is_cal_open else "<div>", unsafe_allow_html=True)
        cal_btn_label = "🗓  Calendário\nDias úteis automáticos (mês atual)\n" + ("Clique para fechar" if is_cal_open else "Clique para abrir")
        if st.button(cal_btn_label, use_container_width=True, key="btn_toggle_calendar_card"):
            st.session_state["show_calendar"] = not is_cal_open
            st.rerun()
        st.markdown("</div></div>", unsafe_allow_html=True)

    with top2:
        st.markdown("<div class='dp-action-select'>", unsafe_allow_html=True)
        st.markdown("<div class='dp-action-selected'>" if is_open else "<div>", unsafe_allow_html=True)
        upload_btn_label = "⬆️  Nova análise\nUpload e validação\n" + ("Clique para fechar" if is_open else "Clique para abrir")
        if st.button(upload_btn_label, use_container_width=True, key="btn_toggle_upload_top"):
            st.session_state["show_upload"] = not is_open
            st.rerun()
        st.markdown("</div></div>", unsafe_allow_html=True)

    if bool(st.session_state.get("show_calendar")):
        st.markdown("<div class='dp-card' style='padding:14px 14px;margin: 0 0 10px 0;'>", unsafe_allow_html=True)
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

        st.caption(
            "Regras: sábado e domingo não entram; feriados BR (e estaduais se informar a UF) são excluídos. "
            "A data de hoje segue o fuso America/Sao_Paulo. "
            "Úteis **restantes** = do dia de hoje até o fim do mês (inclui o próprio dia útil de hoje). "
            "Úteis **trabalhados** = corridos no mês até hoje, inclusive o dia atual quando for útil (base de ritmo / projeção)."
        )
        st.markdown("</div>", unsafe_allow_html=True)

    if bool(st.session_state.get("show_upload")):
        page_upload(settings, conn, embedded=True)

    st.markdown("### Selecione o Dashboard:")
    if st.session_state.get("dash_selector") not in {
        "Dashboard de Bônus",
        "Dashboard de Performance",
        "Sala de Gestão",
        "Orçamento x Conversão",
        "Análise Histórica",
    }:
        st.session_state["dash_selector"] = "Sala de Gestão"

    st.markdown(
        """
<style>
  .dp-dash-select{ margin: 6px 0 10px 0; }
  .dp-dash-select [data-testid="stButton"] > button{
    width: 100% !important;
    text-align: left !important;
    border-radius: 16px !important;
    border: 1px solid rgba(255,255,255,.10) !important;
    background: linear-gradient(180deg, rgba(17,26,46,.90), rgba(11,18,32,.92)) !important;
    padding: 12px 12px !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18) !important;
    transition: transform .12s ease, border-color .12s ease, box-shadow .12s ease, background .12s ease !important;
    min-height: 74px !important;
  }
  .dp-dash-select [data-testid="stButton"] > button:hover{
    transform: translateY(-2px) !important;
    border-color: rgba(59,130,246,.32) !important;
    box-shadow: 0 18px 40px rgba(0,0,0,.28) !important;
    background: rgba(255,255,255,.03) !important;
  }
  .dp-dash-select [data-testid="stButton"] > button:focus{
    outline: none !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,.22), 0 18px 40px rgba(0,0,0,.28) !important;
  }
  /* “assinatura” de cor por dashboard (borda superior + glow leve) */
  .dp-dash-select .dp-dash-bonus [data-testid="stButton"] > button{
    border-top: 3px solid rgba(251,191,36,.65) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18), 0 0 0 1px rgba(251,191,36,.08) inset !important;
  }
  .dp-dash-select .dp-dash-perf [data-testid="stButton"] > button{
    border-top: 3px solid rgba(59,130,246,.65) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18), 0 0 0 1px rgba(59,130,246,.08) inset !important;
  }
  .dp-dash-select .dp-dash-sg [data-testid="stButton"] > button{
    border-top: 3px solid rgba(110,231,183,.65) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18), 0 0 0 1px rgba(110,231,183,.08) inset !important;
  }
  .dp-dash-select .dp-dash-orc [data-testid="stButton"] > button{
    border-top: 3px solid rgba(196,181,253,.70) !important;
    box-shadow: 0 10px 26px rgba(0,0,0,.18), 0 0 0 1px rgba(196,181,253,.08) inset !important;
  }
  /* Selecionado */
  .dp-dash-select .dp-dash-selected [data-testid="stButton"] > button{
    border-color: rgba(110,231,183,.65) !important;
    background: radial-gradient(900px 260px at 15% 0%, rgba(110,231,183,.28), transparent 62%),
                radial-gradient(900px 260px at 88% 12%, rgba(59,130,246,.18), transparent 58%),
                linear-gradient(180deg, rgba(17,26,46,.96), rgba(11,18,32,.98)) !important;
    /* “borda” interna + leve zoom pra ficar gritante */
    transform: translateY(-2px) scale(1.01) !important;
    box-shadow: inset 0 0 0 1px rgba(110,231,183,.36), 0 30px 64px rgba(0,0,0,.44) !important;
  }
  .dp-dash-title{ margin:0; font-weight:900; color:#E5E7EB; font-size:1.02rem; letter-spacing:.2px; }
  .dp-dash-sub{ margin:6px 0 0 0; color:#94A3B8; font-size:.86rem; line-height:1.35; }
</style>
""",
        unsafe_allow_html=True,
    )

    dash = str(st.session_state.get("dash_selector"))
    st.markdown("<div class='dp-dash-select'>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            "<div class='dp-dash-selected dp-dash-bonus'>" if dash == "Dashboard de Bônus" else "<div class='dp-dash-bonus'>",
            unsafe_allow_html=True,
        )
        if st.button("🎯 Dashboard de Bônus", use_container_width=True, key="dash_pick_bonus"):
            st.session_state["dash_selector"] = "Dashboard de Bônus"
            st.rerun()
        st.markdown("<p class='dp-dash-sub'>Bônus, evolução e insights</p></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(
            "<div class='dp-dash-selected dp-dash-perf'>" if dash == "Dashboard de Performance" else "<div class='dp-dash-perf'>",
            unsafe_allow_html=True,
        )
        if st.button("📈 Dashboard de Performance", use_container_width=True, key="dash_pick_perf"):
            st.session_state["dash_selector"] = "Dashboard de Performance"
            st.rerun()
        st.markdown("<p class='dp-dash-sub'>Indicadores, projeção e ranking</p></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(
            "<div class='dp-dash-selected dp-dash-sg'>" if dash == "Sala de Gestão" else "<div class='dp-dash-sg'>",
            unsafe_allow_html=True,
        )
        if st.button("🧭 Sala de Gestão", use_container_width=True, key="dash_pick_sg"):
            st.session_state["dash_selector"] = "Sala de Gestão"
            st.rerun()
        st.markdown("<p class='dp-dash-sub'>Reunião diária: consolidado e deptos</p></div>", unsafe_allow_html=True)
    with c4:
        st.markdown(
            "<div class='dp-dash-selected dp-dash-orc'>" if dash == "Orçamento x Conversão" else "<div class='dp-dash-orc'>",
            unsafe_allow_html=True,
        )
        if st.button("📑 Orçamento x Conversão", use_container_width=True, key="dash_pick_orc"):
            st.session_state["dash_selector"] = "Orçamento x Conversão"
            st.rerun()
        st.markdown("<p class='dp-dash-sub'>Pendentes, finalizados e conversão</p></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Segunda linha: Análise Histórica (mensal) — separado para não apertar o layout
    st.markdown("<div class='dp-dash-select' style='margin-top:10px;'>", unsafe_allow_html=True)
    st.markdown(
        "<div class='dp-dash-selected dp-dash-perf'>" if dash == "Análise Histórica" else "<div class='dp-dash-perf'>",
        unsafe_allow_html=True,
    )
    if st.button("🗂️ Análise Histórica", use_container_width=True, key="dash_pick_hist_month"):
        st.session_state["dash_selector"] = "Análise Histórica"
        st.rerun()
    st.markdown("<p class='dp-dash-sub'>Comparação mensal (MM/AAAA) — meses fechados</p></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    dash = str(st.session_state.get("dash_selector"))

    if dash == "Dashboard de Bônus":
        bonus_tab = st.radio(
            "",
            options=["Dashboard", "Evolução", "Edição Manual", "Análise com IA", "Histórico"],
            horizontal=True,
            label_visibility="collapsed",
            key="bonus_tab",
        )
        if bonus_tab == "Dashboard":
            page_dashboard(settings, conn)
        elif bonus_tab == "Evolução":
            page_evolution(settings, conn)
        elif bonus_tab == "Edição Manual":
            page_edit(settings, conn)
        elif bonus_tab == "Análise com IA":
            page_insights(settings, conn)
        else:
            page_history(settings, conn)
    elif dash == "Dashboard de Performance":
        perf_tab = st.radio(
            "",
            options=["Visão Geral", "Highlights (Semanal/Mensal)", "Simulação/Projeção", "Feedback STAR", "Análise com IA", "Histórico"],
            horizontal=True,
            label_visibility="collapsed",
            key="perf_tab",
        )
        if perf_tab == "Visão Geral":
            page_performance(settings, conn, key_prefix="perf_overview")
        elif perf_tab == "Highlights (Semanal/Mensal)":
            page_highlights(settings, conn)
        elif perf_tab == "Simulação/Projeção":
            page_projection(settings, conn)
        elif perf_tab == "Feedback STAR":
            page_star(settings, conn)
        elif perf_tab == "Análise com IA":
            page_insights(settings, conn)
        else:
            page_history(settings, conn)
    elif dash == "Orçamento x Conversão":
        page_orcamentos(settings, conn)
    elif dash == "Análise Histórica":
        page_analise_historica(settings, conn)
    else:
        # Header aqui é redundante com os cards de seleção acima
        page_sala_gestao(settings, conn, show_header=False)


if __name__ == "__main__":
    main()

