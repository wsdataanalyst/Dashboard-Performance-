from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import numpy as np


@dataclass(frozen=True)
class OcrCell:
    text: str
    x: int
    y: int
    w: int
    h: int

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2


def _to_image(b: bytes):
    import cv2
    import numpy as np

    arr = np.frombuffer(b, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Imagem inválida para OCR.")
    return img


def _crop_for_kind(img, kind: str):
    """Crops tailored to known print layouts (reduces noise)."""
    h, w = img.shape[:2]
    k = kind
    if k in {"print1", "print2", "print3"}:
        # dark tables: remove top title bar and left nav gutter
        y0 = int(h * 0.12)
        x0 = int(w * 0.03)
        return img[y0:h, x0:w]
    if k == "print4":
        # small orange/white table: keep most of it
        y0 = int(h * 0.05)
        x0 = int(w * 0.02)
        return img[y0:h, x0:w]
    if k == "print5":
        # white table: keep header + rows
        y0 = int(h * 0.08)
        x0 = int(w * 0.02)
        return img[y0:h, x0:w]
    return img


def _preprocess(img):
    # Aumenta resolução e melhora contraste para OCR de tabelas
    import cv2
    import numpy as np

    h, w = img.shape[:2]
    scale = 2.0 if max(h, w) < 1400 else 1.3
    img = cv2.resize(img, (int(w * scale), int(h * scale)), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # Detecta fundo escuro (prints 1-3) e inverte para texto escuro em fundo claro
    if float(np.mean(gray)) < 120:
        gray = cv2.bitwise_not(gray)
    # contraste local
    clahe = cv2.createCLAHE(clipLimit=2.4, tileGridSize=(8, 8))
    gray = clahe.apply(gray)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thr = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    # remove pequenos ruídos
    thr = cv2.morphologyEx(thr, cv2.MORPH_OPEN, np.ones((2, 2), np.uint8), iterations=1)
    return thr


def _extract_cells(img_bin) -> list[OcrCell]:
    # OCR com bounding boxes (palavra a palavra)
    import pytesseract

    # Auto-detect no Windows quando tesseract não está no PATH
    if shutil.which("tesseract") is None:
        candidates = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        local = os.environ.get("LOCALAPPDATA")
        if local:
            candidates.append(os.path.join(local, "Programs", "Tesseract-OCR", "tesseract.exe"))
        for p in candidates:
            if os.path.exists(p):
                pytesseract.pytesseract.tesseract_cmd = p
                break

    # tenta português (cloud) e cai para eng se não existir
    try:
        data = pytesseract.image_to_data(
            img_bin, output_type=pytesseract.Output.DICT, config="--psm 6", lang="por"
        )
    except Exception:
        data = pytesseract.image_to_data(
            img_bin, output_type=pytesseract.Output.DICT, config="--psm 6", lang="eng"
        )
    out: list[OcrCell] = []
    n = len(data.get("text", []))
    for i in range(n):
        t = (data["text"][i] or "").strip()
        if not t:
            continue
        conf = float(data.get("conf", [0] * n)[i] or 0)
        if conf >= 0 and conf < 30:
            continue
        out.append(
            OcrCell(
                text=t,
                x=int(data["left"][i]),
                y=int(data["top"][i]),
                w=int(data["width"][i]),
                h=int(data["height"][i]),
            )
        )
    return out


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9%]+", "", (s or "").lower())


def _clean_name(name: str) -> str:
    s = (name or "").strip()
    s = s.replace("_", " ")
    # remove ids "(1234)" e múltiplos espaços
    s = re.sub(r"\(\s*\d+\s*\)", "", s).strip()
    # remove ruídos comuns de OCR (moeda/percent/dígitos soltos)
    s = re.sub(r"r\$\s*\d[\d\.,]*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\d{2,}", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _name_key(nome: str) -> str:
    """Chave estável para casar nomes entre prints (primeiro+último)."""
    s = _clean_name(nome).lower()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    parts = [p for p in s.split() if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]}_{parts[-1]}"


def _is_noise_name(nome: str) -> bool:
    s = _clean_name(nome).strip().lower()
    if not s:
        return True
    if s in {"canal", "total"}:
        return True
    if s.startswith("central"):
        return True
    # se ainda tiver muito número/símbolo, é lixo
    if sum(ch.isdigit() for ch in s) >= 2:
        return True
    return False


def _norm_name_match(s: str) -> str:
    s = _clean_name(s).lower()
    s = re.sub(r"[^a-z\s]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def _is_excluded_nome(nome: str) -> bool:
    # remove qualquer lixo antes do primeiro token (|, etc)
    s = _norm_name_match(nome)
    if not s:
        return False
    first = s.split()[0]
    return first == "laila"


def _merge_records(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    # nome: manter o mais longo
    if len(str(b.get("nome") or "")) > len(str(out.get("nome") or "")):
        out["nome"] = b.get("nome")
    for k, v in b.items():
        if k == "nome":
            continue
        if v is None:
            continue
        # se já existe, mantém o atual; senão preenche
        if out.get(k) is None:
            out[k] = v
    return out


def _consolidate_by_similarity(recs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Consolida vendedores duplicados por similaridade do nome (layout fixo)."""
    clusters: list[dict[str, Any]] = []
    keys: list[str] = []
    for r in recs:
        nome = str(r.get("nome") or "").strip()
        if not nome or _is_noise_name(nome) or _is_excluded_nome(nome):
            continue
        nk = _norm_name_match(nome)
        if not nk or len(nk) < 4:
            continue

        best_i = -1
        best = 0.0
        for i, kk in enumerate(keys):
            score = SequenceMatcher(None, nk, kk).ratio()
            if score > best:
                best = score
                best_i = i
        if best_i >= 0 and best >= 0.78:
            clusters[best_i] = _merge_records(clusters[best_i], r)
        else:
            clusters.append(dict(r))
            keys.append(nk)
    clusters.sort(key=lambda x: str(x.get("nome") or ""))
    return clusters


def _group_rows(cells: list[OcrCell]) -> list[list[OcrCell]]:
    # Agrupa por linha (y) aproximada
    cells = sorted(cells, key=lambda c: (c.y, c.x))
    if not cells:
        return []
    heights = sorted([c.h for c in cells])
    med_h = heights[len(heights) // 2]
    y_tol = max(10, int(med_h * 0.7))
    rows: list[list[OcrCell]] = []
    for c in cells:
        if not rows:
            rows.append([c])
            continue
        if abs(c.y - rows[-1][0].y) <= y_tol:
            rows[-1].append(c)
        else:
            rows.append([c])
    # ordenar cada linha por x
    for r in rows:
        r.sort(key=lambda c: c.x)
    return rows


def _find_header_centers(rows: list[list[OcrCell]], keywords: list[str]) -> dict[str, float]:
    # Procura keywords no topo e pega centro x do header
    centers: dict[str, float] = {}
    top = rows[:8]
    for r in top:
        for c in r:
            t = _norm(c.text)
            for k in keywords:
                if k in centers:
                    continue
                if k in t:
                    centers[k] = c.cx
    return centers


def _row_to_record(row: list[OcrCell], centers: dict[str, float], mapping: dict[str, str]) -> dict[str, Any]:
    # Coluna "nome" = texto mais à esquerda (até bater em 1ª coluna numérica)
    tokens = [c for c in row if c.text]
    if not tokens:
        return {}

    # pega um "nome" juntando tokens iniciais até encontrar algo numérico
    name_parts: list[str] = []
    for c in tokens[:8]:
        if re.search(r"\d", c.text):
            break
        name_parts.append(c.text)
    nome = _clean_name(" ".join(name_parts).strip())
    if len(nome) < 3:
        return {}

    rec: dict[str, Any] = {"nome": nome}

    # para cada centro, pegar token mais próximo em x
    for k, field in mapping.items():
        if k not in centers:
            continue
        cx = centers[k]
        best = min(tokens, key=lambda c: abs(c.cx - cx))
        rec[field] = best.text
    return rec


def _row_to_record_fixed(
    row: list[OcrCell], *, img_w: int, mapping_fixed: dict[str, tuple[float, float]]
) -> dict[str, Any]:
    """
    Parser por posição fixa (x/width). Mais robusto para prints com fundo escuro.
    mapping_fixed: field -> (xmin_ratio, xmax_ratio)
    """
    tokens = [c for c in row if c.text]
    if not tokens:
        return {}

    # nome: tudo que estiver à esquerda do primeiro campo numérico mais "cedo"
    first_band_min = min((v[0] for v in mapping_fixed.values()), default=0.25)
    name_tokens = [t for t in tokens if (t.cx / img_w) < first_band_min]
    nome = _clean_name(" ".join(t.text for t in name_tokens).strip())
    if len(nome) < 3:
        # fallback: primeiros tokens até ver número
        parts: list[str] = []
        for t in tokens[:10]:
            if re.search(r"\d", t.text):
                break
            parts.append(t.text)
        nome = _clean_name(" ".join(parts).strip())
    if len(nome) < 3 or _is_noise_name(nome):
        return {}

    rec: dict[str, Any] = {"nome": nome}

    for field, (xmin, xmax) in mapping_fixed.items():
        band = [t for t in tokens if xmin <= (t.cx / img_w) <= xmax]
        if not band:
            continue
        # escolhe token mais "à direita" na banda (normalmente o valor)
        best = max(band, key=lambda c: c.cx)
        rec[field] = best.text
    return rec


def _coerce_number(v: Any) -> float | int | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {"—", "-", "None"}:
        return None
    # tempo "00h 35m 52s" -> minutos decimais
    mt = re.search(r"(\d+)\s*h\s*(\d+)\s*m\s*(\d+)\s*s", s.lower())
    if mt:
        hh = int(mt.group(1))
        mm = int(mt.group(2))
        ss = int(mt.group(3))
        return round(hh * 60 + mm + ss / 60.0, 2)
    s = s.replace("%", "").replace("R$", "").replace(".", "").replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    num = float(m.group(0))
    return int(num) if abs(num - int(num)) < 1e-6 else num


def _merge_by_name(base: dict[str, dict], updates: list[dict[str, Any]]) -> None:
    for u in updates:
        nome = (u.get("nome") or "").strip()
        if not nome:
            continue
        key = _name_key(nome)
        if not key:
            continue
        b = base.setdefault(key, {"nome": nome})
        # manter o nome mais completo
        if len(str(nome)) > len(str(b.get("nome") or "")):
            b["nome"] = nome
        for k, v in u.items():
            if k == "nome":
                continue
            if v is None:
                continue
            b[k] = v


def extract_payload_from_prints_ocr(
    images: list[tuple[str, bytes]], *, debug: bool = False
) -> tuple[dict[str, Any], dict[str, Any]] | dict[str, Any]:
    """
    Fallback OCR (sem IA) para o layout padrão dos 5 prints.
    Retorna payload no mesmo formato do app.
    """
    by_name: dict[str, dict] = {}
    dbg: dict[str, Any] = {"prints": []}

    # Heurística por nome do upload (Print 1..5)
    for name, b in images:
        tag = _norm(name)
        kind = "print1" if "print1" in tag else "print2" if "print2" in tag else "print3" if "print3" in tag else "print4" if "print4" in tag else "print5" if "print5" in tag else "unknown"

        img = _to_image(b)
        img = _crop_for_kind(img, kind)
        bin_img = _preprocess(img)
        cells = _extract_cells(bin_img)
        rows = _group_rows(cells)
        img_w = int(getattr(bin_img, "shape")[1])

        # Specs fixos por print (ratios aproximados para o layout enviado)
        fixed: dict[str, tuple[float, float]] | None = None
        if kind == "print1":
            centers = _find_header_centers(rows, ["alcance", "projetado", "margem", "faturamento", "meta"])
            mapping = {
                "alcance": "alcance_projetado_pct",
                "margem": "margem_pct",
                "faturamento": "faturamento",
                "meta": "meta_faturamento",
            }
            fixed = {
                "meta_faturamento": (0.26, 0.36),
                "faturamento": (0.37, 0.50),
                "alcance_projetado_pct": (0.72, 0.86),
                "margem_pct": (0.90, 0.99),
            }
        elif kind == "print2":
            centers = _find_header_centers(rows, ["pmedio", "medio", "prazo"])
            mapping = {"prazo": "prazo_medio"}
            fixed = {"prazo_medio": (0.90, 0.99)}
        elif kind == "print3":
            centers = _find_header_centers(rows, ["qtdfatur", "faturad", "nf", "faturamento"])
            mapping = {"qtdfatur": "qtd_faturadas", "faturad": "qtd_faturadas", "nf": "qtd_faturadas", "faturamento": "faturamento"}
            fixed = {
                "faturamento": (0.26, 0.40),
                "qtd_faturadas": (0.70, 0.82),
            }
        elif kind == "print4":
            centers = _find_header_centers(rows, ["cham", "chamada"])
            mapping = {"cham": "chamadas", "chamada": "chamadas"}
            fixed = {"chamadas": (0.72, 0.98)}
        elif kind == "print5":
            centers = _find_header_centers(rows, ["iniciad", "recebid", "finaliz", "tme"])
            mapping = {"tme": "tme_minutos", "iniciad": "iniciados", "recebid": "recebidos"}
            fixed = {
                "iniciados": (0.18, 0.36),
                "recebidos": (0.52, 0.66),
                "finalizados": (0.66, 0.80),
                "tme_minutos": (0.88, 0.99),
            }
        else:
            continue

        if debug:
            # texto agrupado por linhas (amostra) + headers encontrados
            sample_lines: list[str] = []
            for rr in rows[:20]:
                sample_lines.append(" ".join(c.text for c in rr[:30]))
            dbg["prints"].append(
                {
                    "nome_print": name,
                    "kind": kind,
                    "headers_detectados": {k: round(v, 1) for k, v in centers.items()},
                    "amostra_texto": sample_lines,
                }
            )

        # data rows: depois do header (pula primeiras linhas)
        updates: list[dict[str, Any]] = []
        for r in rows[5:]:
            rec = {}
            # 1) tenta por posição fixa
            if fixed:
                rec = _row_to_record_fixed(r, img_w=img_w, mapping_fixed=fixed)
            # 2) fallback antigo por centers/headers
            if not rec:
                rec = _row_to_record(r, centers, mapping)
            if not rec:
                continue
            updates.append(rec)

        # normalizar números
        for u in updates:
            for k, v in list(u.items()):
                if k == "nome":
                    continue
                u[k] = _coerce_number(v)

        _merge_by_name(by_name, updates)

    # 1) base por chave simples; 2) consolidação por similaridade
    vendedores = _consolidate_by_similarity(list(by_name.values()))

    # Totais (quando existir faturamento/meta)
    fat_total = sum(float(v.get("faturamento") or 0) for v in vendedores)
    meta_total = sum(float(v.get("meta_faturamento") or 0) for v in vendedores)
    totais: dict[str, Any] = {}
    if fat_total > 0:
        totais["faturamento_total"] = fat_total
    if meta_total > 0:
        totais["meta_total"] = meta_total

    payload: dict[str, Any] = {
        "vendedores": vendedores,
    }
    if totais:
        payload["totais"] = totais
    if debug:
        return payload, dbg
    return payload

