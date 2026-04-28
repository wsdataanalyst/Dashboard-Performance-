from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.config import load_settings
from src.app.ocr_fallback import extract_payload_from_prints_ocr
from src.app.storage import connect, get_upload_blob_bytes, init_db, list_uploads


def _has_desconto(payload: dict) -> bool:
    raw = payload.get("vendedores")
    if not isinstance(raw, list):
        return False
    for v in raw:
        if isinstance(v, dict) and v.get("desconto_pct") is not None:
            return True
    return False


def _load_upload_bytes(settings, rel_path: str) -> bytes | None:
    # rel_path foi salvo como "uploads\\..."; no disco fica em settings.data_dir/uploads/...
    base = Path(settings.data_dir)
    p = base / Path(rel_path)
    try:
        return p.read_bytes()
    except Exception:
        return None


def _load_upload_bytes_any(settings, conn, upload_row: dict) -> bytes | None:
    # prefer blob do DB (quando existir)
    try:
        uid = int(upload_row.get("id") or 0)
        if uid:
            b = get_upload_blob_bytes(conn, uid)
            if b:
                return b
    except Exception:
        pass
    rel = str(upload_row.get("rel_path") or "")
    if rel:
        return _load_upload_bytes(settings, rel)
    return None


def main() -> int:
    settings = load_settings()
    conn = connect(settings.db_path, settings.database_url)
    init_db(conn)

    # Só faz sentido no modo local (uploads no disco).
    if settings.uses_postgres:
        print("ERRO: backfill de uploads só funciona com SQLite/local (DATABASE_URL desabilitado).")
        return 2

    rows = conn.execute(
        """
SELECT id, payload_json
FROM analyses
ORDER BY id DESC
LIMIT 200
"""
    ).fetchall()

    changed = 0
    scanned = 0
    for r in rows:
        aid = int(r["id"])
        try:
            payload = json.loads(str(r["payload_json"] or "{}"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if payload.get("_kind"):
            continue
        scanned += 1
        if _has_desconto(payload):
            continue

        ups = list_uploads(conn, aid)
        if not ups:
            continue

        # Pega os prints salvos (png) e roda OCR fallback pra recuperar desconto_pct.
        imgs: list[tuple[str, bytes]] = []
        for it in ups:
            fn = str(it.get("filename") or "")
            if not fn.lower().endswith(".png"):
                continue
            b = _load_upload_bytes_any(settings, conn, it)
            if b:
                imgs.append((fn, b))
        if not imgs:
            continue

        try:
            extracted = extract_payload_from_prints_ocr(imgs, debug=False)
        except Exception:
            continue
        if not isinstance(extracted, dict):
            continue
        vend2 = extracted.get("vendedores")
        if not isinstance(vend2, list) or not vend2:
            continue

        # Mapeia desconto por nome
        disc_by_name: dict[str, dict] = {}
        for v in vend2:
            if not isinstance(v, dict):
                continue
            nm = str(v.get("nome") or "").strip()
            if not nm:
                continue
            disc_by_name[nm.lower()] = v

        vend = payload.get("vendedores")
        if not isinstance(vend, list) or not vend:
            continue

        updated = False
        new_vend = []
        for v in vend:
            if not isinstance(v, dict):
                new_vend.append(v)
                continue
            nm = str(v.get("nome") or "").strip()
            vv = dict(v)
            src = disc_by_name.get(nm.lower())
            if isinstance(src, dict):
                for k in ("desconto_pct", "desconto_valor", "qtd_desconto", "qtd_desconto_pct"):
                    if vv.get(k) is None and src.get(k) is not None:
                        vv[k] = src.get(k)
                        updated = True
            new_vend.append(vv)

        if not updated:
            continue

        payload2 = dict(payload)
        payload2["vendedores"] = new_vend
        conn.execute("UPDATE analyses SET payload_json = ? WHERE id = ?", (json.dumps(payload2, ensure_ascii=False), aid))
        conn.commit()
        changed += 1
        print("OK backfill desconto in analysis", aid)

    print("done. scanned:", scanned, "changed:", changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

