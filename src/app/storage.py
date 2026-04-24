from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2


@dataclass(frozen=True)
class AnalysisRow:
    id: int
    created_at: str
    periodo: str
    provider_used: str
    model_used: str
    payload_json: str
    total_bonus: float


def _ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    _ensure_parent(db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""
    )
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS analyses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  periodo TEXT NOT NULL,
  provider_used TEXT NOT NULL,
  model_used TEXT NOT NULL,
  parent_analysis_id INTEGER,
  payload_json TEXT NOT NULL,
  total_bonus REAL NOT NULL,
  FOREIGN KEY (parent_analysis_id) REFERENCES analyses(id) ON DELETE SET NULL
);
"""
    )
    # Migração leve: adiciona coluna parent_analysis_id se DB já existir
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(analyses)").fetchall()]
        if "parent_analysis_id" not in cols:
            conn.execute("ALTER TABLE analyses ADD COLUMN parent_analysis_id INTEGER")
            conn.commit()
    except Exception:
        pass
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_id INTEGER NOT NULL,
  filename TEXT NOT NULL,
  content_type TEXT,
  sha256 TEXT NOT NULL,
  rel_path TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (analysis_id) REFERENCES analyses(id) ON DELETE CASCADE
);
"""
    )

    conn.execute(
        """
CREATE TABLE IF NOT EXISTS feedbacks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_id INTEGER NOT NULL,
  seller_name TEXT NOT NULL,
  provider_used TEXT NOT NULL,
  model_used TEXT NOT NULL,
  feedback_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (analysis_id) REFERENCES analyses(id) ON DELETE CASCADE
);
"""
    )
    conn.execute(
        "INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    conn.commit()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def save_analysis(
    conn: sqlite3.Connection,
    *,
    periodo: str,
    provider_used: str,
    model_used: str,
    parent_analysis_id: int | None = None,
    payload: dict[str, Any],
    total_bonus: float,
) -> int:
    from .bonus import calcular_time
    from .domain import filter_excluded_sellers_from_payload, parse_sellers

    payload = filter_excluded_sellers_from_payload(dict(payload))
    sellers = parse_sellers(payload)
    _, total_bonus = calcular_time(sellers) if sellers else ([], 0.0)

    payload_json = json.dumps(payload, ensure_ascii=False)
    cur = conn.execute(
        """
INSERT INTO analyses(created_at, periodo, provider_used, model_used, parent_analysis_id, payload_json, total_bonus)
VALUES(?,?,?,?,?,?,?)
""",
        (now_iso(), periodo, provider_used, model_used, parent_analysis_id, payload_json, float(total_bonus)),
    )
    conn.commit()
    return int(cur.lastrowid)


def list_analyses(conn: sqlite3.Connection, limit: int = 50) -> list[AnalysisRow]:
    rows = conn.execute(
        """
SELECT id, created_at, periodo, provider_used, model_used, payload_json, total_bonus
FROM analyses
ORDER BY id DESC
LIMIT ?
""",
        (int(limit),),
    ).fetchall()
    out: list[AnalysisRow] = []
    for r in rows:
        out.append(
            AnalysisRow(
                id=int(r["id"]),
                created_at=str(r["created_at"]),
                periodo=str(r["periodo"]),
                provider_used=str(r["provider_used"]),
                model_used=str(r["model_used"]),
                payload_json=str(r["payload_json"]),
                total_bonus=float(r["total_bonus"]),
            )
        )
    return out


def get_analysis(conn: sqlite3.Connection, analysis_id: int) -> AnalysisRow | None:
    r = conn.execute(
        """
SELECT id, created_at, periodo, provider_used, model_used, payload_json, total_bonus
FROM analyses
WHERE id = ?
""",
        (int(analysis_id),),
    ).fetchone()
    if not r:
        return None
    return AnalysisRow(
        id=int(r["id"]),
        created_at=str(r["created_at"]),
        periodo=str(r["periodo"]),
        provider_used=str(r["provider_used"]),
        model_used=str(r["model_used"]),
        payload_json=str(r["payload_json"]),
        total_bonus=float(r["total_bonus"]),
    )


def delete_analysis(conn: sqlite3.Connection, analysis_id: int) -> None:
    conn.execute("DELETE FROM analyses WHERE id = ?", (int(analysis_id),))
    conn.commit()


def delete_feedbacks_excluded_sellers(conn: sqlite3.Connection) -> int:
    """Remove registros de feedback cujo colaborador é nome excluído (ex.: Laila)."""
    cur = conn.execute(
        """
DELETE FROM feedbacks
WHERE lower(trim(seller_name)) = 'laila'
   OR lower(trim(seller_name)) LIKE 'laila %'
"""
    )
    conn.commit()
    return int(cur.rowcount if cur.rowcount is not None else 0)


def purge_excluded_sellers_from_all_analyses(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """
    Atualiza todas as linhas de `analyses`: remove vendedores excluídos do JSON,
    recalcula total_bonus e apaga feedbacks ligados a nomes excluídos.
    Retorna (analyses_alteradas, vendedores_removidos_estimado, feedbacks_apagados).
    """
    from .bonus import calcular_time
    from .domain import filter_excluded_sellers_from_payload, is_excluded_seller_name, parse_sellers

    fb_deleted = delete_feedbacks_excluded_sellers(conn)

    rows = conn.execute("SELECT id, payload_json FROM analyses").fetchall()
    analyses_changed = 0
    sellers_removed = 0
    for r in rows:
        aid = int(r["id"])
        payload = json.loads(r["payload_json"])
        raw = payload.get("vendedores") or []
        if not isinstance(raw, list):
            continue
        before_ex = sum(1 for x in raw if isinstance(x, dict) and is_excluded_seller_name(str(x.get("nome") or "")))
        if before_ex == 0:
            continue
        sellers_removed += before_ex
        new_payload = filter_excluded_sellers_from_payload(payload)
        sellers = parse_sellers(new_payload)
        _, total = calcular_time(sellers) if sellers else ([], 0.0)
        conn.execute(
            "UPDATE analyses SET payload_json = ?, total_bonus = ? WHERE id = ?",
            (json.dumps(new_payload, ensure_ascii=False), float(total), aid),
        )
        analyses_changed += 1
    conn.commit()
    return analyses_changed, sellers_removed, fb_deleted


def save_upload_file(
    conn: sqlite3.Connection,
    *,
    analysis_id: int,
    filename: str,
    content_type: str | None,
    sha256: str,
    rel_path: str,
) -> None:
    conn.execute(
        """
INSERT INTO uploads(analysis_id, filename, content_type, sha256, rel_path, created_at)
VALUES(?,?,?,?,?,?)
""",
        (int(analysis_id), filename, content_type, sha256, rel_path, now_iso()),
    )
    conn.commit()


def list_uploads(conn: sqlite3.Connection, analysis_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
SELECT id, filename, content_type, sha256, rel_path, created_at
FROM uploads
WHERE analysis_id = ?
ORDER BY id ASC
""",
        (int(analysis_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def save_feedback(
    conn: sqlite3.Connection,
    *,
    analysis_id: int,
    seller_name: str,
    provider_used: str,
    model_used: str,
    feedback_text: str,
) -> int:
    cur = conn.execute(
        """
INSERT INTO feedbacks(analysis_id, seller_name, provider_used, model_used, feedback_text, created_at)
VALUES(?,?,?,?,?,?)
""",
        (int(analysis_id), seller_name, provider_used, model_used, feedback_text, now_iso()),
    )
    conn.commit()
    return int(cur.lastrowid)


def list_feedbacks(conn: sqlite3.Connection, analysis_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
SELECT id, seller_name, provider_used, model_used, feedback_text, created_at
FROM feedbacks
WHERE analysis_id = ?
ORDER BY id DESC
""",
        (int(analysis_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def base_data_dir(db_path: str) -> Path:
    # Usa a pasta do DB como "raiz de dados"
    return Path(os.path.abspath(db_path)).parent

