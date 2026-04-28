from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from src.app.storage import connect_postgres, init_db, save_upload_file


def _read_sqlite_rows(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    cur = conn.execute(sql, params)
    return list(cur.fetchall())


def _load_bytes(data_dir: Path, rel_path: str) -> bytes | None:
    try:
        p = data_dir / Path(rel_path)
        return p.read_bytes()
    except Exception:
        return None


def main() -> int:
    sqlite_path = os.environ.get("SQLITE_PATH", "data/app.db")
    data_dir = Path(os.environ.get("DATA_DIR", "data")).resolve()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("ERRO: defina DATABASE_URL (Neon) no ambiente.")

    src = sqlite3.connect(sqlite_path)
    src.row_factory = sqlite3.Row
    dst = connect_postgres(database_url)
    init_db(dst)

    # analyses
    analyses = _read_sqlite_rows(
        src,
        """
SELECT id, created_at, periodo, provider_used, model_used, parent_analysis_id, owner_user_id, payload_json, total_bonus
FROM analyses
ORDER BY id ASC
""",
    )
    id_map: dict[int, int] = {}
    for r in analyses:
        params = (
            str(r["created_at"]),
            str(r["periodo"]),
            str(r["provider_used"]),
            str(r["model_used"]),
            int(r["parent_analysis_id"]) if r["parent_analysis_id"] is not None else None,
            int(r["owner_user_id"]) if r["owner_user_id"] is not None else None,
            str(r["payload_json"]),
            float(r["total_bonus"] or 0.0),
        )
        rr = dst.execute(
            """
INSERT INTO analyses(created_at, periodo, provider_used, model_used, parent_analysis_id, owner_user_id, payload_json, total_bonus)
VALUES(?,?,?,?,?,?,?,?) RETURNING id
""",
            params,
        ).fetchone()
        dst.commit()
        new_id = int(rr["id"])
        id_map[int(r["id"])] = new_id

    # feedbacks
    feedbacks = _read_sqlite_rows(
        src,
        """
SELECT analysis_id, seller_name, provider_used, model_used, feedback_text, created_at
FROM feedbacks
ORDER BY id ASC
""",
    )
    for r in feedbacks:
        aid_new = id_map.get(int(r["analysis_id"]))
        if not aid_new:
            continue
        params = (
            int(aid_new),
            str(r["seller_name"]),
            str(r["provider_used"]),
            str(r["model_used"]),
            str(r["feedback_text"]),
            str(r["created_at"]),
        )
        dst.execute(
            """
INSERT INTO feedbacks(analysis_id, seller_name, provider_used, model_used, feedback_text, created_at)
VALUES(?,?,?,?,?,?)
""",
            params,
        )
    dst.commit()

    # uploads + blobs
    uploads = _read_sqlite_rows(
        src,
        """
SELECT analysis_id, filename, content_type, sha256, rel_path, created_at
FROM uploads
ORDER BY id ASC
""",
    )
    missing = 0
    for r in uploads:
        old_aid = int(r["analysis_id"])
        new_aid = id_map.get(old_aid)
        if not new_aid:
            continue
        rel = str(r["rel_path"] or "")
        # tenta carregar do disco (data_dir/uploads/...)
        blob = _load_bytes(data_dir, rel) if rel else None
        if blob is None:
            missing += 1
        # reescreve rel_path para o novo id (mantém padrão do app)
        try:
            rel_new = str(Path(rel).parts[0] / Path(rel).parts[1] / str(new_aid) / Path(rel).name)  # type: ignore[operator]
        except Exception:
            rel_new = rel
        save_upload_file(
            dst,
            analysis_id=int(new_aid),
            filename=str(r["filename"]),
            content_type=str(r["content_type"]) if r["content_type"] is not None else None,
            sha256=str(r["sha256"]),
            rel_path=rel_new,
            blob_bytes=blob,
        )

    print("OK migrate.")
    print("analyses:", len(analyses), "feedbacks:", len(feedbacks), "uploads:", len(uploads), "uploads_missing_bytes:", missing)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

