"""
Testa a conexão com o banco (SQLite ou PostgreSQL/Neon) usando as mesmas
variáveis do app (load_settings + connect + init_db).

Uso (na raiz do projeto):
  python scripts/test_db_connection.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Raiz do projeto = pai de scripts/
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)

from src.app.config import load_settings
from src.app.storage import connect, init_db, is_postgres_conn


def _mask_url(u: str) -> str:
    if "@" not in u or "://" not in u:
        return "***"
    try:
        head, rest = u.split("://", 1)
        if "@" in rest:
            userinfo, hostpart = rest.rsplit("@", 1)
            return f"{head}://***:***@{hostpart[:48]}…"
    except Exception:
        pass
    return "***"


def main() -> int:
    s = load_settings()
    durl = (s.database_url or "").strip()
    if durl:
        print("Modo: PostgreSQL (DATABASE_URL definida)")
        print(f"  String (mascarada): {_mask_url(durl)}")
    else:
        print("Modo: SQLite (sem DATABASE_URL)")
        print(f"  Arquivo: {s.db_path}")

    try:
        conn = connect(s.db_path, s.database_url)
        init_db(conn)
        if is_postgres_conn(conn):
            r = conn.execute("SELECT 1 AS ok").fetchone()
            assert r and (r["ok"] == 1 or r[0] == 1)  # type: ignore[index]
        else:
            r = conn.execute("SELECT 1 AS ok").fetchone()
            assert r and int(r["ok"]) == 1
        print("Resultado: OK - banco acessivel e tabelas inicializadas.")
    except Exception as e:
        print(f"Falha: {e!r}", file=sys.stderr)
        print(
            "\nDicas: confira a URL no Neon (Connection), use sslmode=require, "
            "e não deixe espaços/aspas extras no .env.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
