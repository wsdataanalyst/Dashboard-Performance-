from __future__ import annotations

import sqlite3
from pathlib import Path


def main() -> None:
    db_path = Path("data/app.db")
    print("db_path:", db_path.resolve())
    print("exists:", db_path.exists())
    if not db_path.exists():
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    print("tables:", [r[0] for r in cur.fetchall()])

    try:
        cur.execute("SELECT id, username, role, active FROM users ORDER BY id")
        print("users:", cur.fetchall())
    except Exception as e:
        print("users query failed:", repr(e))


if __name__ == "__main__":
    main()

