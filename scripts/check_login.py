from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

try:
    import tomllib  # py3.11+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.auth import verify_password


def main() -> None:
    root = ROOT
    secrets_path = root / ".streamlit" / "secrets.toml"
    data = tomllib.loads(secrets_path.read_text(encoding="utf-8"))
    db_path = str(data.get("DB_PATH") or "data/app.db")
    username = str(data.get("ADMIN_USERNAME") or "").strip()
    admin_pass = str(data.get("ADMIN_PASSWORD") or "")

    print("db_path:", (root / db_path).resolve())
    print("admin_username:", repr(username))
    print("admin_password_len:", len(admin_pass))

    conn = sqlite3.connect(str(root / db_path))
    conn.row_factory = sqlite3.Row
    r = conn.execute(
        "SELECT id, username, password_hash, role, active FROM users WHERE username = ?",
        (username,),
    ).fetchone()
    if not r:
        print("user not found")
        return
    rec = dict(r)
    ok = verify_password(admin_pass, str(rec.get("password_hash") or ""))
    print("db_user:", {k: rec[k] for k in ["id", "username", "role", "active"]})
    print("password_matches_secrets:", ok)


if __name__ == "__main__":
    main()

