from __future__ import annotations

import sqlite3
from pathlib import Path

try:
    import tomllib  # py3.11+
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def main() -> None:
    secrets_path = Path(".streamlit/secrets.toml")
    if not secrets_path.exists():
        raise SystemExit("Missing .streamlit/secrets.toml")

    data = tomllib.loads(secrets_path.read_text(encoding="utf-8"))
    admin_user = str(data.get("ADMIN_USERNAME") or "").strip()
    admin_pass = str(data.get("ADMIN_PASSWORD") or "").strip()
    db_path = str(data.get("DB_PATH") or "data/app.db")

    if not admin_user or not admin_pass:
        raise SystemExit("Missing ADMIN_USERNAME/ADMIN_PASSWORD in secrets.toml")

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Ensure tables exist (minimal subset for users)
    conn.execute(
        """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'user',
  active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
)
""".strip()
    )

    r = conn.execute("SELECT id, username, active, role FROM users WHERE username = ?", (admin_user,)).fetchone()
    if r:
        print("admin already exists:", dict(r))
        return

    ph = pwd_context.hash(admin_pass)
    conn.execute(
        "INSERT INTO users(username, name, password_hash, role, active, created_at) VALUES(?,?,?,?,?, datetime('now'))",
        (admin_user, "Administrador", ph, "admin", 1),
    )
    conn.commit()
    r2 = conn.execute("SELECT id, username, active, role FROM users WHERE username = ?", (admin_user,)).fetchone()
    print("admin created:", dict(r2) if r2 else None)


if __name__ == "__main__":
    main()

