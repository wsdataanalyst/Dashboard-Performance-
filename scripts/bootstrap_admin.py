from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.auth import hash_password
from src.app.config import load_settings
from src.app.storage import connect, ensure_admin_user, get_user_by_username, init_db


def main() -> None:
    s = load_settings()
    admin_user = (s.admin_username or "").strip()
    admin_pass = (s.admin_password or "").strip()
    if not admin_user or not admin_pass:
        raise SystemExit("Missing ADMIN_USERNAME/ADMIN_PASSWORD in secrets/.env")

    conn = connect(s.db_path, s.database_url)
    init_db(conn)

    admin_id = ensure_admin_user(
        conn,
        username=admin_user,
        password_hash=hash_password(admin_pass),
        name="Administrador",
    )
    rec = get_user_by_username(conn, admin_user)
    print("admin_id:", admin_id)
    print("admin_record:", rec)


if __name__ == "__main__":
    main()

