from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.config import load_settings
from src.app.storage import connect, init_db


def main() -> None:
    s = load_settings()
    conn = connect(s.db_path, s.database_url)
    init_db(conn)
    r = conn.execute(
        "SELECT id, periodo, provider_used, payload_json FROM analyses ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if not r:
        print("no analyses")
        return
    p = json.loads(str(r["payload_json"] or "{}"))
    vend = p.get("vendedores") if isinstance(p, dict) else None
    has = False
    if isinstance(vend, list):
        has = any(isinstance(v, dict) and v.get("desconto_pct") is not None for v in vend)
    print("latest analysis:", int(r["id"]), str(r["periodo"]), "provider:", str(r["provider_used"]))
    print("has desconto_pct:", has)


if __name__ == "__main__":
    main()

