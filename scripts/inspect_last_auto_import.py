from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.config import load_settings
from src.app.storage import connect


def main() -> None:
    s = load_settings()
    conn = connect(s.db_path, s.database_url)
    r = conn.execute(
        "SELECT id, periodo, provider_used, payload_json FROM analyses WHERE provider_used = ? ORDER BY id DESC LIMIT 1",
        ("auto_import",),
    ).fetchone()
    if not r:
        print("no auto_import analysis found")
        return
    rid = int(r["id"])
    print("auto_import analysis:", rid, str(r["periodo"]))
    p = json.loads(str(r["payload_json"] or "{}"))
    dd = p.get("_sg_dept")
    deps = dd.get("departamentos") if isinstance(dd, dict) else None
    print("has _sg_dept:", isinstance(dd, dict), "deps:", len(deps) if isinstance(deps, list) else None)
    if isinstance(deps, list) and deps:
        d0 = deps[0]
        print("sample dept:", d0.get("departamento"))
        print("meta_margem_pct:", d0.get("meta_margem_pct"))
        print("margem_pct:", d0.get("margem_pct"))
        eq = sum(
            1
            for d in deps
            if isinstance(d, dict)
            and d.get("meta_margem_pct") is not None
            and d.get("margem_pct") is not None
            and d.get("meta_margem_pct") == d.get("margem_pct")
        )
        nn = sum(
            1
            for d in deps
            if isinstance(d, dict) and d.get("meta_margem_pct") is not None and d.get("margem_pct") is not None
        )
        print("equal count:", eq, "of", nn)


if __name__ == "__main__":
    main()

