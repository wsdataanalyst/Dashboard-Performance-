from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.config import load_settings
from src.app.excel_import import import_5_files_to_payload
from src.app.kpi_import import import_faturamento_atendidos_daily_df, import_faturamento_atendidos_xlsx
from src.app.dept_import import import_departamentos
from src.app.security import sha256_hex
from src.app.storage import (
    connect,
    ensure_admin_user,
    get_user_by_username,
    init_db,
    save_analysis,
    save_upload_file,
)
from src.app.auth import hash_password


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v if isinstance(v, str) else default).strip() or default


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _safe_stem(name: str) -> str:
    s = re.sub(r"\s+", "_", name.strip())
    s = "".join(ch for ch in s if ch.isalnum() or ch in ("-", "_", "."))
    return s.strip("._") or "file"


def _default_periodo() -> str:
    import datetime as _dt

    now = _dt.datetime.now()
    # Formato pedido para o Histórico: "Até DD/MM/AAAA"
    return f"Até {now:%d/%m/%Y}"


def _uploads_dir(settings) -> Path:
    # Mantém compatível com `streamlit_app.py`: sempre em data/uploads (ou data_dir quando Postgres)
    from src.app.storage import resolve_data_dir

    data_dir = resolve_data_dir(db_path=settings.db_path, database_url=settings.database_url, data_dir=settings.data_dir)
    p = data_dir / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def main() -> int:
    settings = load_settings()
    conn = connect(settings.db_path, settings.database_url)
    init_db(conn)

    # Garante admin do sistema existir (necessário para owner_id padrão)
    admin_user = (settings.admin_username or "").strip()
    admin_pass = (settings.admin_password or "").strip()
    if not admin_user or not admin_pass:
        print("ERRO: defina ADMIN_USERNAME e ADMIN_PASSWORD (secrets/.env).")
        return 2

    admin_id = ensure_admin_user(conn, username=admin_user, password_hash=hash_password(admin_pass), name="Administrador")

    # Visibilidade no app:
    # - Se owner_user_id ficar NULL, qualquer usuário logado consegue ver/ativar (regra do Histórico: owner_id OU NULL).
    # - Se quiser "privado", defina AUTO_IMPORT_OWNER_USERNAME.
    owner_username = _env("AUTO_IMPORT_OWNER_USERNAME", "")
    if owner_username:
        owner = get_user_by_username(conn, owner_username)
        owner_id = int(owner["id"]) if owner else admin_id
    else:
        owner_id = None

    inbox_dir = Path(_env("AUTO_IMPORT_DIR", str(ROOT / "auto_inbox")))
    inbox_dir.mkdir(parents=True, exist_ok=True)
    archive_dir = Path(_env("AUTO_IMPORT_ARCHIVE_DIR", str(ROOT / "auto_archive")))
    archive_dir.mkdir(parents=True, exist_ok=True)

    # Por padrão: pega Excel/HTML exportado
    patterns = [p.strip() for p in _env("AUTO_IMPORT_GLOB", "*.xls*;*.xlsx").split(";") if p.strip()]
    paths: list[Path] = []
    for pat in patterns:
        paths.extend(sorted(inbox_dir.glob(pat)))
    # Ignora arquivos temporários do Excel (ex.: "~$arquivo.xlsx")
    paths = [p for p in paths if p.is_file() and not p.name.startswith("~$")]

    if not paths:
        print(f"OK: nenhuma planilha encontrada em {inbox_dir}")
        return 0

    files_bytes: list[tuple[str, bytes]] = []
    for p in paths:
        files_bytes.append((p.name, p.read_bytes()))

    # Mesma classificação do app (por conteúdo)
    perf_files: list[tuple[str, bytes]] = []
    dept_files: list[tuple[str, bytes]] = []
    sg_daily_df: pd.DataFrame | None = None
    sg_daily_meta: dict[str, Any] | None = None
    sg_daily_source: str | None = None
    sg_kpis: dict[str, Any] | None = None

    for fname, b in files_bytes:
        # 1) Faturamento e Atendidos (evolução diária)
        try:
            dres = import_faturamento_atendidos_daily_df(b)
            if isinstance(dres.df_daily, pd.DataFrame) and not dres.df_daily.empty:
                sg_daily_df = dres.df_daily
                sg_daily_meta = dres.meta
                sg_daily_source = fname
                try:
                    kres = import_faturamento_atendidos_xlsx(b)
                    sg_kpis = dict(kres.kpis or {}) if getattr(kres, "kpis", None) else None
                except Exception:
                    pass
                continue
        except Exception:
            pass

        # 2) Departamentos
        try:
            dpt1 = import_departamentos([(fname, b)])
            dept_rows = (dpt1.payload or {}).get("departamentos") if isinstance(dpt1.payload, dict) else None
            if isinstance(dept_rows, list) and len(dept_rows) > 0:
                dept_files.append((fname, b))
                continue
        except Exception:
            pass

        # 3) Performance vendedores
        perf_files.append((fname, b))

    if not perf_files:
        print("ERRO: não encontrei arquivos de Performance (vendedores) no lote.")
        return 3

    res = import_5_files_to_payload(perf_files)
    payload = dict(res.payload or {})

    periodo = _env("AUTO_IMPORT_PERIODO", "") or str(payload.get("periodo") or "").strip() or _default_periodo()
    payload["periodo"] = periodo

    # Vincula bases auxiliares, igual o app faz ao salvar
    if isinstance(sg_daily_df, pd.DataFrame) and not sg_daily_df.empty:
        cols = [c for c in ["dia", "faturamento", "nfs_emitidas", "clientes_atendidos"] if c in sg_daily_df.columns]
        rows_daily = sg_daily_df[cols].copy().to_dict(orient="records") if cols else sg_daily_df.to_dict(orient="records")
        payload["_sg_daily"] = {"rows": rows_daily, "meta": sg_daily_meta or {}, "source": sg_daily_source}
        if isinstance(sg_kpis, dict) and sg_kpis:
            payload["_sg_kpis"] = sg_kpis

    if dept_files:
        try:
            dpt = import_departamentos(dept_files)
            if isinstance(dpt.payload, dict) and isinstance(dpt.payload.get("departamentos"), list) and dpt.payload.get("departamentos"):
                payload["_sg_dept"] = {
                    "departamentos": dpt.payload.get("departamentos"),
                    "meta": dpt.meta if isinstance(dpt.meta, dict) else {},
                    "source": [n for (n, _) in dept_files],
                }
        except Exception:
            pass

    analysis_id = save_analysis(
        conn,
        periodo=periodo,
        provider_used="auto_import",
        model_used="excel_import",
        parent_analysis_id=None,
        owner_user_id=owner_id,
        payload=payload,
        total_bonus=0.0,
    )

    # Uploads:
    # - Em SQLite/local, salvar no disco ajuda auditoria.
    # - Em Postgres/Cloud, salvar em disco local NÃO aparece no Cloud; por padrão, desliga.
    save_uploads = _env_bool("AUTO_IMPORT_SAVE_UPLOADS", not settings.uses_postgres)
    if save_uploads:
        up_dir = _uploads_dir(settings) / str(owner_id or "anon") / str(analysis_id)
        up_dir.mkdir(parents=True, exist_ok=True)

        # Salva anexos e registra auditoria
        for fname, b in files_bytes:
            digest = sha256_hex(b)
            ext = Path(fname).suffix.lower()[:10] or ".bin"
            filename = f"{_safe_stem(Path(fname).stem)}_{digest[:10]}{ext}"
            rel_path = str(Path("uploads") / str(owner_id or "anon") / str(analysis_id) / filename)
            (up_dir / filename).write_bytes(b)
            save_upload_file(
                conn,
                analysis_id=analysis_id,
                filename=filename,
                content_type=None,
                sha256=digest,
                rel_path=rel_path,
                blob_bytes=b,
            )

    # Arquiva os originais (opcional)
    if _env_bool("AUTO_IMPORT_MOVE_PROCESSED", True):
        dest = archive_dir / str(analysis_id)
        dest.mkdir(parents=True, exist_ok=True)
        for p in paths:
            try:
                shutil.move(str(p), str(dest / p.name))
            except Exception:
                pass

    print(f"OK: análise salva no histórico. id={analysis_id} periodo={periodo} arquivos={len(paths)}")
    if res.warnings:
        print("warnings:", res.warnings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

