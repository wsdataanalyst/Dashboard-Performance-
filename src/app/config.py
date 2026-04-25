from __future__ import annotations

import os
from dataclasses import dataclass

import streamlit as st
from dotenv import load_dotenv


def _secret(key: str) -> str | None:
    try:
        v = st.secrets.get(key)  # type: ignore[attr-defined]
        if isinstance(v, str) and v.strip():
            return v.strip()
    except Exception:
        pass

    v = os.getenv(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


@dataclass(frozen=True)
class Settings:
    google_api_key: str | None
    openai_api_key: str | None
    gemini_model: str
    openai_model: str
    db_path: str
    """String de conexão `postgresql://...` (ex.: Neon). Se preenchida, o app usa PostgreSQL e ignora o arquivo SQLite."""
    database_url: str | None
    """Pasta para `uploads/` e dados locais quando o banco é remoto (Neon/Postgres)."""
    data_dir: str
    admin_username: str | None
    admin_password: str | None

    @property
    def uses_postgres(self) -> bool:
        u = (self.database_url or "").strip()
        return u.startswith("postgresql://") or u.startswith("postgres://")


def load_settings() -> Settings:
    load_dotenv()
    durl = _secret("DATABASE_URL")
    if not durl and os.getenv("DATABASE_URL", "").strip():
        durl = os.getenv("DATABASE_URL", "").strip()
    ddir = _secret("DATA_DIR")
    if not ddir:
        ddir = (os.getenv("DATA_DIR", "") or "").strip() or "data"
    return Settings(
        google_api_key=_secret("GOOGLE_API_KEY"),
        openai_api_key=_secret("OPENAI_API_KEY"),
        gemini_model=_secret("GEMINI_MODEL") or "gemini-1.5-flash",
        openai_model=_secret("OPENAI_MODEL") or "gpt-4o-mini",
        db_path=_secret("DB_PATH") or os.getenv("DB_PATH", "data/app.db"),
        database_url=(durl.strip() if durl and durl.strip() else None),
        data_dir=ddir,
        admin_username=_secret("ADMIN_USERNAME"),
        admin_password=_secret("ADMIN_PASSWORD"),
    )

