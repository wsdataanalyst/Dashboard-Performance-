from __future__ import annotations

import secrets
import time
from typing import NamedTuple

from passlib.context import CryptContext


# PBKDF2 é puro Python e funciona bem no Streamlit Cloud.
# (evita dependências nativas como bcrypt, que podem falhar em alguns runtimes)
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bool(pwd_context.verify(password, password_hash))
    except Exception:
        return False


def new_invite_code() -> str:
    # Curto o suficiente para digitar, longo o suficiente para evitar brute-force casual
    return secrets.token_urlsafe(12)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


class UserSession(NamedTuple):
    id: int
    username: str
    name: str
    role: str  # 'admin' | 'user'

    @property
    def is_admin(self) -> bool:
        return (self.role or "").lower() == "admin"

