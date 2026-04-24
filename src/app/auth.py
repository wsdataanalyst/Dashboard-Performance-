from __future__ import annotations

import secrets
import time
from dataclasses import dataclass

from passlib.context import CryptContext


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


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


@dataclass(frozen=True)
class UserSession:
    id: int
    username: str
    name: str
    role: str  # 'admin' | 'user'

    @property
    def is_admin(self) -> bool:
        return (self.role or "").lower() == "admin"

