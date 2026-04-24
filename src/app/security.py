from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_password(password: str, salt: str) -> str:
    # Simples, suficiente para "login local". Para produção séria, use argon2/bcrypt.
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


@dataclass(frozen=True)
class AdminAuth:
    username: str
    salt: str
    password_hash: str


def build_admin_auth(username: str, password: str) -> AdminAuth:
    salt = hashlib.sha256(username.encode("utf-8")).hexdigest()[:16]
    return AdminAuth(username=username, salt=salt, password_hash=hash_password(password, salt))

