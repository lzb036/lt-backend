from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet, InvalidToken

from app.core.config import settings


def _fernet() -> Fernet:
    digest = hashlib.sha256(settings.credential_encryption_secret.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    return _fernet().encrypt(text.encode("utf-8")).decode("ascii")


def decrypt_text(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    try:
        return _fernet().decrypt(text.encode("ascii")).decode("utf-8")
    except (InvalidToken, UnicodeError, ValueError):
        return ""


def mask_secret(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if len(text) <= 8:
        return "****"
    return f"{text[:4]}****{text[-4:]}"
