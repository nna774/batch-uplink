"""HMAC-SHA256 によるデバイス認証。firmware の HmacSha256.h と対応。"""

from __future__ import annotations

import hashlib
import hmac
import os


class AuthError(Exception):
    pass


def secret_for_device(device_id: str) -> str:
    """device_id -> 共有鍵。まずは環境変数 NAMZ_HMAC_SECRET_<id>、無ければ NAMZ_HMAC_SECRET。"""
    key = os.environ.get(f"NAMZ_HMAC_SECRET_{device_id}")
    if key:
        return key
    key = os.environ.get("NAMZ_HMAC_SECRET")
    if not key:
        raise AuthError("HMAC secret not configured")
    return key


def verify(device_id: str, body: bytes, signature_hex: str) -> None:
    """署名を検証。不一致なら AuthError。"""
    if not signature_hex:
        raise AuthError("missing signature")
    secret = secret_for_device(device_id)
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature_hex.strip().lower()):
        raise AuthError("signature mismatch")
