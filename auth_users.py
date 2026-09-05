"""Local email/password accounts with PumpPortal credentials."""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any

from werkzeug.security import check_password_hash, generate_password_hash

DATA_DIR = Path(__file__).resolve().parent / "data"
USERS_PATH = DATA_DIR / "users.json"

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_lock = threading.Lock()


def _empty() -> dict[str, Any]:
    return {"users": {}}


def _load() -> dict[str, Any]:
    if not USERS_PATH.exists():
        return _empty()
    try:
        data = json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _empty()
    if not isinstance(data, dict) or not isinstance(data.get("users"), dict):
        return _empty()
    return data


def _save(data: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = USERS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(USERS_PATH)


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def validate_email(email: str) -> str | None:
    e = normalize_email(email)
    if not e or not _EMAIL_RE.match(e):
        return "Enter a valid email address"
    if len(e) > 120:
        return "Email is too long"
    return None


def validate_password(password: str) -> str | None:
    if not password or len(password) < 8:
        return "Password must be at least 8 characters"
    if len(password) > 128:
        return "Password is too long"
    return None


def get_user(email: str) -> dict[str, Any] | None:
    key = normalize_email(email)
    with _lock:
        user = _load()["users"].get(key)
    if not user or not isinstance(user, dict):
        return None
    return dict(user)


def create_user(
    email: str,
    password: str,
    *,
    api_key: str,
    wallet_pubkey: str,
    private_key: str,
) -> tuple[dict[str, Any] | None, str | None]:
    err = validate_email(email) or validate_password(password)
    if err:
        return None, err
    key = normalize_email(email)
    api_key = (api_key or "").strip()
    wallet_pubkey = (wallet_pubkey or "").strip()
    private_key = (private_key or "").strip()
    if not api_key or not wallet_pubkey or not private_key:
        return None, "PumpPortal wallet provisioning incomplete"

    with _lock:
        data = _load()
        if key in data["users"]:
            return None, "An account with this email already exists"
        user = {
            "email": key,
            "password_hash": generate_password_hash(password),
            "pp_api_key": api_key,
            "wallet_pubkey": wallet_pubkey,
            "private_key": private_key,
        }
        data["users"][key] = user
        _save(data)
    out = dict(user)
    out.pop("password_hash", None)
    return out, None


def verify_login(email: str, password: str) -> tuple[dict[str, Any] | None, str | None]:
    err = validate_email(email)
    if err:
        return None, err
    if not password:
        return None, "Enter your password"
    user = get_user(email)
    if not user or not check_password_hash(user.get("password_hash") or "", password):
        return None, "Invalid email or password"
    out = dict(user)
    out.pop("password_hash", None)
    return out, None


def public_user_payload(user: dict[str, Any], *, include_secrets: bool = False) -> dict[str, Any]:
    payload = {
        "email": user.get("email") or "",
        "wallet_pubkey": user.get("wallet_pubkey") or "",
    }
    if include_secrets:
        payload["pp_api_key"] = user.get("pp_api_key") or ""
        payload["private_key"] = user.get("private_key") or ""
    return payload
