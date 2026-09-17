"""Server-side strategy presets (SQLite) per logged-in user."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from paper_bot import StrategyParams

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "stonkbot.db"

MODES = ("sim", "live", "trade")
SLOT_COUNT = 3
BUY_SOL_MAX = 0.05

_lock = threading.Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _lock:
        conn = _connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS user_presets (
                    user_email TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    slot INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    params TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (user_email, mode, slot)
                );
                CREATE TABLE IF NOT EXISTS user_mode_params (
                    user_email TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    params TEXT NOT NULL,
                    active_slot INTEGER,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (user_email, mode)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()


def _valid_mode(mode: str) -> bool:
    return mode in MODES


def _valid_slot(slot: int) -> bool:
    return isinstance(slot, int) and 0 <= slot < SLOT_COUNT


def _parse_active_slot(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        slot = int(raw)
    except (TypeError, ValueError):
        return None
    return slot if _valid_slot(slot) else None


def normalize_params(data: dict[str, Any] | None) -> dict[str, Any]:
    """Validate strategy fields + optional buy_sol for Trade."""
    if not isinstance(data, dict):
        return {}
    params = StrategyParams.from_dict(data)
    out: dict[str, Any] = asdict(params)
    raw_buy = data.get("buy_sol")
    if raw_buy is not None:
        try:
            buy = float(raw_buy)
            if 0 < buy <= BUY_SOL_MAX:
                out["buy_sol"] = buy
        except (TypeError, ValueError):
            pass
    return out


def _empty_slots() -> list[dict[str, Any] | None]:
    return [None] * SLOT_COUNT


def _empty_presets_state() -> dict[str, Any]:
    return {mode: {"activeSlot": None, "slots": _empty_slots()} for mode in MODES}


def get_user_state(email: str) -> dict[str, Any]:
    key = (email or "").strip().lower()
    if not key:
        return {"presets": _empty_presets_state(), "modeParams": {m: None for m in MODES}}

    presets = _empty_presets_state()
    mode_params: dict[str, Any] = {m: None for m in MODES}

    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT mode, slot, name, params
                FROM user_presets
                WHERE user_email = ?
                ORDER BY mode, slot
                """,
                (key,),
            ).fetchall()
            for row in rows:
                mode = row["mode"]
                slot = int(row["slot"])
                if not _valid_mode(mode) or not _valid_slot(slot):
                    continue
                try:
                    params = json.loads(row["params"])
                except Exception:
                    params = {}
                presets[mode]["slots"][slot] = {
                    "name": str(row["name"] or f"Preset {slot + 1}"),
                    "params": normalize_params(params),
                }

            mp_rows = conn.execute(
                """
                SELECT mode, params, active_slot
                FROM user_mode_params
                WHERE user_email = ?
                """,
                (key,),
            ).fetchall()
            for row in mp_rows:
                mode = row["mode"]
                if not _valid_mode(mode):
                    continue
                try:
                    params = json.loads(row["params"])
                except Exception:
                    params = {}
                mode_params[mode] = normalize_params(params)
                presets[mode]["activeSlot"] = _parse_active_slot(row["active_slot"])
        finally:
            conn.close()

    return {"presets": presets, "modeParams": mode_params}


def has_any_data(email: str) -> bool:
    key = (email or "").strip().lower()
    if not key:
        return False
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """
                SELECT 1 FROM user_presets WHERE user_email = ? LIMIT 1
                """,
                (key,),
            ).fetchone()
            if row:
                return True
            row = conn.execute(
                """
                SELECT 1 FROM user_mode_params WHERE user_email = ? LIMIT 1
                """,
                (key,),
            ).fetchone()
            return bool(row)
        finally:
            conn.close()


def save_preset_slot(
    email: str,
    mode: str,
    slot: int,
    name: str,
    params: dict[str, Any],
    *,
    active_slot: int | None = None,
) -> dict[str, Any]:
    key = (email or "").strip().lower()
    if not key:
        raise ValueError("Missing user email")
    if not _valid_mode(mode):
        raise ValueError("Invalid mode")
    if not _valid_slot(slot):
        raise ValueError("Invalid slot")
    clean_name = (name or "").strip() or f"Preset {slot + 1}"
    clean_params = normalize_params(params)
    now = _utc_now()

    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT INTO user_presets (user_email, mode, slot, name, params, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_email, mode, slot) DO UPDATE SET
                    name = excluded.name,
                    params = excluded.params,
                    updated_at = excluded.updated_at
                """,
                (key, mode, slot, clean_name, json.dumps(clean_params), now),
            )
            if active_slot is not None and _valid_slot(active_slot):
                conn.execute(
                    """
                    INSERT INTO user_mode_params (user_email, mode, params, active_slot, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(user_email, mode) DO UPDATE SET
                        active_slot = excluded.active_slot,
                        updated_at = excluded.updated_at
                    """,
                    (key, mode, json.dumps(clean_params), active_slot, now),
                )
            conn.commit()
        finally:
            conn.close()

    return get_user_state(key)


def clear_preset_slot(email: str, mode: str, slot: int) -> dict[str, Any]:
    key = (email or "").strip().lower()
    if not key:
        raise ValueError("Missing user email")
    if not _valid_mode(mode):
        raise ValueError("Invalid mode")
    if not _valid_slot(slot):
        raise ValueError("Invalid slot")

    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """
                DELETE FROM user_presets
                WHERE user_email = ? AND mode = ? AND slot = ?
                """,
                (key, mode, slot),
            )
            row = conn.execute(
                """
                SELECT active_slot FROM user_mode_params
                WHERE user_email = ? AND mode = ?
                """,
                (key, mode),
            ).fetchone()
            if row and row["active_slot"] == slot:
                conn.execute(
                    """
                    UPDATE user_mode_params
                    SET active_slot = NULL, updated_at = ?
                    WHERE user_email = ? AND mode = ?
                    """,
                    (_utc_now(), key, mode),
                )
            conn.commit()
        finally:
            conn.close()

    return get_user_state(key)


def save_mode_params(
    email: str,
    mode: str,
    params: dict[str, Any],
    *,
    active_slot: int | None = None,
) -> dict[str, Any]:
    key = (email or "").strip().lower()
    if not key:
        raise ValueError("Missing user email")
    if not _valid_mode(mode):
        raise ValueError("Invalid mode")
    clean_params = normalize_params(params)
    now = _utc_now()

    with _lock:
        conn = _connect()
        try:
            if active_slot is not None and not _valid_slot(active_slot):
                active_slot = None
            existing = conn.execute(
                """
                SELECT active_slot FROM user_mode_params
                WHERE user_email = ? AND mode = ?
                """,
                (key, mode),
            ).fetchone()
            slot_val = active_slot if active_slot is not None else (
                int(existing["active_slot"]) if existing and existing["active_slot"] is not None else None
            )
            conn.execute(
                """
                INSERT INTO user_mode_params (user_email, mode, params, active_slot, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_email, mode) DO UPDATE SET
                    params = excluded.params,
                    active_slot = COALESCE(excluded.active_slot, user_mode_params.active_slot),
                    updated_at = excluded.updated_at
                """,
                (key, mode, json.dumps(clean_params), slot_val, now),
            )
            conn.commit()
        finally:
            conn.close()

    return get_user_state(key)


def set_active_slot(email: str, mode: str, active_slot: int | None) -> dict[str, Any]:
    key = (email or "").strip().lower()
    if not key:
        raise ValueError("Missing user email")
    if not _valid_mode(mode):
        raise ValueError("Invalid mode")
    if active_slot is not None and not _valid_slot(active_slot):
        active_slot = None

    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """
                SELECT params FROM user_mode_params
                WHERE user_email = ? AND mode = ?
                """,
                (key, mode),
            ).fetchone()
            params_json = row["params"] if row else json.dumps(normalize_params({}))
            conn.execute(
                """
                INSERT INTO user_mode_params (user_email, mode, params, active_slot, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_email, mode) DO UPDATE SET
                    active_slot = excluded.active_slot,
                    updated_at = excluded.updated_at
                """,
                (key, mode, params_json, active_slot, _utc_now()),
            )
            conn.commit()
        finally:
            conn.close()

    return get_user_state(key)


def import_client_state(
    email: str,
    presets: dict[str, Any] | None,
    mode_params: dict[str, Any] | None,
    *,
    replace: bool = False,
) -> dict[str, Any]:
    """Import presets/mode params from browser localStorage shape."""
    key = (email or "").strip().lower()
    if not key:
        raise ValueError("Missing user email")

    if not replace and has_any_data(key):
        return get_user_state(key)

    with _lock:
        conn = _connect()
        try:
            if replace:
                conn.execute("DELETE FROM user_presets WHERE user_email = ?", (key,))
                conn.execute("DELETE FROM user_mode_params WHERE user_email = ?", (key,))

            if isinstance(presets, dict):
                for mode in MODES:
                    block = presets.get(mode)
                    if not isinstance(block, dict):
                        continue
                    slots = block.get("slots") or []
                    for slot_idx in range(SLOT_COUNT):
                        entry = slots[slot_idx] if slot_idx < len(slots) else None
                        if not entry or not isinstance(entry, dict) or not entry.get("params"):
                            continue
                        name = str(entry.get("name") or f"Preset {slot_idx + 1}").strip()
                        params = normalize_params(entry.get("params"))
                        conn.execute(
                            """
                            INSERT INTO user_presets (user_email, mode, slot, name, params, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(user_email, mode, slot) DO UPDATE SET
                                name = excluded.name,
                                params = excluded.params,
                                updated_at = excluded.updated_at
                            """,
                            (key, mode, slot_idx, name, json.dumps(params), _utc_now()),
                        )
                    active_slot = None
                    active = block.get("activeSlot")
                    if active is not None:
                        try:
                            a = int(active)
                            if _valid_slot(a):
                                active_slot = a
                        except (TypeError, ValueError):
                            pass
                    if active_slot is not None or isinstance(mode_params, dict):
                        mp = mode_params.get(mode) if isinstance(mode_params, dict) else None
                        params = normalize_params(mp if isinstance(mp, dict) else {})
                        conn.execute(
                            """
                            INSERT INTO user_mode_params (user_email, mode, params, active_slot, updated_at)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(user_email, mode) DO UPDATE SET
                                params = excluded.params,
                                active_slot = COALESCE(excluded.active_slot, user_mode_params.active_slot),
                                updated_at = excluded.updated_at
                            """,
                            (key, mode, json.dumps(params), active_slot, _utc_now()),
                        )

            if isinstance(mode_params, dict):
                for mode in MODES:
                    mp = mode_params.get(mode)
                    if not isinstance(mp, dict):
                        continue
                    params = normalize_params(mp)
                    conn.execute(
                        """
                        INSERT INTO user_mode_params (user_email, mode, params, active_slot, updated_at)
                        VALUES (?, ?, ?, COALESCE(
                            (SELECT active_slot FROM user_mode_params WHERE user_email = ? AND mode = ?),
                            NULL
                        ), ?)
                        ON CONFLICT(user_email, mode) DO UPDATE SET
                            params = excluded.params,
                            updated_at = excluded.updated_at
                        """,
                        (key, mode, json.dumps(params), key, mode, _utc_now()),
                    )
            conn.commit()
        finally:
            conn.close()

    return get_user_state(key)


init_db()
