"""Persistent data directory (users, presets DB, Flask secret).

Set STONKBOT_DATA_DIR on hosts like PythonAnywhere so accounts survive
outside the git checkout (e.g. /home/you/StonkBot3/data).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent


def _try_data_dir(path: Path) -> Path | None:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_test"
        probe.write_text("", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return path
    except OSError:
        return None


def project_data_dir() -> Path:
    default = _PROJECT_ROOT / "data"
    raw = (os.getenv("STONKBOT_DATA_DIR") or "").strip()
    if not raw:
        _try_data_dir(default)
        return default
    preferred = Path(raw).expanduser().resolve()
    resolved = _try_data_dir(preferred)
    if resolved is not None:
        return resolved
    fallback = _try_data_dir(default)
    if fallback is not None:
        print(
            f"WARNING: STONKBOT_DATA_DIR={preferred} is not writable; "
            f"using {fallback} instead. On Render, attach a disk at that mount "
            f"or remove STONKBOT_DATA_DIR until you do.",
            file=sys.stderr,
        )
        return fallback
    raise PermissionError(
        f"Cannot create a writable data directory at {preferred} or {default}"
    )
