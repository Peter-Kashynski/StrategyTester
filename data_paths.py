"""Persistent data directory (users, presets DB, Flask secret).

Set STONKBOT_DATA_DIR on hosts like PythonAnywhere so accounts survive
outside the git checkout (e.g. /home/you/StonkBot3/data).
"""

from __future__ import annotations

import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent


def project_data_dir() -> Path:
    raw = (os.getenv("STONKBOT_DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _PROJECT_ROOT / "data"
