"""PumpPortal wallet / API key helpers."""

from __future__ import annotations

from typing import Any

import aiohttp

CREATE_WALLET_URL = "https://pumpportal.fun/api/create-wallet"


def _host_unreachable_hint(exc: BaseException) -> str:
    msg = str(exc).lower()
    if "connect call failed" in msg or "cannot connect to host" in msg:
        return (
            " Outbound HTTPS may be blocked (on PythonAnywhere free tier, whitelist "
            "pumpportal.fun under Web → Allowlist, then Reload)."
        )
    return ""


async def create_pp_wallet() -> dict[str, Any]:
    """
    Create a new PumpPortal Lightning wallet + API key.
    Returns {api_key, wallet_pubkey, private_key} or raises.
    """
    timeout = aiohttp.ClientTimeout(total=45)
    try:
        async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
            async with session.get(CREATE_WALLET_URL) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"create-wallet HTTP {resp.status}: {text[:300]}")
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    raise RuntimeError(f"create-wallet returned non-JSON: {text[:300]}")
    except aiohttp.ClientError as e:
        raise RuntimeError(f"{e}{_host_unreachable_hint(e)}") from e
    except OSError as e:
        raise RuntimeError(f"{e}{_host_unreachable_hint(e)}") from e

    if not isinstance(data, dict):
        raise RuntimeError("create-wallet response was not an object")

    api_key = str(data.get("apiKey") or data.get("api_key") or "").strip()
    pubkey = str(
        data.get("walletPublicKey")
        or data.get("publicKey")
        or data.get("wallet_public_key")
        or ""
    ).strip()
    privkey = str(data.get("privateKey") or data.get("private_key") or "").strip()

    if not api_key or not pubkey or not privkey:
        raise RuntimeError(f"create-wallet missing fields: {list(data.keys())}")

    return {
        "api_key": api_key,
        "wallet_pubkey": pubkey,
        "private_key": privkey,
    }
