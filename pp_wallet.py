"""PumpPortal wallet / API key helpers."""

from __future__ import annotations

from typing import Any

import aiohttp

CREATE_WALLET_URL = "https://pumpportal.fun/api/create-wallet"


async def create_pp_wallet() -> dict[str, Any]:
    """
    Create a new PumpPortal Lightning wallet + API key.
    Returns {api_key, wallet_pubkey, private_key} or raises.
    """
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
        async with session.get(CREATE_WALLET_URL, ssl=False) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"create-wallet HTTP {resp.status}: {text[:300]}")
            try:
                data = await resp.json(content_type=None)
            except Exception:
                raise RuntimeError(f"create-wallet returned non-JSON: {text[:300]}")

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
