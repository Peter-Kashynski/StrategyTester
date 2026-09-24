"""Shared helpers for the web app (app.py, paper_bot, trade_bot).

Does not load personal trading keys from .env at import time.
Standalone scripts that need your keys should use legacy_config instead.
"""

from __future__ import annotations

import aiohttp
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TokenAccountOpts
from solders.pubkey import Pubkey

# PumpPortal requires a funded API wallet for metered websocket streams
PP_MIN_SOL = 0.02


async def get_wallet_sol(pubkey: Pubkey | str) -> float | None:
    """Return SOL balance for a wallet, or None if the RPC call fails.

    Always uses the pubkey you pass — never falls back to .env keys.
    """
    try:
        key = Pubkey.from_string(pubkey.strip()) if isinstance(pubkey, str) else pubkey
        async with AsyncClient("https://api.mainnet-beta.solana.com") as rpc:
            resp = await rpc.get_balance(key)
            return resp.value / 1_000_000_000
    except Exception:
        return None


async def get_sol_price() -> float | None:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "solana", "vs_currencies": "usd"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return data.get("solana", {}).get("usd", 137)


async def get_token_balance(key, token):
    mint = Pubkey.from_string(token)
    client = Client("https://api.mainnet-beta.solana.com")
    token_account = client.get_token_accounts_by_owner(key, TokenAccountOpts(mint=mint))
    if token_account.value:
        token_account_pubkey = token_account.value[0].pubkey
        balance = client.get_token_account_balance(token_account_pubkey)
        return int(balance.value.ui_amount) if balance.value.ui_amount else 0
    return 0
