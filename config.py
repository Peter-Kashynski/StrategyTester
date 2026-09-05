

from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solders.system_program import transfer, TransferParams
from solders.transaction import VersionedTransaction 
from solders.commitment_config import CommitmentLevel
from solders.rpc.requests import SendVersionedTransaction 
from solders.rpc.config import RpcSendTransactionConfig 
from solders.message import MessageV0
from solders.pubkey import Pubkey 
from solana.rpc.api import Client 
from solana.rpc.types import TokenAccountOpts   

# HTTP + Websockets
import requests
import aiohttp
import websockets

# Standard library
import asyncio
import json
import time
import os 
from collections import deque 
from collections import defaultdict

# Environment variables
from dotenv import load_dotenv 



load_dotenv() 

axiom_pubkey = Pubkey.from_string(os.getenv("AXIOM_PUBLIC_KEY")) # type: ignore
axiom_privkey = Keypair.from_base58_string(os.getenv("AXIOM_PRIVATE_KEY")) # type: ignore

phantom_pubkey = Pubkey.from_string(os.getenv("PHANTOM_PUBLIC_KEY")) # type: ignore
phantom_privkey = Keypair.from_base58_string(os.getenv("PHANTOM_PRIVATE_KEY")) # type: ignore 

phantom_pubkey_test = Pubkey.from_string(os.getenv("PHANTOM_PUBLIC_KEY_TEST")) # type: ignore
phantom_privkey_test = Keypair.from_base58_string(os.getenv("PHANTOM_PRIVATE_KEY_TEST")) # type: ignore


pp_pubkey = Pubkey.from_string(os.getenv("PP_PUBLIC_KEY")) # type: ignore
pp_privkey = Keypair.from_base58_string(os.getenv("PP_PRIVATE_KEY")) # type: ignore 
pp_apikey = os.getenv("PP_API_KEY")   

account_pubkey = Pubkey.from_string(os.getenv("ACCOUNT_PUBLIC_KEY")) # type: ignore
account_privkey = Keypair.from_base58_string(os.getenv("ACCOUNT_PRIVATE_KEY")) # type: ignore
account_apikey = os.getenv("ACCOUNT_API_KEY")

helius_key = os.getenv("HELIUS_API_KEY") 

# PumpPortal requires a funded API wallet for metered websocket streams
PP_MIN_SOL = 0.02


async def get_wallet_sol(pubkey: Pubkey | str) -> float | None:
    """Return SOL balance for a wallet, or None if the RPC call fails.

    Always uses the pubkey you pass — never falls back to .env PP_PUBLIC_KEY.
    """
    try:
        key = Pubkey.from_string(pubkey.strip()) if isinstance(pubkey, str) else pubkey
        async with AsyncClient("https://api.mainnet-beta.solana.com") as rpc:
            resp = await rpc.get_balance(key)
            return resp.value / 1_000_000_000
    except Exception:
        return None


async def get_sol_price():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "solana", "vs_currencies": "usd"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return data.get("solana", {}).get("usd", 137)  # fallback if None


async def get_token_balance(key, token):
    mint = Pubkey.from_string(token)
    client = Client("https://api.mainnet-beta.solana.com")
    token_account = client.get_token_accounts_by_owner(key, TokenAccountOpts(mint=mint))
    # print(token_account.value) 
    # return
    if token_account.value: 
        token_account_pubkey = token_account.value[0].pubkey
        balance = client.get_token_account_balance(token_account_pubkey)
        # print(f"{int(balance.value.ui_amount)}") if balance.value.ui_amount else print(f"No tokens of token {token[:3]}")
        return int(balance.value.ui_amount) if balance.value.ui_amount else 0
    else: 
        # print(f"No tokens of token {token[:3]}") 
        return 0