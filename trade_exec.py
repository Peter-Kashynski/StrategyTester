"""Real PumpPortal Local Trading API buys/sells (adapted from realTest.execute)."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from solders.commitment_config import CommitmentLevel
from solders.keypair import Keypair
from solders.rpc.config import RpcSendTransactionConfig
from solders.rpc.requests import SendVersionedTransaction
from solders.transaction import VersionedTransaction

TRADE_LOCAL_URL = "https://pumpportal.fun/api/trade-local"
SELL_RETRIES = 5
SELL_RETRY_SLEEP_SEC = 1.0
TX_POLL_ATTEMPTS = 20
TX_POLL_SLEEP_SEC = 0.5


def normalize_helius_rpc(raw: str) -> str:
    """Accept a full Helius RPC URL or a bare API key."""
    s = (raw or "").strip().strip('"').strip("'")
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    s = s.replace("HELIUS_API_KEY=", "").strip()
    return f"https://mainnet.helius-rpc.com/?api-key={s}"


def helius_rpc_url(helius_api_key: str | None = None) -> str:
    """Resolve Helius RPC URL from the Trade UI key only — never .env."""
    url = normalize_helius_rpc(helius_api_key or "")
    if url:
        return url
    raise RuntimeError("Helius API key is required")


async def _rpc_call(rpc_url: str, method: str, params: list[Any]) -> Any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
        async with session.post(
            rpc_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            ssl=False,
        ) as resp:
            data = await resp.json(content_type=None)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data.get("result") if isinstance(data, dict) else None


def _account_key_str(key: Any) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, dict):
        return str(key.get("pubkey") or "")
    return str(key)


def _sol_delta_from_tx_result(result: dict[str, Any], public_key: str) -> float | None:
    """Wallet SOL change (post − pre) in SOL, including fees for that account."""
    try:
        meta = result["meta"]
        if meta.get("err"):
            return None
        message = result["transaction"]["message"]
        keys = message.get("accountKeys") or []
        idx = 0
        pub = public_key.strip()
        for i, key in enumerate(keys):
            if _account_key_str(key) == pub:
                idx = i
                break
        else:
            # Fee payer is almost always index 0 for our signed trades
            idx = 0
        pre = meta["preBalances"][idx]
        post = meta["postBalances"][idx]
        return (post - pre) / 1_000_000_000
    except Exception:
        return None


async def sol_delta_from_signature(
    signature: str,
    public_key: str,
    *,
    rpc_url: str | None = None,
    helius_api_key: str | None = None,
) -> float | None:
    """Poll getTransaction until confirmed; return wallet SOL delta (post − pre)."""
    url = rpc_url or helius_rpc_url(helius_api_key)
    sig = (signature or "").strip()
    pub = (public_key or "").strip()
    if not sig or not pub:
        return None

    params = [
        sig,
        {
            "encoding": "json",
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        },
    ]
    for _ in range(TX_POLL_ATTEMPTS):
        try:
            result = await _rpc_call(url, "getTransaction", params)
        except Exception:
            result = None
        if isinstance(result, dict):
            return _sol_delta_from_tx_result(result, pub)
        await asyncio.sleep(TX_POLL_SLEEP_SEC)
    return None


async def execute(
    action: str,
    mint: str,
    amount: float | int,
    *,
    public_key: str,
    private_key: str,
    helius_api_key: str = "",
    retries: int = 1,
) -> tuple[bool, Any]:
    """
    action: 'buy' or 'sell'
    amount: SOL float when buying; percent 0–100 when selling
    Returns (ok, detail). On success detail is
      {"signature": str, "sol_delta": float | None}
    where sol_delta is wallet SOL change (post − pre) for that tx.
    """
    pub = (public_key or "").strip()
    priv = (private_key or "").strip()
    if not pub or not priv:
        return False, "Missing wallet public or private key"

    try:
        keypair = Keypair.from_base58_string(priv)
    except Exception as e:
        return False, f"Invalid private key: {e}"

    try:
        rpc_url = helius_rpc_url(helius_api_key)
    except Exception as e:
        return False, str(e)

    attempts = max(1, retries if action == "sell" else 1)
    last_err: Any = None

    for attempt in range(attempts):
        ok, detail = await _execute_once(
            action, mint, amount, public_key=pub, keypair=keypair, rpc_url=rpc_url
        )
        if ok:
            sig = detail
            delta = await sol_delta_from_signature(sig, pub, rpc_url=rpc_url)
            return True, {"signature": sig, "sol_delta": delta}
        last_err = detail
        if attempt + 1 < attempts:
            await asyncio.sleep(SELL_RETRY_SLEEP_SEC)

    return False, last_err


async def _execute_once(
    action: str,
    mint: str,
    amount: float | int,
    *,
    public_key: str,
    keypair: Keypair,
    rpc_url: str,
) -> tuple[bool, Any]:
    if action == "buy":
        data = {
            "publicKey": public_key,
            "action": action,
            "mint": mint,
            "amount": amount,
            "denominatedInSol": "true",
            "slippage": 50,
            "priorityFee": 0.000001,
            "pool": "auto",
        }
    else:
        data = {
            "publicKey": public_key,
            "action": action,
            "mint": mint,
            "amount": f"{int(amount)}%",
            "denominatedInSol": "true",
            "slippage": 100,
            "priorityFee": 0.000001,
            "pool": "auto",
        }

    # ssl=False is the reliable aiohttp bypass for expired pumpportal.fun certs
    connector = aiohttp.TCPConnector(ssl=False)
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
            async with session.post(TRADE_LOCAL_URL, data=data, ssl=False) as resp:
                raw_bytes = await resp.read()
                if resp.status >= 400:
                    text = raw_bytes.decode("utf-8", errors="replace")[:400]
                    return False, f"trade-local HTTP {resp.status}: {text}"
    except Exception as e:
        return False, f"trade-local request failed: {e}"

    try:
        tx = VersionedTransaction(
            VersionedTransaction.from_bytes(raw_bytes).message,
            [keypair],
        )
    except Exception as e:
        preview = raw_bytes[:200].decode("utf-8", errors="replace")
        return False, f"Failed to build tx: {e} | body={preview}"

    commitment = CommitmentLevel.Confirmed
    config = RpcSendTransactionConfig(preflight_commitment=commitment)
    send_payload = SendVersionedTransaction(tx, config).to_json()

    connector = aiohttp.TCPConnector(ssl=False)
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
            async with session.post(
                rpc_url,
                data=send_payload,
                headers={"Content-Type": "application/json"},
                ssl=False,
            ) as resp:
                rpc_data = await resp.json(content_type=None)
    except Exception as e:
        return False, f"RPC send failed: {e}"

    if isinstance(rpc_data, dict) and "result" in rpc_data:
        return True, rpc_data["result"]
    return False, rpc_data
