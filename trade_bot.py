"""Live REAL trading bot — same strategy as paper_bot, on-chain fills via PumpPortal."""

from __future__ import annotations

import asyncio
import json
import ssl
import threading
import time
from collections import deque
from dataclasses import asdict
from typing import Any

import websockets
from urllib.parse import quote

from solders.keypair import Keypair
from solders.pubkey import Pubkey

from config import PP_MIN_SOL, get_sol_price, get_wallet_sol
from paper_bot import (
    DEFAULT_SOL_PRICE,
    PP_API_KEY_MIN_LEN,
    PP_API_KEY_PROBE_MINT,
    StrategyParams,
    TradeRecord,
    check_top_ten,
    get_volume,
    holder_top_pct,
    holder_top3_pct,
    resolve_entry_size,
    update_top_ten,
)
from trade_exec import SELL_RETRIES, execute, normalize_helius_rpc

BUY_SOL_MAX = 0.05
BUY_SOL_DEFAULT = 0.001


class TradeBot:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self.params = StrategyParams()
        self.running = False
        self.realized_pnl_sol = 0.0
        self.coins_sold = 0
        self.open_positions: dict[str, dict[str, Any]] = {}
        self.trades: list[TradeRecord] = []
        self.events: list[dict[str, str]] = []
        self._event_seq = 0
        self._feed_ws = None
        self._watched_tokens: set | None = None
        self._pp_api_key = ""
        self._wallet_pubkey = ""
        self._wallet_privkey = ""
        self._helius_api_key = ""
        self._buy_sol = BUY_SOL_DEFAULT
        self.trade_msg_count = 0
        self._force_selling = False
        self.buy_count = 0
        self._session_start_equity = 0.0
        self._buy_limit_hit = False
        self._wallet_stop_hit = False
        self._new_token_feed_paused = False
        self._cached_wallet_sol: float | None = None
        self._wallet_sol_updated_at = 0.0

    def _open_position_mints(self) -> set[str]:
        with self._lock:
            return set(self.open_positions.keys())

    async def _pause_new_token_feed(self, websocket) -> None:
        if self._new_token_feed_paused:
            return
        try:
            await websocket.send(json.dumps({"method": "unsubscribeNewToken"}))
            await websocket.send(json.dumps({"method": "unsubscribeMigration"}))
        except Exception:
            pass
        self._new_token_feed_paused = True

    async def _resume_open_position_streams(
        self,
        websocket,
        watched_tokens: set,
    ) -> None:
        for mint in self._open_position_mints():
            await self._watch_token(websocket, watched_tokens, mint)

    async def _refresh_wallet_sol_cache(self, *, force: bool = False) -> None:
        if not self._wallet_pubkey:
            return
        now = time.time()
        if not force and now - self._wallet_sol_updated_at < 15:
            return
        try:
            sol = await get_wallet_sol(self._wallet_pubkey)
        except Exception:
            return
        if sol is not None:
            self._cached_wallet_sol = float(sol)
            self._wallet_sol_updated_at = now

    async def _current_session_equity(self) -> float:
        try:
            wallet_sol = await get_wallet_sol(self._wallet_pubkey)
        except Exception:
            wallet_sol = None
        if wallet_sol is not None:
            return float(wallet_sol)
        with self._lock:
            realized = self.realized_pnl_sol
            positions = list(self.open_positions.values())
        unrealized = 0.0
        for pos in positions:
            entry = float(pos.get("entry_mc") or 0)
            current = float(pos.get("current_mc") or 0)
            sol = float(pos.get("sol_spent") or pos.get("buy_sol") or 0)
            if entry > 0 and sol > 0:
                unrealized += sol * ((current - entry) / entry)
        return self._session_start_equity + realized + unrealized

    def _wallet_stop_triggered(self, current_equity: float) -> bool:
        params = self.params
        if not params.use_wallet_stop:
            return False
        start = self._session_start_equity
        if start <= 0:
            return False
        drop = start - current_equity
        if drop <= 0:
            return False
        if params.wallet_stop_is_pct:
            return (drop / start) * 100 >= float(params.wallet_stop_value)
        return drop >= float(params.wallet_stop_value)

    def _entries_blocked(self) -> bool:
        return self._buy_limit_hit or self._wallet_stop_hit

    async def _pause_entry_subscriptions(self) -> None:
        ws = getattr(self, "_feed_ws", None)
        watched = getattr(self, "_watched_tokens", None)
        if ws is None:
            return
        await self._pause_new_token_feed(ws)
        if watched is None:
            return
        keep = self._open_position_mints()
        to_drop = [m for m in list(watched) if m not in keep]
        if to_drop:
            await self._unwatch_tokens(
                ws, watched, to_drop, reason="no new entries"
            )

    async def _ensure_buy_limit_paused(self) -> None:
        params = self.params
        if not params.use_max_buys:
            return
        with self._lock:
            if self.buy_count < int(params.max_buys):
                return
        if not self._buy_limit_hit:
            self._buy_limit_hit = True
            self._log(
                f"Buy limit reached ({int(params.max_buys)} coins) — no new entries",
                "status",
            )
        await self._pause_entry_subscriptions()
        await self._maybe_auto_stop_after_buy_limit()

    async def _ensure_wallet_stop_paused(self) -> None:
        if self._wallet_stop_hit:
            await self._pause_entry_subscriptions()
            return
        params = self.params
        if not params.use_wallet_stop:
            return
        equity = await self._current_session_equity()
        if not self._wallet_stop_triggered(equity):
            return
        self._wallet_stop_hit = True
        drop = self._session_start_equity - equity
        if params.wallet_stop_is_pct:
            pct = (drop / self._session_start_equity) * 100 if self._session_start_equity else 0
            self._log(
                f"Wallet stop loss hit ({pct:.2f}% SOL down) — no new entries",
                "status",
            )
        else:
            self._log(
                f"Wallet stop loss hit ({drop:.6f} SOL down) — no new entries",
                "status",
            )
        await self._pause_entry_subscriptions()

    async def _can_buy_new(self) -> bool:
        if self._entries_blocked():
            return False
        await self._ensure_buy_limit_paused()
        if self._buy_limit_hit:
            return False
        await self._ensure_wallet_stop_paused()
        if self._wallet_stop_hit:
            return False
        return True

    async def _maybe_auto_stop_after_buy_limit(self) -> None:
        if not self.params.use_max_buys or not self._buy_limit_hit:
            return
        with self._lock:
            if self.open_positions or not self.running:
                return
        self._log(
            "Buy limit reached and all positions closed — stopping bot",
            "status",
        )
        self._stop.set()

    def _log(self, message: str, kind: str = "info") -> None:
        with self._lock:
            self._event_seq += 1
            self.events.append({
                "id": str(self._event_seq),
                "time": time.strftime("%H:%M:%S"),
                "kind": kind,
                "message": message,
            })
            if len(self.events) > 300:
                self.events = self.events[-300:]

    def clear_events(self) -> None:
        with self._lock:
            self.events = []

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            positions = [dict(p) for p in self.open_positions.values()]
            unrealized = 0.0
            for pos in positions:
                entry = float(pos.get("entry_mc") or 0)
                current = float(pos.get("current_mc") or 0)
                # Prefer actual SOL spent (fees/slippage); fall back to requested size
                sol = float(pos.get("sol_spent") or pos.get("buy_sol") or 0)
                if entry > 0 and sol > 0:
                    unrealized += sol * ((current - entry) / entry)
            realized = self.realized_pnl_sol
            return {
                "running": self.running,
                "mode": "trade",
                # balance kept for SSE/UI back-compat — Trade shows PnL, not paper cash
                "balance": round(realized + unrealized, 6),
                "pnl_sol": round(realized, 6),
                "unrealized_pnl_sol": round(unrealized, 6),
                "coins_sold": self.coins_sold,
                "params": asdict(self.params),
                "open_positions": positions,
                "open_position": positions[0] if positions else None,
                "trades": [asdict(t) for t in self.trades[-50:]],
                "events": list(self.events[-250:]),
                "trade_msg_count": self.trade_msg_count,
                "buy_sol": self._buy_sol,
                "buy_count": self.buy_count,
                "buy_limit_hit": self._buy_limit_hit,
                "wallet_stop_hit": self._wallet_stop_hit,
                "wallet_sol": self._cached_wallet_sol,
            }

    def start(
        self,
        params: StrategyParams,
        *,
        pp_api_key: str = "",
        wallet_pubkey: str = "",
        wallet_privkey: str = "",
        helius_api_key: str = "",
        buy_sol: float = BUY_SOL_DEFAULT,
    ) -> tuple[bool, str]:
        api_key = (pp_api_key or "").strip()
        pubkey = (wallet_pubkey or "").strip()
        privkey = (wallet_privkey or "").strip()
        helius = normalize_helius_rpc(helius_api_key or "")
        try:
            buy = float(buy_sol)
        except (TypeError, ValueError):
            return False, "Buy size (SOL) must be a number"

        if not api_key:
            return False, "PumpPortal API key is required for Trade"
        if len(api_key) < PP_API_KEY_MIN_LEN:
            return False, (
                f"PumpPortal API key looks invalid (got {len(api_key)} chars, "
                f"expected ≥ {PP_API_KEY_MIN_LEN})"
            )
        if not pubkey:
            return False, "Wallet public key is required for Trade"
        try:
            Pubkey.from_string(pubkey)
        except Exception:
            return False, f"Wallet public key looks invalid (got {len(pubkey)} chars)"
        if not privkey:
            return False, "Wallet private key is required for Trade"
        try:
            kp = Keypair.from_base58_string(privkey)
        except Exception:
            return False, "Wallet private key looks invalid (base58 keypair)"
        derived = str(kp.pubkey())
        if derived != pubkey:
            return False, (
                "Wallet public key does not match private key "
                f"(pubkey is {pubkey[:8]}…, keypair is {derived[:8]}…)"
            )
        if not helius:
            return False, "Helius API key is required for Trade (send + confirm txs)"
        if buy <= 0:
            return False, "Buy size (SOL) must be > 0"
        if buy > BUY_SOL_MAX:
            return False, f"Buy size (SOL) must be ≤ {BUY_SOL_MAX}"

        # Hard-fail empty/unfunded signing wallets so we never silently use another key
        try:
            wallet_sol = asyncio.run(get_wallet_sol(pubkey))
        except Exception:
            wallet_sol = None
        if wallet_sol is None:
            return False, (
                "Could not fetch signing wallet SOL balance — check pubkey / RPC"
            )
        if wallet_sol < buy:
            return False, (
                f"Signing wallet {pubkey[:8]}…{pubkey[-4:]} has {wallet_sol:.4f} SOL "
                f"(need ≥ {buy:g} SOL for buy size). Fund this wallet — "
                "there is no fallback to .env keys."
            )

        with self._lock:
            if self.running:
                return False, "Already running"
            self.params = params
            self._pp_api_key = api_key
            self._wallet_pubkey = pubkey
            self._wallet_privkey = privkey
            self._helius_api_key = helius
            self._buy_sol = buy
            self.realized_pnl_sol = 0.0
            self.coins_sold = 0
            self.trade_msg_count = 0
            self.buy_count = 0
            self._session_start_equity = float(wallet_sol)
            self._buy_limit_hit = False
            self._wallet_stop_hit = False
            self._new_token_feed_paused = False
            self._cached_wallet_sol = float(wallet_sol)
            self._wallet_sol_updated_at = time.time()
            self.open_positions = {}
            self.trades = []
            self.events = []
            self._event_seq = 0
            self._force_selling = False
            self._stop.clear()
            self.running = True

        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        self._log(
            f"Trade bot started — REAL buys/sells @ {buy:g} SOL each",
            "status",
        )
        return True, "Started"

    def stop(self) -> tuple[bool, str]:
        with self._lock:
            if not self.running:
                return False, "Not running"
        self._stop.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(lambda: None)
        if self._thread:
            self._thread.join(timeout=60)
        with self._lock:
            self.running = False
            self._wallet_privkey = ""
            self._helius_api_key = ""
            self.open_positions = {}
        if self._wallet_pubkey:
            try:
                sol = asyncio.run(get_wallet_sol(self._wallet_pubkey))
                if sol is not None:
                    self._cached_wallet_sol = float(sol)
                    self._wallet_sol_updated_at = time.time()
            except Exception:
                pass
        self._log("Trade bot stopped", "status")
        return True, "Stopped"

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as e:
            self._log(f"Bot crashed: {e}", "error")
        finally:
            with self._lock:
                self.running = False
                self._wallet_privkey = ""
                self._helius_api_key = ""
            self._loop.close()
            self._loop = None

    def _should_stop(self) -> bool:
        return self._stop.is_set()

    def _should_watch_create(self, usd_mc: int, params: StrategyParams) -> bool:
        if not params.use_mc_range:
            return True
        if not usd_mc:
            return True
        return usd_mc <= params.mc_max

    async def _watch_token(self, websocket, watched_tokens: set, mint: str) -> None:
        if self._entries_blocked() and mint not in self._open_position_mints():
            return
        if mint in watched_tokens:
            return
        watched_tokens.add(mint)
        await websocket.send(json.dumps({
            "method": "subscribeTokenTrade",
            "keys": [mint],
        }))
        self._log(f"Subscribed — https://pump.fun/coin/{mint}", "sub")

    async def _unwatch_tokens(
        self,
        websocket,
        watched_tokens: set,
        mints: list[str] | set[str],
        *,
        reason: str | None = None,
    ) -> None:
        keys = [m for m in mints if m in watched_tokens]
        if not keys:
            return
        for i in range(0, len(keys), 50):
            chunk = keys[i:i + 50]
            try:
                await websocket.send(json.dumps({
                    "method": "unsubscribeTokenTrade",
                    "keys": chunk,
                }))
            except Exception:
                pass
            for m in chunk:
                watched_tokens.discard(m)
        if reason:
            if len(keys) == 1:
                self._log(
                    f"Unsubscribed 1 token(s) — {reason} — https://pump.fun/coin/{keys[0]}",
                    "unsub",
                )
            else:
                links = ", ".join(f"https://pump.fun/coin/{m}" for m in keys[:5])
                extra = f" (+{len(keys) - 5} more)" if len(keys) > 5 else ""
                self._log(
                    f"Unsubscribed {len(keys)} token(s) — {reason} — {links}{extra}",
                    "unsub",
                )

    async def _verify_pp_api_key(self, websocket) -> tuple[bool, str]:
        await websocket.send(json.dumps({
            "method": "subscribeTokenTrade",
            "keys": [PP_API_KEY_PROBE_MINT],
        }))
        deadline = time.time() + 8.0
        while time.time() < deadline:
            if self._should_stop():
                return False, "Stopped during API key check"
            remaining = deadline - time.time()
            try:
                raw = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=max(0.1, min(2.0, remaining)),
                )
            except asyncio.TimeoutError:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            err = str(data.get("errors") or data.get("error") or "")
            msg = str(data.get("message") or "")
            text = f"{err} {msg}".lower()

            if "invalid api key" in text:
                return False, "Invalid PumpPortal API key"
            if "successfully subscribed to keys" in text:
                try:
                    await websocket.send(json.dumps({
                        "method": "unsubscribeTokenTrade",
                        "keys": [PP_API_KEY_PROBE_MINT],
                    }))
                except Exception:
                    pass
                return True, "ok"
            if "only available when connecting with an api key" in text:
                return False, (
                    f"API key rejected or its PumpPortal wallet is underfunded "
                    f"(need ≥ {PP_MIN_SOL:.2f} SOL for trade streams)"
                )

        return False, "Timed out verifying PumpPortal API key"

    async def _real_buy(self, mint: str, sol_amount: float) -> tuple[bool, Any]:
        return await execute(
            "buy",
            mint,
            sol_amount,
            public_key=self._wallet_pubkey,
            private_key=self._wallet_privkey,
            helius_api_key=self._helius_api_key,
        )

    async def _real_sell(self, mint: str, percent: int = 100) -> tuple[bool, Any]:
        pct = max(1, min(100, int(percent)))
        return await execute(
            "sell",
            mint,
            pct,
            public_key=self._wallet_pubkey,
            private_key=self._wallet_privkey,
            helius_api_key=self._helius_api_key,
            retries=SELL_RETRIES,
        )

    def manual_sell(self, mint: str, pct: float) -> tuple[bool, str]:
        if pct not in (25, 50, 75, 100):
            return False, "Sell amount must be 25, 50, 75, or 100%"
        with self._lock:
            if not self.running:
                return False, "Bot not running"
            pos = self.open_positions.get(mint)
            if not pos:
                return False, "No open position"
            exit_mc = float(pos.get("current_mc") or pos.get("entry_mc") or 0)
        loop = self._loop
        if loop is None or not loop.is_running():
            return False, "Bot not ready"
        future = asyncio.run_coroutine_threadsafe(
            self._sell_fraction(mint, exit_mc, pct / 100.0, f"Manual sell ({int(pct)}%)"),
            loop,
        )
        try:
            return future.result(timeout=90)
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _unpack_exec(detail: Any) -> tuple[str, float | None]:
        """Normalize execute() success payload → (signature, sol_delta)."""
        if isinstance(detail, dict):
            sig = str(detail.get("signature") or "")
            raw = detail.get("sol_delta")
            try:
                delta = float(raw) if raw is not None else None
            except (TypeError, ValueError):
                delta = None
            return sig, delta
        return str(detail or ""), None

    async def _force_sell_open(self) -> None:
        self._force_selling = True
        with self._lock:
            mints = list(self.open_positions.keys())
        for mint in mints:
            with self._lock:
                pos = self.open_positions.get(mint)
                exit_mc = pos["current_mc"] if pos else 0
            await self._close_position(mint, exit_mc, "Trade stopped")
        self._force_selling = False

    async def _run(self) -> None:
        api_key = self._pp_api_key
        if not api_key:
            self._log("Missing PumpPortal API key", "error")
            return

        self._log(f"Using API key {api_key[:4]}…", "status")
        self._log(
            f"Signing wallet {self._wallet_pubkey[:8]}…{self._wallet_pubkey[-4:]} "
            f"| buy size {self._buy_sol:g} SOL",
            "status",
        )
        self._log(
            f"Make sure the PumpPortal wallet linked to this API key has ≥ {PP_MIN_SOL:.2f} SOL "
            f"(0.01 SOL per 10,000 trade messages)",
            "status",
        )

        uri = f"wss://pumpportal.fun/api/data?api-key={quote(api_key, safe='')}"
        sol_price = await get_sol_price() or DEFAULT_SOL_PRICE

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        volume_history: dict = {}
        creation_time: dict = {}
        token_distribution: dict = {}
        top_ten_holders: dict = {}
        ath_tracker: dict = {}
        watched_tokens: set = set()
        bought_coins: set = set()
        banned_coins: set = set()
        api_key_ok = False

        while not self._should_stop():
            try:
                async with websockets.connect(uri, ssl=ssl_ctx) as websocket:
                    watched_tokens.clear()
                    self._feed_ws = websocket
                    self._watched_tokens = watched_tokens

                    if not api_key_ok:
                        ok, verify_msg = await self._verify_pp_api_key(websocket)
                        if not ok:
                            self._log(verify_msg, "error")
                            return
                        api_key_ok = True
                        self._log("PumpPortal API key accepted for trade streams", "status")
                        self._log(f"Connected feed — SOL ${sol_price}", "status")

                    self._new_token_feed_paused = False
                    if not self._entries_blocked():
                        await websocket.send(json.dumps({"method": "subscribeNewToken"}))
                        await websocket.send(json.dumps({"method": "subscribeMigration"}))
                        self._log("Subscribed to new tokens", "status")
                    else:
                        self._new_token_feed_paused = True
                        self._log("New token feed paused — no new entries", "status")
                        await self._resume_open_position_streams(
                            websocket, watched_tokens
                        )

                    while not self._should_stop():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            await self._refresh_wallet_sol_cache()
                            await self._check_stagnation()
                            await self._maybe_auto_stop_after_buy_limit()
                            continue

                        await self._refresh_wallet_sol_cache()
                        await self._check_stagnation()
                        await self._maybe_auto_stop_after_buy_limit()
                        if self._should_stop():
                            break

                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        err = str(data.get("errors") or data.get("error") or "")
                        if err and "invalid api key" in err.lower():
                            self._log("Invalid PumpPortal API key", "error")
                            return

                        mint = data.get("mint", "")
                        if not mint:
                            continue

                        if mint[-4:] != "pump":
                            banned_coins.add(mint)

                        tx_type = data.get("txType")
                        if not tx_type:
                            continue

                        if tx_type in ("buy", "sell"):
                            self.trade_msg_count += 1
                            if self.trade_msg_count % 1000 == 0:
                                self._log(
                                    f"Received {self.trade_msg_count:,} buy/sell messages",
                                    "status",
                                )

                        sol_mc = data.get("marketCapSol", 0) or 0
                        usd_mc = int(sol_mc * sol_price)
                        params = self.params
                        ticker = mint[:3].upper()
                        link = f"https://pump.fun/coin/{mint}"

                        if tx_type == "create":
                            if mint in bought_coins or mint in banned_coins:
                                continue
                            now = time.time()
                            if mint not in creation_time:
                                creation_time[mint] = now
                            if usd_mc and mint not in ath_tracker:
                                ath_tracker[mint] = usd_mc
                            if not self._entries_blocked() and self._should_watch_create(usd_mc, params):
                                await self._watch_token(websocket, watched_tokens, mint)
                            continue

                        if not usd_mc:
                            continue

                        with self._lock:
                            holding = mint in self.open_positions
                        if holding and tx_type in ("buy", "sell"):
                            await self._update_position(mint, usd_mc, reason_hint=None)
                            continue

                        if mint in bought_coins or mint in banned_coins:
                            continue
                        if tx_type not in ("buy", "sell"):
                            continue
                        if mint not in creation_time:
                            continue

                        if params.use_mc_range and usd_mc > params.mc_max:
                            banned_coins.add(mint)
                            await self._unwatch_tokens(
                                websocket, watched_tokens, [mint], reason="above MC max"
                            )
                            continue

                        if mint not in ath_tracker:
                            ath_tracker[mint] = usd_mc
                        if usd_mc > ath_tracker[mint]:
                            ath_tracker[mint] = usd_mc
                        if params.use_ath_ban and usd_mc < (ath_tracker[mint] * params.ath_ban_ratio):
                            banned_coins.add(mint)
                            await self._unwatch_tokens(
                                websocket, watched_tokens, [mint], reason="ATH unsubscribe"
                            )
                            continue

                        now = time.time()
                        sol_amount = data.get("solAmount")
                        wallet = data.get("traderPublicKey")
                        tokens = data.get("tokenAmount")
                        if not sol_amount or not wallet or not tokens:
                            continue
                        usd_value = sol_amount * sol_price

                        age = now - creation_time[mint]
                        if params.use_min_age and age < params.min_age_sec:
                            continue

                        if mint not in top_ten_holders:
                            top_ten_holders[mint] = []
                        if mint not in token_distribution:
                            token_distribution[mint] = {}
                        if wallet not in token_distribution[mint]:
                            token_distribution[mint][wallet] = 0.0

                        if tx_type == "buy":
                            token_distribution[mint][wallet] += tokens
                        else:
                            token_distribution[mint][wallet] -= tokens

                        update_top_ten(
                            mint, wallet, token_distribution[mint][wallet], top_ten_holders
                        )

                        if mint not in volume_history:
                            volume_history[mint] = {"buy": deque(), "sell": deque()}
                        volume_history[mint][tx_type].append((now, usd_value))
                        cutoff = now - 60
                        for trade_type in ("buy", "sell"):
                            deck = volume_history[mint][trade_type]
                            while deck and deck[0][0] < cutoff:
                                deck.popleft()

                        if params.use_mc_range and usd_mc < params.mc_min:
                            continue

                        buy_volume = get_volume(volume_history, mint, 60, "buy")
                        if params.use_min_buy_volume and buy_volume < params.min_buy_volume:
                            if params.ban_on_low_volume:
                                self._log(
                                    f"Not enough volume ({buy_volume:.0f}) — {link}", "skip"
                                )
                                banned_coins.add(mint)
                                await self._unwatch_tokens(
                                    websocket, watched_tokens, [mint], reason="volume unsubscribe"
                                )
                            continue

                        if params.use_holder_checks:
                            message, res = check_top_ten(top_ten_holders, mint, params)
                            if message == "Less than ten holders":
                                continue
                            if message == "Top Holder owns too much":
                                self._log(f"Top holder too much {res} — {link}", "skip")
                                if params.enforce_top_holder_max:
                                    banned_coins.add(mint)
                                    await self._unwatch_tokens(
                                        websocket, watched_tokens, [mint], reason="holder unsubscribe"
                                    )
                                    continue
                            elif message == "Top Holder owns too little":
                                self._log(f"Top holder too little {res} — {link}", "skip")
                                if params.enforce_top_holder_min:
                                    banned_coins.add(mint)
                                    await self._unwatch_tokens(
                                        websocket, watched_tokens, [mint], reason="holder unsubscribe"
                                    )
                                    continue
                            elif message == "Top 3 holders own too much":
                                self._log(f"Top 3 too much {res} — {link}", "skip")
                                if params.enforce_top3_max:
                                    banned_coins.add(mint)
                                    await self._unwatch_tokens(
                                        websocket, watched_tokens, [mint], reason="holder unsubscribe"
                                    )
                                    continue
                            elif message == "Top 3 holders own too little":
                                self._log(f"Top 3 too little {res} — {link}", "skip")
                                if params.enforce_top3_min:
                                    banned_coins.add(mint)
                                    await self._unwatch_tokens(
                                        websocket, watched_tokens, [mint], reason="holder unsubscribe"
                                    )
                                    continue

                        if not await self._can_buy_new():
                            continue

                        top_pct = holder_top_pct(top_ten_holders, mint)
                        top3_pct = holder_top3_pct(top_ten_holders, mint)
                        units, boost_reasons, boost_mult = resolve_entry_size(
                            params,
                            buy_volume=buy_volume,
                            top_holder_pct=top_pct,
                            top3_pct=top3_pct,
                        )
                        boosted = bool(boost_reasons)
                        # Boost multiplies on-chain SOL (same multipliers as paper entry units)
                        sol_amount = round(self._buy_sol * boost_mult, 9)

                        bought_coins.add(mint)
                        banned_coins.add(mint)
                        entry = usd_mc
                        boost_note = (
                            f" (×{boost_mult:g}: {', '.join(boost_reasons)})"
                            if boosted else ""
                        )
                        self._log(
                            f"Buying {ticker} @ MC {entry} | {sol_amount:g} SOL"
                            f"{boost_note} | vol {buy_volume:.0f} — {link}",
                            "buy",
                        )
                        ok, detail = await self._real_buy(mint, sol_amount)
                        if not ok:
                            self._log(f"BUY FAILED {ticker}: {detail}", "error")
                            continue

                        tx_sig, sol_delta = self._unpack_exec(detail)
                        # Buy: wallet SOL falls (negative delta). Cost includes fees/slippage.
                        if sol_delta is not None and sol_delta < 0:
                            sol_spent = -sol_delta
                            spent_note = f"spent {sol_spent:.6f} SOL (wallet)"
                        else:
                            sol_spent = sol_amount
                            spent_note = (
                                f"spent ~{sol_spent:g} SOL (requested; wallet delta unavailable)"
                            )
                        with self._lock:
                            self.open_positions[mint] = {
                                "mint": mint,
                                "ticker": ticker,
                                "link": link,
                                "entry_mc": entry,
                                "current_mc": entry,
                                "ath": entry,
                                "ath_timer": time.time(),
                                "last_msg": time.time(),
                                "entry_units": units,
                                "size_boosted": boosted,
                                "boost_reasons": boost_reasons,
                                "buy_sol": sol_amount,
                                "sol_spent": sol_spent,
                                "buy_sol_delta": sol_delta,
                                "buy_sol_base": self._buy_sol,
                                "boost_mult": boost_mult,
                                "buy_tx": tx_sig,
                            }
                            self.buy_count += 1
                        await self._ensure_buy_limit_paused()
                        self._log(
                            f"BUY OK {ticker} @ MC {entry} | {spent_note}"
                            f"{boost_note} — {link}",
                            "buy",
                        )
                        self._log(
                            f"Solscan buy {ticker}: https://solscan.io/tx/{tx_sig}",
                            "tx",
                        )

            except websockets.exceptions.ConnectionClosedError:
                self._feed_ws = None
                self._log("Connection dropped — reconnecting…", "status")
                await asyncio.sleep(1)
            except Exception as e:
                self._feed_ws = None
                self._log(f"Unexpected error: {e}", "error")
                await asyncio.sleep(1)

        await self._force_sell_open()
        self._feed_ws = None
        self._watched_tokens = None
        self._log("Feed loop exited", "status")

    async def _check_stagnation(self) -> None:
        with self._lock:
            positions = list(self.open_positions.values())
            params = self.params
        if not positions:
            return

        now = time.time()
        for pos in positions:
            mint = pos["mint"]
            if params.use_stagnation and now - pos["last_msg"] > params.stagnation_sec:
                await self._close_position(mint, pos["current_mc"], "Stagnation timeout")
                continue

            if pos["ath_timer"]:
                current = pos["current_mc"]
                elapsed = now - pos["ath_timer"]
                in_high = (
                    params.use_high_mc_threshold
                    and current > params.high_mc_threshold
                )
                if in_high:
                    if (
                        params.use_ath_stagnation_high
                        and elapsed > params.ath_stagnation_high_sec
                    ):
                        await self._close_position(mint, current, "High MC ATH stagnation")
                elif params.use_ath_stagnation and elapsed > params.ath_stagnation_sec:
                    await self._close_position(mint, current, "ATH stagnation")

    async def _update_position(self, mint: str, usd_mc: float, reason_hint: str | None) -> None:
        with self._lock:
            pos = self.open_positions.get(mint)
            params = self.params
            if not pos:
                return
            pos["current_mc"] = usd_mc
            pos["last_msg"] = time.time()
            if usd_mc > pos["ath"]:
                pos["ath"] = usd_mc
                pos["ath_timer"] = time.time()
            entry = pos["entry_mc"]
            ath = pos["ath"]
            high = params.high_mc_threshold
            use_high = params.use_high_mc_threshold

        if usd_mc > 1_000:
            entry_hit = params.use_stoploss_entry and usd_mc < entry * params.stoploss_entry_ratio
            ath_low_hit = (
                params.use_stoploss_ath_low
                and (not use_high or usd_mc < high)
                and usd_mc < ath * params.stoploss_ath_low
            )
            ath_high_hit = (
                use_high
                and params.use_stoploss_ath_high
                and usd_mc >= high
                and usd_mc < ath * params.stoploss_ath_high
            )
            if entry_hit or ath_low_hit:
                await self._close_position(mint, usd_mc, "Stoploss")
                return
            if ath_high_hit:
                await self._close_position(mint, usd_mc, "Stoploss (high MC)")
                return

        if reason_hint:
            await self._close_position(mint, usd_mc, reason_hint)

    async def _close_position(self, mint: str, exit_mc: float, reason: str) -> None:
        await self._sell_fraction(mint, exit_mc, 1.0, reason)

    async def _sell_fraction(
        self,
        mint: str,
        exit_mc: float,
        fraction: float,
        reason: str,
    ) -> tuple[bool, str]:
        fraction = max(0.0, min(1.0, float(fraction)))
        if fraction <= 0:
            return False, "Invalid sell amount"

        with self._lock:
            pos = self.open_positions.get(mint)
            if not pos:
                return False, "Position not found"
            snapshot = dict(pos)

        ticker = snapshot["ticker"]
        link = snapshot["link"]
        entry = float(snapshot["entry_mc"])
        buy_sol = float(snapshot.get("buy_sol") or 0)
        sol_spent = float(snapshot.get("sol_spent") or buy_sol or 0)
        sell_pct = max(1, min(100, int(round(fraction * 100))))
        self._log(
            f"Selling {sell_pct}% {ticker} @ MC {exit_mc} | {reason} — {link}",
            "sell",
        )
        ok, detail = await self._real_sell(mint, sell_pct)
        if not ok:
            self._log(f"SELL FAILED {ticker}: {detail}", "error")
            if not self._force_selling:
                return False, str(detail)
            with self._lock:
                self.open_positions.pop(mint, None)
            return False, str(detail)

        tx_sig, sol_delta = self._unpack_exec(detail)
        gain = ((exit_mc - entry) / entry) * 100 if entry else 0
        cost_basis = sol_spent * fraction
        if sol_delta is not None:
            sol_received = sol_delta
            trade_pnl = sol_received - cost_basis
            pnl_note = (
                f"recv {sol_received:+.6f} SOL | actual {trade_pnl:+.6f} SOL"
            )
        else:
            trade_pnl = cost_basis * (gain / 100) if cost_basis else 0.0
            pnl_note = f"est {trade_pnl:+.6f} SOL (MC; wallet delta unavailable)"

        remaining_frac = 1.0 - fraction
        full_close = remaining_frac <= 1e-9 or fraction >= 1.0 - 1e-9

        with self._lock:
            pos = self.open_positions.get(mint)
            if not pos:
                return False, "Position closed elsewhere"
            self.realized_pnl_sol += trade_pnl
            self.coins_sold += 1
            record = TradeRecord(
                ticker=ticker,
                mint=mint,
                link=link,
                entry_mc=entry,
                exit_mc=exit_mc,
                gain_pct=round(gain, 2),
                reason=reason,
                time=time.strftime("%H:%M:%S"),
                buy_tx=str(snapshot.get("buy_tx") or "") or None,
                sell_tx=tx_sig or None,
            )
            self.trades.append(record)
            pnl = self.realized_pnl_sol
            if full_close:
                self.open_positions.pop(mint, None)
            else:
                for key in ("buy_sol", "sol_spent", "entry_units"):
                    if key in pos and pos[key] is not None:
                        pos[key] = float(pos[key]) * remaining_frac

        self._log(
            f"SELL OK {ticker} @ MC {exit_mc} | MC {gain:+.2f}% | sold {sell_pct}% | "
            f"{pnl_note} | session PnL {pnl:+.6f} SOL | {reason} — {link}",
            "sell",
        )
        if tx_sig:
            self._log(
                f"Solscan sell {ticker}: https://solscan.io/tx/{tx_sig}",
                "tx",
            )
        if full_close:
            ws = getattr(self, "_feed_ws", None)
            watched = getattr(self, "_watched_tokens", None)
            if ws is not None and watched is not None:
                await self._unwatch_tokens(ws, watched, [mint], reason="position closed")
        await self._maybe_auto_stop_after_buy_limit()
        return True, f"Sold {sell_pct}%"


trade_bot = TradeBot()
