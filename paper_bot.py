"""Live paper-trading bot for strategy testing. No real buys/sells."""

from __future__ import annotations

import asyncio
import json
import ssl
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any

import websockets

from urllib.parse import quote

from config import PP_MIN_SOL, get_sol_price


DEFAULT_SOL_PRICE = 78
# Temporary UI test fill for closed-trades scroll (set False when done)
TEST_TRADES_FILL = False
TEST_TRADES_FILL_COUNT = 40
# Live credentials come only from the UI — never config.pp_apikey / .env
PP_API_KEY_MIN_LEN = 32
# Probe mint to confirm subscribeTokenTrade accepts this API key
PP_API_KEY_PROBE_MINT = "So11111111111111111111111111111111111111112"


BOOL_PARAMS = {
    "use_mc_range",
    "use_min_age",
    "use_min_buy_volume",
    "use_ath_ban",
    "use_stoploss_entry",
    "use_stoploss_ath_low",
    "use_stoploss_ath_high",
    "use_stagnation",
    "use_ath_stagnation",
    "use_ath_stagnation_high",
    "use_high_mc_threshold",
    "use_holder_checks",
    "enforce_top_holder_max",
    "enforce_top_holder_min",
    "enforce_top3_max",
    "enforce_top3_min",
    "boost_on_buy_volume",
    "boost_on_top_holder",
    "boost_on_top3",
    "ban_on_low_volume",
    "use_max_buys",
    "use_wallet_stop",
    "wallet_stop_is_pct",
}


@dataclass
class StrategyParams:
    mc_min: float = 10_000
    mc_max: float = 30_000
    min_age_sec: float = 5
    min_buy_volume: float = 500
    buy_volume_boost_at: float = 1000
    volume_boost_multiplier: float = 2
    ath_ban_ratio: float = 0.75
    entry_units: float = 10
    starting_balance: float = 1000
    top_holder_boost_below_pct: float = 3
    top_holder_boost_multiplier: float = 2
    top3_boost_below_pct: float = 8
    top3_boost_multiplier: float = 2
    stoploss_entry_ratio: float = 0.95
    stoploss_ath_low: float = 0.90
    stoploss_ath_high: float = 0.85
    high_mc_threshold: float = 20_000
    stagnation_sec: float = 5
    ath_stagnation_sec: float = 7
    ath_stagnation_high_sec: float = 10
    # Enable / disable each rule
    use_mc_range: bool = True
    use_min_age: bool = True
    use_min_buy_volume: bool = True
    ban_on_low_volume: bool = True
    use_ath_ban: bool = True
    use_stoploss_entry: bool = True
    use_stoploss_ath_low: bool = True
    use_stoploss_ath_high: bool = True
    use_stagnation: bool = True
    use_ath_stagnation: bool = True
    use_ath_stagnation_high: bool = True
    use_high_mc_threshold: bool = True
    boost_on_buy_volume: bool = False
    boost_on_top_holder: bool = False
    boost_on_top3: bool = False
    # Holder distribution (check_top_ten)
    use_holder_checks: bool = True
    top_holder_max_pct: float = 5
    top_holder_min_pct: float = 1
    top3_max_pct: float = 10
    top3_min_pct: float = 3
    enforce_top_holder_max: bool = True
    enforce_top_holder_min: bool = True
    enforce_top3_max: bool = False
    enforce_top3_min: bool = False
    use_max_buys: bool = False
    max_buys: float = 10
    use_wallet_stop: bool = False
    wallet_stop_is_pct: bool = True
    wallet_stop_value: float = 10

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyParams":
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        cleaned: dict[str, Any] = {}
        for k, v in data.items():
            if k not in known:
                continue
            if k in BOOL_PARAMS:
                if isinstance(v, bool):
                    cleaned[k] = v
                elif isinstance(v, (int, float)):
                    cleaned[k] = bool(v)
                else:
                    cleaned[k] = str(v).lower() in ("1", "true", "on", "yes")
                continue
            try:
                cleaned[k] = float(v)
            except (TypeError, ValueError):
                continue
        return cls(**cleaned)

    def high_mc_exit_error(self) -> str | None:
        if self.use_high_mc_threshold and not (
            self.use_stoploss_ath_high or self.use_ath_stagnation_high
        ):
            return (
                "High MC threshold is on but no high-MC exit rule is enabled. "
                "Enable Stoploss vs ATH (high MC) and/or ATH stagnation (high MC), "
                "or turn off High MC threshold."
            )
        return None


@dataclass
class TradeRecord:
    ticker: str
    mint: str
    link: str
    entry_mc: float
    exit_mc: float | None
    gain_pct: float | None
    reason: str
    time: str
    buy_tx: str | None = None
    sell_tx: str | None = None


class PaperBot:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self.params = StrategyParams()
        self.running = False
        self.balance = self.params.starting_balance
        self.coins_sold = 0
        self.open_positions: dict[str, dict[str, Any]] = {}
        self.trades: list[TradeRecord] = []
        self.events: list[dict[str, str]] = []
        self._event_seq = 0
        self._feed_ws = None
        self._watched_tokens: set | None = None
        self._pp_api_key = ""
        self.trade_msg_count = 0
        self.buy_count = 0
        self._session_start_equity = 0.0
        self._buy_limit_hit = False
        self._wallet_stop_hit = False
        self._new_token_feed_paused = False

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

    def _paper_session_equity(self) -> float:
        with self._lock:
            equity = float(self.balance)
            positions = list(self.open_positions.values())
        for pos in positions:
            entry = float(pos.get("entry_mc") or 0)
            current = float(pos.get("current_mc") or 0)
            units = float(pos.get("entry_units") or 0)
            if entry > 0 and units:
                equity += units * ((current - entry) / entry / 100)
        return equity

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
        equity = self._paper_session_equity()
        if not self._wallet_stop_triggered(equity):
            return
        self._wallet_stop_hit = True
        drop = self._session_start_equity - equity
        if params.wallet_stop_is_pct:
            pct = (drop / self._session_start_equity) * 100 if self._session_start_equity else 0
            self._log(
                f"Wallet stop loss hit ({pct:.2f}% paper balance down) — no new entries",
                "status",
            )
        else:
            self._log(
                f"Wallet stop loss hit ({drop:.4f} paper balance down) — no new entries",
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
            return {
                "running": self.running,
                "mode": "live",
                "balance": round(self.balance, 2),
                "coins_sold": self.coins_sold,
                "params": asdict(self.params),
                "open_positions": positions,
                # Back-compat for older UI snippets
                "open_position": positions[0] if positions else None,
                "trades": [asdict(t) for t in self.trades[-50:]],
                "events": list(self.events[-250:]),
                "trade_msg_count": self.trade_msg_count,
                "buy_count": self.buy_count,
                "buy_limit_hit": self._buy_limit_hit,
                "wallet_stop_hit": self._wallet_stop_hit,
            }

    def start(
        self,
        params: StrategyParams,
        *,
        pp_api_key: str = "",
    ) -> tuple[bool, str]:
        api_key = (pp_api_key or "").strip()
        if not api_key:
            return False, "PumpPortal API key is required for Live"
        if len(api_key) < PP_API_KEY_MIN_LEN:
            return False, (
                f"PumpPortal API key looks invalid (got {len(api_key)} chars, "
                f"expected ≥ {PP_API_KEY_MIN_LEN})"
            )

        with self._lock:
            if self.running:
                return False, "Already running"
            self.params = params
            self._pp_api_key = api_key
            self.balance = params.starting_balance
            self.coins_sold = 0
            self.trade_msg_count = 0
            self.buy_count = 0
            self._session_start_equity = float(params.starting_balance)
            self._buy_limit_hit = False
            self._wallet_stop_hit = False
            self._new_token_feed_paused = False
            self.open_positions = {}
            self.trades = []
            if TEST_TRADES_FILL:
                reasons = ("Stoploss", "ATH stagnation", "Stagnation timeout", "Stoploss (high MC)")
                for i in range(1, TEST_TRADES_FILL_COUNT + 1):
                    mint = f"TestTradeMint{i:04d}pump"
                    entry = 12_000 + i * 37
                    exit_mc = entry * (0.92 + (i % 7) * 0.03)
                    gain = ((exit_mc - entry) / entry) * 100
                    self.trades.append(TradeRecord(
                        ticker=mint[:3].upper(),
                        mint=mint,
                        link=f"https://pump.fun/coin/{mint}",
                        entry_mc=float(entry),
                        exit_mc=round(exit_mc, 2),
                        gain_pct=round(gain, 2),
                        reason=reasons[i % len(reasons)],
                        time=time.strftime("%H:%M:%S"),
                    ))
                self.coins_sold = len(self.trades)
            self.events = []
            self._event_seq = 0
            self._stop.clear()
            self.running = True

        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        self._log("Paper bot started", "status")
        if TEST_TRADES_FILL:
            self._log(
                f"TEST: filled {TEST_TRADES_FILL_COUNT} closed trades for scroll check",
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
            self._thread.join(timeout=30)
        with self._lock:
            self.running = False
            self.open_positions = {}
        self._log("Paper bot stopped", "status")
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
            self._loop.close()
            self._loop = None

    def _should_stop(self) -> bool:
        return self._stop.is_set()

    def _should_watch_create(self, usd_mc: int, params: StrategyParams) -> bool:
        """Watch creates at/below MC max so they can pump into range. Skip already-above-max."""
        if not params.use_mc_range:
            return True
        if not usd_mc:
            return True
        return usd_mc <= params.mc_max

    async def _watch_token(
        self,
        websocket,
        watched_tokens: set,
        mint: str,
    ) -> None:
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
        """Confirm this socket's api-key can use metered trade streams.

        Free create events work even with a bad key, so we must probe
        subscribeTokenTrade. Never falls back to .env PP_API_KEY.
        """
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

    async def _run(self) -> None:
        # Credentials must be the ones passed into start() from the UI.
        api_key = self._pp_api_key
        if not api_key:
            self._log("Missing PumpPortal API key", "error")
            return

        self._log(f"Using API key {api_key[:4]}…", "status")
        self._log(
            f"Make sure the PumpPortal wallet linked to this key has ≥ {PP_MIN_SOL:.2f} SOL "
            f"(0.01 SOL per 10,000 trade messages)",
            "status",
        )

        # Plain-text API key from the UI — URL-encode for the query string only.
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
                    watched_tokens.clear()  # subscriptions die with the socket
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
                            await self._check_stagnation()
                            await self._maybe_auto_stop_after_buy_limit()
                            continue

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

                        # Free create feed; meter trades only if create MC ≤ max
                        # (below min is OK — may pump into range). Skip above-max creates.
                        if tx_type == "create":
                            if mint in bought_coins or mint in banned_coins:
                                continue
                            now = time.time()
                            if mint not in creation_time:
                                creation_time[mint] = now
                            if usd_mc and mint not in ath_tracker:
                                ath_tracker[mint] = usd_mc

                            if not self._entries_blocked() and self._should_watch_create(usd_mc, params):
                                await self._watch_token(
                                    websocket, watched_tokens, mint
                                )
                            continue

                        if not usd_mc:
                            continue

                        # Manage open positions first
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

                        # Ran above max — drop the stream (no dump-into-range hunting)
                        if params.use_mc_range and usd_mc > params.mc_max:
                            banned_coins.add(mint)
                            await self._unwatch_tokens(
                                websocket, watched_tokens, [mint], reason="above MC max"
                            )
                            continue

                        # ATH drop unsubscribe on every trade (not only in-range) so we can
                        # unsubscribe early and stop metering doomed coins.
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
                            mint,
                            wallet,
                            token_distribution[mint][wallet],
                            top_ten_holders,
                        )

                        if mint not in volume_history:
                            volume_history[mint] = {"buy": deque(), "sell": deque()}
                        volume_history[mint][tx_type].append((now, usd_value))
                        cutoff = now - 60
                        for trade_type in ("buy", "sell"):
                            deck = volume_history[mint][trade_type]
                            while deck and deck[0][0] < cutoff:
                                deck.popleft()

                        # Below min — keep watching until it pumps into range
                        if params.use_mc_range and usd_mc < params.mc_min:
                            continue

                        buy_volume = get_volume(volume_history, mint, 60, "buy")
                        if params.use_min_buy_volume and buy_volume < params.min_buy_volume:
                            if params.ban_on_low_volume:
                                self._log(
                                    f"Not enough volume ({buy_volume:.0f}) — {link}",
                                    "skip",
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
                                self._log(
                                    f"{top_holder_max_skip_message(res, params.top_holder_max_pct)} — {link}",
                                    "skip",
                                )
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

                        # Paper buy — size boost if any boost condition hits
                        top_pct = holder_top_pct(top_ten_holders, mint)
                        top3_pct = holder_top3_pct(top_ten_holders, mint)
                        units, boost_reasons, boost_mult = resolve_entry_size(
                            params,
                            buy_volume=buy_volume,
                            top_holder_pct=top_pct,
                            top3_pct=top3_pct,
                        )
                        boosted = bool(boost_reasons)

                        bought_coins.add(mint)
                        banned_coins.add(mint)  # don't re-enter
                        entry = usd_mc
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
                            }
                            self.buy_count += 1
                        await self._ensure_buy_limit_paused()
                        boost_note = (
                            f" (×{boost_mult:g}: {', '.join(boost_reasons)})"
                            if boosted
                            else ""
                        )
                        self._log(
                            f"PAPER BUY {ticker} @ MC {entry} | size {units:g}{boost_note} | vol {buy_volume:.0f} — {link}",
                            "buy",
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

    async def _force_sell_open(self) -> None:
        with self._lock:
            mints = list(self.open_positions.keys())
        for mint in mints:
            with self._lock:
                pos = self.open_positions.get(mint)
            if not pos:
                continue
            exit_mc = float(pos.get("current_mc") or pos.get("entry_mc") or 0)
            await self._close_position(mint, exit_mc, "Test stopped")

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
                elif (
                    params.use_ath_stagnation
                    and elapsed > params.ath_stagnation_sec
                ):
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
            return future.result(timeout=30)
        except Exception as e:
            return False, str(e)

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
            entry = float(pos["entry_mc"])
            units = float(pos["entry_units"])
            sell_units = units * fraction
            if sell_units <= 0:
                return False, "Nothing to sell"
            gain = ((exit_mc - entry) / entry) * 100 if entry else 0
            self.balance += sell_units * (gain / 100)
            self.coins_sold += 1
            record = TradeRecord(
                ticker=pos["ticker"],
                mint=pos["mint"],
                link=pos["link"],
                entry_mc=entry,
                exit_mc=exit_mc,
                gain_pct=round(gain, 2),
                reason=reason,
                time=time.strftime("%H:%M:%S"),
            )
            self.trades.append(record)
            bal = self.balance
            remaining = units - sell_units
            full_close = remaining <= max(units * 1e-9, 1e-9) or fraction >= 1.0 - 1e-9
            if full_close:
                self.open_positions.pop(mint, None)
            else:
                pos["entry_units"] = remaining

        pct_sold = int(round(fraction * 100))
        self._log(
            f"PAPER SELL {record.ticker} @ MC {exit_mc} | {gain:+.2f}% | {reason} | "
            f"sold {pct_sold}% | bal {bal:.2f} — {record.link}",
            "sell",
        )
        if full_close:
            ws = getattr(self, "_feed_ws", None)
            watched = getattr(self, "_watched_tokens", None)
            if ws is not None and watched is not None:
                await self._unwatch_tokens(
                    ws, watched, [record.mint], reason="position closed"
                )
        await self._maybe_auto_stop_after_buy_limit()
        return True, f"Sold {pct_sold}%"


def holder_top_pct(top_ten_holders: dict, mint: str) -> float | None:
    """Top-holder % from tracked top-10, or None if fewer than 10 holders."""
    lst = top_ten_holders.get(mint, [])
    if len(lst) < 10:
        return None
    return round((lst[0][1] / 1_000_000_000) * 100, 4)


def holder_top3_pct(top_ten_holders: dict, mint: str) -> float | None:
    """Combined top-3 holder % from tracked top-10, or None if fewer than 10 holders."""
    lst = top_ten_holders.get(mint, [])
    if len(lst) < 10:
        return None
    total = sum(x[1] for x in lst[:3])
    return round((total / 1_000_000_000) * 100, 4)


def resolve_entry_size(
    params: StrategyParams,
    *,
    buy_volume: float,
    top_holder_pct: float | None = None,
    top3_pct: float | None = None,
) -> tuple[float, list[str], float]:
    """Return (units, reasons, multiplier). Largest matching boost multiplier wins."""
    candidates: list[tuple[float, str]] = []

    if (
        params.use_min_buy_volume
        and params.boost_on_buy_volume
        and buy_volume >= params.buy_volume_boost_at
    ):
        mult = params.volume_boost_multiplier if params.volume_boost_multiplier > 0 else 1
        candidates.append((mult, f"buy vol ≥ {params.buy_volume_boost_at:g}"))

    if (
        params.boost_on_top_holder
        and top_holder_pct is not None
        and top_holder_pct < params.top_holder_boost_below_pct
    ):
        mult = (
            params.top_holder_boost_multiplier
            if params.top_holder_boost_multiplier > 0
            else 1
        )
        candidates.append(
            (
                mult,
                f"top holder {top_holder_pct:g}% < {params.top_holder_boost_below_pct:g}%",
            )
        )

    if (
        params.boost_on_top3
        and top3_pct is not None
        and top3_pct < params.top3_boost_below_pct
    ):
        mult = (
            params.top3_boost_multiplier
            if params.top3_boost_multiplier > 0
            else 1
        )
        candidates.append(
            (
                mult,
                f"top 3 {top3_pct:g}% < {params.top3_boost_below_pct:g}%",
            )
        )

    if not candidates:
        return params.entry_units, [], 1.0

    best_mult = max(m for m, _ in candidates)
    reasons = [r for m, r in candidates if m == best_mult]
    return params.entry_units * best_mult, reasons, best_mult


def get_volume(volume_history: dict, mint: str, seconds: float, trade_type: str | None = None) -> float:
    if mint not in volume_history:
        return 0
    now = time.time()
    cutoff = now - seconds
    if trade_type is None:
        return (
            sum(v for t, v in volume_history[mint]["buy"] if t >= cutoff)
            + sum(v for t, v in volume_history[mint]["sell"] if t >= cutoff)
        )
    return sum(v for t, v in volume_history[mint][trade_type] if t >= cutoff)


def update_top_ten(mint, wallet, amount, top_ten_holders) -> None:
    if amount <= 0:
        top_ten_holders[mint] = [x for x in top_ten_holders[mint] if x[0] != wallet]
        return

    lst = [x for x in top_ten_holders[mint] if x[0] != wallet]
    if len(lst) < 10:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        top_ten_holders[mint] = lst
        return

    if amount > lst[-1][1]:
        lst.append((wallet, amount))
        lst.sort(key=lambda x: x[1], reverse=True)
        lst = lst[:10]
    top_ten_holders[mint] = lst


def top_holder_max_skip_message(percents, max_pct: float) -> str:
    top = percents[0] if percents else 0.0
    return (
        f"No buy: largest holder owns {top:g}% of supply "
        f"(your max top-holder limit is {max_pct:g}%)"
    )


def check_top_ten(top_ten_holders, mint, params: StrategyParams | None = None):
    p = params or StrategyParams()
    lst = top_ten_holders.get(mint, [])
    if len(lst) < 10:
        return "Less than ten holders", ""
    if len(lst) == 10:
        percents = [round((amount / 1_000_000_000) * 100, 4) for wallet, amount in lst]
        top3 = percents[0] + percents[1] + percents[2]
        if percents[0] > p.top_holder_max_pct:
            return "Top Holder owns too much", percents
        if percents[0] < p.top_holder_min_pct:
            return "Top Holder owns too little", percents
        if top3 > p.top3_max_pct:
            return "Top 3 holders own too much", percents
        if top3 < p.top3_min_pct:
            return "Top 3 holders own too little", percents
        return "Concentration good", percents
    return "Unexpected holder count", lst


bot = PaperBot()
