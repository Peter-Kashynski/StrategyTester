"""Free offline strategy simulator — same rules as paper_bot, no PumpPortal."""

from __future__ import annotations

import random
import string
import threading
import time
from collections import deque
from dataclasses import asdict
from typing import Any

from paper_bot import (
    StrategyParams,
    TradeRecord,
    check_top_ten,
    get_volume,
    holder_top_pct,
    holder_top3_pct,
    resolve_entry_size,
    update_top_ten,
)


def _fake_mint() -> str:
    body = "".join(random.choices(string.ascii_letters + string.digits, k=40))
    return f"{body}pump"


def _fake_wallet() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=32))


class SimBot:
    """Generates synthetic creates/trades and runs the paper strategy locally."""

    def __init__(self) -> None:
        self.params = StrategyParams()
        self.running = False
        self.balance = self.params.starting_balance
        self.coins_sold = 0
        self.open_positions: dict[str, dict[str, Any]] = {}
        self.trades: list[TradeRecord] = []
        self.events: list[dict[str, Any]] = []
        self._event_seq = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.trade_msg_count = 0
        self.buy_count = 0
        self._buy_limit_hit = False
        self._sim_coins: dict | None = None

    def _prune_sim_pipeline(self) -> None:
        coins = self._sim_coins
        if not coins or not self._buy_limit_hit:
            return
        with self._lock:
            open_mints = set(self.open_positions.keys())
        for mint, coin in list(coins.items()):
            if mint not in open_mints and not coin.get("done"):
                coin["done"] = True

    def _ensure_buy_limit_paused(self) -> None:
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
        self._prune_sim_pipeline()
        self._maybe_auto_stop_after_buy_limit()

    def _can_buy_new(self) -> bool:
        if self._buy_limit_hit:
            return False
        self._ensure_buy_limit_paused()
        return not self._buy_limit_hit

    def _maybe_auto_stop_after_buy_limit(self) -> None:
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

    def _log(self, message: str, kind: str = "status") -> None:
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
                "mode": "sim",
                "balance": round(self.balance, 2),
                "coins_sold": self.coins_sold,
                "params": asdict(self.params),
                "open_positions": positions,
                "open_position": positions[0] if positions else None,
                "trades": [asdict(t) for t in self.trades[-50:]],
                "events": list(self.events[-250:]),
                "trade_msg_count": self.trade_msg_count,
                "buy_count": self.buy_count,
                "buy_limit_hit": self._buy_limit_hit,
            }

    def start(self, params: StrategyParams) -> tuple[bool, str]:
        with self._lock:
            if self.running:
                return False, "Simulator already running"
            self.params = params
            self.balance = params.starting_balance
            self.coins_sold = 0
            self.trade_msg_count = 0
            self.buy_count = 0
            self._buy_limit_hit = False
            self._sim_coins = None
            self.open_positions = {}
            self.trades = []
            self.events = []
            self._event_seq = 0
            self._stop.clear()
            self.running = True

        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()
        self._log("Sim bot started — free synthetic feed (no PumpPortal)", "status")
        return True, "Simulator started"

    def stop(self) -> tuple[bool, str]:
        with self._lock:
            if not self.running:
                return False, "Simulator not running"
            self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        with self._lock:
            self.running = False
            self._thread = None
        self._log("Sim bot stopped", "status")
        return True, "Simulator stopped"

    def _thread_main(self) -> None:
        try:
            self._run()
        except Exception as e:
            self._log(f"Sim crashed: {e}", "error")
        finally:
            with self._lock:
                self.running = False

    def _run(self) -> None:
        coins: dict[str, dict[str, Any]] = {}
        self._sim_coins = coins
        volume_history: dict = {}
        top_ten_holders: dict = {}
        next_spawn = time.time() + 0.4

        while not self._stop.is_set():
            now = time.time()
            params = self.params

            if now >= next_spawn and len(coins) < 18 and not self._buy_limit_hit:
                self._spawn_coin(coins, volume_history, top_ten_holders, params)
                next_spawn = now + random.uniform(1.2, 3.5)

            dead: list[str] = []
            for mint, coin in list(coins.items()):
                if coin.get("done"):
                    dead.append(mint)
                    continue
                self._tick_coin(coin, volume_history, top_ten_holders, params)
                self.trade_msg_count += 1
                # Drop finished / aged-out coins
                if coin.get("done") or (now - coin["created_at"] > 90 and mint not in self.open_positions):
                    dead.append(mint)

            for mint in dead:
                coins.pop(mint, None)
                volume_history.pop(mint, None)
                top_ten_holders.pop(mint, None)

            self._check_stagnation()
            self._maybe_auto_stop_after_buy_limit()
            if self._stop.is_set():
                break
            time.sleep(0.2)

        # Force-close leftover positions on stop
        with self._lock:
            mints = list(self.open_positions.keys())
        for mint in mints:
            with self._lock:
                pos = self.open_positions.get(mint)
                mc = pos["current_mc"] if pos else 0
            if pos:
                self._close_position(mint, mc, "Sim stopped")

        self._log("Sim loop exited", "status")
        self._sim_coins = None

    def _spawn_coin(
        self,
        coins: dict,
        volume_history: dict,
        top_ten_holders: dict,
        params: StrategyParams,
    ) -> None:
        if self._buy_limit_hit:
            return
        mint = _fake_mint()
        ticker = mint[:3].upper()
        link = f"https://pump.fun/coin/{mint}"

        lo = max(500.0, params.mc_min * 0.4)
        hi = max(lo + 500.0, params.mc_max * 1.6)
        roll = random.random()
        if roll < 0.55:
            # Often spawn inside / near entry range
            mc = random.uniform(params.mc_min * 0.85, params.mc_max * 1.05)
        elif roll < 0.75:
            mc = random.uniform(lo, params.mc_min * 0.9)  # below min — may pump in
        else:
            mc = random.uniform(params.mc_max * 1.05, hi)  # above max — should skip/unsub

        path = random.choices(
            ["pump", "dump", "chop", "moon", "fail_vol", "fail_holder"],
            weights=[28, 18, 22, 12, 10, 10],
            k=1,
        )[0]

        holders = self._build_holders(params, bad=(path == "fail_holder"))
        top_ten_holders[mint] = []
        for wallet, amount in holders:
            update_top_ten(mint, wallet, amount, top_ten_holders)

        volume_history[mint] = {"buy": deque(), "sell": deque()}
        coins[mint] = {
            "mint": mint,
            "ticker": ticker,
            "link": link,
            "created_at": time.time(),
            "mc": float(mc),
            "ath": float(mc),
            "path": path,
            "bought": False,
            "banned": False,
            "ticks": 0,
            "done": False,
            "vol_burst": path != "fail_vol",
        }
        self._log(f"Subscribed — {link}", "sub")

    def _build_holders(self, params: StrategyParams, *, bad: bool) -> list[tuple[str, float]]:
        """Ten holders as raw token amounts (1e9 = 100%)."""
        if bad and random.random() < 0.5:
            # Top holder too large
            top_pct = params.top_holder_max_pct + random.uniform(1, 8)
        elif bad:
            top_pct = max(0.2, params.top_holder_min_pct - random.uniform(0.2, 0.8))
        else:
            # Valid-ish distribution
            top_pct = random.uniform(
                params.top_holder_min_pct + 0.2,
                max(params.top_holder_min_pct + 0.3, params.top_holder_max_pct - 0.3),
            )

        rem = max(5.0, 100.0 - top_pct)
        parts = [random.random() for _ in range(9)]
        s = sum(parts) or 1.0
        pcts = [top_pct] + [rem * (p / s) for p in parts]
        # Normalize to ~100
        total = sum(pcts) or 1.0
        pcts = [p * (100.0 / total) for p in pcts]
        return [(_fake_wallet(), p / 100.0 * 1_000_000_000) for p in pcts]

    def _tick_coin(
        self,
        coin: dict,
        volume_history: dict,
        top_ten_holders: dict,
        params: StrategyParams,
    ) -> None:
        mint = coin["mint"]
        coin["ticks"] += 1
        now = time.time()
        age = now - coin["created_at"]

        # Price path
        mc = coin["mc"]
        path = coin["path"]
        if path == "pump":
            mc *= random.uniform(0.992, 1.035)
        elif path == "dump":
            mc *= random.uniform(0.96, 1.008)
        elif path == "moon":
            mc *= random.uniform(1.01, 1.06)
        elif path == "chop":
            mc *= random.uniform(0.985, 1.015)
        else:
            mc *= random.uniform(0.99, 1.02)
        mc = max(200.0, mc)
        coin["mc"] = mc
        if mc > coin["ath"]:
            coin["ath"] = mc

        # Volume ticks (buys)
        if coin["vol_burst"]:
            usd = random.uniform(40, 220)
            if path in ("pump", "moon"):
                usd = random.uniform(80, 400)
        else:
            usd = random.uniform(5, 35)
        volume_history[mint]["buy"].append((now, usd))
        cutoff = now - 60
        for trade_type in ("buy", "sell"):
            deck = volume_history[mint][trade_type]
            while deck and deck[0][0] < cutoff:
                deck.popleft()

        usd_mc = int(mc)
        ticker = coin["ticker"]
        link = coin["link"]

        # Manage open position
        with self._lock:
            holding = mint in self.open_positions
        if holding:
            self._update_position(mint, float(usd_mc))
            return

        if coin["bought"] or coin["banned"]:
            return

        # ATH unsubscribe before entry
        if params.use_ath_ban and usd_mc < (coin["ath"] * params.ath_ban_ratio):
            coin["banned"] = True
            coin["done"] = True
            self._log(f"Unsubscribed 1 token(s) — ATH unsubscribe — {link}", "unsub")
            return

        if params.use_mc_range and usd_mc > params.mc_max:
            coin["banned"] = True
            coin["done"] = True
            self._log(f"Unsubscribed 1 token(s) — above MC max — {link}", "unsub")
            return

        if params.use_min_age and age < params.min_age_sec:
            return

        if params.use_mc_range and usd_mc < params.mc_min:
            return

        buy_volume = get_volume(volume_history, mint, 60, "buy")
        if params.use_min_buy_volume and buy_volume < params.min_buy_volume:
            if params.ban_on_low_volume:
                # Keep accumulating unless path is fail_vol and we've waited a bit
                if path == "fail_vol" and age > max(params.min_age_sec, 3) + 4:
                    self._log(f"Not enough volume ({buy_volume:.0f}) — {link}", "skip")
                    coin["banned"] = True
                    coin["done"] = True
                    self._log(f"Unsubscribed 1 token(s) — volume unsubscribe — {link}", "unsub")
            return

        if params.use_holder_checks:
            message, res = check_top_ten(top_ten_holders, mint, params)
            if message == "Less than ten holders":
                return
            if message == "Top Holder owns too much":
                self._log(f"Top holder too much {res} — {link}", "skip")
                if params.enforce_top_holder_max:
                    coin["banned"] = True
                    coin["done"] = True
                    self._log(f"Unsubscribed 1 token(s) — holder unsubscribe — {link}", "unsub")
                    return
            elif message == "Top Holder owns too little":
                self._log(f"Top holder too little {res} — {link}", "skip")
                if params.enforce_top_holder_min:
                    coin["banned"] = True
                    coin["done"] = True
                    self._log(f"Unsubscribed 1 token(s) — holder unsubscribe — {link}", "unsub")
                    return
            elif message == "Top 3 holders own too much":
                self._log(f"Top 3 too much {res} — {link}", "skip")
                if params.enforce_top3_max:
                    coin["banned"] = True
                    coin["done"] = True
                    self._log(f"Unsubscribed 1 token(s) — holder unsubscribe — {link}", "unsub")
                    return
            elif message == "Top 3 holders own too little":
                self._log(f"Top 3 too little {res} — {link}", "skip")
                if params.enforce_top3_min:
                    coin["banned"] = True
                    coin["done"] = True
                    self._log(f"Unsubscribed 1 token(s) — holder unsubscribe — {link}", "unsub")
                    return

        if not self._can_buy_new():
            return

        top_pct = holder_top_pct(top_ten_holders, mint)
        top3_pct = holder_top3_pct(top_ten_holders, mint)
        units, boost_reasons, boost_mult = resolve_entry_size(
            params,
            buy_volume=buy_volume,
            top_holder_pct=top_pct,
            top3_pct=top3_pct,
        )
        boosted = bool(boost_reasons)
        entry = float(usd_mc)
        coin["bought"] = True
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
        self._ensure_buy_limit_paused()
        boost_note = (
            f" (×{boost_mult:g}: {', '.join(boost_reasons)})"
            if boosted
            else ""
        )
        self._log(
            f"PAPER BUY {ticker} @ MC {entry} | size {units:g}{boost_note} | vol {buy_volume:.0f} — {link}",
            "buy",
        )

    def _check_stagnation(self) -> None:
        with self._lock:
            positions = list(self.open_positions.values())
            params = self.params
        if not positions:
            return
        now = time.time()
        for pos in positions:
            mint = pos["mint"]
            # Sim always has ticks; use a softer last_msg bump so stagnation still fires
            # when path goes quiet — mark last_msg only on meaningful moves in _update.
            if params.use_stagnation and now - pos["last_msg"] > params.stagnation_sec:
                self._close_position(mint, pos["current_mc"], "Stagnation timeout")
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
                        self._close_position(mint, current, "High MC ATH stagnation")
                elif (
                    params.use_ath_stagnation
                    and elapsed > params.ath_stagnation_sec
                ):
                    self._close_position(mint, current, "ATH stagnation")

    def _update_position(self, mint: str, usd_mc: float) -> None:
        with self._lock:
            pos = self.open_positions.get(mint)
            params = self.params
            if not pos:
                return
            prev = pos["current_mc"]
            pos["current_mc"] = usd_mc
            # Refresh tape only when MC moves enough (lets trade stagnation fire)
            if abs(usd_mc - prev) / max(prev, 1) > 0.002:
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
                self._close_position(mint, usd_mc, "Stoploss")
                return
            if ath_high_hit:
                self._close_position(mint, usd_mc, "Stoploss (high MC)")
                return

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
        return self._sell_fraction(mint, exit_mc, pct / 100.0, f"Manual sell ({int(pct)}%)")

    def _close_position(self, mint: str, exit_mc: float, reason: str) -> None:
        self._sell_fraction(mint, exit_mc, 1.0, reason)

    def _sell_fraction(
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
            self._log(f"Unsubscribed 1 token(s) — position closed — {record.link}", "unsub")
        self._maybe_auto_stop_after_buy_limit()
        return True, f"Sold {pct_sold}%"


sim_bot = SimBot()
