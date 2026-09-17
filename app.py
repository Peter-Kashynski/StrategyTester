
# start - 95,480,866


"""Flask UI for live paper strategy testing + free simulator + real trade."""

from __future__ import annotations

import asyncio
import os
import secrets
from functools import wraps
from pathlib import Path

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from solders.pubkey import Pubkey

from auth_users import (
    create_user,
    get_user,
    public_user_payload,
    verify_login,
)
from config import PP_MIN_SOL, get_wallet_sol
from paper_bot import StrategyParams, bot
from pp_wallet import create_pp_wallet
from sim_bot import sim_bot
from preset_store import (
    clear_preset_slot,
    get_user_state,
    import_client_state,
    save_mode_params,
    save_preset_slot,
    set_active_slot,
)
from trade_bot import BUY_SOL_DEFAULT, trade_bot

app = Flask(__name__)


def _load_secret_key() -> str:
    env = (os.getenv("FLASK_SECRET_KEY") or "").strip()
    if env:
        return env
    path = Path(__file__).resolve().parent / "data" / "secret_key.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        key = path.read_text(encoding="utf-8").strip()
        if key:
            return key
    key = secrets.token_hex(32)
    path.write_text(key, encoding="utf-8")
    return key


app.secret_key = _load_secret_key()
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)


def _session_user() -> dict | None:
    email = session.get("user_email")
    if not email:
        return None
    return get_user(email)


def _is_authed() -> bool:
    return bool(session.get("user_email") or session.get("guest"))


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not _is_authed():
            if request.path.startswith("/api/"):
                return jsonify({"ok": False, "message": "Sign in required"}), 401
            return redirect(url_for("login_page"))
        return view(*args, **kwargs)

    return wrapped


@app.before_request
def _require_login():
    if request.endpoint in (
        "login_page",
        "api_auth_login",
        "api_auth_register",
        "api_auth_logout",
        "api_auth_guest",
        "static",
    ):
        return None
    if request.endpoint is None:
        return None
    if not _is_authed():
        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "message": "Sign in required"}), 401
        if request.endpoint != "login_page":
            return redirect(url_for("login_page"))
    return None


def _sse_payload(snap: dict) -> dict:
    events = snap.get("events", [])
    return {
        "running": snap["running"],
        "mode": snap.get("mode", "live"),
        "balance": snap["balance"],
        "coins_sold": snap["coins_sold"],
        "open_position": snap["open_position"],
        "open_positions": snap.get("open_positions") or [],
        "trades": snap["trades"],
        "events": events,
        "all_events": events[-250:],
        "trade_msg_count": snap.get("trade_msg_count", 0),
        "pnl_sol": snap.get("pnl_sol"),
        "unrealized_pnl_sol": snap.get("unrealized_pnl_sol"),
        "wallet_sol": snap.get("wallet_sol"),
    }


def _event_stream(get_snap):
    import json
    import time

    last_id = 0
    while True:
        snap = get_snap()
        events = snap.get("events", [])
        new_events = [e for e in events if int(e["id"]) > last_id]
        if new_events:
            last_id = int(new_events[-1]["id"])
        payload = _sse_payload(snap)
        payload["events"] = new_events if new_events else []
        yield f"data: {json.dumps(payload)}\n\n"
        time.sleep(0.8)


def _busy_other(*, excluding: str) -> str | None:
    """Return error message if another bot mode is already running."""
    if excluding != "live" and bot.running:
        return "Stop Live paper first"
    if excluding != "sim" and sim_bot.running:
        return "Stop Simulate first"
    if excluding != "trade" and trade_bot.running:
        return "Stop Trade first"
    return None


@app.route("/login")
def login_page():
    if session.get("user_email"):
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/api/auth/guest", methods=["POST"])
def api_auth_guest():
    session.clear()
    session["guest"] = True
    session.permanent = True
    return jsonify({"ok": True, "guest": True})


@app.route("/api/auth/register", methods=["POST"])
def api_auth_register():
    data = request.get_json(silent=True) or {}
    email = str(data.get("email") or "")
    password = str(data.get("password") or "")
    try:
        wallet = asyncio.run(create_pp_wallet())
    except Exception as e:
        return jsonify({
            "ok": False,
            "message": f"Could not create PumpPortal wallet: {e}",
        }), 502

    user, err = create_user(
        email,
        password,
        api_key=wallet["api_key"],
        wallet_pubkey=wallet["wallet_pubkey"],
        private_key=wallet["private_key"],
    )
    if err:
        return jsonify({"ok": False, "message": err}), 400

    session.clear()
    session["user_email"] = user["email"]
    session.permanent = True
    return jsonify({
        "ok": True,
        "is_new": True,
        "email": user["email"],
        "wallet_pubkey": user["wallet_pubkey"],
        "pp_min_sol": PP_MIN_SOL,
    })


@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.get_json(silent=True) or {}
    user, err = verify_login(str(data.get("email") or ""), str(data.get("password") or ""))
    if err:
        return jsonify({"ok": False, "message": err}), 401
    session.clear()
    session["user_email"] = user["email"]
    session.permanent = True
    return jsonify({
        "ok": True,
        "is_new": False,
        "email": user["email"],
        "wallet_pubkey": user["wallet_pubkey"],
        "pp_min_sol": PP_MIN_SOL,
    })


@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/me")
def api_me():
    user = _session_user()
    if user:
        return jsonify({
            "ok": True,
            "guest": False,
            "has_account": True,
            "user": public_user_payload(user, include_secrets=True),
            "pp_min_sol": PP_MIN_SOL,
        })
    if session.get("guest"):
        return jsonify({
            "ok": True,
            "guest": True,
            "has_account": False,
            "user": None,
            "pp_min_sol": PP_MIN_SOL,
        })
    return jsonify({"ok": False, "message": "Sign in required"}), 401


@app.route("/")
def index():
    user = _session_user()
    return render_template(
        "index.html",
        defaults=StrategyParams(),
        buy_sol_default=BUY_SOL_DEFAULT,
        user_email=(user or {}).get("email") or "",
        has_account=bool(user),
        is_guest=bool(session.get("guest") and not user),
        pp_min_sol=PP_MIN_SOL,
    )


@app.route("/api/pp/create_wallet", methods=["POST"])
def api_pp_create_wallet():
    """Provision a new PumpPortal Lightning wallet + API key (new-user flow)."""
    try:
        data = asyncio.run(create_pp_wallet())
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 502
    return jsonify({"ok": True, **data, "pp_min_sol": PP_MIN_SOL})


@app.route("/api/start", methods=["POST"])
def api_start():
    busy = _busy_other(excluding="live")
    if busy:
        return jsonify({"ok": False, "message": busy, "state": bot.snapshot()}), 400
    data = request.get_json(silent=True) or {}
    params = StrategyParams.from_dict(data)
    mc_err = params.high_mc_exit_error()
    if mc_err:
        return jsonify({"ok": False, "message": mc_err, "state": bot.snapshot()}), 400
    ok, msg = bot.start(
        params,
        pp_api_key=str(data.get("pp_api_key") or ""),
    )
    return jsonify({"ok": ok, "message": msg, "state": bot.snapshot()}), (200 if ok else 400)


@app.route("/api/stop", methods=["POST"])
def api_stop():
    ok, msg = bot.stop()
    return jsonify({"ok": ok, "message": msg, "state": bot.snapshot()}), (200 if ok else 400)


@app.route("/api/clear_log", methods=["POST"])
def api_clear_log():
    bot.clear_events()
    return jsonify({"ok": True, "state": bot.snapshot()})


@app.route("/api/manual_sell", methods=["POST"])
def api_manual_sell():
    data = request.get_json(silent=True) or {}
    mint = str(data.get("mint") or "").strip()
    try:
        pct = float(data.get("pct"))
    except (TypeError, ValueError):
        pct = 0.0
    if not mint:
        return jsonify({"ok": False, "message": "Missing mint", "state": bot.snapshot()}), 400
    ok, msg = bot.manual_sell(mint, pct)
    return jsonify({"ok": ok, "message": msg, "state": bot.snapshot()}), (200 if ok else 400)


@app.route("/api/status")
def api_status():
    return jsonify(bot.snapshot())


@app.route("/api/stream")
def api_stream():
    return Response(
        _event_stream(bot.snapshot),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/wallet_balance", methods=["POST"])
def api_wallet_balance():
    data = request.get_json(silent=True) or {}
    pubkey = str(data.get("pubkey") or "").strip()
    if not pubkey:
        return jsonify({"ok": False, "message": "Enter a wallet public key"}), 400
    try:
        Pubkey.from_string(pubkey)
    except Exception:
        return jsonify({
            "ok": False,
            "message": f"Invalid Solana public key (got {len(pubkey)} chars)",
        }), 400

    sol = asyncio.run(get_wallet_sol(pubkey))
    if sol is None:
        return jsonify({
            "ok": False,
            "message": "Could not fetch balance — check pubkey / RPC",
        }), 502

    return jsonify({
        "ok": True,
        "pubkey": pubkey,
        "sol": sol,
        "lamports": int(round(sol * 1_000_000_000)),
        "below_pp_min": sol < PP_MIN_SOL,
        "pp_min_sol": PP_MIN_SOL,
    })


@app.route("/api/sim/start", methods=["POST"])
def api_sim_start():
    busy = _busy_other(excluding="sim")
    if busy:
        return jsonify({"ok": False, "message": busy, "state": sim_bot.snapshot()}), 400
    data = request.get_json(silent=True) or {}
    params = StrategyParams.from_dict(data)
    mc_err = params.high_mc_exit_error()
    if mc_err:
        return jsonify({"ok": False, "message": mc_err, "state": sim_bot.snapshot()}), 400
    ok, msg = sim_bot.start(params)
    return jsonify({"ok": ok, "message": msg, "state": sim_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/sim/stop", methods=["POST"])
def api_sim_stop():
    ok, msg = sim_bot.stop()
    return jsonify({"ok": ok, "message": msg, "state": sim_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/sim/clear_log", methods=["POST"])
def api_sim_clear_log():
    sim_bot.clear_events()
    return jsonify({"ok": True, "state": sim_bot.snapshot()})


@app.route("/api/sim/manual_sell", methods=["POST"])
def api_sim_manual_sell():
    data = request.get_json(silent=True) or {}
    mint = str(data.get("mint") or "").strip()
    try:
        pct = float(data.get("pct"))
    except (TypeError, ValueError):
        pct = 0.0
    if not mint:
        return jsonify({"ok": False, "message": "Missing mint", "state": sim_bot.snapshot()}), 400
    ok, msg = sim_bot.manual_sell(mint, pct)
    return jsonify({"ok": ok, "message": msg, "state": sim_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/sim/status")
def api_sim_status():
    return jsonify(sim_bot.snapshot())


@app.route("/api/sim/stream")
def api_sim_stream():
    return Response(
        _event_stream(sim_bot.snapshot),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/trade/start", methods=["POST"])
def api_trade_start():
    busy = _busy_other(excluding="trade")
    if busy:
        return jsonify({"ok": False, "message": busy, "state": trade_bot.snapshot()}), 400
    data = request.get_json(silent=True) or {}
    params = StrategyParams.from_dict(data)
    mc_err = params.high_mc_exit_error()
    if mc_err:
        return jsonify({"ok": False, "message": mc_err, "state": trade_bot.snapshot()}), 400
    ok, msg = trade_bot.start(
        params,
        pp_api_key=str(data.get("pp_api_key") or ""),
        wallet_pubkey=str(data.get("wallet_pubkey") or ""),
        wallet_privkey=str(data.get("wallet_privkey") or ""),
        helius_api_key=str(data.get("helius_api_key") or ""),
        buy_sol=data.get("buy_sol", BUY_SOL_DEFAULT),
    )
    return jsonify({"ok": ok, "message": msg, "state": trade_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/trade/stop", methods=["POST"])
def api_trade_stop():
    ok, msg = trade_bot.stop()
    return jsonify({"ok": ok, "message": msg, "state": trade_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/trade/clear_log", methods=["POST"])
def api_trade_clear_log():
    trade_bot.clear_events()
    return jsonify({"ok": True, "state": trade_bot.snapshot()})


@app.route("/api/trade/manual_sell", methods=["POST"])
def api_trade_manual_sell():
    data = request.get_json(silent=True) or {}
    mint = str(data.get("mint") or "").strip()
    try:
        pct = float(data.get("pct"))
    except (TypeError, ValueError):
        pct = 0.0
    if not mint:
        return jsonify({"ok": False, "message": "Missing mint", "state": trade_bot.snapshot()}), 400
    ok, msg = trade_bot.manual_sell(mint, pct)
    return jsonify({"ok": ok, "message": msg, "state": trade_bot.snapshot()}), (200 if ok else 400)


@app.route("/api/trade/status")
def api_trade_status():
    return jsonify(trade_bot.snapshot())


@app.route("/api/trade/stream")
def api_trade_stream():
    return Response(
        _event_stream(trade_bot.snapshot),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _account_user_or_403():
    user = _session_user()
    if not user:
        return None, (jsonify({"ok": False, "message": "Account required for cloud presets"}), 403)
    return user, None


@app.route("/api/presets", methods=["GET"])
def api_presets_get():
    user, err = _account_user_or_403()
    if err:
        return err
    state = get_user_state(user["email"])
    return jsonify({"ok": True, **state})


@app.route("/api/presets/import", methods=["POST"])
def api_presets_import():
    user, err = _account_user_or_403()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    replace = bool(data.get("replace"))
    try:
        state = import_client_state(
            user["email"],
            data.get("presets"),
            data.get("modeParams"),
            replace=replace,
        )
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    return jsonify({"ok": True, **state})


@app.route("/api/presets/slot", methods=["PUT"])
def api_presets_save_slot():
    user, err = _account_user_or_403()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode") or "")
    try:
        slot = int(data.get("slot"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Invalid slot"}), 400
    name = str(data.get("name") or "").strip()
    params = data.get("params")
    if not isinstance(params, dict):
        cur = get_user_state(user["email"])
        slots = (cur.get("presets") or {}).get(mode, {}).get("slots") or []
        existing = slots[slot] if 0 <= slot < len(slots) else None
        if not existing or not isinstance(existing, dict):
            return jsonify({"ok": False, "message": "Preset slot is empty"}), 400
        params = existing.get("params") or {}
        if not name:
            name = str(existing.get("name") or f"Preset {slot + 1}")
    if not name:
        name = f"Preset {slot + 1}"
    active_slot = data.get("activeSlot")
    try:
        active_int = int(active_slot) if active_slot is not None else slot
    except (TypeError, ValueError):
        active_int = slot
    try:
        state = save_preset_slot(
            user["email"],
            mode,
            slot,
            name,
            params,
            active_slot=active_int,
        )
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    return jsonify({"ok": True, **state})


@app.route("/api/presets/slot", methods=["DELETE"])
def api_presets_delete_slot():
    user, err = _account_user_or_403()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode") or "")
    try:
        slot = int(data.get("slot"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "Invalid slot"}), 400
    try:
        state = clear_preset_slot(user["email"], mode, slot)
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    return jsonify({"ok": True, **state})


@app.route("/api/presets/mode-params", methods=["PUT"])
def api_presets_mode_params():
    user, err = _account_user_or_403()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode") or "")
    params = data.get("params")
    if not isinstance(params, dict):
        return jsonify({"ok": False, "message": "Missing params"}), 400
    active_slot = data.get("activeSlot")
    active_int = None
    if active_slot is not None:
        try:
            active_int = int(active_slot)
        except (TypeError, ValueError):
            active_int = None
    try:
        state = save_mode_params(
            user["email"],
            mode,
            params,
            active_slot=active_int,
        )
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    return jsonify({"ok": True, **state})


@app.route("/api/presets/active-slot", methods=["PUT"])
def api_presets_active_slot():
    user, err = _account_user_or_403()
    if err:
        return err
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode") or "")
    active_slot = data.get("activeSlot")
    active_int = None
    if active_slot is not None:
        try:
            active_int = int(active_slot)
        except (TypeError, ValueError):
            active_int = None
    try:
        state = set_active_slot(user["email"], mode, active_int)
    except ValueError as e:
        return jsonify({"ok": False, "message": str(e)}), 400
    return jsonify({"ok": True, **state})


if __name__ == "__main__":
    # use_reloader=False so bot threads are not started twice
    app.run(debug=True, threaded=True, use_reloader=False, port=5000)
