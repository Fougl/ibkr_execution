#!/usr/bin/env python3
"""
executor.py
Flask webhook receiver on Lightsail. Receives TradingView webhook JSON (forwarded by Lambda),
verifies actual broker state (IBKR) before every action, enforces pre-close close + post-open re-open,
and logs all decisions for CloudWatch.

Expected webhook examples:
  {"alert": "Exit Long", "symbol": "MES1!"}
  {"alert": "Exit Short", "symbol": "MES1!"}

DynamoDB GLOBAL settings item:
{
  "PK": "GLOBAL",
  "SK": "SETTINGS",
  "delay_sec": 2,
  "pre_close_min": 10,
  "post_open_min": 5,
  "market_open": "09:30",
  "market_close": "16:00",
  "timezone": "America/New_York",
  "execution_delay": 2
}
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional, Tuple

import boto3
import pytz
from flask import Flask, jsonify, request

# ---- IBKR (ib_insync) ----
# pip install ib_insync
from ib_insync import IB, MarketOrder, Contract, Future  # type: ignore


# ---------------------------
# Config (ENV)
# ---------------------------
LOG_PATH = os.getenv("EXECUTOR_LOG_PATH", "/opt/ibc/execution/executor_webhooks.log")
STATE_PATH = os.getenv("EXECUTOR_STATE_PATH", "/opt/ibc/execution/executor_state.json")

DDB_TABLE = os.getenv("DDB_TABLE", "ankro-global-settings")
DDB_PK = os.getenv("DDB_PK", "GLOBAL")
DDB_SK = os.getenv("DDB_SK", "SETTINGS")
SETTINGS_CACHE_TTL_SEC = int(os.getenv("SETTINGS_CACHE_TTL_SEC", "10"))

# IBKR connection
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4001"))  # 4001/4002/4003 depending on your gateway config
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "77"))

# Web server
BIND_HOST = os.getenv("BIND_HOST", "0.0.0.0")
BIND_PORT = int(os.getenv("BIND_PORT", "5001"))

# Dedupe window for repeated alerts (since payload has no unique ID)
DEDUPE_TTL_SEC = int(os.getenv("DEDUPE_TTL_SEC", "15"))

# Retry behavior
MAX_STATE_CHECKS = int(os.getenv("MAX_STATE_CHECKS", "15"))  # checks when waiting for a close to reflect
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))

# Contract mapping (IMPORTANT)
# TradingView symbol "MES1!" is not an IB contract id. You must map it.
# Use an explicit map for production (contract details vary).
# Below is a safe placeholder for CME Micro E-mini S&P (MES). Adjust as needed.
SYMBOL_MAP_JSON = os.getenv("SYMBOL_MAP_JSON", "")  # optional JSON mapping override
DEFAULT_SYMBOL_MAP = {
    # TradingView : {type, exchange, currency, lastTradeDateOrContractMonth, localSymbol}
    # You should replace lastTradeDateOrContractMonth with the current front month, or use your own mapping logic.
    "MES1!": {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "MES", "lastTradeDateOrContractMonth": ""},
    "ES1!": {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "ES", "lastTradeDateOrContractMonth": ""},
}

# ---------------------------
# Logging
# ---------------------------
logger = logging.getLogger("executor")
logger.setLevel(logging.INFO)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
_handler = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=5)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)


# ---------------------------
# State + settings
# ---------------------------
@dataclass
class Settings:
    delay_sec: int = 2
    execution_delay: int = 2
    pre_close_min: int = 10
    post_open_min: int = 5
    market_open: str = "09:30"
    market_close: str = "16:00"
    timezone: str = "America/New_York"

    @staticmethod
    def from_ddb(item: Dict[str, Any]) -> "Settings":
        # item may be native types if using DynamoDB Table resource, or "S"/"N" typed if using client.
        def get_any(k: str, default: Any) -> Any:
            if k not in item:
                return default
            v = item[k]
            if isinstance(v, dict) and len(v) == 1:
                # DynamoDB client style: {"S": "..."} or {"N": "..."}
                t, raw = next(iter(v.items()))
                if t == "N":
                    try:
                        return int(raw)
                    except Exception:
                        return default
                if t == "S":
                    return raw
                return default
            return v

        return Settings(
            delay_sec=int(get_any("delay_sec", 2)),
            execution_delay=int(get_any("execution_delay", get_any("execution_delay:", 2))),
            pre_close_min=int(get_any("pre_close_min", 10)),
            post_open_min=int(get_any("post_open_min", 5)),
            market_open=str(get_any("market_open", "09:30")),
            market_close=str(get_any("market_close", "16:00")),
            timezone=str(get_any("timezone", "America/New_York")),
        )


class SettingsCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cached: Optional[Settings] = None
        self._cached_at: float = 0.0

    def get(self) -> Settings:
        now = time.time()
        with self._lock:
            if self._cached and (now - self._cached_at) < SETTINGS_CACHE_TTL_SEC:
                return self._cached
        s = load_settings_from_ddb()
        with self._lock:
            self._cached = s
            self._cached_at = now
        return s


settings_cache = SettingsCache()


class DedupeCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seen: Dict[str, float] = {}

    def seen_recently(self, key: str) -> bool:
        now = time.time()
        with self._lock:
            # expire
            for k, ts in list(self._seen.items()):
                if now - ts > DEDUPE_TTL_SEC:
                    self._seen.pop(k, None)
            if key in self._seen:
                return True
            self._seen[key] = now
            return False


dedupe_cache = DedupeCache()


def load_settings_from_ddb() -> Settings:
    try:
        ddb = boto3.client("dynamodb")
        resp = ddb.get_item(
            TableName=DDB_TABLE,
            Key={"PK": {"S": DDB_PK}, "SK": {"S": DDB_SK}},
            ConsistentRead=True,
        )
        item = resp.get("Item", {})
        if not item:
            logger.warning("DynamoDB settings not found; using defaults.")
            return Settings()
        return Settings.from_ddb(item)
    except Exception as e:
        logger.exception(f"Failed to load DynamoDB settings; using defaults. err={e}")
        return Settings()


def load_state() -> Dict[str, Any]:
    try:
        if not os.path.exists(STATE_PATH):
            return {}
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.exception(f"Failed reading state file; starting empty. err={e}")
        return {}


def save_state(state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        logger.exception(f"Failed writing state file. err={e}")


# ---------------------------
# Market time logic
# ---------------------------
def parse_hhmm(s: str) -> Tuple[int, int]:
    parts = s.strip().split(":")
    return int(parts[0]), int(parts[1])


def market_datetimes(now_local: datetime, settings: Settings) -> Tuple[datetime, datetime, datetime, datetime]:
    """
    Returns: (open_dt, close_dt, preclose_dt, reopen_dt)
    open_dt/close_dt are for today's date in market timezone.
    preclose_dt = close_dt - pre_close_min
    reopen_dt = open_dt + post_open_min
    """
    tz = pytz.timezone(settings.timezone)
    d = now_local.date()
    oh, om = parse_hhmm(settings.market_open)
    ch, cm = parse_hhmm(settings.market_close)
    open_dt = tz.localize(datetime(d.year, d.month, d.day, oh, om, 0))
    close_dt = tz.localize(datetime(d.year, d.month, d.day, ch, cm, 0))
    preclose_dt = close_dt - timedelta(minutes=settings.pre_close_min)
    reopen_dt = open_dt + timedelta(minutes=settings.post_open_min)
    return open_dt, close_dt, preclose_dt, reopen_dt


def now_in_market_tz(settings: Settings) -> datetime:
    tz = pytz.timezone(settings.timezone)
    return datetime.now(tz)


# ---------------------------
# IBKR helpers
# ---------------------------
_ib = IB()
_ib_lock = threading.Lock()


def ib_connect_if_needed() -> bool:
    with _ib_lock:
        if _ib.isConnected():
            return True
        try:
            _ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=3.0)
            return _ib.isConnected()
        except Exception as e:
            logger.warning(f"IB connect failed: {e}")
            return False


def get_symbol_map() -> Dict[str, Dict[str, str]]:
    if SYMBOL_MAP_JSON.strip():
        try:
            return json.loads(SYMBOL_MAP_JSON)
        except Exception:
            logger.warning("Invalid SYMBOL_MAP_JSON; using default mapping.")
    return DEFAULT_SYMBOL_MAP


def build_contract(tv_symbol: str) -> Contract:
    m = get_symbol_map()
    if tv_symbol not in m:
        raise ValueError(f"Unknown symbol mapping for '{tv_symbol}'. Configure SYMBOL_MAP_JSON or DEFAULT_SYMBOL_MAP.")
    info = m[tv_symbol]
    if info.get("secType") == "FUT":
        symbol = info.get("symbol", "")
        exchange = info.get("exchange", "CME")
        currency = info.get("currency", "USD")
        ltm = info.get("lastTradeDateOrContractMonth", "")  # empty is risky; set this properly in prod
        c = Future(symbol=symbol, lastTradeDateOrContractMonth=ltm, exchange=exchange, currency=currency)
        return c
    raise ValueError(f"Unsupported secType for {tv_symbol}: {info.get('secType')}")


def current_position_qty(contract: Contract) -> int:
    """
    Returns net position quantity for that contract (positive long, negative short, 0 flat).
    """
    with _ib_lock:
        positions = _ib.positions()
    qty = 0
    for p in positions:
        try:
            # match by conId if available; else by symbol/exchange/etc after qualification
            if getattr(p.contract, "conId", None) and getattr(contract, "conId", None):
                if p.contract.conId == contract.conId:
                    qty += int(p.position)
            else:
                # fallback match
                if p.contract.symbol == getattr(contract, "symbol", None) and p.contract.secType == contract.secType:
                    qty += int(p.position)
        except Exception:
            continue
    return qty


def close_position(contract: Contract, qty: int) -> None:
    """
    If qty>0 => sell qty to close long. If qty<0 => buy abs(qty) to close short.
    """
    if qty == 0:
        return
    action = "SELL" if qty > 0 else "BUY"
    size = abs(int(qty))
    order = MarketOrder(action, size)
    with _ib_lock:
        _ib.placeOrder(contract, order)


def open_position(contract: Contract, direction: int, qty: int) -> None:
    """
    direction: +1 long, -1 short
    """
    if direction not in (+1, -1):
        raise ValueError("direction must be +1 (long) or -1 (short)")
    size = abs(int(qty))
    action = "BUY" if direction > 0 else "SELL"
    order = MarketOrder(action, size)
    with _ib_lock:
        _ib.placeOrder(contract, order)


def wait_until_flat(contract: Contract, settings: Settings) -> bool:
    """
    Poll positions until flat or timeout. Uses settings.delay_sec between checks.
    """
    for i in range(MAX_STATE_CHECKS):
        qty = current_position_qty(contract)
        if qty == 0:
            return True
        logger.info(f"Waiting for close to reflect (attempt {i+1}/{MAX_STATE_CHECKS}), qty still {qty}")
        time.sleep(max(1, int(settings.delay_sec)))
    return False


# ---------------------------
# Signal parsing
# ---------------------------
@dataclass
class Signal:
    symbol: str
    # desired_direction: +1 long, -1 short, 0 = exit/flat
    desired_direction: int
    # desired_qty: for entries; for exits we ignore
    desired_qty: int
    raw_alert: str


def parse_signal(payload: Dict[str, Any]) -> Signal:
    alert = str(payload.get("alert", "")).strip()
    symbol = str(payload.get("symbol", "")).strip()
    if not symbol:
        raise ValueError("Missing 'symbol' in payload")
    if not alert:
        raise ValueError("Missing 'alert' in payload")

    a = alert.lower()

    # Optional qty (if you ever add it to TV)
    qty = payload.get("qty", payload.get("quantity", DEFAULT_QTY))
    try:
        qty_i = int(qty)
    except Exception:
        qty_i = DEFAULT_QTY

    # Exit signals
    if "exit" in a and "long" in a:
        return Signal(symbol=symbol, desired_direction=0, desired_qty=0, raw_alert=alert)
    if "exit" in a and "short" in a:
        return Signal(symbol=symbol, desired_direction=0, desired_qty=0, raw_alert=alert)

    # Entry signals (supported)
    if ("enter" in a and "long" in a) or a in ("long", "buy", "enter long"):
        return Signal(symbol=symbol, desired_direction=+1, desired_qty=qty_i, raw_alert=alert)
    if ("enter" in a and "short" in a) or a in ("short", "sell", "enter short"):
        return Signal(symbol=symbol, desired_direction=-1, desired_qty=qty_i, raw_alert=alert)

    # If client uses different wording, adjust here.
    raise ValueError(f"Unrecognized alert format: '{alert}'")


# ---------------------------
# Pre-close / post-open state machine
# ---------------------------
def state_key_for_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def ensure_preclose_close_if_needed(settings: Settings) -> None:
    """
    If we're past preclose_dt and not yet processed today, close all positions and store snapshot in state file.
    """
    now_local = now_in_market_tz(settings)
    open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)
    if now_local < preclose_dt or now_local >= close_dt:
        return

    st = load_state()
    dayk = state_key_for_day(now_local.date())
    preclose_done = st.get("preclose", {}).get(dayk, {}).get("done", False)
    if preclose_done:
        return

    if not ib_connect_if_needed():
        logger.warning("Preclose: IB not connected; will not close positions.")
        return

    # snapshot positions
    snapshot = {}
    with _ib_lock:
        pos = _ib.positions()
    for p in pos:
        if int(p.position) != 0:
            snapshot[str(getattr(p.contract, "conId", "")) or f"{p.contract.secType}:{p.contract.symbol}:{p.contract.exchange}"] = {
                "secType": p.contract.secType,
                "symbol": p.contract.symbol,
                "exchange": getattr(p.contract, "exchange", ""),
                "currency": getattr(p.contract, "currency", ""),
                "lastTradeDateOrContractMonth": getattr(p.contract, "lastTradeDateOrContractMonth", ""),
                "position": int(p.position),
            }

    # close all positions
    for p in pos:
        q = int(p.position)
        if q == 0:
            continue
        logger.info(f"Preclose: closing position {p.contract.secType} {p.contract.symbol} qty={q}")
        close_position(p.contract, q)

    # record state
    st.setdefault("preclose", {})
    st["preclose"][dayk] = {
        "done": True,
        "at": now_local.isoformat(),
        "snapshot": snapshot,
        "reopen_done": False,
        "reopen_at": None,
    }
    save_state(st)
    logger.info(f"Preclose: completed. Snapshot count={len(snapshot)}")


def ensure_postopen_reopen_if_needed(settings: Settings) -> None:
    """
    If we're past reopen_dt and preclose snapshot exists and reopen not done, re-open previous position(s)
    ONLY if broker state is flat and verified.
    """
    now_local = now_in_market_tz(settings)
    open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)
    if now_local < reopen_dt or now_local >= close_dt:
        return

    st = load_state()
    dayk = state_key_for_day(now_local.date())
    pre = st.get("preclose", {}).get(dayk)
    if not pre or not pre.get("done"):
        return
    if pre.get("reopen_done"):
        return

    snapshot: Dict[str, Any] = pre.get("snapshot", {})
    if not snapshot:
        st["preclose"][dayk]["reopen_done"] = True
        st["preclose"][dayk]["reopen_at"] = now_local.isoformat()
        save_state(st)
        logger.info("Postopen: nothing to reopen (empty snapshot).")
        return

    if not ib_connect_if_needed():
        logger.warning("Postopen: IB not connected; cannot reopen.")
        return

    # Only reopen if currently flat (strict rule)
    with _ib_lock:
        pos = _ib.positions()
    any_open = any(int(p.position) != 0 for p in pos)
    if any_open:
        logger.info("Postopen: broker not flat; will not reopen. (strict one-to-one)")
        return

    # Reopen all snapshot positions
    for _, meta in snapshot.items():
        try:
            secType = meta.get("secType")
            if secType != "FUT":
                logger.info(f"Postopen: skipping non-FUT snapshot item: {meta}")
                continue
            c = Future(
                symbol=meta.get("symbol", ""),
                lastTradeDateOrContractMonth=meta.get("lastTradeDateOrContractMonth", ""),
                exchange=meta.get("exchange", ""),
                currency=meta.get("currency", "USD"),
            )
            direction = +1 if int(meta.get("position", 0)) > 0 else -1
            qty = abs(int(meta.get("position", 0)))
            logger.info(f"Postopen: reopening {c.symbol} dir={'LONG' if direction>0 else 'SHORT'} qty={qty}")
            open_position(c, direction, qty)
            time.sleep(max(1, int(settings.execution_delay)))
        except Exception as e:
            logger.exception(f"Postopen: failed reopening snapshot item meta={meta} err={e}")

    st["preclose"][dayk]["reopen_done"] = True
    st["preclose"][dayk]["reopen_at"] = now_local.isoformat()
    save_state(st)
    logger.info("Postopen: completed reopen cycle.")


def background_market_manager() -> None:
    """
    Runs continuously; enforces preclose close and postopen reopen based on time,
    even when no webhooks arrive.
    """
    while True:
        try:
            s = settings_cache.get()
            ensure_preclose_close_if_needed(s)
            ensure_postopen_reopen_if_needed(s)
        except Exception as e:
            logger.exception(f"Market manager loop error: {e}")
        time.sleep(30)  # check twice per minute


# ---------------------------
# Main webhook execution logic
# ---------------------------
def in_trading_window(now_local: datetime, settings: Settings) -> bool:
    open_dt, close_dt, _, _ = market_datetimes(now_local, settings)
    return open_dt <= now_local <= close_dt


def within_preclose_window(now_local: datetime, settings: Settings) -> bool:
    _, close_dt, preclose_dt, _ = market_datetimes(now_local, settings)
    return preclose_dt <= now_local < close_dt


def webhook_dedupe_key(payload: Dict[str, Any]) -> str:
    # Best-effort because payload has no unique id/time.
    # If you later include "id" or "timestamp" from TradingView, use that instead.
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def handle_signal(sig: Signal, settings: Settings) -> Dict[str, Any]:
    now_local = now_in_market_tz(settings)

    # Enforce preclose close (and snapshot) even if no webhooks
    ensure_preclose_close_if_needed(settings)

    if not in_trading_window(now_local, settings):
        logger.info(f"Ignored: outside market window. now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
        return {"ignored": True, "reason": "outside_market_hours"}

    if within_preclose_window(now_local, settings):
        # Strict: do not open anything near close; client wants risk avoided.
        logger.info(f"Ignored: within preclose window. now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
        return {"ignored": True, "reason": "within_preclose_window"}

    if not ib_connect_if_needed():
        logger.warning("IB not connected; not proceeding.")
        return {"ok": False, "reason": "ib_not_connected"}

    # Build and qualify contract
    contract = build_contract(sig.symbol)
    with _ib_lock:
        # qualification resolves conId; improves position matching
        _ib.qualifyContracts(contract)

    # Verify current state
    qty = current_position_qty(contract)
    logger.info(f"State before: symbol={sig.symbol} alert={sig.raw_alert} current_qty={qty}")

    # Exit signal: close if position exists (any direction)
    if sig.desired_direction == 0:
        if qty == 0:
            logger.info("Exit requested but already flat. No action.")
            return {"ok": True, "action": "none_already_flat"}

        logger.info(f"Exit requested. Closing qty={qty}")
        close_position(contract, qty)
        time.sleep(max(1, int(settings.execution_delay)))

        if not wait_until_flat(contract, settings):
            logger.warning("Exit close not confirmed; will not proceed further.")
            return {"ok": False, "action": "exit_pending_not_confirmed"}

        logger.info("Exit confirmed flat.")
        return {"ok": True, "action": "exit_closed"}

    # Entry/reversal logic
    desired_dir = sig.desired_direction
    desired_qty = sig.desired_qty if sig.desired_qty > 0 else DEFAULT_QTY

    # If already in same direction, do nothing (one-to-one)
    if qty != 0 and ((qty > 0 and desired_dir > 0) or (qty < 0 and desired_dir < 0)):
        logger.info("Same-direction signal received but position already open; doing nothing.")
        return {"ok": True, "action": "none_same_direction_already_open", "current_qty": qty}

    # If opposite direction exists -> close first, verify closed, then open
    if qty != 0 and ((qty > 0 and desired_dir < 0) or (qty < 0 and desired_dir > 0)):
        logger.info(f"Reversal: closing existing qty={qty} before opening new dir={desired_dir} qty={desired_qty}")
        close_position(contract, qty)
        time.sleep(max(1, int(settings.execution_delay)))

        if not wait_until_flat(contract, settings):
            logger.warning("Reversal close not confirmed; will not open new position.")
            return {"ok": False, "action": "reversal_close_not_confirmed"}

        # Extra safety delay between exit and entry
        time.sleep(max(1, int(settings.delay_sec)))

        logger.info(f"Reversal: now flat confirmed. Opening new dir={desired_dir} qty={desired_qty}")
        open_position(contract, desired_dir, desired_qty)
        return {"ok": True, "action": "reversal_opened", "opened_dir": desired_dir, "opened_qty": desired_qty}

    # If flat -> open directly
    if qty == 0:
        logger.info(f"Flat: opening new position dir={desired_dir} qty={desired_qty}")
        time.sleep(max(0, int(settings.execution_delay)))
        open_position(contract, desired_dir, desired_qty)
        return {"ok": True, "action": "opened", "opened_dir": desired_dir, "opened_qty": desired_qty}

    # Should not reach
    logger.warning(f"Ambiguous state; not proceeding. qty={qty} desired_dir={desired_dir}")
    return {"ok": False, "action": "ambiguous_state"}


# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)


@app.get("/health")
def health() -> Any:
    ok = ib_connect_if_needed()
    return jsonify({"ok": True, "ib_connected": ok})


@app.post("/webhook")
def webhook() -> Any:
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    key = webhook_dedupe_key(payload)
    if dedupe_cache.seen_recently(key):
        logger.info(f"Deduped duplicate webhook (within {DEDUPE_TTL_SEC}s). payload={payload}")
        return jsonify({"ok": True, "deduped": True}), 200

    try:
        settings = settings_cache.get()
        sig = parse_signal(payload)
        result = handle_signal(sig, settings)
        logger.info(f"Result: {result} payload={payload}")
        status = 200 if result.get("ok", True) else 503
        return jsonify(result), status
    except Exception as e:
        logger.exception(f"Webhook handling failed. payload={payload} err={e}")
        return jsonify({"ok": False, "error": str(e)}), 400


def start_background_threads() -> None:
    t = threading.Thread(target=background_market_manager, name="market_manager", daemon=True)
    t.start()


if __name__ == "__main__":
    logger.info(f"Starting executor Flask on {BIND_HOST}:{BIND_PORT} log={LOG_PATH}")
    start_background_threads()
    app.run(host=BIND_HOST, port=BIND_PORT)
