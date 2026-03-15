#!/usr/bin/env python3
"""
executor.py (multi-account, parallel)

- Flask webhook receiver on Lightsail.
- On EVERY webhook:
  - loads GLOBAL market settings from DynamoDB (PK=GLOBAL, SK=SETTINGS)
  - lists all Secrets Manager secrets whose name contains "ibkr" (case-insensitive)
    (AWS region resolved from instance via boto3.Session().region_name)
  - for EACH account secret:
      - derives IB API port = 4002 + short_name
      - derives IB clientId = IB_CLIENT_ID_BASE + short_name
      - connects to that gateway and executes the signal
  - executions happen IN PARALLEL across accounts (ThreadPoolExecutor)

Expected webhook examples:
  {"alert": "Exit Long", "symbol": "MES1!"
  {"alert": "Exit Short", "symbol": "MES1!"}
Optional:
  {"alert": "Enter Long", "symbol": "MES1!", "qty": 2}

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

Secret JSON schema (minimum):
{
  "short_name": "37",
  "username": "...",
  "password": "...",
  "account_type": "live"
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
from datetime import datetime, date, timedelta, time as dtime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pytz
from flask import Flask, jsonify, request
from ib_insync import IB, MarketOrder, Contract, Future, StopOrder, LimitOrder, BracketOrder  # type: ignore
import asyncio
from decimal import Decimal, ROUND_HALF_UP


# ---------------------------
# Config (ENV)
# ---------------------------
LOG_PATH = os.getenv("EXECUTOR_LOG_PATH", "/opt/ibc/execution/executor.log")
TRADES_LOG_PATH = os.getenv("EXECUTOR_TRADES_LOG_PATH", "")
STATE_PATH = os.getenv("EXECUTOR_STATE_PATH",
                       "/opt/ibc/execution/executor_state.json")
DERIVED_ID = int(os.getenv("DERIVED_ID"))
ACCOUNT_SHORT_NAME = os.getenv("ACCOUNT_SHORT_NAME")

# DynamoDB settings
# DDB_TABLE = os.getenv("DDB_TABLE", "ankro-global-settings")
# DDB_PK = os.getenv("DDB_PK", "GLOBAL")
# DDB_SK = os.getenv("DDB_SK", "SETTINGS")
SETTINGS_CACHE_TTL_SEC = int(os.getenv("SETTINGS_CACHE_TTL_SEC", "240"))


# IBKR connection base rules (match your orchestrator rule)
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
# port = base + short_name
IB_API_PORT_BASE = int(os.getenv("IB_API_PORT_BASE", "4002"))
# clientId = base + short_name
IB_CLIENT_ID_BASE = int(os.getenv("IB_CLIENT_ID_BASE", "1000"))

# Web server
BIND_HOST = os.getenv("BIND_HOST", "0.0.0.0")
BIND_PORT = int(os.getenv("BIND_PORT", "5001"))

# Dedupe window
DEDUPE_TTL_SEC = int(os.getenv("DEDUPE_TTL_SEC", "15"))

# Retry behavior
MAX_STATE_CHECKS = int(os.getenv("MAX_STATE_CHECKS", "15"))
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))


IB_CONNECT_TIMEOUT_SEC = float(os.getenv("IB_CONNECT_TIMEOUT_SEC", "3.0"))

# Contract mapping
SYMBOL_MAP_JSON = os.getenv("SYMBOL_MAP_JSON", "")
DEFAULT_SYMBOL_MAP = {
    "MES1!": {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "MES", "lastTradeDateOrContractMonth": "", "localSymbol": "MESH6"},
    "ES1!":  {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "ES",  "lastTradeDateOrContractMonth": "", "localSymbol": "ESH6"},
}


EXECUTOR_START_TIME = time.time()
CONNECTION_GRACE_SEC = 45  # seconds to suppress startup alarms

_MIN_TICK_CACHE: dict[str, float] = {}
_MIN_TICK_LOCK = threading.Lock()

# Trading-hours schedules (GLOBAL — shared across accounts on the host)
SYMBOL_SCHEDULE_PATH = os.path.join(
    os.path.dirname(STATE_PATH),
    "symbol_schedules.json"
)
_symbol_schedule_lock = threading.Lock()


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def register_symbol_usage_from_signal(sig: "Signal") -> None:
    """
    From the webhook thread: ensure we have trading-hours data in
    symbol_schedules.json for the current day for this IB-mapped symbol.
    No separate known_symbols.json is maintained anymore.
    """
    local_symbol = getattr(sig, "symbol", None)
    exchange = getattr(sig, "exchange", None)

    if not local_symbol:
        return

    # Minimal contract info needed to request ContractDetails
    entry: dict = {
        "localSymbol": local_symbol,
        "exchange": exchange or "CME",
        "secType": "FUT",
        "currency": "USD",
    }

    # Only fetch trading hours if we don't already have an entry for *today*
    # for this symbol in symbol_schedules.json.
    today_key = datetime.utcnow().date().isoformat()
    need_fetch = True

    with _symbol_schedule_lock:
        try:
            if os.path.exists(SYMBOL_SCHEDULE_PATH):
                with open(SYMBOL_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                    sched_all = json.load(f)
                sym_sched = sched_all.get(local_symbol, {})
                if today_key in sym_sched:
                    need_fetch = False
        except Exception:
            # On any error, fall back to fetching
            need_fetch = True

    if not need_fetch or IB_LOOP is None or not IB_READY.is_set():
        return

    try:
        async def _fetch_one():
            sched = await _fetch_trading_hours_for_symbol(local_symbol, entry)
            return {local_symbol: sched} if sched else {}

        fut = asyncio.run_coroutine_threadsafe(_fetch_one(), IB_LOOP)
        new_data = fut.result(timeout=30)
        if new_data:
            _merge_symbol_schedules(new_data)
            logger.info("[TRADING_HOURS] Filled hours for symbol %s via webhook", local_symbol)
        else:
            logger.info("[TRADING_HOURS] No tradingHours data yet for symbol %s", local_symbol)
    except Exception as e:
        logger.warning("[TRADING_HOURS] Failed to fill hours for symbol %s: %s", local_symbol, e)


def _merge_symbol_schedules(new_data: dict) -> None:
    with _symbol_schedule_lock:
        try:
            if os.path.exists(SYMBOL_SCHEDULE_PATH):
                with open(SYMBOL_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            else:
                existing = {}
        except Exception:
            existing = {}

        for sym, sched in new_data.items():
            existing.setdefault(sym, {}).update(sched)

        _atomic_write_json(SYMBOL_SCHEDULE_PATH, existing)


# Resolve AWS region ONCE at startup
try:
    _SESSION = boto3.Session()
    REGION = _SESSION.region_name
    if not REGION:
        raise RuntimeError("Could not resolve AWS region at startup")
except Exception as e:
    print(f"FATAL: Could not detect AWS region: {e}")
    raise


os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.getLogger("ib_insync").setLevel(logging.ERROR)
logging.getLogger("ibapi").setLevel(logging.ERROR)
logger = logging.getLogger()  # CLEAN: no redundant logger name prefix
logger.setLevel(logging.INFO)

handler = logging.FileHandler(LOG_PATH, mode="a")
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)

logger.handlers = [handler]    # IMPORTANT: removes stdout handler
logger.propagate = False

IB_LOCK = threading.Lock()
IB_INSTANCE: IB | None = None
IB_READY = threading.Event()
IB_LOOP = None
import signal
import sys
import atexit

_shutdown_called = False

def _ib_disconnect():
    if IB_INSTANCE and IB_INSTANCE.isConnected():
        IB_INSTANCE.disconnect()

def shutdown_handler(signum=None, frame=None):
    global _shutdown_called

    if _shutdown_called:
        return
    _shutdown_called = True

    logger.info("[IB] shutdown requested")

    try:
        if IB_INSTANCE and IB_LOOP:
            future = asyncio.run_coroutine_threadsafe(
                asyncio.to_thread(_ib_disconnect),
                IB_LOOP
            )
            future.result(timeout=3)

            # stop the asyncio loop cleanly
            IB_LOOP.call_soon_threadsafe(IB_LOOP.stop)

            logger.info("[IB] disconnected cleanly")

    except Exception as e:
        logger.error(f"[IB] disconnect error: {e}")

    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)
atexit.register(shutdown_handler)


async def ib_connect_persistent():
    """
    Connect to IB gateway. Retries until connected or 2 minutes have elapsed.
    """
    global IB_INSTANCE

    port = 4002 + DERIVED_ID
    cid = 1 + DERIVED_ID
    attempt = 0
    start_time = time.time()
    connect_timeout_sec = 120  # 2 minutes

    while True:
        attempt += 1
        ib = IB()
        try:
            await ib.connectAsync(
                "127.0.0.1",
                port,
                clientId=cid,
                timeout=5
            )
            IB_INSTANCE = ib
            IB_READY.set()
            logger.info("[IB] Persistent async connection established (attempt %d)", attempt)
            return
        except Exception as e:
            try:
                if ib.isConnected():
                    ib.disconnect()
            except Exception:
                pass
            elapsed = time.time() - start_time
            if elapsed >= connect_timeout_sec:
                logger.info(
                    "[ALARM] IB still not connected after %.0f seconds (%d attempts); stopping connect retries",
                    elapsed, attempt
                )
                return
            logger.info(
                "[IB] connection attempt %d failed (gateway may still be starting): %s",
                attempt, e
            )
            await asyncio.sleep(10)


# Used by watchdog to schedule reconnect on IB_LOOP; guard to avoid overlapping reconnects.
_ib_reconnect_in_progress = False
_ib_reconnect_lock = threading.Lock()


async def _ib_reconnect_async():
    """
    Run on IB_LOOP: disconnect current connection (if any), then reconnect.
    Called from watchdog via asyncio.run_coroutine_threadsafe(..., IB_LOOP).
    """
    global IB_INSTANCE, _ib_reconnect_in_progress
    with _ib_reconnect_lock:
        if _ib_reconnect_in_progress:
            return
        _ib_reconnect_in_progress = True
    try:
        # Disconnect and clear state (we are on the thread that owns the connection)
        if IB_INSTANCE is not None:
            try:
                if IB_INSTANCE.isConnected():
                    IB_INSTANCE.disconnect()
            except Exception as e:
                logger.warning("[IB] disconnect before reconnect: %s", e)
            IB_READY.clear()
            IB_INSTANCE = None

        port = 4002 + DERIVED_ID
        cid = 1 + DERIVED_ID
        for attempt in range(1, 4):
            ib = IB()
            try:
                await ib.connectAsync(
                    "127.0.0.1",
                    port,
                    clientId=cid,
                    timeout=5
                )
                IB_INSTANCE = ib
                IB_READY.set()
                logger.info("[IB] Reconnect successful (attempt %d)", attempt)
                return
            except Exception as e:
                try:
                    if ib.isConnected():
                        ib.disconnect()
                except Exception:
                    pass
                logger.warning("[IB] reconnect attempt %d failed: %s", attempt, e)
                if attempt < 3:
                    await asyncio.sleep(5)
        logger.error("[IB] Reconnect failed after 3 attempts")
    finally:
        with _ib_reconnect_lock:
            _ib_reconnect_in_progress = False


def start_ib():
    logger.info("[IB] starting async connection thread")
    global IB_LOOP
    IB_LOOP = asyncio.new_event_loop()
    def runner():
        asyncio.set_event_loop(IB_LOOP)
    
        try:
            IB_LOOP.run_until_complete(ib_connect_persistent())
            IB_LOOP.run_forever()
        except Exception as e:
            logger.error(f"[IB] connection failed: {e}")

    threading.Thread(target=runner, daemon=True).start()

def run_ib(coro, timeout=10):
    future = asyncio.run_coroutine_threadsafe(coro, IB_LOOP)
    return future.result(timeout=timeout)

def ib_connection_watchdog():
    last_logged_state = None
    last_disconnect_reminder = 0.0
    last_reconnect_scheduled = 0.0
    DISCONNECT_REMINDER_INTERVAL = 60.0  # log "still disconnected" and retry reconnect every 60s
    RECONNECT_COOLDOWN = 65.0  # don't schedule reconnect more often than this (s)

    while True:
        try:
            connected = (
                IB_INSTANCE is not None
                and IB_READY.is_set()
                and IB_INSTANCE.isConnected()
            )

            now = time.time()

            # ---- ignore alarms during startup grace period ----
            if now - EXECUTOR_START_TIME < 120:
                last_logged_state = connected
                time.sleep(5)
                continue
            # ---------------------------------------------------

            if connected:
                if last_logged_state is not True:
                    #logger.info("[IB] connection healthy")
                    last_logged_state = True
            else:
                if last_logged_state is not False:
                    logger.error("[ALARM] IB disconnected")
                    last_logged_state = False
                    last_disconnect_reminder = now
                    last_reconnect_scheduled = 0.0  # allow immediate reconnect on transition

                # Schedule reconnect on IB_LOOP (async); respect cooldown and avoid overlapping
                if IB_LOOP is not None and (now - last_reconnect_scheduled) >= RECONNECT_COOLDOWN:
                    with _ib_reconnect_lock:
                        if not _ib_reconnect_in_progress:
                            try:
                                asyncio.run_coroutine_threadsafe(
                                    _ib_reconnect_async(), IB_LOOP
                                )
                                last_reconnect_scheduled = now
                                logger.info("[IB] Watchdog scheduled reconnect on event loop")
                            except Exception as e:
                                logger.warning("[IB] Watchdog failed to schedule reconnect: %s", e)

                if now - last_disconnect_reminder >= DISCONNECT_REMINDER_INTERVAL:
                    logger.warning("[ALARM] IB still disconnected (watchdog check)")
                    last_disconnect_reminder = now

        except Exception as e:
            logger.error(f"[ALARM] IB watchdog error: {e}")
            last_logged_state = "error"

        time.sleep(5)

async def ib_place_order(contract, order):
    return IB_INSTANCE.placeOrder(contract, order)


async def _ib_cancel_all_open_orders(symbol=None, contract=None):
    """Runs on IB_LOOP: fetch open trades, filter, cancel each. Returns count of trades considered."""
    trades = IB_INSTANCE.openTrades()
    for t in trades:
        o = getattr(t, "order", None)
        if not o:
            continue
        c = t.contract
        if contract is not None:
            if getattr(c, "conId", None) != getattr(contract, "conId", None):
                continue
        elif symbol is not None:
            if getattr(c, "localSymbol", None) != symbol and getattr(c, "symbol", None) != symbol:
                continue
        if not t.isDone():
            IB_INSTANCE.cancelOrder(o)
    return len(trades)


def disconnect_ib(ib: IB):
    try:
        if ib and ib.isConnected():
            ib.disconnect()
    except:
        pass


_account_logs = {}       # { short_name: [str, str, ...] }
_account_logs_lock = threading.Lock()


def log_step(text: str):
    """
    Append a line to that account's log buffer.
    """
    with _account_logs_lock:
        if ACCOUNT_SHORT_NAME not in _account_logs:
            _account_logs[ACCOUNT_SHORT_NAME] = []
        _account_logs[ACCOUNT_SHORT_NAME].append(text)


def flush_account_log(header: str):
    try:
        lines = _account_logs.get(ACCOUNT_SHORT_NAME, [])
        if not lines:
            return
        #f"[=== {header} acct={ACCOUNT_SHORT_NAME} BEGIN ===]\n"
        msg = (
            # HEADER LINE                                     # <-- REQUIRED BLANK LINE
            f"[=== {header}===]\n"
            # BODY INDENTED (so CW hides it)
            + "\n".join(" " + ln for ln in lines)
        )
        logger.info(msg)

    finally:
        _account_logs[ACCOUNT_SHORT_NAME] = []


def log_trade_event(obj: Dict[str, Any]) -> None:
    """
    Append one JSON line to the account's trades log (executor-{broker}-{short_name}-trades.log).
    Each line starts with '{' so CloudWatch multiline groups one event per line.
    """
    if not TRADES_LOG_PATH:
        return
    try:
        os.makedirs(os.path.dirname(TRADES_LOG_PATH), exist_ok=True)
        with open(TRADES_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
            f.flush()
    except Exception as e:
        logger.warning("Failed to write trades log: %s", e)


# ---------------------------
def resolved_region() -> str:
    # INFO: region resolution
    session = boto3.Session()
    rn = session.region_name
    logger.info(f"[AWS] Resolved region={rn}")
    if not rn:
        raise RuntimeError(
            "AWS region could not be resolved from environment / metadata")
    return rn
# ---------------------------
# Settings (DynamoDB)
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
        def get_any(k: str, default: Any) -> Any:
            if k not in item:
                return default
            v = item[k]
            if isinstance(v, dict) and len(v) == 1:
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

        # support both delay_sec/execution_delay keys (your screenshot has both delay_sec and execution_delay)
        exec_delay = get_any("execution_delay", get_any("execution_delay:", 2))

        return Settings(
            delay_sec=int(get_any("delay_sec", 2)),
            execution_delay=int(exec_delay),
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
        s = load_settings_from_ssm()
        with self._lock:
            self._cached = s
            self._cached_at = now
        return s


settings_cache = SettingsCache()


_market_times_lock = threading.Lock()

_market_times = {
    "prev_open": None,
    "prev_close": None,
    "next_open": None,
    "next_close": None,
    "prev_preclose": None,
    "next_preclose": None,
    "prev_postopen": None,
    "next_postopen": None
}


# PARAM_PATHS = [
#     "/ankro/settings/delay_sec",
#     "/ankro/settings/execution_delay",
#     "/ankro/settings/pre_close_min",
#     "/ankro/settings/post_open_min",
#     "/ankro/settings/market_open",
#     "/ankro/settings/market_close",
#     "/ankro/settings/timezone",
# ]

def load_settings_from_ssm() -> "Settings":
    ssm = boto3.client("ssm", region_name=REGION)

    try:
        resp = ssm.get_parameter(
            Name="/ankro/settings",
            WithDecryption=False
        )
        raw = resp["Parameter"]["Value"]
        data = json.loads(raw)
        # log_step(0, f"[SSM] Loaded JSON settings: {data}")

    except Exception as e:
        log_step(f"[SSM ERROR] Failed loading /ankro/settings: {e}")
        data = {}

    def get(key, default):
        return data.get(key, default)

    return Settings(
        delay_sec=int(get("delay_sec", 2)),
        execution_delay=int(get("execution_delay", 2)),
        pre_close_min=int(get("pre_close_min", 10)),
        post_open_min=int(get("post_open_min", 5)),
        market_open=str(get("market_open", "09:30")),
        market_close=str(get("market_close", "16:00")),
        timezone=str(get("timezone", "America/New_York")),
    )


# ---------------------------
# State (for preclose/postopen snapshots) — now per account
# ---------------------------
_state_lock = threading.Lock()


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


def state_key_for_day(d: date) -> str:
    return d.strftime("%Y-%m-%d")


# ---------------------------
# Market time logic
# ---------------------------
def parse_hhmm(s: str) -> Tuple[int, int]:
    parts = s.strip().split(":")
    return int(parts[0]), int(parts[1])


def now_in_market_tz(settings: Settings) -> datetime:
    tz = pytz.timezone(settings.timezone)
    return datetime.now(tz)


def market_datetimes(now_local: datetime, settings: Settings):
    tz = pytz.timezone(settings.timezone)
    d = now_local.date()

    # Parse HH:MM
    oh, om = parse_hhmm(settings.market_open)
    ch, cm = parse_hhmm(settings.market_close)

    open_dt = tz.localize(datetime(d.year, d.month, d.day, oh, om))
    close_dt = tz.localize(datetime(d.year, d.month, d.day, ch, cm))
    
    preclose_dt = close_dt - timedelta(minutes=settings.pre_close_min)
    reopen_dt = open_dt + timedelta(minutes=settings.post_open_min)


    if now_local < reopen_dt:
        open_dt = open_dt - timedelta(days=1)
        reopen_dt = reopen_dt - timedelta(days=1)
    
    if now_local < preclose_dt:
        close_dt = close_dt - timedelta(days=1)
        preclose_dt = preclose_dt - timedelta(days=1)


    

    # FINAL LOGGING
    # logger.info(
    #     f"[DEBUG/MH] FINAL window: open_dt={open_dt.isoformat()}  "
    #     f"close_dt={close_dt.isoformat()} "
    #     f"preclose_dt={preclose_dt.isoformat()} "
    #     f"reopen_dt={reopen_dt.isoformat()}"
    # )

    return open_dt, close_dt, preclose_dt, reopen_dt

def rebuild_market_timeline(settings: Settings):
    now_local = now_in_market_tz(settings)

    open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)

    with _market_times_lock:


        prev_open = open_dt
        prev_close = close_dt
        next_open = open_dt + timedelta(days=1)
        next_close = close_dt + timedelta(days=1)


        prev_preclose = preclose_dt
        next_preclose = next_close - timedelta(minutes=settings.pre_close_min)

        prev_postopen = reopen_dt
        next_postopen = next_open + timedelta(minutes=settings.post_open_min)

        _market_times.update({
            "prev_open": prev_open,
            "prev_close": prev_close,
            "next_open": next_open,
            "next_close": next_close,
            "prev_preclose": prev_preclose,
            "next_preclose": next_preclose,
            "prev_postopen": prev_postopen,
            "next_postopen": next_postopen,
        })


# def in_trading_window(now_local: datetime, settings: Settings) -> bool:
#     """
#     Trading window:

#       trading_open  = market_open + post_open_min
#       trading_close = market_close - pre_close_min

#     Both values come from the SSM /ankro/settings JSON.
#     Overnight sessions still handled by market_datetimes().
#     """

#     open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
#         now_local, settings)

#     trading_open = reopen_dt         
#     trading_close = preclose_dt     

#     ok = trading_open <= now_local <= trading_close

#     return ok

def in_trading_window(now_local):

    with _market_times_lock:
        next_preclose = _market_times["next_preclose"]
        #prev_postopen = _market_times["prev_postopen"]
        next_postopen = _market_times["next_postopen"]

    return  next_preclose<next_postopen


def within_preclose_window(now_local: datetime, settings: Settings) -> bool:
    _, close_dt, preclose_dt, _ = market_datetimes(now_local, settings)
    return preclose_dt <= now_local < close_dt


# ---------------------------
# Contract mapping
# ---------------------------
def get_symbol_map() -> Dict[str, Dict[str, str]]:
    if SYMBOL_MAP_JSON.strip():
        try:
            return json.loads(SYMBOL_MAP_JSON)
        except Exception:
            logger.info("Invalid SYMBOL_MAP_JSON; using default mapping.")
    return DEFAULT_SYMBOL_MAP


# def build_contract(tv_symbol: str) -> Contract:
#     m = get_symbol_map()
#     if tv_symbol not in m:
#         raise ValueError(f"Unknown symbol mapping for '{tv_symbol}'. Configure SYMBOL_MAP_JSON or DEFAULT_SYMBOL_MAP.")

#     info = m[tv_symbol]

#     if info.get("secType") == "FUT":
#         symbol = info.get("symbol", "")
#         exchange = info.get("exchange", "CME")
#         currency = info.get("currency", "USD")

#         # FIXED: local_symbol should come from localSymbol, NOT currency
#         local_symbol = info.get("localSymbol", "")
#         ltm = info.get("lastTradeDateOrContractMonth", "")

#         # logger.info(
#         #     f"[CONTRACT] tv_symbol={tv_symbol} | symbol={symbol} | exchange={exchange} "
#         #     f"| currency={currency} | local_symbol={local_symbol}"
#         # )

#         return Future(
#             symbol=symbol,
#             lastTradeDateOrContractMonth=ltm,
#             exchange=exchange,
#             currency=currency,
#             localSymbol=local_symbol,
#         )

#     raise ValueError(f"Unsupported secType for {tv_symbol}: {info.get('secType')}")

def build_contract(sig: Signal) -> Contract:
    c = Future(
        localSymbol=sig.symbol,     # Already full code like MNQH5, NQH5, ESZ4
        exchange=sig.exchange,      # Provided by webhook
        currency="USD"
    )
    return c


# ---------------------------
# Signal parsing
# ---------------------------
@dataclass
class Signal:
    symbol: str
    exchange: str
    desired_direction: int  # +1 long, -1 short, 0 exit/flat
    desired_qty: int
    raw_alert: str
    take_profit: float | None = None
    stop_loss: float | None = None
    target_percentage: float | None = None
    signal_timestamp: float | None = None
    risk_valid: bool | None = None


def parse_signal(payload: Dict[str, Any]) -> Signal:
    import re
    alert = str(payload.get("alert", "")).strip()
    symbol_raw = str(payload.get("symbol", "")).strip()
    symbol_raw = symbol_raw.rstrip("!")

    if not symbol_raw:
        raise ValueError("Missing 'symbol' in payload")
    if not alert:
        raise ValueError("Missing 'alert' in payload")

    m = re.match(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d{4})$", symbol_raw)
    if m:
        root = m.group(1)            # MES
        month = m.group(2)           # H
        year_last = m.group(3)[-1]   # "2026" → "6"
        symbol = f"{root}{month}{year_last}"
    else:
        # fallback: use raw
        symbol = symbol_raw
    exchange_raw = str(payload.get("exchange", "")).strip()
    # logger.info(
    #     f"[DBG_PARSE] RAW symbol={symbol_raw!r} RAW exchange={exchange_raw!r}")
    exchange = exchange_raw.split("_")[0]
    # logger.info(
    #     f"[DBG_PARSE] Normalized symbol={symbol!r} exchange={exchange!r}")

    a = alert.lower()

    # NEW: read signal timestamp from JSON
    signal_ts = payload.get("signalTimestamp")
    try:
        signal_ts = float(signal_ts)
        signal_ts = parse_timestamp(signal_ts)
    except:
        signal_ts = None

    # Accept qty aliases: qty / quantity / size
    qty = payload.get("qty", payload.get(
        "quantity", payload.get("size", DEFAULT_QTY)))
    try:
        qty_i = int(qty)
    except Exception:
        qty_i = DEFAULT_QTY

    def _to_float(v: Any) -> float | None:
        if v is None:
            return None
        try:
            s = str(v).strip()
            if s == "":
                return None
            return float(s)
        except Exception:
            return None

    take_profit = _to_float(payload.get("takeProfit"))
    stop_loss = _to_float(payload.get("stopLoss"))
    target_pct = _to_float(payload.get("targetPercentage"))
    # risk_ok = (
    # (take_profit is not None and stop_loss is not None)
    # or
    # (target_pct is not None and stop_loss is not None)
    # )
    risk_ok = (take_profit is not None and stop_loss is not None)

    # EXIT LONG ONLY
    if "exit long" in a:
        return Signal(
            symbol=symbol,
            exchange=exchange,
            desired_direction=None,   # change
            desired_qty=0,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts,
            risk_valid=None,
        )

    # EXIT SHORT ONLY
    if "exit short" in a or "exit sell" in a:
        return Signal(
            symbol=symbol,
            exchange=exchange,
            desired_direction=None,   # change
            desired_qty=0,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts,
            risk_valid=None,
        )

    # Long entries
    if ("entry" in a and "long" in a) or ("enter" in a and "long" in a) or a in ("long", "buy", "enter long", "entry long"):
        return Signal(
            symbol=symbol,
            exchange=exchange,
            desired_direction=+1,
            desired_qty=qty_i,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts,   # ⭐ ADD HERE
            risk_valid=risk_ok
        )

    # Short entries
    if ("entry" in a and "short" in a) or ("enter" in a and "short" in a) or a in ("short", "sell", "enter short", "entry short"):
        return Signal(
            symbol=symbol,
            exchange=exchange,
            desired_direction=-1,
            desired_qty=qty_i,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts,
            risk_valid=risk_ok  # ⭐ ADD HERE
        )

    raise ValueError(f"Unrecognized alert format: '{alert}'")


def cancel_all_open_orders(IB_INSTANCE, reason="", symbol=None, contract=None):
    log_step(f"CANCEL_OPEN_ORDERS reason={reason}")

    try:
        run_ib(_ib_cancel_all_open_orders(symbol=symbol, contract=contract), timeout=15)
    except Exception as e:
        log_step(f"[ALARM] Error cancelling order: {e}")
        raise RuntimeError("openTrades/cancelOrder failed") from e

    # WAIT UNTIL ORDERS ACTUALLY DISAPPEAR
    timeout = time.time() + 5

    while time.time() < timeout:

        trades = IB_INSTANCE.openTrades()

        filtered = []
        for t in trades:
            try:
                c = t.contract
                if contract and c.conId != contract.conId:
                    continue
                filtered.append(t)
            except:
                continue

        if not filtered:
            log_step("CANCEL_OPEN_ORDERS: DONE")
            return


    log_step("[ALARM] CANCEL_OPEN_ORDERS timeout waiting for cancel")


def current_position_qty(IB_INSTANCE, contract: Contract) -> int:
    qty = 0
    try:
        for p in IB_INSTANCE.positions():
            try:
                if getattr(p.contract, "conId", None) and getattr(contract, "conId", None):
                    if p.contract.conId == contract.conId:
                        qty += int(p.position)
                # else:
                #     if p.contract.symbol == getattr(contract, "symbol", None) and p.contract.secType == contract.secType:
                #         qty += int(p.position)
            except Exception:
                log_step("[ALARM] Error fetching open position.")
                continue
    except Exception as e:
        log_step("[ALARM] Error fetching open positions.")
        raise

    return qty


def close_position(IB_INSTANCE, contract: Contract, qty: int, trade_reason: str = "close_position") -> float | None:
    action = "SELL" if qty > 0 else "BUY"
    log_step(f"CLOSE_POSITION: sending {action} {abs(qty)}")
    log_step(
        f"[CLOSE_DEBUG] Contract before placeOrder: "
        f"secType={getattr(contract,'secType',None)} "
        f"symbol={getattr(contract,'symbol',None)} "
        f"localSymbol={getattr(contract,'localSymbol',None)} "
        f"exchange={getattr(contract,'exchange',None)!r} "
        f"conId={getattr(contract,'conId',None)} "
        f"ltm={getattr(contract,'lastTradeDateOrContractMonth',None)!r}"
    )
    
    try:
        order = MarketOrder(action, abs(int(qty)))
        trade = run_ib(ib_place_order(contract, order))
    
        timeout = time.time() + 30
        fill_price = None
        
        while time.time() < timeout:
        
            if trade.isDone() or trade.fills:
                if trade.fills:
                    fill_price = trade.fills[-1].execution.price
                break
    
        
        if not fill_price:
            raise RuntimeError("fill_timeout")

        #log_trade_event({"trade": "success", "fill_price": fill_price, "action": "close"})
        log_trade_event({"trade reason": trade_reason, "trade direction": action, "fill_price": fill_price})
        log_step(
        f"CLOSE_TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        log_step("CLOSE_POSITION_SUCCESS")
        return fill_price

    except Exception as e:
        # Enrich diagnostics so we can see *why* IB refused or failed to fill.
        try:
            status = getattr(trade, "orderStatus", None) if "trade" in locals() else None
            status_text = getattr(status, "status", None)
            remaining = getattr(status, "remaining", None)
            log_msgs = getattr(trade, "log", []) if "trade" in locals() else []
        except Exception:
            status_text = None
            remaining = None
            log_msgs = []

        if "TimeoutError" in str(e) or str(e) in ("fill_timeout", "close_position_fill_timeout"):
            log_step("[ALARM] CLOSE_POSITION_FAIL no fills within timeout")
            log_step(
                f"[CLOSE_DEBUG] status={status_text} remaining={remaining} "
                f"log_msgs={[str(m) for m in log_msgs]}"
            )
            log_trade_event({
                "trade": "fail",
                "reason": "close_fill_timeout",
                "fill_price": None,
                "status": status_text,
                "remaining": remaining,
                "log_msgs": [str(m) for m in log_msgs],
            })
        else:
            log_step(f"CLOSE_POSITION_FAIL: error={e} status={status_text} remaining={remaining}")
            log_step(f"[CLOSE_DEBUG] log_msgs={[str(m) for m in log_msgs]}")
            log_trade_event({
                "trade": "fail",
                "reason": str(e),
                "fill_price": None,
                "status": status_text,
                "remaining": remaining,
                "log_msgs": [str(m) for m in log_msgs],
            })
        raise
    log_step("[ALARM] CLOSE_POSITION_FAIL no fills within timeout")
    log_trade_event({"trade": "fail", "reason": "close_fill_timeout", "fill_price": None})
    raise RuntimeError("close_position_fill_timeout")
    # # More robust wait loop: IB updates positions asynchronously
    # for i in range(15):      # ~15 seconds worst-case
    #     # IB_INSTANCE.waitOnUpdate(timeout=1.0)   # consume API messages
    #     # IB_INSTANCE.sleep(0.2)

    #     # Force refresh from IB — important!
    #     # IB_INSTANCE.reqPositions()
    #     # IB_INSTANCE.waitOnUpdate(timeout=0.5)

    #     remaining = current_position_qty(IB_INSTANCE, contract)
    #     if remaining == 0:
    #         log_step(f"CLOSE_POSITION_SUCCESS (confirmed after {i+1} checks)")
    #         return

    # log_step(f"CLOSE_POSITION_FAIL: still_open={remaining} after retries")


def _round_to_tick(price: float, tick: float) -> float:
    """
    Round price to nearest valid tick (handles 0.25, 0.01, 0.005, etc).
    Uses Decimal to avoid float weirdness.
    """
    if tick <= 0:
        return float(price)

    p = Decimal(str(price))
    t = Decimal(str(tick))
    # nearest multiple of tick:
    n = (p / t).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    out = (n * t)

    # keep a sane number of decimals for display/IB (based on tick)
    decimals = max(0, -t.as_tuple().exponent)
    return float(out.quantize(Decimal("1") if decimals == 0 else Decimal("1").scaleb(-decimals)))


def get_min_tick(IB_INSTANCE, contract: Contract) -> float:

    key = None
    cid = getattr(contract, "conId", None)
    if cid:
        key = f"conId:{cid}"
    else:
        key = f"localSymbol:{getattr(contract, 'localSymbol', '')}"

    with _MIN_TICK_LOCK:
        if key in _MIN_TICK_CACHE:
            return _MIN_TICK_CACHE[key]

    try:

        async def _req():
            return await IB_INSTANCE.reqContractDetailsAsync(contract)

        future = asyncio.run_coroutine_threadsafe(_req(), IB_LOOP)
        details = future.result(timeout=5)

        if not details:
            raise RuntimeError("reqContractDetails returned empty")

        mt = float(details[0].minTick)

    except Exception as e:
        log_step(f"[ALARM] MIN_TICK_FAIL: using fallback 0.01 err={e}")
        mt = 0.01

    with _MIN_TICK_LOCK:
        _MIN_TICK_CACHE[key] = mt

    log_step(f"MIN_TICK: {mt}")

    return mt


def open_position_with_brackets(IB_INSTANCE,
                                contract: Contract,
                                direction: int,
                                qty: int,
                                take_profit: float | None,
                                stop_loss: float | None,
                                target_percentage: float | None,
                                tp_sl_are_multipliers: bool = False,
                                trade_reason: str = "open_position"
                                ) -> None:

    if take_profit is None or stop_loss is None:
        log_step("[ALARM] OPEN_ENTRY_SKIPPED: TP or SL missing")
        return

    action = "BUY" if direction > 0 else "SELL"
    exit_action = "SELL" if direction > 0 else "BUY"

    # ------------------------------------------------
    # 1️⃣ SEND MARKET PARENT ONLY
    # ------------------------------------------------
    #log_step("start")
    parent = MarketOrder(action, abs(int(qty)))
    trade = run_ib(ib_place_order(contract, parent))
    #log_step("finish")

    #log_step("PARENT_ORDER_SUBMITTED")

    # ------------------------------------------------
    # 2️⃣ WAIT FOR FILL
    # ------------------------------------------------
    fill_price = None
    timeout = time.time() + 10

    try:
        timeout = time.time() + 30
        fill_price = None
        
        while time.time() < timeout:
        
            if trade.isDone() or trade.fills:
                if trade.fills:
                    fill_price = trade.fills[-1].execution.price
                break
        
            #time.sleep(0.05)
        
        # if not fill_price:
        #     raise RuntimeError("fill_timeout")


    except Exception:
        log_step("[ALARM] FILL_FAIL: parent not filled")
        log_trade_event({"trade": "fail", "reason": "fill_timeout", "fill_price": None})
        return {
            "ok": True,                         # <-- critical
            "action": "fill_timeout_no_entry",
            "reason": "market_not_filling",
            "executed": False
        }
    log_step(
        f"FILL_TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    if not fill_price:
        log_step("[ALARM] FILL_FAIL: parent not filled")
        log_trade_event({"trade": "fail", "reason": "fill_timeout", "fill_price": None})
        return {
            "ok": True,                         # <-- critical
            "action": "fill_timeout_no_entry",
            "reason": "market_not_filling",
            "executed": False
        }

    log_step(f"FILL_PRICE: {fill_price}")

    # ------------------------------------------------
    # 3️⃣ CALCULATE TP / SL FROM REAL FILL
    # ------------------------------------------------
    if not tp_sl_are_multipliers:
        tp_pct = float(take_profit) / 100.0
        sl_pct = float(stop_loss) / 100.0

        if direction > 0:
            tp_price = fill_price * (1 + tp_pct)
            sl_price = fill_price * (1 - sl_pct)
        else:
            tp_price = fill_price * (1 - tp_pct)
            sl_price = fill_price * (1 + sl_pct)

        tick = get_min_tick(IB_INSTANCE, contract)
        tp_price = _round_to_tick(tp_price, tick)
        sl_price = _round_to_tick(sl_price, tick)
    else:
        tp_price=take_profit
        sl_price=stop_loss

    log_step(f"TP_PRICE: {tp_price}")
    log_step(f"SL_PRICE: {sl_price}")

    # ------------------------------------------------
    # 4️⃣ SEND OCO CHILD ORDERS
    # ------------------------------------------------
    tp_order = LimitOrder(exit_action, abs(int(qty)), tp_price)
    sl_order = StopOrder(exit_action, abs(int(qty)), sl_price)

    IB_INSTANCE.bracketOrder = None  # avoid confusion

    # OCA group
    oca_group = f"OCA_{int(time.time()*1000)}"
    tp_order.ocaGroup = oca_group
    tp_order.ocaType = 1
    sl_order.ocaGroup = oca_group
    sl_order.ocaType = 1

    run_ib(ib_place_order(contract, tp_order))
    run_ib(ib_place_order(contract, sl_order))

    # Trades log: confirmation with fill price
    log_trade_event({"trade reason": "open_position", "trade direction": action, "fill_price": fill_price, "TP": tp_price, "SL": sl_price})
    

    #log_step("BRACKET_CHILDREN_SUBMITTED")
    pos=len(IB_INSTANCE.positions())
    orders=len(IB_INSTANCE.openTrades())
    #log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
    log_step(f"PositionsAfter: {pos}")
    log_step(f"OrdersAfter:    {orders}")
    if pos == 0:
        log_step("[ALARM] No positions were opened")
    if orders == 0:
        log_step("[ALARM] No orders were opened")


def wait_until_flat(IB_INSTANCE, contract: Contract, settings: Settings) -> bool:
    for i in range(MAX_STATE_CHECKS):
        qty = current_position_qty(IB_INSTANCE, contract)
        if qty == 0:
            return True
        # logger.info(f"Waiting for close to reflect (attempt {i+1}/{MAX_STATE_CHECKS}), qty still {qty}")
        # IB_INSTANCE.sleep(0.1)
        # time.sleep(1)
    return False

def qualify_contract(contract):

    async def _qualify():
        return await IB_INSTANCE.qualifyContractsAsync(contract)

    future = asyncio.run_coroutine_threadsafe(
        _qualify(),
        IB_LOOP
    )

    return future.result(timeout=5)
# ---------------------------
# Pre-close / post-open logic (ONLY runs when webhook arrives)
# ---------------------------


async def _fetch_trading_hours_for_symbol(local_symbol: str, info: dict) -> dict:
    """
    Runs on IB_LOOP: request ContractDetails and parse tradingHours into UTC
    open/close times keyed by date (YYYY-MM-DD).
    """
    c = Future(
        localSymbol=local_symbol,
        exchange=info.get("exchange", "CME"),
        currency=info.get("currency", "USD"),
    )

    details_list = await IB_INSTANCE.reqContractDetailsAsync(c)
    if not details_list:
        return {}

    cd = details_list[0]
    tz_name = getattr(cd, "timeZoneId", "America/New_York")
    try:
        from zoneinfo import ZoneInfo
        local_tz = ZoneInfo(tz_name)
    except Exception:
        from zoneinfo import ZoneInfo
        local_tz = ZoneInfo("America/New_York")

    out: dict[str, dict] = {}
    th_str = getattr(cd, "tradingHours", "") or ""
    for segment in th_str.split(";"):
        segment = segment.strip()
        if not segment or ":" not in segment:
            continue
        day_part, hours_part = segment.split(":", 1)
        if hours_part.upper() == "CLOSED":
            continue
        for rng in hours_part.split(","):
            if "-" not in rng:
                continue
            start_str, end_str = rng.split("-", 1)
            if len(start_str) != 12 or len(end_str) != 12:
                continue
            try:
                start_local = datetime.strptime(start_str, "%Y%m%d%H%M").replace(tzinfo=local_tz)
                end_local = datetime.strptime(end_str, "%Y%m%d%H%M").replace(tzinfo=local_tz)
            except Exception:
                continue

            date_key = start_local.date().isoformat()
            start_utc = start_local.astimezone(timezone.utc).isoformat()
            end_utc = end_local.astimezone(timezone.utc).isoformat()

            d = out.setdefault(date_key, {})
            if "open" not in d or start_utc < d["open"]:
                d["open"] = start_utc
            if "close" not in d or end_utc > d.get("close", ""):
                d["close"] = end_utc

    return out


def _weekly_trading_hours_worker():
    """
    Once per week on Sunday ~01:00 UTC, fetch tradingHours for all symbols
    in the registry and store them into SYMBOL_SCHEDULE_PATH.
    """
    last_week_key = None

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            iso_year, iso_week, _ = now_utc.isocalendar()
            week_key = f"{iso_year}-W{iso_week:02d}"

            # Sunday (6) between 01:00 and 01:10 UTC, once per ISO week
            if (
                now_utc.weekday() == 6
                and dtime(1, 0) <= now_utc.time() <= dtime(1, 10)
                and last_week_key != week_key
            ):
                # Load all symbols we already have in schedules and refresh them.
                try:
                    with _symbol_schedule_lock:
                        if os.path.exists(SYMBOL_SCHEDULE_PATH):
                            with open(SYMBOL_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                                existing_sched = json.load(f)
                        else:
                            existing_sched = {}
                except Exception:
                    existing_sched = {}

                symbols = list(existing_sched.keys())
                if IB_INSTANCE is None or not IB_READY.is_set():
                    logger.warning("[TRADING_HOURS] IB not ready; skipping this run")
                elif not symbols:
                    logger.info("[TRADING_HOURS] No symbols in schedules yet")
                else:
                    async def _job():
                        all_sched: dict[str, dict] = {}
                        for lsym, info in symbols.items():
                            try:
                                sched = await _fetch_trading_hours_for_symbol(lsym, info)
                                if sched:
                                    all_sched[lsym] = sched
                            except Exception as e:
                                logger.warning("[TRADING_HOURS] Failed for %s: %s", lsym, e)
                        return all_sched

                    fut = asyncio.run_coroutine_threadsafe(_job(), IB_LOOP)
                    try:
                        new_data = fut.result(timeout=120)
                        _merge_symbol_schedules(new_data)
                        logger.info(
                            "[TRADING_HOURS] Updated schedules for %d symbols (week %s)",
                            len(new_data),
                            week_key,
                        )
                        last_week_key = week_key
                    except Exception as e:
                        logger.warning("[TRADING_HOURS] Scheduler error: %s", e)

        except Exception as e:
            logger.warning("[TRADING_HOURS] Worker loop error: %s", e)

        time.sleep(60)

# ---------------------------
def ensure_preclose_close_if_needed(IB_INSTANCE, settings: Settings) -> None:
    now_local = now_in_market_tz(settings)

    dayk = state_key_for_day(now_local.date())

    with _state_lock:
        st = load_state()
        st.setdefault("preclose", {})
        st["preclose"].setdefault(dayk, {})

    # if not IB_INSTANCE.isConnected():

    #     log_step( "[ALARM] Preclose: IB not connected")
    #     flush_account_log("PRECLOSE_EXEC")
    #     return

    log_step("Preclose potential position closing")
    try:
        with _state_lock:
            st = load_state()
            already = bool(
                st.get("preclose", {})
                  .get(dayk, {})
                  .get(str(ACCOUNT_SHORT_NAME), {})
                  .get("done", False)
            )
        # if already:
        #     continue

        # ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)
        pos = IB_INSTANCE.positions()

        

        # =======================================================
        # POSITION SNAPSHOT
        # =======================================================
        snapshot: Dict[str, Any] = {}

        if not pos:
            log_step("No open position to close")
        else:
            for p in pos:
                if int(p.position) != 0:
                    c = p.contract
                    key = (
                        str(getattr(c, "conId", "")) or
                        f"{c.secType}:{c.symbol}:{getattr(c, 'exchange', '')}"
                    )

                    # IB position object exposes the average entry price as avgCost / avgPrice
                    avg_entry = getattr(p, "avgCost", None)
                    if avg_entry is None:
                        avg_entry = getattr(p, "avgPrice", 0.0)

                    snapshot[key] = {
                        "secType": c.secType,
                        "symbol": c.symbol,
                        "exchange": getattr(c, "exchange", ""),
                        "currency": getattr(c, "currency", ""),
                        "lastTradeDateOrContractMonth": getattr(c, "lastTradeDateOrContractMonth", ""),
                        "position": int(p.position),
                        "conId": getattr(c, "conId", None),
                        "entry_avg_price": float(avg_entry) if avg_entry is not None else None,
                    }

        # =======================================================
        # CLOSE POSITION using paired, qualified contract
        # =======================================================
        # Map of conId -> qualified contract (may be populated/updated later)
        qualified_by_conid = {}

        for p in pos:
                q = int(p.position)
                if q == 0:
                    continue

                pc = p.contract
                cid = getattr(pc, "conId", None)

                # try match correct contract by conId
                c = qualified_by_conid.get(cid, pc)

                # safety — if missing exchange, qualify automatically
                if not getattr(c, "exchange", None):
                    try:
                        qc = qualify_contract(c)
                        if qc:
                            c = qc[0]
                    except:
                        pass

                log_step(
                    f"Preclose: acct={ACCOUNT_SHORT_NAME} closing {c.secType} {c.symbol} conId={cid} exch={getattr(c,'exchange',None)} qty={q}"
                )
                log_trade_event({"preclose closing for symbol": c.localSymbol,  "quantity": q})

                # We still call close_position (which returns the close fill),
                # but for state we persist the *entry* average price from the
                # snapshot above, not the close fill.
                close_position(IB_INSTANCE, c, q, "preclose closing")

        # IB_INSTANCE.disconnect()

        # =======================================================
        # SNAPSHOT OPEN TRADES (NOT openOrders)
        # =======================================================
        try:
            IB_INSTANCE.OpenOrders()
            #IB_INSTANCE.waitOnUpdate(timeout=2)
        except Exception:
            pass

        trades = list(IB_INSTANCE.openTrades())
        orders_snapshot: List[Dict[str, Any]] = []

        if not trades:
            log_step("No open orders to cancel")
        else:
            for t in trades:
                o = t.order
                c = t.contract

                orders_snapshot.append({
                    "orderId": getattr(o, "orderId", None),
                    "action": getattr(o, "action", None),
                    "totalQuantity": getattr(o, "totalQuantity", None),
                    "orderType": getattr(o, "orderType", None),
                    "lmtPrice": getattr(o, "lmtPrice", None),
                    "auxPrice": getattr(o, "auxPrice", None),
                    "tif": getattr(o, "tif", None),

                    "conId": getattr(c, "conId", None),
                    "symbol": getattr(c, "symbol", None),
                    "secType": getattr(c, "secType", None),
                    "exchange": getattr(c, "exchange", None),
                    "localSymbol": getattr(c, "localSymbol", None),
                    "ltm": getattr(c, "lastTradeDateOrContractMonth", None),
                    "currency": getattr(c, "currency", None),
                })

            cancel_all_open_orders(IB_INSTANCE, reason="preclose")

        # =======================================================
        # BUILD / UPDATE MAP FOR CORRECT CONTRACT PAIRING BY conId
        # =======================================================
        try:
            more_trades = IB_INSTANCE.openTrades()
            for t in more_trades:
                qc = t.contract
                cid = getattr(qc, "conId", None)
                if cid:
                    qualified_by_conid[cid] = qc
        except:
            pass

        # =======================================================
        # SAVE PRE-CLOSE STATE
        # =======================================================
        with _state_lock:
            st = load_state()
            st.setdefault("preclose", {})
            st["preclose"].setdefault(dayk, {})
            st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)] = {
                "done": True,
                "at": now_local.isoformat(),
                "snapshot": snapshot,
                "orders_snapshot": orders_snapshot,
                "reopen_done": False,
                "reopen_at": None,
            }
            save_state(st)

        log_step(
            f"Preclose: acct={ACCOUNT_SHORT_NAME} completed snapshot_count={len(snapshot)}")

    except Exception as e:
        log_step(f"[ALARM] Preclose error acct={ACCOUNT_SHORT_NAME}: {e}")

    finally:
        flush_account_log("PRECLOSE_EXEC")


def ensure_postopen_reopen_if_needed(IB_INSTANCE, settings: Settings) -> None:

    # now_local = now_in_market_tz(settings)

    # dayk = state_key_for_day(now_local.date())
    
    now_local = now_in_market_tz(settings)

    #open_dt, close_dt, _, _ = market_datetimes(now_local, settings)
    state_day=_market_times["prev_close"].date()
    # --------------------------------------------------
    # FIX: determine correct state day
    # --------------------------------------------------
    # if open_dt < close_dt:
    #     # normal daytime session
    #     state_day = now_local.date()
    # else:
    #     # overnight session → snapshot stored on previous day
    #     state_day = (now_local - timedelta(days=1)).date()
    dayk = state_key_for_day(state_day)

    # Ensure persistent global IB connection is alive
    # if not IB_INSTANCE or not IB_INSTANCE.isConnected():
    #     logger.info("[ALARM] Global IB_INSTANCE is not connected")
    #     flush_account_log("POSTOPEN_EXEC")
    #     return
    # return jsonify({"ok": False, "error": "ib_not_connected"}), 503

    log_step("Postopen potential position reopen")
    try:
        with _state_lock:
            st = load_state()
            entry = st.get("preclose", {}).get(
                dayk, {}).get(str(ACCOUNT_SHORT_NAME))

        # must have preclose + not already reopened
        if not entry:
            log_step("Nothing to reopen")
            return
        if entry.get("reopen_done"):# or entry.get("done"):
            log_step("Postopen was already triggered")
            return

        snapshot: Dict[str, Any] = entry.get("snapshot", {}) or {}
        orders_snapshot: List[Dict[str, Any]] = entry.get(
            "orders_snapshot", []) or []

        # ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)

        # must be flat right now
        any_open = any(int(p.position) != 0 for p in IB_INSTANCE.positions())
        if any_open:
            log_step(
                f"[ALARM] Postopen: acct={ACCOUNT_SHORT_NAME} not flat; will not reopen.")
            # IB_INSTANCE.disconnect()
            return

        # no snapshot → mark done
        if not snapshot:
            with _state_lock:
                st = load_state()
                st["preclose"][dayk][str(
                    ACCOUNT_SHORT_NAME)]["reopen_done"] = True
                st["preclose"][dayk][str(
                    ACCOUNT_SHORT_NAME)]["reopen_at"] = now_local.isoformat()
                save_state(st)
            log_step(
                f"Postopen: acct={ACCOUNT_SHORT_NAME} nothing to reopen (empty snapshot).")
            # IB_INSTANCE.disconnect()
            return

        # =====================================================
        # MAIN LOOP — rebuild original contracts using conId
        # =====================================================
        for conId_key, meta in snapshot.items():

            # conId is stored as dict key
            try:
                conId = int(conId_key)
            except:
                log_step(
                    f"[ALARM][POSTOPEN] Invalid conId key={conId_key}, skipping.")
                continue

            # Build by conId (BEST POSSIBLE METHOD)
            c = Contract()
            c.conId = conId

            try:
                qc = qualify_contract(c)
                if qc:
                    c = qc[0]   # fully qualified contract
            except Exception as e:
                log_step(
                    f"[ALARM][POSTOPEN] qualify failed conId={conId}: {e}")
            #log_trade_event({"postopen reopen for symbol": c.localSymbol, "orders_after_trade_execution": orders})
            direction = +1 if int(meta.get("position", 0)) > 0 else -1
            action = "BUY" if int(meta.get("position", 0)) > 0 else "SELL"
            qty = abs(int(meta.get("position", 0)))
            qty2 = int(meta.get("position", 0))
            log_trade_event({"postopen reopen for symbol": c.localSymbol, "action": action, "quantity": qty2})

            # ------------------------------
            # Match TP/SL from snapshot
            # ------------------------------
            tp_price = None
            sl_price = None

            for om in orders_snapshot:
                if om.get("conId") != conId:
                    continue

                ot = om.get("orderType")
                lmt = om.get("lmtPrice")
                aux = om.get("auxPrice")

                if ot == "LMT" and lmt is not None:
                    tp_price = float(lmt)
                if ot == "STP" and aux is not None:
                    sl_price = float(aux)

            # ------------------------------
            # Derive TP/SL percentages from stored entry_avg_price (if any)
            # ------------------------------
            tp_arg = tp_price
            sl_arg = sl_price
            use_percentages = False

            entry_avg = None
            try:
                entry_avg = float(meta.get("entry_avg_price")) if meta.get("entry_avg_price") is not None else None
            except Exception:
                entry_avg = None

            if entry_avg and tp_price is not None and sl_price is not None:
                if direction > 0:
                    # Long: TP above, SL below
                    tp_pct = (tp_price - entry_avg) / entry_avg * 100.0
                    sl_pct = (entry_avg - sl_price) / entry_avg * 100.0
                else:
                    # Short: TP below, SL above
                    tp_pct = (entry_avg - tp_price) / entry_avg * 100.0
                    sl_pct = (sl_price - entry_avg) / entry_avg * 100.0

                if tp_pct > 0 and sl_pct > 0:
                    tp_arg = tp_pct
                    sl_arg = sl_pct
                    use_percentages = True

            log_step(
                f"[POSTOPEN] Reopening with bracket acct={ACCOUNT_SHORT_NAME} "
                f"symbol={c.symbol} conId={conId} dir={direction} qty={qty} "
                f"tp={tp_price} sl={sl_price} entry_avg={entry_avg} "
                f"use_percentages={use_percentages}"
            )
            if qty <= 0 or tp_price is None or sl_price is None:
                log_step(
                    f"[ALARM] Missing TP/SL for conId={conId}; skipping reopen.")
                continue

            open_position_with_brackets(IB_INSTANCE,
                                        c,
                                        direction,
                                        qty,
                                        take_profit=tp_arg,
                                        stop_loss=sl_arg,
                                        target_percentage=None,
                                        tp_sl_are_multipliers=not use_percentages,
                                        trade_reason="postopen reopening"
                                        )

            # time.sleep(1)

        # IB_INSTANCE.disconnect()

        # Mark reopen done
        with _state_lock:
            st = load_state()
            st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)]["reopen_done"] = True
            st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)
                                 ]["reopen_at"] = now_local.isoformat()
            save_state(st)

    except Exception as e:
        # IB_INSTANCE.disconnect()
        log_step(f"[ALARM] Postopen error acct={ACCOUNT_SHORT_NAME}: {e}")
    finally:
        # try:
        #     IB_INSTANCE.disconnect()
        # except:
        #     pass

        # Give IB time to release clientID to avoid "clientId already in use"
        # time.sleep(1.2)
        flush_account_log("POSTOPEN_EXEC")


def parse_timestamp(value) -> float | None:
    """
    Accepts:
      - UNIX seconds (1706400000)
      - UNIX milliseconds (1706400000000)
      - ISO8601 datetime ("2025-01-27T13:15:02Z")
    Returns float UNIX seconds or None.
    """
    if value is None:
        return None

    # If numeric → may be seconds or ms
    try:
        v = float(value)
        # Heuristic: if too large → it's ms
        if v > 1e12:  # more than 10^12 → ms
            return v / 1000.0
        if v > 1e10:  # also ms range
            return v / 1000.0
        if v > 1e5:   # valid seconds
            return v
    except:
        pass

    # Try ISO8601
    try:
        from datetime import datetime
        # Auto ISO8601 detection
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.timestamp()
    except:
        return None

# ---------------------------
# Per-account signal execution
# ---------------------------


def execute_signal_for_account(IB_INSTANCE, sig: Signal, settings: Settings, contract: Contract) -> Dict[str, Any]:

    # if not IB_INSTANCE.isConnected():
    #     log_step( "IB_NOT_CONNECTED")
    #     flush_account_log("WEBHOOK_EXEC")

    #     return {
    #         "short_name": ACCOUNT_SHORT_NAME,
    #         "api_port": 4002+DERIVED_ID,
    #         "client_id": 1+DERIVED_ID,
    #         "ok": False,
    #         "error": "ib_not_connected",
    #     }
    # log_step(
    #     f"DEBUG acct={ACCOUNT_SHORT_NAME} port={4002+DERIVED_ID} clientId={1+DERIVED_ID}")

    # log_step(
    #     f"EXEC_START alert={sig.raw_alert} "
    #     f"symbol={sig.symbol} "
    #     f"dir={'SELL' if sig.desired_direction == -1 else 'BUY'} "
    #     f"qty={sig.desired_qty}"
    # )
    log_step(
        f"alert={sig.raw_alert} "
        f"symbol={sig.symbol} "
        f"dir={'SELL' if sig.desired_direction == -1 else 'BUY'} "
        f"qty={sig.desired_qty}"
    )
    result = {
        "short_name": ACCOUNT_SHORT_NAME,
        "api_port": 4002+DERIVED_ID,
        "client_id": 1+DERIVED_ID,
    }

    # try:
    #     asyncio.get_running_loop()
    # except RuntimeError:
    #     # No loop in this thread → create a dummy one so ib_insync won't try async
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)

    try:

        #logger.info(f"{IB_INSTANCE.positions()}")
        

        # Use the resolved contract
        # contract = qualified[0]

        qty = current_position_qty(IB_INSTANCE, contract)
        # logger.info(f"two")
        log_step("#############STATE CHECK######################")
        if qty != 0:
            side = "BUY" if qty > 0 else "SELL"
            log_step(f"Current position for symbol: {side} {abs(qty)}")
            log_trade_event({"before trade state_check":  "opened_positions", "quantity": abs(qty), "side": side})
        else:
            log_step("Current position for symbol: No opened positions")
            log_trade_event({"before trade state_check":  "opened_positions", "quantity": 0})

        try:
            trades = IB_INSTANCE.openTrades()

        except:
            log_step("[ALARM] Error fetching open trades.")
            result.update({
                "ok": False,
                "action": "error_fetchin_open_trades",                # INFO: early return
                "reason": "error_fetchin_open_trades"
            })
            flush_account_log("WEBHOOK_EXEC")
            return result
        filtered = []
        for t in trades:
            try:
                if t.contract and t.contract.conId == contract.conId:
                    filtered.append(t)
            except:
                continue

        if filtered:
            log_step("Open orders for symbol:")
            for t in filtered:
                try:
                    c = t.contract
                    o = t.order
                    # os = t.orderState

                    log_step(
                        "  " + " ".join([
                            f"type={getattr(o,'orderType',None)}",
                            f"action={getattr(o,'action',None)}",
                            f"qty={getattr(o,'totalQuantity',None)}"
                        ])
                    )
                    log_trade_event({"before trade state_check":  "opened_orders", "direction": getattr(o,'action',None), "order_type": getattr(o,'orderType',None), "quantity": qty})
                except Exception as e:
                    log_step(f"ERROR printing open order: {e}")
                    raise
        else:
            log_step("Open orders for symbol: NONE")

        log_step("#############END OF STATE CHECK#################")

        # ----------------------------------------------------------
        # NEW: latency check (using auto-detected timestamp)
        # ----------------------------------------------------------
        now_ts = time.time()
        sig_age = None
        if sig.signal_timestamp:
            try:
                sig_age = now_ts - sig.signal_timestamp
            except:
                sig_age = None
        # log_step( f"[EXEC] Signal timestamp {sig.signal_timestamp}  {int(settings.execution_delay)}")
        allow_entry = False
        allow_entry = True
        if sig.desired_direction != 0 and sig_age is not None:
            if sig_age > int(settings.execution_delay):
                log_step(
                    f"[ALARM][EXEC] Entry not executed: the execution is delayed by more than {int(settings.execution_delay)} relative to the signal")
                allow_entry = False
        # logger.info(f"[EXEC] latency_check sig_age={sig_age} execution_delay={int(settings.execution_delay)} allow_entry={allow_entry}")

        # ----------------------------------------------------------
        # EXIT (ALWAYS perform exit, ignore latency)
        # ----------------------------------------------------------
        if sig.desired_direction == 0:
            # logger.info(f"[EXEC] Branch=EXIT acct={ACCOUNT_SHORT_NAME} current_qty={qty}")
            
            if qty == 0:
                cancel_all_open_orders(IB_INSTANCE,
                                       reason="exit_signal",
                                       contract=contract
                                       )
                log_step("No positions to exit for the exit signal")
                result.update({"ok": True, "action": "none_already_flat"})
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                # pos=len(IB_INSTANCE.positions())
                # orders=len(IB_INSTANCE.openTrades())
                # log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
                flush_account_log("WEBHOOK_EXEC")
                return result

            # Close existing position
            close_position(IB_INSTANCE, contract, qty)
            # time.sleep(1)

            # Retry logic
            if not wait_until_flat(IB_INSTANCE, contract, settings):
                log_step("[ALARM] Exit close not reflected — retrying")
                # close_position(contract, qty)
                # time.sleep(1)

                result.update(
                    {"ok": False, "action": "exit_failed_after_retry"})
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            cancel_all_open_orders(IB_INSTANCE,
                                   reason="exit_signal",
                                   contract=contract
                                   )
            # pos=len(IB_INSTANCE.positions())
            # orders=len(IB_INSTANCE.openTrades())
            # log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
            log_step("Position closed successfully")
            result.update({"ok": True, "action": "exit_closed"})
            # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            # IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # EXIT LONG ONLY
        # ----------------------------------------------------------
        if getattr(sig, "raw_alert", "").lower().strip().startswith("exit long"):
            log_step("[EXEC] Exit Long signal received")

            if qty > 0:   # only close long positions
                
                close_position(IB_INSTANCE, contract, qty)
                if not wait_until_flat(IB_INSTANCE, contract, settings):
                    log_step("[ALARM] Exit close not reflected — retrying")
                    # close_position(contract, qty)
                    # time.sleep(1)

                    result.update(
                        {"ok": False, "action": "exit_failed_after_retry"})
                    # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                    # IB_INSTANCE.disconnect()
                    # pos=len(IB_INSTANCE.positions())
                    # orders=len(IB_INSTANCE.openTrades())
                    # log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
                    flush_account_log("WEBHOOK_EXEC")
                    return result
                cancel_all_open_orders(
                    IB_INSTANCE, reason="exit_long", contract=contract)
                result.update({"ok": True, "action": "exit_long_closed"})
                # pos=len(IB_INSTANCE.positions())
                # orders=len(IB_INSTANCE.openTrades())
                # log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
                flush_account_log("WEBHOOK_EXEC")
                return result
            else:
                result.update(
                    {"ok": True, "action": "exit_long_no_long_position"})

                #log_trade_event({"event": "no_long_postions_opened"})
                flush_account_log("WEBHOOK_EXEC")
                return result

        # ----------------------------------------------------------
        # EXIT SHORT ONLY
        # ----------------------------------------------------------
        if getattr(sig, "raw_alert", "").lower().strip().startswith("exit short") or \
           getattr(sig, "raw_alert", "").lower().strip().startswith("exit sell"):
            log_step("[EXEC] Exit Short signal received")

            if qty < 0:   # only close short positions
                
                close_position(IB_INSTANCE, contract, qty)
                if not wait_until_flat(IB_INSTANCE, contract, settings):
                    log_step("[ALARM] Exit close not reflected — retrying")
                    # close_position(contract, qty)
                    # time.sleep(1)

                    result.update(
                        {"ok": False, "action": "exit_failed_after_retry"})
                    # pos=len(IB_INSTANCE.positions())
                    # orders=len(IB_INSTANCE.openTrades())
                    # log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
                    # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                    # IB_INSTANCE.disconnect()
                    flush_account_log("WEBHOOK_EXEC")
                    return result
                cancel_all_open_orders(
                    IB_INSTANCE, reason="exit_short", contract=contract)
                result.update({"ok": True, "action": "exit_short_closed"})
                # pos=len(IB_INSTANCE.positions())
                # orders=len(IB_INSTANCE.openTrades())
                #log_trade_event({"positions_after_trade_execution": pos, "orders_after_trade_execution": orders})
                flush_account_log("WEBHOOK_EXEC")
                return result
            else:
                result.update(
                    {"ok": True, "action": "exit_short_no_short_position"})
                flush_account_log("WEBHOOK_EXEC")
                #log_trade_event({"event": "no_short_postions_opened"})
                return result

        # ----------------------------------------------------------
        # SAME DIRECTION → NO-OP
        # ----------------------------------------------------------
        desired_dir = sig.desired_direction
        desired_qty = sig.desired_qty if sig.desired_qty > 0 else DEFAULT_QTY

        if qty != 0 and ((qty > 0 and desired_dir > 0) or (qty < 0 and desired_dir < 0)):
            log_step(
                "[EXEC] Same direction position already opened. Skipping execution")
            # pos=len(IB_INSTANCE.positions())
            # orders=len(IB_INSTANCE.openTrades())
            #log_trade_event({"event": "same_position_already_open_skipping", "positions_already_opened": pos, "orders_alreay_opened": orders})
            result.update(
                {"ok": True, "action": "none_same_direction_already_open", "current_qty": qty})
            # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            # IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # REVERSAL (close ALWAYS, but entry obeys latency)
        # ----------------------------------------------------------
        if qty != 0 and ((qty > 0 and desired_dir < 0) or (qty < 0 and desired_dir > 0)):
            log_step(
                f"[EXEC] Opposite direction singal: Closing position and opening new one.")
            close_position(IB_INSTANCE, contract, qty)
            
            # time.sleep(1)

            # Retry close
            if not wait_until_flat(IB_INSTANCE, contract, settings):
                log_step(
                    "[ALARM] Reversal close not confirmed — not clear if existing postions were closed. Skipping execution")
                # close_position(contract, qty)
                # time.sleep(1)
                result.update(
                    {"ok": False, "action": "reversal_close_not_confirmed"})
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            cancel_all_open_orders(
                IB_INSTANCE, reason="before_reversal_entry",  contract=contract)

                # if not wait_until_flat(contract, settings):
                #     result.update({"ok": False, "action": "reversal_close_not_confirmed"})
                #     logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #     IB_INSTANCE.disconnect()
                #     return result

            # TOO OLD → do not open new position
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

            if not sig.risk_valid:
                log_step(
                    "[ALARM][EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

            # cancel_all_open_orders(reason="before_reversal_entry", acct=ACCOUNT_SHORT_NAME, contract=contract)
            # Fresh enough → open reversed position
            # time.sleep(max(1, int(settings.delay_sec)))

            # open_position_with_brackets(IB_INSTANCE,
            #     contract,
            #     desired_dir,
            #     desired_qty,
            #     sig.take_profit,
            #     sig.stop_loss,
            #     sig.target_percentage,
            #     ACCOUNT_SHORT_NAME    # <<< NEW
            # )

            op = open_position_with_brackets(IB_INSTANCE,
                                             contract,
                                             desired_dir,
                                             desired_qty,
                                             sig.take_profit,
                                             sig.stop_loss,
                                             sig.target_percentage,
                                             )

            if isinstance(op, dict) and not op.get("executed", False):
                # child says: fill_timeout or some other skip reason
                result.update({
                    "ok": True,            # important → SQS stops retrying
                    "action": "entry_skipped",
                    "executed": False,
                    "reason": op.get("reason")
                })
                return result

            # logger.info(f"[EXEC] Entry order submitted acct={ACCOUNT_SHORT_NAME} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "reversal_opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            # IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # FLAT → NEW ENTRY
        # ----------------------------------------------------------
        if qty == 0:
            # Skip stale entries
            cancel_all_open_orders(
                IB_INSTANCE, reason="before_new_entry", contract=contract)
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

            if desired_dir != 0 and not sig.risk_valid:
                log_step("[EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "entry_ignored_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

            # Fresh entry → open position
            # time.sleep(max(0, int(settings.execution_delay)))
            log_step(
            f"Calling open position: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            op = open_position_with_brackets(IB_INSTANCE,
                                             contract,
                                             desired_dir,
                                             desired_qty,
                                             sig.take_profit,
                                             sig.stop_loss,
                                             sig.target_percentage
                                             )

            if isinstance(op, dict) and not op.get("executed", False):
                # child says: fill_timeout or some other skip reason
                result.update({
                    "ok": True,            # important → SQS stops retrying
                    "action": "entry_skipped",
                    "executed": False,
                    "reason": op.get("reason")
                })
                return result

            # logger.info(f"[EXEC] Entry order submitted acct={ACCOUNT_SHORT_NAME} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            # IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # Should never reach here
        # ----------------------------------------------------------
        result.update(
            {"ok": False, "action": "ambiguous_state", "current_qty": qty})
        # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
        # IB_INSTANCE.disconnect()
        flush_account_log("WEBHOOK_EXEC")
        return result

    except Exception as e:
        # logger.info("error: {str(e)}")
        log_step(f"[ALARM][EXEC] error: {str(e)}")
        result.update({"ok": False, "error": str(e)})
        flush_account_log("WEBHOOK_EXEC")
        return result
    finally:
        flush_account_log("WEBHOOK_EXEC")


def _effective_market_settings(settings: Settings) -> Optional[Settings]:
    """If settings.market_open is None, try to get open/close/timezone from IB contract details. Otherwise use settings."""
    if settings.market_open:
        return settings
    ib_open, ib_close, ib_tz = get_market_hours_from_ib()
    if ib_open and ib_close and ib_tz:
        return Settings(
            delay_sec=settings.delay_sec,
            execution_delay=settings.execution_delay,
            pre_close_min=settings.pre_close_min,
            post_open_min=settings.post_open_min,
            market_open=ib_open,
            market_close=ib_close,
            timezone=ib_tz,
        )
    return None


def background_scheduler_loop():
    global IB_INSTANCE
    """
    If SSM market_open is None, uses IB contract details (liquidHours/tradingHours, timeZoneId) for open/close/tz.
    Runs rebuild_market_timeline only when market open is set (from SSM or IB), inside the loop.
    Runs ensure_preclose_close_if_needed() and ensure_postopen_reopen_if_needed() once per market day.
    """
    last_preclose_run_day = None   # date of market_open for the last run
    last_postopen_run_day = None   # date of market_open for the last run
    settings = settings_cache.get()
    rebuild_market_timeline(settings)
    while True:
        try:
            settings = settings_cache.get()
            now_local = now_in_market_tz(settings)

            #rebuild_market_timeline(settings)

            # open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
            #     now_local, settings)
            # logger.info(
            #     f"[SCHEDULER] now_local={now_local.isoformat()} | "
            #     f"open_dt={open_dt.isoformat()} | "
            #     f"close_dt={close_dt.isoformat()} | "
            #     f"preclose_dt={preclose_dt.isoformat()} | "
            #     f"reopen_dt={reopen_dt.isoformat()} | "
            #     f"last_preclose_run_day={last_preclose_run_day} | "
            #     f"last_postopen_run_day={last_postopen_run_day}"
            # )
            # This defines the “trading day” — the day the market opens.
            # market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            # if last_preclose_run_day != market_day:
            #     last_preclose_run_day = None
            # if last_postopen_run_day != market_day:
            #     last_postopen_run_day = None

            # open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
            #     now_local, settings)
            # logger.info(
            #     f"[SCHEDULER] now_local={now_local.isoformat()} | "
            #     f"open_dt={open_dt.isoformat()} | "
            #     f"close_dt={close_dt.isoformat()} | "
            #     f"preclose_dt={preclose_dt.isoformat()} | "
            #     f"reopen_dt={reopen_dt.isoformat()} | "
            #     f"last_preclose_run_day={last_preclose_run_day} | "
            #     f"last_postopen_run_day={last_postopen_run_day}"
            # )
            # This defines the “trading day” — the day the market opens.
            # market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            # if last_preclose_run_day != market_day:
            #     last_preclose_run_day = None
            # if last_postopen_run_day != market_day:
            #     last_postopen_run_day = None

            # open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
            #     now_local, settings)
            # logger.info(
            #     f"[SCHEDULER] now_local={now_local.isoformat()} | "
            #     f"open_dt={open_dt.isoformat()} | "
            #     f"close_dt={close_dt.isoformat()} | "
            #     f"preclose_dt={preclose_dt.isoformat()} | "
            #     f"reopen_dt={reopen_dt.isoformat()} | "
            #     f"last_preclose_run_day={last_preclose_run_day} | "
            #     f"last_postopen_run_day={last_postopen_run_day}"
            # )
            # This defines the “trading day” — the day the market opens.
            # market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            # if last_preclose_run_day != market_day:
            #     last_preclose_run_day = None
            # if last_postopen_run_day != market_day:
            #     last_postopen_run_day = None

            # open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
            # logger.info(
            #     f"[SCHEDULER] now_local={now_local.isoformat()} | "
            #     f"open_dt={open_dt.isoformat()} | "
            #     f"close_dt={close_dt.isoformat()} | "
            #     f"preclose_dt={preclose_dt.isoformat()} | "
            #     f"reopen_dt={reopen_dt.isoformat()} | "
            #     f"last_preclose_run_day={last_preclose_run_day} | "
            #     f"last_postopen_run_day={last_postopen_run_day}"
            # )
            #rebuild_market_timeline(settings)

            # open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(
            #     now_local, settings)
            # logger.info(
            #     f"[SCHEDULER] now_local={now_local.isoformat()} | "
            #     f"open_dt={open_dt.isoformat()} | "
            #     f"close_dt={close_dt.isoformat()} | "
            #     f"preclose_dt={preclose_dt.isoformat()} | "
            #     f"reopen_dt={reopen_dt.isoformat()} | "
            #     f"last_preclose_run_day={last_preclose_run_day} | "
            #     f"last_postopen_run_day={last_postopen_run_day}"
            # )
            # This defines the “trading day” — the day the market opens.
            # market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            # if last_preclose_run_day != market_day:
            #     last_preclose_run_day = None
            # if last_postopen_run_day != market_day:
            #     last_postopen_run_day = None

            # market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            # if last_preclose_run_day != market_day:
            #     last_preclose_run_day = None
            # if last_postopen_run_day != market_day:
            #     last_postopen_run_day = None

            # ==========================================
            # PRE-CLOSE WINDOW — run ONCE per market day
            # ==========================================
            if _market_times["next_preclose"] <= now_local:
                logger.info(
                    "Triggering pre-close ensure")
                #IB_INSTANCE = connect_ib_for_webhook()
                if IB_INSTANCE and IB_INSTANCE.isConnected():
                    with IB_LOCK:
                        ensure_preclose_close_if_needed(IB_INSTANCE, settings)

                    #IB_INSTANCE.disconnect()
                    #logger.info("[IB] Clean disconnect after preclose")
                else:
                    logger.info(
                        "[ALARM] Preclose: IB not able to connected")

            # ==========================================
            # POST-OPEN WINDOW — run ONCE per market day
            # ==========================================
            if _market_times["next_postopen"] <= now_local and settings.post_open_min:
                logger.info(
                    "Triggering post-open ensure")

                #IB_INSTANCE = connect_ib_for_webhook()
                if IB_INSTANCE and IB_INSTANCE.isConnected():
                    with IB_LOCK:
                        ensure_postopen_reopen_if_needed(IB_INSTANCE, settings)
                    #IB_INSTANCE.disconnect()
                    #logger.info("[IB] Clean disconnect after postopen")
                else:
                    logger.info(
                        "[ALARM] Postopen: IB not able to connected")

                    
            rebuild_market_timeline(settings)

        except Exception as e:
            logger.info(f"Scheduler error: {e}")

        time.sleep(20)


_scheduler_started_flag = False


def start_scheduler():
    global _scheduler_started_flag

    if _scheduler_started_flag:
        return

    _scheduler_started_flag = True

    t = threading.Thread(target=background_scheduler_loop, daemon=True)
    t.start()
    # logger.info("Background scheduler thread started.")

# START scheduler on module import (works with Waitress)


start_scheduler()


# def start_ib_connection():
#     t = threading.Thread(target=ib_connection_manager, daemon=True)
#     t.start()
    
start_ib()

threading.Thread(
    target=ib_connection_watchdog,
    daemon=True
).start()
# start_ib_connection()
# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)


@app.post("/webhook")
def webhook() -> Any:
    global IB_INSTANCE
    logger.info("===HTTP /webhook received")

    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    # Trades log: whole webhook as single JSON line (multiline start with {)
    log_trade_event(payload)

    try:
        # logger.info(f"FILL_TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        settings = settings_cache.get()
        # logger.info(f"FILL_TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        sig = parse_signal(payload)
        # Track which IB-mapped symbols we have seen so we can refresh tradingHours weekly
        register_symbol_usage_from_signal(sig)
        # logger.info(
        #     f"[HTTP] Received Tradin View alert={sig.raw_alert} symbol={sig.symbol} desired_dir={sig.desired_direction} desired_qty={sig.desired_qty} take_profit={sig.take_profit} stop_loss={sig.stop_loss}")

        now_local = now_in_market_tz(settings)
        #open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)

        # delay of 1 minute after reopen (custom)
        extra_webhook_delay_min = 1
        webhook_allowed_dt = _market_times["prev_postopen"] + timedelta(minutes=extra_webhook_delay_min)
        if now_local < webhook_allowed_dt:
            logger.info(
                f"[CHECK] GATE webhook_blocked_until={webhook_allowed_dt.isoformat()} "
                f"now={now_local.isoformat()} alert={sig.raw_alert}"
            )
            og_trade_event({"event": "outside_market_hours_skipping_execution"})
            return jsonify({
                "ok": True,
                "ignored": True,
                "reason": "webhook_wait_reopen_delay",
                "allowed_after": webhook_allowed_dt.isoformat()
            }), 200

        # market hours gating
        if not in_trading_window(now_local):
            logger.info(
                f"[CHECK] GATE outside_market_hours now={now_local.isoformat()} open={_market_times['next_postopen']} close={_market_times['next_preclose']} ")
            logger.info(
                f"Ignored: outside market window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
            log_trade_event({"event": "outside_market_hours_skipping_execution"})
            return jsonify({"ok": True, "ignored": True, "reason": "outside_market_hours"}), 200

        # if within_preclose_window(now_local, settings):
        #     logger.info(
        #         f"[CHECK] GATE within_preclose_window now={now_local.isoformat()}")
        #     logger.info(
        #         f"Ignored: within preclose window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
        #     return jsonify({"ok": True, "ignored": True, "reason": "within_preclose_window"}), 200

        #IB_INSTANCE = connect_ib_for_webhook()
        if not IB_INSTANCE or not IB_INSTANCE.isConnected():
            logger.info("[ALARM][WEBHOOK] Global IB_INSTANCE is not connected")
            return jsonify({"ok": False, "error": "ib_not_connected"}), 503
        with IB_LOCK:
            contract = build_contract(sig)
            #logger.info(f"{contract}")
            # Qualify contract and detect ambiguity
            qualified = qualify_contract(contract)
            # qualified = True
            # logger.info(f"two")
            # logger.info(f"[EXEC] qualifyContracts returned count={len(qualified) if qualified is not None else 0}")

            # If 0 or more than 1 contract returned → ambiguous
            result=None
            if not qualified or len(qualified) != 1:
                log_step(
                    f"[ALARM] Ambiguous or unresolved contract for symbol={sig.symbol}; skipping execution. "
                    f"qualified_count={len(qualified)}"
                )
                # logger.info(
                #     f"Ambiguous or unresolved contract for symbol={sig.symbol}; skipping execution. "
                #     f"qualified_count={len(qualified)}"
                # )
                # logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                # IB_INSTANCE.disconnect()
                result.update({
                    "ok": True,
                    "action": "skipped_ambiguous_contract",                # INFO: early return

                    "reason": "ambiguous_contract",
                    "qualified_count": len(qualified)
                })
                flush_account_log("WEBHOOK_EXEC")
            else:
                result = execute_signal_for_account(IB_INSTANCE, sig, settings,contract)
                qty = current_position_qty(IB_INSTANCE, contract)
                trades = IB_INSTANCE.openTrades()
                filtered = []
                for t in trades:
                    try:
                        if t.contract and t.contract.conId == contract.conId:
                            filtered.append(t)
                    except:
                        continue
                log_trade_event({"positions_after_webhook_execution": abs(qty), "orders_after_webhook_execution": len(filtered)})
        return jsonify({"ok": result["ok"], "result": result}), 200

    except Exception as e:
        log_trade_event({"error": {e}})
        logger.exception(
            f"[ALARM] Webhook handling failed. payload={payload} err={e}")
        return jsonify({"ok": False, "error": str(e)}), 400
    #finally:
        # 🔥 CRITICAL — disconnect exactly once
        # try:
        #     if IB_INSTANCE and IB_INSTANCE.isConnected():
        #         IB_INSTANCE.disconnect()
        #         logger.info("[IB] Clean disconnect after webhook")
        # except Exception:
        #     pass
        # logger.info("[IB] Clean disconnect after webhook")
        #IB_INSTANCE = None  # cleanup

    # except ExecError as e:
    #     # Expected, domain-level problems
    #     alarm_log(
    #         e.code,
    #         str(e),
    #         acct=ACCOUNT_SHORT_NAME,
    #         request_id=request_id,
    #         payload_summary=str(payload)[:500],
    #     )
    #     logger.exception(f"[WEBHOOK_EXEC_ERROR] request_id={request_id}")
    #     return (
    #         jsonify({
    #             "ok": False,
    #             "error": e.code,
    #             "message": str(e),
    #         }),
    #         e.http_status,
    #     )

    # except Exception as e:
    #     # Unexpected bug/edge case
    #     alarm_log(
    #         "unexpected_error",
    #         str(e),
    #         acct=ACCOUNT_SHORT_NAME,
    #         request_id=request_id,
    #     )
    #     logger.exception(f"[WEBHOOK_UNEXPECTED] request_id={request_id}")
    #     return (
    #         jsonify({
    #             "ok": False,
    #             "error": "internal_error",
    #             "message": "Unexpected error in executor",
    #         }),
    #         500,
    #     )

    # finally:
    #     # 6) Always clean up IB and flush account log ONCE
    #     try:
    #         if IB_INSTANCE and IB_INSTANCE.isConnected():
    #             IB_INSTANCE.disconnect()
    #             logger.info(f"[IB] request_id={request_id} Clean disconnect after webhook")
    #     except Exception:
    #         logger.exception(f"[IB] request_id={request_id} Error during disconnect")

    #     flush_account_log("WEBHOOK_EXEC")

# --------------------------------------------------------------------
# def create_app():
#     #start_scheduler()
#     return app
# When running executor.py directly (not via waitress)
if __name__ == "__main__":
    logger.info("===STARTING: Executor initialized")
    
    

