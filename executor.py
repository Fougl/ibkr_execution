#!/usr/bin/env python3
"""
executor.py (multi-account, parallel)

- Flask webhook receiver on Lightsail.
- On EVERY webhook:
  - loads GLOBAL market settings from DynamoDB (PK=GLOBAL, SK=SETTINGS)
  - lists all Secrets Manager secrets whose name contains "ibkr" (case-insensitive)
    (AWS region resolved from instance via boto3.Session().region_name)
  - for EACH account secret:
      - derives IB API port = 4002 + account_number
      - derives IB clientId = IB_CLIENT_ID_BASE + account_number
      - connects to that gateway and executes the signal
  - executions happen IN PARALLEL across accounts (ThreadPoolExecutor)

Expected webhook examples:
  {"alert": "Exit Long", "symbol": "MES1!"}
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
  "account_number": "37",
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pytz
from flask import Flask, jsonify, request
from ib_insync import IB, MarketOrder, Contract, Future, StopOrder, LimitOrder  # type: ignore


# ---------------------------
# Config (ENV)
# ---------------------------
LOG_PATH = os.getenv("EXECUTOR_LOG_PATH", "/opt/ibc/execution/executor.log")
STATE_PATH = os.getenv("EXECUTOR_STATE_PATH", "/opt/ibc/execution/executor_state.json")

# DynamoDB settings
DDB_TABLE = os.getenv("DDB_TABLE", "ankro-global-settings")
DDB_PK = os.getenv("DDB_PK", "GLOBAL")
DDB_SK = os.getenv("DDB_SK", "SETTINGS")
SETTINGS_CACHE_TTL_SEC = int(os.getenv("SETTINGS_CACHE_TTL_SEC", "10"))

# Secrets Manager
SECRETS_FILTER_SUBSTRING = os.getenv("SECRETS_FILTER_SUBSTRING", "ibkr")
SECRETS_CACHE_TTL_SEC = int(os.getenv("SECRETS_CACHE_TTL_SEC", "10"))

# IBKR connection base rules (match your orchestrator rule)
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_API_PORT_BASE = int(os.getenv("IB_API_PORT_BASE", "4002"))       # port = base + account_number
IB_CLIENT_ID_BASE = int(os.getenv("IB_CLIENT_ID_BASE", "1000"))     # clientId = base + account_number

# Web server
BIND_HOST = os.getenv("BIND_HOST", "0.0.0.0")
BIND_PORT = int(os.getenv("BIND_PORT", "5001"))

# Dedupe window
DEDUPE_TTL_SEC = int(os.getenv("DEDUPE_TTL_SEC", "15"))

# Retry behavior
MAX_STATE_CHECKS = int(os.getenv("MAX_STATE_CHECKS", "15"))
DEFAULT_QTY = int(os.getenv("DEFAULT_QTY", "1"))

# Parallelism
MAX_PARALLEL_ACCOUNTS = int(os.getenv("MAX_PARALLEL_ACCOUNTS", "16"))
IB_CONNECT_TIMEOUT_SEC = float(os.getenv("IB_CONNECT_TIMEOUT_SEC", "3.0"))

# Contract mapping
SYMBOL_MAP_JSON = os.getenv("SYMBOL_MAP_JSON", "")
DEFAULT_SYMBOL_MAP = {
    "MES1!": {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "MES", "lastTradeDateOrContractMonth": ""},
    "ES1!":  {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "ES",  "lastTradeDateOrContractMonth": ""},
}


os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logger = logging.getLogger("executor")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(LOG_PATH, mode="a")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

stream_handler = logging.StreamHandler()

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
# ---------------------------
# # Logging
# # ---------------------------
# logger = logging.getLogger("executor")
# logger.setLevel(logging.INFO)
# os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
# _handler = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=5)
# _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
# logger.addHandler(_handler)


# ---------------------------
# Utils: AWS region resolution (like orchestrator)
# ---------------------------
def resolved_region() -> str:
    session = boto3.Session()
    if not session.region_name:
        raise RuntimeError("AWS region could not be resolved from environment / metadata")
    return session.region_name


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
        s = load_settings_from_ddb()
        with self._lock:
            self._cached = s
            self._cached_at = now
        return s


settings_cache = SettingsCache()


def load_settings_from_ddb() -> Settings:
    try:
        ddb = boto3.client("dynamodb", region_name=resolved_region())
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


# ---------------------------
# Secrets (list all ibkr secrets on-demand, cached)
# ---------------------------
@dataclass(frozen=True)
class AccountSpec:
    secret_name: str
    account_number: int
    api_port: int
    client_id: int


class SecretsCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cached: List[AccountSpec] = []
        self._cached_at: float = 0.0

    def get_accounts(self) -> List[AccountSpec]:
        now = time.time()
        with self._lock:
            if self._cached and (now - self._cached_at) < SECRETS_CACHE_TTL_SEC:
                return list(self._cached)

        accounts = list_ibkr_accounts()
        with self._lock:
            self._cached = accounts
            self._cached_at = now
        return list(accounts)


secrets_cache = SecretsCache()


def list_ibkr_accounts() -> List[AccountSpec]:
    region = resolved_region()
    sm = boto3.client("secretsmanager", region_name=region)

    accounts: List[AccountSpec] = []
    paginator = sm.get_paginator("list_secrets")

    for page in paginator.paginate():
        for s in page.get("SecretList", []):
            name = s.get("Name", "")
            if SECRETS_FILTER_SUBSTRING.lower() not in name.lower():
                continue

            try:
                resp = sm.get_secret_value(SecretId=name)
                secret = json.loads(resp["SecretString"])

                raw = secret.get("account_number")
                if raw is None:
                    continue
                if isinstance(raw, str):
                    digits = "".join([c for c in raw if c.isdigit()])
                    if not digits:
                        continue
                    acct = int(digits)
                else:
                    acct = int(raw)

                api_port = IB_API_PORT_BASE + acct
                client_id = IB_CLIENT_ID_BASE + acct

                accounts.append(AccountSpec(secret_name=name, account_number=acct, api_port=api_port, client_id=client_id))
            except Exception as e:
                logger.warning(f"Failed parsing secret '{name}': {e}")

    # deterministic order
    accounts.sort(key=lambda a: a.account_number)
    return accounts


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

    logger.info(f"[DEBUG/MH] Parsed market_open={settings.market_open}, market_close={settings.market_close}")
    logger.info(f"[DEBUG/MH] Initial open_dt={open_dt}, close_dt={close_dt}")

    # ============================================================
    # OVERNIGHT SESSION FIX (CORRECT, SINGLE BLOCK)
    # ============================================================
    if close_dt <= open_dt:
        logger.info("[DEBUG/MH] Overnight session detected")

        if now_local < open_dt:
            # After midnight but before today's open → session started yesterday
            logger.info("[DEBUG/MH] now_local < open_dt → shifting open_dt to previous day")
            open_dt = open_dt - timedelta(days=1)
            # DO NOT shift close_dt here
        else:
            # After today's open → close_dt belongs to the next day
            close_dt_next = close_dt + timedelta(days=1)
            logger.info(f"[DEBUG/MH] now_local >= open_dt → shifting close_dt to next day: {close_dt_next}")
            close_dt = close_dt_next
    else:
        logger.info("[DEBUG/MH] Normal daytime session (no overnight shift).")

    preclose_dt = close_dt - timedelta(minutes=settings.pre_close_min)
    reopen_dt   = open_dt + timedelta(minutes=settings.post_open_min)

    # FINAL LOGGING
    logger.info(
        f"[DEBUG/MH] FINAL window: open_dt={open_dt.isoformat()}  "
        f"close_dt={close_dt.isoformat()} "
        f"preclose_dt={preclose_dt.isoformat()} "
        f"reopen_dt={reopen_dt.isoformat()}"
    )

    return open_dt, close_dt, preclose_dt, reopen_dt





def in_trading_window(now_local: datetime, settings: Settings) -> bool:
    open_dt, close_dt, _, _ = market_datetimes(now_local, settings)
    return open_dt <= now_local <= close_dt


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
        ltm = info.get("lastTradeDateOrContractMonth", "")
        return Future(symbol=symbol, lastTradeDateOrContractMonth=ltm, exchange=exchange, currency=currency)
    raise ValueError(f"Unsupported secType for {tv_symbol}: {info.get('secType')}")


# ---------------------------
# Signal parsing
# ---------------------------
@dataclass
class Signal:
    symbol: str
    desired_direction: int  # +1 long, -1 short, 0 exit/flat
    desired_qty: int
    raw_alert: str
    take_profit: float | None = None
    stop_loss: float | None = None
    target_percentage: float | None = None
    signal_timestamp: float | None = None
    risk_valid: bool | None = None 



    
def parse_signal(payload: Dict[str, Any]) -> Signal:
    alert = str(payload.get("alert", "")).strip()
    symbol = str(payload.get("symbol", "")).strip()
    if not symbol:
        raise ValueError("Missing 'symbol' in payload")
    if not alert:
        raise ValueError("Missing 'alert' in payload")

    a = alert.lower()

    # NEW: read signal timestamp from JSON
    signal_ts = payload.get("signalTimestamp")
    try:
        signal_ts = float(signal_ts)
        signal_ts = parse_timestamp(signal_ts)
    except:
        signal_ts = None

    # Accept qty aliases: qty / quantity / size
    qty = payload.get("qty", payload.get("quantity", payload.get("size", DEFAULT_QTY)))
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
    risk_ok = (
    (take_profit is not None and stop_loss is not None)
    or
    (target_pct is not None and stop_loss is not None)
    )

    # Exits
    if "exit" in a and "long" in a:
        return Signal(
            symbol=symbol,
            desired_direction=0,
            desired_qty=0,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts  # ⭐ ADD HERE
        )

    if "exit" in a and "short" in a:
        return Signal(
            symbol=symbol,
            desired_direction=0,
            desired_qty=0,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts  # ⭐ ADD HERE
        )

    # Long entries
    if ("entry" in a and "long" in a) or ("enter" in a and "long" in a) or a in ("long", "buy", "enter long", "entry long"):
        return Signal(
            symbol=symbol,
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
            desired_direction=-1,
            desired_qty=qty_i,
            raw_alert=alert,
            take_profit=take_profit,
            stop_loss=stop_loss,
            target_percentage=target_pct,
            signal_timestamp=signal_ts,
            risk_valid=risk_ok# ⭐ ADD HERE
        )

    raise ValueError(f"Unrecognized alert format: '{alert}'")


        
# ---------------------------
# IB helpers (per-account IB instance; no shared global IB)
# ---------------------------
def ib_connect(host: str, port: int, client_id: int) -> IB:
    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=IB_CONNECT_TIMEOUT_SEC)
    return ib


def current_position_qty(ib: IB, contract: Contract) -> int:
    qty = 0
    for p in ib.positions():
        try:
            if getattr(p.contract, "conId", None) and getattr(contract, "conId", None):
                if p.contract.conId == contract.conId:
                    qty += int(p.position)
            # else:
            #     if p.contract.symbol == getattr(contract, "symbol", None) and p.contract.secType == contract.secType:
            #         qty += int(p.position)
        except Exception:
            continue
    return qty


def close_position(ib: IB, contract: Contract, qty: int) -> None:
    if qty == 0:
        return
    action = "SELL" if qty > 0 else "BUY"
    order = MarketOrder(action, abs(int(qty)))
    ib.placeOrder(contract, order)


def open_position(ib: IB, contract: Contract, direction: int, qty: int) -> None:
    if direction not in (+1, -1):
        raise ValueError("direction must be +1 (long) or -1 (short)")
    action = "BUY" if direction > 0 else "SELL"
    order = MarketOrder(action, abs(int(qty)))
    ib.placeOrder(contract, order)



def open_position_with_brackets(
    ib: IB,
    contract: Contract,
    direction: int,
    qty: int,
    take_profit: float | None,
    stop_loss: float | None,
    target_percentage: float | None,
) -> None:
    """
    Places a MARKET entry and then (optionally) places TP/SL as OCA child orders.
    - take_profit / stop_loss are interpreted as PRICE OFFSETS (e.g. 1.0 = 1.0 points).
    - target_percentage is interpreted as % of filled entry price (used only if take_profit is not provided).
    """
    # If no risk params provided, keep original behavior
    if take_profit is None and stop_loss is None and target_percentage is None:
        open_position(ib, contract, direction, qty)
        return

    # 1) Market entry
    action = "BUY" if direction > 0 else "SELL"
    parent = MarketOrder(action, abs(int(qty)))
    trade = ib.placeOrder(contract, parent)

    # Wait for fill to get avgFillPrice
    t0 = time.time()
    fill_price = 0.0
    while time.time() - t0 < 20:
        ib.waitOnUpdate(timeout=1)
        try:
            fill_price = float(getattr(trade.orderStatus, "avgFillPrice", 0) or 0)
        except Exception:
            fill_price = 0.0
        if fill_price > 0:
            break

    if fill_price <= 0:
        # If we couldn't get a fill price, fall back to original behavior (no brackets)
        logger.warning("Entry filled price unavailable; skipping TP/SL placement.")
        return

    # Derive TP offset if targetPercentage provided and takeProfit absent
    tp_offset = take_profit
    if tp_offset is None and target_percentage is not None:
        tp_offset = abs(fill_price) * (float(target_percentage) / 100.0)

    sl_offset = stop_loss

    # Nothing to place
    if tp_offset is None and sl_offset is None:
        return

    # Compute TP/SL absolute prices
    # Long: TP above, SL below. Short: TP below, SL above.
    tp_price = None
    sl_price = None
    if tp_offset is not None:
        tp_price = fill_price + (direction * float(tp_offset))
    if sl_offset is not None:
        sl_price = fill_price - (direction * float(sl_offset))

    # 2) Place OCA TP/SL orders
    exit_action = "SELL" if direction > 0 else "BUY"
    oca = f"OCA_{int(time.time()*1000)}_{os.getpid()}"

    if tp_price is not None:
        tp_order = LimitOrder(exit_action, abs(int(qty)), float(tp_price))
        tp_order.ocaGroup = oca
        tp_order.ocaType = 1
        tp_order.transmit = False
        ib.placeOrder(contract, tp_order)

    if sl_price is not None:
        sl_order = StopOrder(exit_action, abs(int(qty)), float(sl_price))
        sl_order.ocaGroup = oca
        sl_order.ocaType = 1
        sl_order.transmit = True
        ib.placeOrder(contract, sl_order)

    logger.info(
        f"Placed TP/SL OCA={oca} fill={fill_price} "
        f"tp={tp_price if tp_price is not None else 'none'} "
        f"sl={sl_price if sl_price is not None else 'none'}"
    )


def wait_until_flat(ib: IB, contract: Contract, settings: Settings) -> bool:
    for i in range(MAX_STATE_CHECKS):
        qty = current_position_qty(ib, contract)
        if qty == 0:
            return True
        logger.info(f"Waiting for close to reflect (attempt {i+1}/{MAX_STATE_CHECKS}), qty still {qty}")
        time.sleep(max(1, int(settings.delay_sec)))
    return False


# ---------------------------
# Pre-close / post-open logic (ONLY runs when webhook arrives)
# ---------------------------
def ensure_preclose_close_if_needed(settings: Settings, accounts: List[AccountSpec]) -> None:
    now_local = now_in_market_tz(settings)
    logger.info("Preclose potential position closing")


    dayk = state_key_for_day(now_local.date())

    with _state_lock:
        st = load_state()
        st.setdefault("preclose", {})
        st["preclose"].setdefault(dayk, {})

    # Close each account independently; sequential is fine here (only 1x/day).
    for acc in accounts:
        try:
            with _state_lock:
                st = load_state()
                already = bool(st.get("preclose", {}).get(dayk, {}).get(str(acc.account_number), {}).get("done", False))
            if already:
                continue

            ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)
            pos = ib.positions()

            snapshot: Dict[str, Any] = {}
            for p in pos:
                if int(p.position) != 0:
                    key = str(getattr(p.contract, "conId", "")) or f"{p.contract.secType}:{p.contract.symbol}:{getattr(p.contract, 'exchange', '')}"
                    snapshot[key] = {
                        "secType": p.contract.secType,
                        "symbol": p.contract.symbol,
                        "exchange": getattr(p.contract, "exchange", ""),
                        "currency": getattr(p.contract, "currency", ""),
                        "lastTradeDateOrContractMonth": getattr(p.contract, "lastTradeDateOrContractMonth", ""),
                        "position": int(p.position),
                    }

            for p in pos:
                q = int(p.position)
                if q == 0:
                    continue
                logger.info(f"Preclose: acct={acc.account_number} closing {p.contract.secType} {p.contract.symbol} qty={q}")
                close_position(ib, p.contract, q)

            ib.disconnect()

            with _state_lock:
                st = load_state()
                st.setdefault("preclose", {})
                st["preclose"].setdefault(dayk, {})
                st["preclose"][dayk][str(acc.account_number)] = {
                    "done": True,
                    "at": now_local.isoformat(),
                    "snapshot": snapshot,
                    "reopen_done": False,
                    "reopen_at": None,
                }
                save_state(st)

            logger.info(f"Preclose: acct={acc.account_number} completed snapshot_count={len(snapshot)}")
        except Exception as e:
            logger.exception(f"Preclose error acct={acc.account_number}: {e}")


def ensure_postopen_reopen_if_needed(settings: Settings, accounts: List[AccountSpec]) -> None:
    now_local = now_in_market_tz(settings)
    logger.info("Postopen potential position reopen")

    dayk = state_key_for_day(now_local.date())

    for acc in accounts:
        try:
            with _state_lock:
                st = load_state()
                entry = st.get("preclose", {}).get(dayk, {}).get(str(acc.account_number))

            if not entry or not entry.get("done"):
                continue
            if entry.get("reopen_done"):
                continue

            snapshot: Dict[str, Any] = entry.get("snapshot", {}) or {}

            ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)

            # Only reopen if currently flat for this account
            any_open = any(int(p.position) != 0 for p in ib.positions())
            if any_open:
                logger.info(f"Postopen: acct={acc.account_number} not flat; will not reopen.")
                ib.disconnect()
                continue

            if not snapshot:
                with _state_lock:
                    st = load_state()
                    st["preclose"][dayk][str(acc.account_number)]["reopen_done"] = True
                    st["preclose"][dayk][str(acc.account_number)]["reopen_at"] = now_local.isoformat()
                    save_state(st)
                logger.info(f"Postopen: acct={acc.account_number} nothing to reopen (empty snapshot).")
                ib.disconnect()
                continue

            for _, meta in snapshot.items():
                if meta.get("secType") != "FUT":
                    continue
                c = Future(
                    symbol=meta.get("symbol", ""),
                    lastTradeDateOrContractMonth=meta.get("lastTradeDateOrContractMonth", ""),
                    exchange=meta.get("exchange", ""),
                    currency=meta.get("currency", "USD"),
                )
                direction = +1 if int(meta.get("position", 0)) > 0 else -1
                qty = abs(int(meta.get("position", 0)))
                logger.info(f"Postopen: acct={acc.account_number} reopening {c.symbol} dir={'LONG' if direction>0 else 'SHORT'} qty={qty}")
                open_position(ib, c, direction, qty)
                time.sleep(max(1, int(settings.execution_delay)))

            ib.disconnect()

            with _state_lock:
                st = load_state()
                st["preclose"][dayk][str(acc.account_number)]["reopen_done"] = True
                st["preclose"][dayk][str(acc.account_number)]["reopen_at"] = now_local.isoformat()
                save_state(st)

            logger.info(f"Postopen: acct={acc.account_number} completed reopen cycle.")
        except Exception as e:
            logger.exception(f"Postopen error acct={acc.account_number}: {e}")


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
def execute_signal_for_account(acc: AccountSpec, sig: Signal, settings: Settings) -> Dict[str, Any]:
    result = {
        "account_number": acc.account_number,
        "secret_name": acc.secret_name,
        "api_port": acc.api_port,
        "client_id": acc.client_id,
    }

    try:
        ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)

        contract = build_contract(sig.symbol)
        ib.qualifyContracts(contract)

        qty = current_position_qty(ib, contract)
        logger.info(f"acct={acc.account_number} BEFORE symbol={sig.symbol} "
                    f"alert={sig.raw_alert} qty={qty}")

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

        allow_entry = True
        if sig.desired_direction != 0 and sig_age is not None:
            if sig_age > int(settings.execution_delay):
                allow_entry = False

        # ----------------------------------------------------------
        # EXIT (ALWAYS perform exit, ignore latency)
        # ----------------------------------------------------------
        if sig.desired_direction == 0:
            if qty == 0:
                result.update({"ok": True, "action": "none_already_flat"})
                ib.disconnect()
                return result

            # Close existing position
            close_position(ib, contract, qty)
            time.sleep(1)

            # Retry logic
            if not wait_until_flat(ib, contract, settings):
                logger.warning("Exit close not reflected — retrying")
                close_position(ib, contract, qty)
                time.sleep(1)

                if not wait_until_flat(ib, contract, settings):
                    result.update({"ok": False, "action": "exit_failed_after_retry"})
                    ib.disconnect()
                    return result

            result.update({"ok": True, "action": "exit_closed"})
            ib.disconnect()
            return result

        # ----------------------------------------------------------
        # SAME DIRECTION → NO-OP
        # ----------------------------------------------------------
        desired_dir = sig.desired_direction
        desired_qty = sig.desired_qty if sig.desired_qty > 0 else DEFAULT_QTY

        if qty != 0 and ((qty > 0 and desired_dir > 0) or (qty < 0 and desired_dir < 0)):
            result.update({"ok": True, "action": "none_same_direction_already_open", "current_qty": qty})
            ib.disconnect()
            return result

        # ----------------------------------------------------------
        # REVERSAL (close ALWAYS, but entry obeys latency)
        # ----------------------------------------------------------
        if qty != 0 and ((qty > 0 and desired_dir < 0) or (qty < 0 and desired_dir > 0)):
            close_position(ib, contract, qty)
            time.sleep(1)

            # Retry close
            if not wait_until_flat(ib, contract, settings):
                logger.warning("Reversal close not reflected — retrying")
                close_position(ib, contract, qty)
                time.sleep(1)

                if not wait_until_flat(ib, contract, settings):
                    result.update({"ok": False, "action": "reversal_close_not_confirmed"})
                    ib.disconnect()
                    return result

            # TOO OLD → do not open new position
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                ib.disconnect()
                return result
            
            if not sig.risk_valid:
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                ib.disconnect()
                return result
            

            # Fresh enough → open reversed position
            time.sleep(max(1, int(settings.delay_sec)))

            open_position_with_brackets(
                ib, contract, desired_dir, desired_qty,
                sig.take_profit, sig.stop_loss, sig.target_percentage
            )

            result.update({
                "ok": True,
                "action": "reversal_opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            ib.disconnect()
            return result

        # ----------------------------------------------------------
        # FLAT → NEW ENTRY
        # ----------------------------------------------------------
        if qty == 0:
            # Skip stale entries
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                ib.disconnect()
                return result
            
            if desired_dir != 0 and not sig.risk_valid:
                result.update({
                    "ok": True,
                    "action": "entry_ignored_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                ib.disconnect()
                return result


            # Fresh entry → open position
            time.sleep(max(0, int(settings.execution_delay)))

            open_position_with_brackets(
                ib, contract, desired_dir, desired_qty,
                sig.take_profit, sig.stop_loss, sig.target_percentage
            )

            result.update({
                "ok": True,
                "action": "opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            ib.disconnect()
            return result

        # ----------------------------------------------------------
        # Should never reach here
        # ----------------------------------------------------------
        result.update({"ok": False, "action": "ambiguous_state", "current_qty": qty})
        ib.disconnect()
        return result

    except Exception as e:
        result.update({"ok": False, "error": str(e)})
        return result



# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)


@app.get("/health")
def health() -> Any:
    try:
        region = resolved_region()
        accounts = secrets_cache.get_accounts()
        return jsonify({"ok": True, "region": region, "accounts_found": len(accounts)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/webhook")
def webhook() -> Any:
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    try:
        settings = settings_cache.get()
        sig = parse_signal(payload)

        # Per your requirement: ONLY do things when JSON arrives:
        # 1) list secrets
        accounts = secrets_cache.get_accounts()
        if not accounts:
            logger.warning("No ibkr secrets found; refusing to trade.")
            return jsonify({"ok": False, "error": "no_ibkr_secrets_found"}), 503

        now_local = now_in_market_tz(settings)


        # market hours gating
        if not in_trading_window(now_local, settings):
            logger.info(f"Ignored: outside market window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
            return jsonify({"ok": True, "ignored": True, "reason": "outside_market_hours"}), 200

        if within_preclose_window(now_local, settings):
            logger.info(f"Ignored: within preclose window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
            return jsonify({"ok": True, "ignored": True, "reason": "within_preclose_window"}), 200

        # Execute on ALL accounts in parallel
        results: List[Dict[str, Any]] = []
        max_workers = min(MAX_PARALLEL_ACCOUNTS, max(1, len(accounts)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futs = [pool.submit(execute_signal_for_account, acc, sig, settings) for acc in accounts]
            for f in as_completed(futs):
                results.append(f.result())

        ok_all = all(r.get("ok") is True for r in results)
        any_failed = any(r.get("ok") is False for r in results)

        logger.info(f"Webhook done symbol={sig.symbol} alert={sig.raw_alert} ok_all={ok_all} results={results}")

        # If any failed, return 503 so Lambda can retry if you want
        status = 200 if not any_failed else 503
        return jsonify({"ok": ok_all, "results": sorted(results, key=lambda r: r.get("account_number", 0))}), status

    except Exception as e:
        logger.exception(f"Webhook handling failed. payload={payload} err={e}")
        return jsonify({"ok": False, "error": str(e)}), 400


def background_scheduler_loop():
    """
    Market-aware scheduler:
      - Runs ensure_preclose_close_if_needed() once per market day
      - Runs ensure_postopen_reopen_if_needed() once per market day
      - Automatically detects new market day by comparing open_dt dates
    """
    logger.info("Background scheduler thread started.")

    last_preclose_run_day = None   # date of market_open for the last run
    last_postopen_run_day = None   # date of market_open for the last run

    while True:
        try:
            settings = settings_cache.get()
            now_local = now_in_market_tz(settings)

            open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)

            # This defines the “trading day” — the day the market opens.
            market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            if last_preclose_run_day != market_day:
                last_preclose_run_day = None
            if last_postopen_run_day != market_day:
                last_postopen_run_day = None

            accounts = secrets_cache.get_accounts()

            # ==========================================
            # PRE-CLOSE WINDOW — run ONCE per market day
            # ==========================================
            if preclose_dt <= now_local:
                if last_preclose_run_day != market_day:
                    logger.info("Triggering pre-close ensure for market day %s", market_day)
                    if accounts:
                        ensure_preclose_close_if_needed(settings, accounts)
                    last_preclose_run_day = market_day

            # ==========================================
            # POST-OPEN WINDOW — run ONCE per market day
            # ==========================================
            if reopen_dt <= now_local:
                if last_postopen_run_day != market_day:
                    logger.info("Triggering post-open ensure for market day %s", market_day)
                    if accounts:
                        ensure_postopen_reopen_if_needed(settings, accounts)
                    last_postopen_run_day = market_day

        except Exception as e:
            logger.exception(f"Scheduler error: {e}")

        time.sleep(20)


if __name__ == "__main__":
    # Start scheduler thread BEFORE Flask
    t = threading.Thread(target=background_scheduler_loop, daemon=True)
    t.start()

    logger.info(f"Starting executor Flask on {BIND_HOST}:{BIND_PORT} log={LOG_PATH}")
    app.run(host=BIND_HOST, port=BIND_PORT)
