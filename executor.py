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
from ib_insync import IB, MarketOrder, Contract, Future, StopOrder, LimitOrder, BracketOrder  # type: ignore
import asyncio


# ---------------------------
# Config (ENV)
# ---------------------------
LOG_PATH = os.getenv("EXECUTOR_LOG_PATH", "/opt/ibc/execution/executor.log")
STATE_PATH = os.getenv("EXECUTOR_STATE_PATH", "/opt/ibc/execution/executor_state.json")

# DynamoDB settings
# DDB_TABLE = os.getenv("DDB_TABLE", "ankro-global-settings")
# DDB_PK = os.getenv("DDB_PK", "GLOBAL")
# DDB_SK = os.getenv("DDB_SK", "SETTINGS")
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
    "MES1!": {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "MES", "lastTradeDateOrContractMonth": "", "localSymbol": "MESH6"},
    "ES1!":  {"secType": "FUT", "exchange": "CME", "currency": "USD", "symbol": "ES",  "lastTradeDateOrContractMonth": "", "localSymbol": "ESH6"},
}

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
# ---------------------------------------------------------
# Per-account log buffers
# ---------------------------------------------------------
_account_logs = {}       # { account_number: [str, str, ...] }
_account_logs_lock = threading.Lock()

def log_step(acct: int, text: str):
    """
    Append a line to that account's log buffer.
    """
    with _account_logs_lock:
        if acct not in _account_logs:
            _account_logs[acct] = []
        _account_logs[acct].append(text)

# def flush_account_log(acct: int, prefix: str):
#     """
#     Emit ONE log to CloudWatch for this account,
#     then clear the buffer.
#     """
#     with _account_logs_lock:
#         buf = _account_logs.get(acct, [])
#         if not buf:
#             return
#         combined = "\n".join(buf)
#         logger.info(f"=== {prefix} acct={acct} BEGIN ===\n{combined}\n=== {prefix} END ===")
#         _account_logs[acct] = []  # clear
        
def flush_account_log(acct: int, header: str):
    try:
        lines = _account_logs.get(acct, [])
        if not lines:
            return

        msg = (
            f"[=== {header} acct={acct} BEGIN ===]\n"   # HEADER LINE                                     # <-- REQUIRED BLANK LINE
            + "\n".join(" " + ln for ln in lines)      # BODY INDENTED (so CW hides it)
        )
        logger.info(msg)

    finally:
        _account_logs[acct] = []



# ---------------------------
def resolved_region() -> str:
    # INFO: region resolution
    session = boto3.Session()
    rn = session.region_name
    logger.info(f"[AWS] Resolved region={rn}")
    if not rn:
        raise RuntimeError("AWS region could not be resolved from environment / metadata")
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


PARAM_PATHS = [
    "/ankro/settings/delay_sec",
    "/ankro/settings/execution_delay",
    "/ankro/settings/pre_close_min",
    "/ankro/settings/post_open_min",
    "/ankro/settings/market_open",
    "/ankro/settings/market_close",
    "/ankro/settings/timezone",
]

def load_settings_from_ssm() -> "Settings":
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameters(Names=PARAM_PATHS, WithDecryption=False)

    kv = {}
    for p in resp["Parameters"]:
        name = p["Name"].split("/")[-1]
        kv[name] = p["Value"]

    return Settings(
        delay_sec          = int(kv.get("delay_sec", 2)),
        execution_delay    = int(kv.get("execution_delay", 2)),
        pre_close_min      = int(kv.get("pre_close_min", 10)),
        post_open_min      = int(kv.get("post_open_min", 5)),
        market_open        = kv.get("market_open", "09:30"),
        market_close       = kv.get("market_close", "16:00"),
        timezone           = kv.get("timezone", "America/New_York"),
    )

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
    # region = resolved_region()
    # logger.info(f"[SECRETS] Listing accounts region={region} filter='{SECRETS_FILTER_SUBSTRING}'")
    
    # sm = boto3.client("secretsmanager", region_name=region)
    
    logger.info(f"[SECRETS] Listing accounts region={REGION} filter='{SECRETS_FILTER_SUBSTRING}'")

    sm = boto3.client("secretsmanager", region_name=REGION)


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
                logger.info(f"Failed parsing secret '{name}': {e}")

    # deterministic order
    accounts.sort(key=lambda a: a.account_number)
    logger.info(f"[SECRETS] Accounts loaded count={len(accounts)}")
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

    #logger.info(f"[DEBUG/MH] Parsed market_open={settings.market_open}, market_close={settings.market_close}")
    #logger.info(f"[DEBUG/MH] Initial open_dt={open_dt}, close_dt={close_dt}")

    # ============================================================
    # OVERNIGHT SESSION FIX (CORRECT, SINGLE BLOCK)
    # ============================================================
    if close_dt <= open_dt:
        #logger.info("[DEBUG/MH] Overnight session detected")

        if now_local < open_dt:
            # After midnight but before today's open → session started yesterday
            #logger.info("[DEBUG/MH] now_local < open_dt → shifting open_dt to previous day")
            open_dt = open_dt - timedelta(days=1)
            # DO NOT shift close_dt here
        else:
            # After today's open → close_dt belongs to the next day
            close_dt_next = close_dt + timedelta(days=1)
            #logger.info(f"[DEBUG/MH] now_local >= open_dt → shifting close_dt to next day: {close_dt_next}")
            close_dt = close_dt_next
    # else:
    #     logger.info("[DEBUG/MH] Normal daytime session (no overnight shift).")

    preclose_dt = close_dt - timedelta(minutes=settings.pre_close_min)
    reopen_dt   = open_dt + timedelta(minutes=settings.post_open_min)

    # FINAL LOGGING
    # logger.info(
    #     f"[DEBUG/MH] FINAL window: open_dt={open_dt.isoformat()}  "
    #     f"close_dt={close_dt.isoformat()} "
    #     f"preclose_dt={preclose_dt.isoformat()} "
    #     f"reopen_dt={reopen_dt.isoformat()}"
    # )

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
    logger.info(f"[DBG_PARSE] RAW symbol={symbol_raw!r} RAW exchange={exchange_raw!r}")
    exchange = exchange_raw.split("_")[0]
    logger.info(f"[DBG_PARSE] Normalized symbol={symbol!r} exchange={exchange!r}")


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
    # risk_ok = (
    # (take_profit is not None and stop_loss is not None)
    # or
    # (target_pct is not None and stop_loss is not None)
    # )
    risk_ok = (take_profit is not None and stop_loss is not None)

    
    # Exits
    if "exit" in a and "long" in a:
        return Signal(
            symbol=symbol,
            exchange=exchange,
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
            exchange=exchange,
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
            risk_valid=risk_ok# ⭐ ADD HERE
        )

    raise ValueError(f"Unrecognized alert format: '{alert}'")


        
# ---------------------------
# IB helpers (per-account IB instance; no shared global IB)
# ---------------------------
# def ib_connect(host: str, port: int, client_id: int) -> IB:
#     logger.info(f"[IB] Connecting host={host} port={port} client_id={client_id} timeout={IB_CONNECT_TIMEOUT_SEC}")
#     ib = IB()
#     ib.connect(host, port, clientId=client_id, timeout=IB_CONNECT_TIMEOUT_SEC)
#     logger.info(f"[IB] Connected host={host} port={port} client_id={client_id}")
#     return ib

def ib_connect(host: str, port: int, client_id: int) -> IB:
    # Force each thread to have its own event loop
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    #logger.info(f"[IB] Connecting host={host} port={port} client_id={client_id} timeout={IB_CONNECT_TIMEOUT_SEC}")
    ib = IB()

    try:
        ib.connect(host, port, clientId=client_id, timeout=IB_CONNECT_TIMEOUT_SEC)
    except Exception as e:
        # logger.error(
        #     f"[IB][ERROR] Connection FAILED host={host} port={port} clientId={client_id} err={e}"
        # )
        raise  # rethrow so execute_signal_for_account() can catch it
    
    #logger.info(f"[IB] Connected host={host} port={port} client_id={client_id}")
    
    # Clean event handlers AFTER successful connection
    ib.execDetailsEvent.clear()
    ib.commissionReportEvent.clear()
    ib.orderStatusEvent.clear()
    ib.openOrderEvent.clear()
    
    return ib


def cancel_all_open_orders(ib, reason="", acct=None, symbol=None, contract=None):
    log_step(acct, f"CANCEL_OPEN_ORDERS reason={reason}")

    try:
        trades = ib.openTrades()
    except:
        trades = []

    if not trades:
        log_step(acct, "CANCEL_OPEN_ORDERS: NONE")
        return

    for t in trades:
        try:
            o = t.order
            c = t.contract

            if contract is not None:
                if getattr(c, "conId", None) != getattr(contract, "conId", None):
                    continue
            elif symbol is not None:
                if getattr(c, "localSymbol", None) != symbol and getattr(c, "symbol", None) != symbol:
                    continue

            ib.cancelOrder(o)

        except:
            continue

    log_step(acct, "CANCEL_OPEN_ORDERS: DONE")




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


def close_position(ib: IB, contract: Contract, qty: int, acct: int) -> None:
    action = "SELL" if qty > 0 else "BUY"
    log_step(acct, f"CLOSE_POSITION: sending {action} {abs(qty)}")
    log_step(acct,
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
        ib.placeOrder(contract, order)
    except Exception as e:
        log_step(acct, f"CLOSE_POSITION_FAIL: error={e}")
        return

    # More robust wait loop: IB updates positions asynchronously
    for i in range(15):      # ~15 seconds worst-case
        ib.waitOnUpdate(timeout=1.0)   # consume API messages
        time.sleep(0.2)

        # Force refresh from IB — important!
        ib.reqPositions()
        ib.waitOnUpdate(timeout=0.5)

        remaining = current_position_qty(ib, contract)
        if remaining == 0:
            log_step(acct, f"CLOSE_POSITION_SUCCESS (confirmed after {i+1} checks)")
            return

    log_step(acct, f"CLOSE_POSITION_FAIL: still_open={remaining} after retries")









def open_position_with_brackets(
    ib: IB,
    contract: Contract,
    direction: int,
    qty: int,
    take_profit: float | None,
    stop_loss: float | None,
    target_percentage: float | None,
    acct: int,
    tp_sl_are_multipliers: bool = True
) -> None:

    if take_profit is None or stop_loss is None:
        if take_profit is None and stop_loss is None:
            log_step(acct, "OPEN_ENTRY_SKIPPED: TP and SL missing")
        elif take_profit is None:
            log_step(acct, "OPEN_ENTRY_SKIPPED: TP missing")
        else:
            log_step(acct, "OPEN_ENTRY_SKIPPED: SL missing")
        return

    action = "BUY" if direction > 0 else "SELL"

    # ----------------------------------------------------
    # MARKET PRICE (reference for multipliers)
    # ----------------------------------------------------
    # ref_price = None

    # if tp_sl_are_multipliers:
    #     t = ib.reqMktData(contract, "", False, False)
    #     t0 = time.time()
    #     while time.time() - t0 < 3:
    #         ib.waitOnUpdate(timeout=0.5)
    #         mp = t.marketPrice()
    #         if mp and mp > 0:
    #             ref_price = float(mp)
    #             break
    #     ib.cancelMktData(contract)

    #     if not ref_price:
    #         log_step(acct, "ENTRY_PRICE_FAIL: cannot compute TP/SL — no market data")
    #         return

    # ----------------------------------------------------
    # COMPUTE TP / SL
    # ----------------------------------------------------

    if tp_sl_are_multipliers:
        # incoming values are multipliers, e.g. 1.02, 0.995
        try:
            tp_price = 6900 * float(take_profit)
        except:
            tp_price = None

        try:
            sl_price = 6900 * float(stop_loss)
        except:
            sl_price = None

    else:
        # incoming values are absolute prices (postopen restore)
        tp_price = float(take_profit) if take_profit is not None else None
        sl_price = float(stop_loss) if stop_loss is not None else None

    log_step(acct, f"TP_PRICE: {tp_price}")
    log_step(acct, f"SL_PRICE: {sl_price}")

    exit_action = "SELL" if direction > 0 else "BUY"

    parent = MarketOrder(action, abs(int(qty)))

    tp_order = LimitOrder(exit_action, abs(int(qty)), tp_price)
    sl_order = StopOrder(exit_action, abs(int(qty)), sl_price)

    br = BracketOrder(parent, tp_order, sl_order)

    # ----------------------------------------------------
    # SUBMIT + FINAL STATES
    # ----------------------------------------------------
    bracket_lines = []
    bracket_lines.append(f"BRACKET SUBMIT: {contract.localSymbol} dir={action} qty={qty}")
    bracket_lines.append("-" * 40)

    trades = []

    for o in br:
        try:
            trade = ib.placeOrder(contract, o)
            trades.append(trade)
            bracket_lines.append(
                f"SUBMIT: {o.orderType} {o.action} qty={o.totalQuantity}"
            )
        except Exception as e:
            bracket_lines.append(f"SUBMIT_FAIL: {e}")
            break

    ib.sleep(0.5)
    ib.waitOnUpdate(timeout=2)

    for t in trades:
        st = t.orderStatus
        bracket_lines.append(
            f"FINAL: orderId={t.order.orderId} type={t.order.orderType} "
            f"action={t.order.action} status={st.status} "
            f"filled={st.filled} remaining={st.remaining}"
        )

    bracket_lines.append("-" * 40)
    bracket_lines.append(f"PositionsAfter: {len(ib.positions())}")
    bracket_lines.append(f"TradesAfter:    {len(ib.openTrades())}")

    log_step(acct, "\n".join(bracket_lines))





def wait_until_flat(ib: IB, contract: Contract, settings: Settings) -> bool:
    for i in range(MAX_STATE_CHECKS):
        qty = current_position_qty(ib, contract)
        if qty == 0:
            return True
        #logger.info(f"Waiting for close to reflect (attempt {i+1}/{MAX_STATE_CHECKS}), qty still {qty}")
        ib.sleep(0.1)
        time.sleep(1)
    return False


# ---------------------------
# Pre-close / post-open logic (ONLY runs when webhook arrives)
# ---------------------------
def ensure_preclose_close_if_needed(settings: Settings, accounts: List[AccountSpec]) -> None:
    now_local = now_in_market_tz(settings)

    dayk = state_key_for_day(now_local.date())

    with _state_lock:
        st = load_state()
        st.setdefault("preclose", {})
        st["preclose"].setdefault(dayk, {})

    for acc in accounts:
        log_step(acc.account_number, "Preclose potential position closing")
        try:
            with _state_lock:
                st = load_state()
                already = bool(
                    st.get("preclose", {})
                      .get(dayk, {})
                      .get(str(acc.account_number), {})
                      .get("done", False)
                )
            # if already:
            #     continue

            ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)
            pos = ib.positions()

            # =======================================================
            # SNAPSHOT OPEN TRADES (NOT openOrders)
            # =======================================================
            try:
                ib.reqOpenOrders()
                ib.waitOnUpdate(timeout=2)
            except Exception:
                pass

            trades = list(ib.openTrades())
            orders_snapshot: List[Dict[str, Any]] = []

            if not trades:
                log_step(acc.account_number, "No open orders to cancel")
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

                cancel_all_open_orders(ib, reason="preclose", acct=acc.account_number)

            # =======================================================
            # BUILD MAP FOR CORRECT CONTRACT PAIRING BY conId
            # =======================================================
            qualified_by_conid = {}
            try:
                more_trades = ib.openTrades()
                for t in more_trades:
                    qc = t.contract
                    cid = getattr(qc, "conId", None)
                    if cid:
                        qualified_by_conid[cid] = qc
            except:
                pass

            # =======================================================
            # POSITION SNAPSHOT
            # =======================================================
            snapshot: Dict[str, Any] = {}

            if not pos:
                log_step(acc.account_number, "No open position to close")
            else:
                for p in pos:
                    if int(p.position) != 0:
                        c = p.contract
                        key = (
                            str(getattr(c, "conId", "")) or
                            f"{c.secType}:{c.symbol}:{getattr(c, 'exchange', '')}"
                        )
                        snapshot[key] = {
                            "secType": c.secType,
                            "symbol": c.symbol,
                            "exchange": getattr(c, "exchange", ""),
                            "currency": getattr(c, "currency", ""),
                            "lastTradeDateOrContractMonth": getattr(c, "lastTradeDateOrContractMonth", ""),
                            "position": int(p.position),
                            "conId": getattr(c, "conId", None),    # <-- THIS IS THE FIX
                        }

                # =======================================================
                # CLOSE POSITION using paired, qualified contract
                # =======================================================
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
                            qc = ib.qualifyContracts(c)
                            if qc:
                                c = qc[0]
                        except:
                            pass

                    log_step(
                        acc.account_number,
                        f"Preclose: acct={acc.account_number} closing {c.secType} {c.symbol} conId={cid} exch={getattr(c,'exchange',None)} qty={q}"
                    )

                    close_position(ib, c, q, acc.account_number)

            ib.disconnect()

            # =======================================================
            # SAVE PRE-CLOSE STATE
            # =======================================================
            with _state_lock:
                st = load_state()
                st.setdefault("preclose", {})
                st["preclose"].setdefault(dayk, {})
                st["preclose"][dayk][str(acc.account_number)] = {
                    "done": True,
                    "at": now_local.isoformat(),
                    "snapshot": snapshot,
                    "orders_snapshot": orders_snapshot,
                    "reopen_done": False,
                    "reopen_at": None,
                }
                save_state(st)

            log_step(acc.account_number, f"Preclose: acct={acc.account_number} completed snapshot_count={len(snapshot)}")

        except Exception as e:
            log_step(acc.account_number, f"Preclose error acct={acc.account_number}: {e}")

        finally:
            flush_account_log(acc.account_number, "PRECLOSE_EXEC")


        


def ensure_postopen_reopen_if_needed(settings: Settings, accounts: List[AccountSpec]) -> None:
    now_local = now_in_market_tz(settings)

    dayk = state_key_for_day(now_local.date())

    for acc in accounts:
        log_step(acc.account_number, "Postopen potential position reopen")
        try:
            with _state_lock:
                st = load_state()
                entry = st.get("preclose", {}).get(dayk, {}).get(str(acc.account_number))

            # must have preclose + not already reopened
            if not entry:
                log_step(acc.account_number, "Nothing to reopen")
                continue
            if entry.get("reopen_done") or entry.get("done"):
                log_step(acc.account_number, "Postopen was already triggered")
                continue

            snapshot: Dict[str, Any] = entry.get("snapshot", {}) or {}
            orders_snapshot: List[Dict[str, Any]] = entry.get("orders_snapshot", []) or []

            ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)

            # must be flat right now
            any_open = any(int(p.position) != 0 for p in ib.positions())
            if any_open:
                log_step(acc.account_number, f"Postopen: acct={acc.account_number} not flat; will not reopen.")
                ib.disconnect()
                continue

            # no snapshot → mark done
            if not snapshot:
                with _state_lock:
                    st = load_state()
                    st["preclose"][dayk][str(acc.account_number)]["reopen_done"] = True
                    st["preclose"][dayk][str(acc.account_number)]["reopen_at"] = now_local.isoformat()
                    save_state(st)
                log_step(acc.account_number, f"Postopen: acct={acc.account_number} nothing to reopen (empty snapshot).")
                ib.disconnect()
                continue

            # =====================================================
            # MAIN LOOP — rebuild original contracts using conId
            # =====================================================
            for conId_key, meta in snapshot.items():

                # conId is stored as dict key
                try:
                    conId = int(conId_key)
                except:
                    log_step(acc.account_number, f"[POSTOPEN] Invalid conId key={conId_key}, skipping.")
                    continue

                # Build by conId (BEST POSSIBLE METHOD)
                c = Contract()
                c.conId = conId

                try:
                    qc = ib.qualifyContracts(c)
                    if qc:
                        c = qc[0]   # fully qualified contract
                except Exception as e:
                    log_step(acc.account_number, f"[POSTOPEN] qualify failed conId={conId}: {e}")

                direction = +1 if int(meta.get("position", 0)) > 0 else -1
                qty = abs(int(meta.get("position", 0)))

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

                log_step(
                    acc.account_number,
                    f"[POSTOPEN] Reopening with bracket acct={acc.account_number} "
                    f"symbol={c.symbol} conId={conId} dir={direction} qty={qty} "
                    f"tp={tp_price} sl={sl_price}"
                )

                open_position_with_brackets(
                    ib,
                    c,
                    direction,
                    qty,
                    take_profit=tp_price,
                    stop_loss=sl_price,
                    target_percentage=None,
                    acct=acc.account_number,
                    tp_sl_are_multipliers=False
                )

                time.sleep(1)

            ib.disconnect()

            # Mark reopen done
            with _state_lock:
                st = load_state()
                st["preclose"][dayk][str(acc.account_number)]["reopen_done"] = True
                st["preclose"][dayk][str(acc.account_number)]["reopen_at"] = now_local.isoformat()
                save_state(st)

        except Exception as e:
            ib.disconnect()
            log_step(acc.account_number, f"Postopen error acct={acc.account_number}: {e}")
        finally:
            try:
                ib.disconnect()
            except:
                pass
        
            # Give IB time to release clientID to avoid "clientId already in use"
            time.sleep(1.2)
            flush_account_log(acc.account_number, "POSTOPEN_EXEC")



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


def ib_connect_retry(host, port, client_id, attempts=3, delay=1):
    for i in range(attempts):
        try:
            return ib_connect(host, port, client_id)
        except Exception as e:
            if i < attempts - 1:
                time.sleep(delay)
                continue
            raise

# ---------------------------
# Per-account signal execution
# ---------------------------
def execute_signal_for_account(acc: AccountSpec, sig: Signal, settings: Settings) -> Dict[str, Any]:
    #logger.info(f"[EXEC] Start acct={acc.account_number} port={acc.api_port} client_id={acc.client_id} alert={sig.raw_alert} symbol={sig.symbol} desired_dir={sig.desired_direction} desired_qty={sig.desired_qty}")
    # log_step(acc.account_number,
    #      f"EXEC_START alert={sig.raw_alert} symbol={sig.symbol} dir=SELL if {sig.desired_direction==-1} else 'BUY' qty={sig.desired_qty}")
    log_step(
        acc.account_number,
        f"EXEC_START alert={sig.raw_alert} "
        f"symbol={sig.symbol} "
        f"dir={'SELL' if sig.desired_direction == -1 else 'BUY'} "
        f"qty={sig.desired_qty}"
    )
    result = {
        "account_number": acc.account_number,
        "secret_name": acc.secret_name,
        "api_port": acc.api_port,
        "client_id": acc.client_id,
    }
    
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop in this thread → create a dummy one so ib_insync won't try async
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        try:
            ib = ib_connect_retry(IB_HOST, acc.api_port, acc.client_id, attempts=3, delay=1)
        except Exception as e:
            # LOG PER ACCOUNT (your new unified log)
            log_step(acc.account_number, "IB_CONNECT_FAIL port")
            
            # Return failure cleanly
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
            return {
                "account_number": acc.account_number,
                "secret_name": acc.secret_name,
                "api_port": acc.api_port,
                "client_id": acc.client_id,
                "ok": False,
                "error": f"connection_failed: {e}",
            }


        contract = build_contract(sig)
        # Qualify contract and detect ambiguity
        qualified = ib.qualifyContracts(contract)
        #logger.info(f"[EXEC] qualifyContracts returned count={len(qualified) if qualified is not None else 0}")
        
        # If 0 or more than 1 contract returned → ambiguous
        if not qualified or len(qualified) != 1:
            log_step(acc.account_number, 
                f"Ambiguous or unresolved contract for symbol={sig.symbol}; skipping execution. "
                f"qualified_count={len(qualified)}"
            )
            #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
            ib.disconnect()
            result.update({
                "ok": True,
                "action": "skipped_ambiguous_contract",                # INFO: early return

                "reason": "ambiguous_contract",
                "qualified_count": len(qualified)
            })
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
            return result
        
        # Use the resolved contract
        contract = qualified[0]


        qty = current_position_qty(ib, contract)
        
        log_step(acc.account_number, "#############STATE CHECK######################")
        if qty != 0:
            side = "BUY" if qty > 0 else "SELL"
            log_step(acc.account_number, f"Current position for symbol: {side} {abs(qty)}")
        else:
            log_step(acc.account_number, "Current position for symbol: No opened positions")
        
        try:
            trades = ib.openTrades()
            
        except:
            trades = []
        
        filtered = []
        for t in trades:
            try:
                if t.contract and t.contract.conId == contract.conId:
                    filtered.append(t)
            except:
                continue
        
        if filtered:
            log_step(acc.account_number, "Open orders for symbol:")
            for t in filtered:
                try:
                    c = t.contract
                    o = t.order
                    #os = t.orderState
        
                    log_step(
                        acc.account_number,
                        "  " + " ".join([
                            f"type={getattr(o,'orderType',None)}",
                            f"action={getattr(o,'action',None)}",
                            f"qty={getattr(o,'totalQuantity',None)}"
                        ])
                    )
                except Exception as e:
                    log_step(acc.account_number, f"ERROR printing open order: {e}")
                    raise
        else:
            log_step(acc.account_number, "Open orders for symbol: NONE")
        
        log_step(acc.account_number, "#############END OF STATE CHECK#################")




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
        log_step(acc.account_number, f"[EXEC] Signal timestamp {sig.signal}  {sig.age} {int(settings.execution_delay)}")
        allow_entry = False
        allow_entry = True
        if sig.desired_direction != 0 and sig_age is not None:
            if sig_age > int(settings.execution_delay):
                log_step(acc.account_number, f"[EXEC] Entry not executed: the execution is delayed by more than {int(settings.execution_delay)} relative to the signal")
                allow_entry = False
        #logger.info(f"[EXEC] latency_check sig_age={sig_age} execution_delay={int(settings.execution_delay)} allow_entry={allow_entry}")

        # ----------------------------------------------------------
        # EXIT (ALWAYS perform exit, ignore latency)
        # ----------------------------------------------------------
        if sig.desired_direction == 0:
            #logger.info(f"[EXEC] Branch=EXIT acct={acc.account_number} current_qty={qty}")
            cancel_all_open_orders(
                ib,
                reason="exit_signal",
                acct=acc.account_number,
                contract=contract
            )
            if qty == 0:
                log_step(acc.account_number, "No positions to exit for the exit signal")
                result.update({"ok": True, "action": "none_already_flat"})
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result

            # Close existing position
            close_position(ib, contract, qty, acc.account_number)
            time.sleep(1)

            # Retry logic
            if not wait_until_flat(ib, contract, settings):
                log_step(acc.account_number, "Exit close not reflected — retrying")
                #close_position(ib, contract, qty)
                time.sleep(1)

                result.update({"ok": False, "action": "exit_failed_after_retry"})
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result
            log_step(acc.account_number, "Position closed successfully")
            result.update({"ok": True, "action": "exit_closed"})
            #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
            ib.disconnect()
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # SAME DIRECTION → NO-OP
        # ----------------------------------------------------------
        desired_dir = sig.desired_direction
        desired_qty = sig.desired_qty if sig.desired_qty > 0 else DEFAULT_QTY

        if qty != 0 and ((qty > 0 and desired_dir > 0) or (qty < 0 and desired_dir < 0)):
            log_step(acc.account_number, "[EXEC] Same direction position already opened. Skipping execution")
            result.update({"ok": True, "action": "none_same_direction_already_open", "current_qty": qty})
            #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
            ib.disconnect()
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # REVERSAL (close ALWAYS, but entry obeys latency)
        # ----------------------------------------------------------
        if qty != 0 and ((qty > 0 and desired_dir < 0) or (qty < 0 and desired_dir > 0)):
            log_step(acc.account_number, f"[EXEC] Opposite direction singal: Closing position and opening new one.")
            close_position(ib, contract, qty, acc.account_number)
            cancel_all_open_orders(ib, reason="before_reversal_entry", acct=acc.account_number, contract=contract)
            time.sleep(1)

            # Retry close
            if not wait_until_flat(ib, contract, settings):
                log_step(acc.account_number, "Reversal close not confirmed — not clear if existing postions were closed. Skipping execution")
                #close_position(ib, contract, qty)
                time.sleep(1)
                result.update({"ok": False, "action": "reversal_close_not_confirmed"})
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result

                # if not wait_until_flat(ib, contract, settings):
                #     result.update({"ok": False, "action": "reversal_close_not_confirmed"})
                #     logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                #     ib.disconnect()
                #     return result

            # TOO OLD → do not open new position
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result
            
            if not sig.risk_valid:
                log_step(acc.account_number, "[EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result
            
            #cancel_all_open_orders(ib, reason="before_reversal_entry", acct=acc.account_number, contract=contract)
            # Fresh enough → open reversed position
            time.sleep(max(1, int(settings.delay_sec)))

            open_position_with_brackets(
                ib,
                contract,
                desired_dir,
                desired_qty,
                sig.take_profit,
                sig.stop_loss,
                sig.target_percentage,
                acc.account_number    # <<< NEW
            )

            #logger.info(f"[EXEC] Entry order submitted acct={acc.account_number} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "reversal_opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
            ib.disconnect()
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
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
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result
            
            if desired_dir != 0 and not sig.risk_valid:
                log_step(acc.account_number, "[EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "entry_ignored_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
                ib.disconnect()
                flush_account_log(acc.account_number, "WEBHOOK_EXEC")
                return result


            # Fresh entry → open position
            #time.sleep(max(0, int(settings.execution_delay)))
            cancel_all_open_orders(ib, reason="before_new_entry", acct=acc.account_number, contract=contract)
            open_position_with_brackets(
                ib,
                contract,
                desired_dir,
                desired_qty,
                sig.take_profit,
                sig.stop_loss,
                sig.target_percentage,
                acc.account_number    # <<< NEW
            )

            #logger.info(f"[EXEC] Entry order submitted acct={acc.account_number} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
            ib.disconnect()
            flush_account_log(acc.account_number, "WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # Should never reach here
        # ----------------------------------------------------------
        result.update({"ok": False, "action": "ambiguous_state", "current_qty": qty})
        #logger.info(f"[IB] Disconnect acct={acc.account_number} port={acc.api_port} client_id={acc.client_id}")
        ib.disconnect()
        flush_account_log(acc.account_number, "WEBHOOK_EXEC")
        return result

    except Exception as e:
        result.update({"ok": False, "error": str(e)})
        flush_account_log(acc.account_number, "WEBHOOK_EXEC")
        return result
    finally:
        flush_account_log(acc.account_number, "WEBHOOK_EXEC")

def background_scheduler_loop():
    """
    Market-aware scheduler:
      - Runs ensure_preclose_close_if_needed() once per market day
      - Runs ensure_postopen_reopen_if_needed() once per market day
      - Automatically detects new market day by comparing open_dt dates
    """
    #logger.info("Background scheduler thread started.")

    last_preclose_run_day = None   # date of market_open for the last run
    last_postopen_run_day = None   # date of market_open for the last run

    while True:
        try:
            settings = settings_cache.get()
            now_local = now_in_market_tz(settings)

            open_dt, close_dt, preclose_dt, reopen_dt = market_datetimes(now_local, settings)
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
            market_day = open_dt.date()

            # 🚨 RESET LOGIC
            # If the market_day changed since last loop iteration => new trading day
            if last_preclose_run_day != market_day:
                last_preclose_run_day = None
            if last_postopen_run_day != market_day:
                last_postopen_run_day = None



            # ==========================================
            # PRE-CLOSE WINDOW — run ONCE per market day
            # ==========================================
            if preclose_dt <= now_local:
                if last_preclose_run_day != market_day:
                    logger.info("Triggering pre-close ensure for market day %s", market_day)
                    accounts = secrets_cache.get_accounts()
                    logger.info(f"[HTTP] Accounts found={len(accounts)}")
                    if accounts:
                        ensure_preclose_close_if_needed(settings, accounts)
                    last_preclose_run_day = market_day

            # ==========================================
            # POST-OPEN WINDOW — run ONCE per market day
            # ==========================================
            if reopen_dt <= now_local:
                if last_postopen_run_day != market_day:
                    logger.info("Triggering post-open ensure for market day %s", market_day)
                    accounts = secrets_cache.get_accounts()
                    logger.info(f"[HTTP] Accounts found={len(accounts)}")
                    if accounts:
                        ensure_postopen_reopen_if_needed(settings, accounts)
                    last_postopen_run_day = market_day

        except Exception as e:
            logger.exception(f"Scheduler error: {e}")

        time.sleep(20)
        
_scheduler_started_flag = False

def start_scheduler():
    global _scheduler_started_flag

    if _scheduler_started_flag:
        return

    _scheduler_started_flag = True

    t = threading.Thread(target=background_scheduler_loop, daemon=True)
    t.start()
    #logger.info("Background scheduler thread started.")

# START scheduler on module import (works with Waitress)

start_scheduler()
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
    logger.info("===HTTP /webhook received")
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    try:
        settings = settings_cache.get()
        sig = parse_signal(payload)
        logger.info(f"[HTTP] Received Tradin View alert={sig.raw_alert} symbol={sig.symbol} desired_dir={sig.desired_direction} desired_qty={sig.desired_qty} take_profit={sig.take_profit} stop_loss={sig.stop_loss}")

        # Per your requirement: ONLY do things when JSON arrives:
        # 1) list secrets
        accounts = secrets_cache.get_accounts()
        if not accounts:
            logger.info("No ibkr secrets found; refusing to trade.")
            return jsonify({"ok": False, "error": "no_ibkr_secrets_found"}), 503

        now_local = now_in_market_tz(settings)


        # market hours gating
        if not in_trading_window(now_local, settings):
            logger.info(f"[HTTP] GATE outside_market_hours now={now_local.isoformat()} open_close={market_datetimes(now_local, settings)[:2]}")
            logger.info(f"Ignored: outside market window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
            return jsonify({"ok": True, "ignored": True, "reason": "outside_market_hours"}), 200

        if within_preclose_window(now_local, settings):
            logger.info(f"[HTTP] GATE within_preclose_window now={now_local.isoformat()}")
            logger.info(f"Ignored: within preclose window now={now_local.isoformat()} alert={sig.raw_alert} symbol={sig.symbol}")
            return jsonify({"ok": True, "ignored": True, "reason": "within_preclose_window"}), 200

        # Execute on ALL accounts in parallel
        results: List[Dict[str, Any]] = []
        max_workers = min(MAX_PARALLEL_ACCOUNTS, max(1, len(accounts)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            #logger.info(f"[HTTP] Executing signal across accounts in parallel workers={max_workers}")
            futs = [pool.submit(execute_signal_for_account, acc, sig, settings) for acc in accounts]
            for f in as_completed(futs):
                results.append(f.result())

        ok_all = all(r.get("ok") is True for r in results)
        any_failed = any(r.get("ok") is False for r in results)

        #logger.info(f"Webhook done symbol={sig.symbol} alert={sig.raw_alert} ok_all={ok_all} results={results}")

        # If any failed, return 503 so Lambda can retry if you want
        status = 200 if not any_failed else 503
        ##logger.info(f"[HTTP] Response status={status} ok_all={ok_all} any_failed={any_failed}")
        return jsonify({"ok": ok_all, "results": sorted(results, key=lambda r: r.get("account_number", 0))}), status

    except Exception as e:
        logger.exception(f"Webhook handling failed. payload={payload} err={e}")
        return jsonify({"ok": False, "error": str(e)}), 400


# --------------------------------------------------------------------

# When running executor.py directly (not via waitress)
if __name__ == "__main__":
    logger.info("===STARTING: Executor initialized")
    ##start_scheduler()