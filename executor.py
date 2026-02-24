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
from datetime import datetime, date, timedelta
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
STATE_PATH = os.getenv("EXECUTOR_STATE_PATH", "/opt/ibc/execution/executor_state.json")
DERIVED_ID = int(os.getenv("DERIVED_ID"))
ACCOUNT_SHORT_NAME= os.getenv("ACCOUNT_SHORT_NAME")

# DynamoDB settings
# DDB_TABLE = os.getenv("DDB_TABLE", "ankro-global-settings")
# DDB_PK = os.getenv("DDB_PK", "GLOBAL")
# DDB_SK = os.getenv("DDB_SK", "SETTINGS")
SETTINGS_CACHE_TTL_SEC = int(os.getenv("SETTINGS_CACHE_TTL_SEC", "240"))



# IBKR connection base rules (match your orchestrator rule)
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_API_PORT_BASE = int(os.getenv("IB_API_PORT_BASE", "4002"))       # port = base + short_name
IB_CLIENT_ID_BASE = int(os.getenv("IB_CLIENT_ID_BASE", "1000"))     # clientId = base + short_name

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


def connect_ib_for_webhook():
    """
    FIX: Waitress threads have no asyncio event loop → ib.connect() fails.
    We create a dummy event loop before ib.connect().
    """
    # ----------------------------
    # 🔧 Install an event loop
    # ----------------------------
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    ib = IB()

    port = 4002 + DERIVED_ID
    cid  = 1 + DERIVED_ID

    try:
        ib.connect(
            host='127.0.0.1',
            port=port,
            clientId=cid,
            timeout=5
        )
        return ib
    except Exception as e:
        logger.error(f"[IB] Webhook-connect failed: {e}")
        return None


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

        msg = (
            f"[=== {header} acct={ACCOUNT_SHORT_NAME} BEGIN ===]\n"   # HEADER LINE                                     # <-- REQUIRED BLANK LINE
            + "\n".join(" " + ln for ln in lines)      # BODY INDENTED (so CW hides it)
        )
        logger.info(msg)

    finally:
        _account_logs[ACCOUNT_SHORT_NAME] = []



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
        #log_step(0, f"[SSM] Loaded JSON settings: {data}")

    except Exception as e:
        log_step(f"[SSM ERROR] Failed loading /ankro/settings: {e}")
        data = {}

    def get(key, default):
        return data.get(key, default)

    return Settings(
        delay_sec          = int(get("delay_sec", 2)),
        execution_delay    = int(get("execution_delay", 2)),
        pre_close_min      = int(get("pre_close_min", 10)),
        post_open_min      = int(get("post_open_min", 5)),
        market_open        = str(get("market_open", "09:30")),
        market_close       = str(get("market_close", "16:00")),
        timezone           = str(get("timezone", "America/New_York")),
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



def cancel_all_open_orders(IB_INSTANCE,reason="", symbol=None, contract=None):
    log_step( f"CANCEL_OPEN_ORDERS reason={reason}")

    try:
        trades = IB_INSTANCE.openTrades()
    except:
        trades = []

    if not trades:
        log_step( "CANCEL_OPEN_ORDERS: NONE")
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

            IB_INSTANCE.cancelOrder(o)

        except:
            continue

    log_step( "CANCEL_OPEN_ORDERS: DONE")




def current_position_qty(IB_INSTANCE, contract: Contract) -> int:
    qty = 0
    for p in IB_INSTANCE.positions():
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


def close_position(IB_INSTANCE, contract: Contract, qty: int) -> None:
    action = "SELL" if qty > 0 else "BUY"
    log_step( f"CLOSE_POSITION: sending {action} {abs(qty)}")
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
        IB_INSTANCE.placeOrder(contract, order)
    except Exception as e:
        log_step( f"CLOSE_POSITION_FAIL: error={e}")
        return

    # More robust wait loop: IB updates positions asynchronously
    for i in range(15):      # ~15 seconds worst-case
        IB_INSTANCE.waitOnUpdate(timeout=1.0)   # consume API messages
        IB_INSTANCE.sleep(0.2)

        # Force refresh from IB — important!
        IB_INSTANCE.reqPositions()
        IB_INSTANCE.waitOnUpdate(timeout=0.5)

        remaining = current_position_qty(IB_INSTANCE,contract)
        if remaining == 0:
            log_step( f"CLOSE_POSITION_SUCCESS (confirmed after {i+1} checks)")
            return

    log_step( f"CLOSE_POSITION_FAIL: still_open={remaining} after retries")



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

def get_min_tick(IB_INSTANCE,contract: Contract) -> float:
    """
    Get ContractDetails.minTick for this contract, cached.
    Requires contract to be qualified (conId known) or at least localSymbol stable.
    """
    key = None
    try:
        cid = getattr(contract, "conId", None)
        if cid:
            key = f"conId:{cid}"
    except Exception:
        pass
    if not key:
        key = f"localSymbol:{getattr(contract, 'localSymbol', '')}"

    with _MIN_TICK_LOCK:
        if key in _MIN_TICK_CACHE:
            return _MIN_TICK_CACHE[key]

    # Ask IB for ContractDetails
    try:
        details = IB_INSTANCE.reqContractDetails(contract)
        IB_INSTANCE.waitOnUpdate(timeout=2)
        if not details:
            raise RuntimeError("reqContractDetails returned empty")
        mt = float(details[0].minTick)
    except Exception as e:
        log_step( f"[ALARM] MIN_TICK_FAIL: using fallback 0.01 err={e}")
        mt = 0.01  # fallback so we don't crash; futures will still likely reject if wrong

    with _MIN_TICK_LOCK:
        _MIN_TICK_CACHE[key] = mt

    log_step( f"MIN_TICK: {mt}")
    return mt

    
def open_position_with_brackets(IB_INSTANCE,
    contract: Contract,
    direction: int,
    qty: int,
    take_profit: float | None,
    stop_loss: float | None,
    target_percentage: float | None,
    tp_sl_are_multipliers: bool = True
) -> None:

    if take_profit is None or stop_loss is None:
        log_step( "OPEN_ENTRY_SKIPPED: TP or SL missing")
        return

    action = "BUY" if direction > 0 else "SELL"
    exit_action = "SELL" if direction > 0 else "BUY"

    # ------------------------------------------------
    # 1️⃣ SEND MARKET PARENT ONLY
    # ------------------------------------------------
    parent = MarketOrder(action, abs(int(qty)))
    trade = IB_INSTANCE.placeOrder(contract, parent)

    log_step( "PARENT_ORDER_SUBMITTED")

    # ------------------------------------------------
    # 2️⃣ WAIT FOR FILL
    # ------------------------------------------------
    fill_price = None
    timeout = time.time() + 10

    while time.time() < timeout:
        IB_INSTANCE.waitOnUpdate(timeout=1)
        if trade.fills:
            fill_price = trade.fills[-1].execution.price
            break

    if not fill_price:
        log_step( "FILL_FAIL: parent not filled")
        return

    log_step( f"FILL_PRICE: {fill_price}")

    # ------------------------------------------------
    # 3️⃣ CALCULATE TP / SL FROM REAL FILL
    # ------------------------------------------------
    tp_pct = float(take_profit) / 100.0
    sl_pct = float(stop_loss) / 100.0

    if direction > 0:
        tp_price = fill_price * (1 + tp_pct)
        sl_price = fill_price * (1 - sl_pct)
    else:
        tp_price = fill_price * (1 - tp_pct)
        sl_price = fill_price * (1 + sl_pct)

    tick = get_min_tick(IB_INSTANCE,contract)
    tp_price = _round_to_tick(tp_price, tick)
    sl_price = _round_to_tick(sl_price, tick)


    log_step( f"TP_PRICE: {tp_price}")
    log_step( f"SL_PRICE: {sl_price}")

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

    IB_INSTANCE.placeOrder(contract, tp_order)
    IB_INSTANCE.placeOrder(contract, sl_order)
    IB_INSTANCE.sleep(0.5)
    IB_INSTANCE.waitOnUpdate(timeout=2)

    log_step( "BRACKET_CHILDREN_SUBMITTED")
    log_step( f"PositionsAfter: {len(IB_INSTANCE.positions())}")
    log_step( f"TradesAfter:    {len(IB_INSTANCE.openTrades())}")








def wait_until_flat(IB_INSTANCE, contract: Contract, settings: Settings) -> bool:
    for i in range(MAX_STATE_CHECKS):
        qty = current_position_qty(IB_INSTANCE, contract)
        if qty == 0:
            return True
        #logger.info(f"Waiting for close to reflect (attempt {i+1}/{MAX_STATE_CHECKS}), qty still {qty}")
        IB_INSTANCE.sleep(0.1)
        #time.sleep(1)
    return False


# ---------------------------
# Pre-close / post-open logic (ONLY runs when webhook arrives)
# ---------------------------
def ensure_preclose_close_if_needed(IB_INSTANCE,settings: Settings) -> None:
    now_local = now_in_market_tz(settings)

    dayk = state_key_for_day(now_local.date())

    with _state_lock:
        st = load_state()
        st.setdefault("preclose", {})
        st["preclose"].setdefault(dayk, {})

    
    if not IB_INSTANCE.isConnected():
        log_step( "[ALARM] Preclose: IB not connected")
        return

    log_step( "Preclose potential position closing")
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

        #ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)
        pos = IB_INSTANCE.positions()

        # =======================================================
        # SNAPSHOT OPEN TRADES (NOT openOrders)
        # =======================================================
        try:
            IB_INSTANCE.reqOpenOrders()
            IB_INSTANCE.waitOnUpdate(timeout=2)
        except Exception:
            pass

        trades = list(IB_INSTANCE.openTrades())
        orders_snapshot: List[Dict[str, Any]] = []

        if not trades:
            log_step( "No open orders to cancel")
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
        # BUILD MAP FOR CORRECT CONTRACT PAIRING BY conId
        # =======================================================
        qualified_by_conid = {}
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
        # POSITION SNAPSHOT
        # =======================================================
        snapshot: Dict[str, Any] = {}

        if not pos:
            log_step( "No open position to close")
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
                        qc = IB_INSTANCE.qualifyContracts(c)
                        if qc:
                            c = qc[0]
                    except:
                        pass

                log_step(
                    f"Preclose: acct={ACCOUNT_SHORT_NAME} closing {c.secType} {c.symbol} conId={cid} exch={getattr(c,'exchange',None)} qty={q}"
                )

                close_position(IB_INSTANCE,c, q)

        #IB_INSTANCE.disconnect()

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

        log_step( f"Preclose: acct={ACCOUNT_SHORT_NAME} completed snapshot_count={len(snapshot)}")

    except Exception as e:
        log_step( f"Preclose error acct={ACCOUNT_SHORT_NAME}: {e}")

    finally:
        flush_account_log("PRECLOSE_EXEC")


        


def ensure_postopen_reopen_if_needed(IB_INSTANCE, settings: Settings) -> None:
    
    now_local = now_in_market_tz(settings)

    dayk = state_key_for_day(now_local.date())

        # Ensure persistent global IB connection is alive
    if not IB_INSTANCE or not IB_INSTANCE.isConnected():
        logger.info("[ALARM] Global IB_INSTANCE is not connected")
        #return jsonify({"ok": False, "error": "ib_not_connected"}), 503


    log_step( "Postopen potential position reopen")
    try:
        with _state_lock:
            st = load_state()
            entry = st.get("preclose", {}).get(dayk, {}).get(str(ACCOUNT_SHORT_NAME))

        # must have preclose + not already reopened
        if not entry:
            log_step( "Nothing to reopen")
            return
        if entry.get("reopen_done") or entry.get("done"):
            log_step( "Postopen was already triggered")
            return

        snapshot: Dict[str, Any] = entry.get("snapshot", {}) or {}
        orders_snapshot: List[Dict[str, Any]] = entry.get("orders_snapshot", []) or []

        #ib = ib_connect(IB_HOST, acc.api_port, acc.client_id)

        # must be flat right now
        any_open = any(int(p.position) != 0 for p in IB_INSTANCE.positions())
        if any_open:
            log_step( f"Postopen: acct={ACCOUNT_SHORT_NAME} not flat; will not reopen.")
            #IB_INSTANCE.disconnect()
            return

        # no snapshot → mark done
        if not snapshot:
            with _state_lock:
                st = load_state()
                st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)]["reopen_done"] = True
                st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)]["reopen_at"] = now_local.isoformat()
                save_state(st)
            log_step( f"Postopen: acct={ACCOUNT_SHORT_NAME} nothing to reopen (empty snapshot).")
            #IB_INSTANCE.disconnect()
            return

        # =====================================================
        # MAIN LOOP — rebuild original contracts using conId
        # =====================================================
        for conId_key, meta in snapshot.items():

            # conId is stored as dict key
            try:
                conId = int(conId_key)
            except:
                log_step( f"[POSTOPEN] Invalid conId key={conId_key}, skipping.")
                continue

            # Build by conId (BEST POSSIBLE METHOD)
            c = Contract()
            c.conId = conId

            try:
                qc = IB_INSTANCE.qualifyContracts(c)
                if qc:
                    c = qc[0]   # fully qualified contract
            except Exception as e:
                log_step( f"[POSTOPEN] qualify failed conId={conId}: {e}")

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
                f"[POSTOPEN] Reopening with bracket acct={ACCOUNT_SHORT_NAME} "
                f"symbol={c.symbol} conId={conId} dir={direction} qty={qty} "
                f"tp={tp_price} sl={sl_price}"
            )

            open_position_with_brackets(IB_INSTANCE,
                c,
                direction,
                qty,
                take_profit=tp_price,
                stop_loss=sl_price,
                target_percentage=None,
                tp_sl_are_multipliers=False
            )

            #time.sleep(1)

        #IB_INSTANCE.disconnect()

        # Mark reopen done
        with _state_lock:
            st = load_state()
            st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)]["reopen_done"] = True
            st["preclose"][dayk][str(ACCOUNT_SHORT_NAME)]["reopen_at"] = now_local.isoformat()
            save_state(st)

    except Exception as e:
        #IB_INSTANCE.disconnect()
        log_step( f"Postopen error acct={ACCOUNT_SHORT_NAME}: {e}")
    finally:
        # try:
        #     IB_INSTANCE.disconnect()
        # except:
        #     pass
    
        # Give IB time to release clientID to avoid "clientId already in use"
        #time.sleep(1.2)
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
def execute_signal_for_account(IB_INSTANCE, sig: Signal, settings: Settings) -> Dict[str, Any]:
    
    if not IB_INSTANCE.isConnected():
        log_step( "IB_NOT_CONNECTED")
        flush_account_log("WEBHOOK_EXEC")
    
        return {
            "short_name": ACCOUNT_SHORT_NAME,
            "api_port": 4002+DERIVED_ID,
            "client_id": 1+DERIVED_ID,
            "ok": False,
            "error": "ib_not_connected",
        }
    log_step( f"DEBUG acct={ACCOUNT_SHORT_NAME} port={4002+DERIVED_ID} clientId={1+DERIVED_ID}")

    log_step(
        f"EXEC_START alert={sig.raw_alert} "
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
        

        logger.info(f"{IB_INSTANCE.positions()}")
        contract = build_contract(sig)
        logger.info(f"{contract}")
        # Qualify contract and detect ambiguity
        qualified = IB_INSTANCE.qualifyContracts(contract)
        #qualified = True
        #logger.info(f"two")
        #logger.info(f"[EXEC] qualifyContracts returned count={len(qualified) if qualified is not None else 0}")
        
        # If 0 or more than 1 contract returned → ambiguous
        if not qualified or len(qualified) != 1:
            log_step( 
                f"Ambiguous or unresolved contract for symbol={sig.symbol}; skipping execution. "
                f"qualified_count={len(qualified)}"
            )
            logger.info( 
                f"Ambiguous or unresolved contract for symbol={sig.symbol}; skipping execution. "
                f"qualified_count={len(qualified)}"
            )
            #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            #IB_INSTANCE.disconnect()
            result.update({
                "ok": True,
                "action": "skipped_ambiguous_contract",                # INFO: early return

                "reason": "ambiguous_contract",
                "qualified_count": len(qualified)
            })
            flush_account_log("WEBHOOK_EXEC")
            return result
        
        # Use the resolved contract
        contract = qualified[0]


        qty = current_position_qty(IB_INSTANCE,contract)
        logger.info(f"two")
        log_step( "#############STATE CHECK######################")
        if qty != 0:
            side = "BUY" if qty > 0 else "SELL"
            log_step( f"Current position for symbol: {side} {abs(qty)}")
        else:
            log_step( "Current position for symbol: No opened positions")
        
        try:
            trades = IB_INSTANCE.openTrades()
            
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
            log_step( "Open orders for symbol:")
            for t in filtered:
                try:
                    c = t.contract
                    o = t.order
                    #os = t.orderState
        
                    log_step(
                        "  " + " ".join([
                            f"type={getattr(o,'orderType',None)}",
                            f"action={getattr(o,'action',None)}",
                            f"qty={getattr(o,'totalQuantity',None)}"
                        ])
                    )
                except Exception as e:
                    log_step( f"ERROR printing open order: {e}")
                    raise
        else:
            log_step( "Open orders for symbol: NONE")
        
        log_step( "#############END OF STATE CHECK#################")




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
        #log_step( f"[EXEC] Signal timestamp {sig.signal_timestamp}  {int(settings.execution_delay)}")
        allow_entry = False
        allow_entry = True
        if sig.desired_direction != 0 and sig_age is not None:
            if sig_age > int(settings.execution_delay):
                log_step( f"[EXEC] Entry not executed: the execution is delayed by more than {int(settings.execution_delay)} relative to the signal")
                allow_entry = False
        #logger.info(f"[EXEC] latency_check sig_age={sig_age} execution_delay={int(settings.execution_delay)} allow_entry={allow_entry}")

        # ----------------------------------------------------------
        # EXIT (ALWAYS perform exit, ignore latency)
        # ----------------------------------------------------------
        if sig.desired_direction == 0:
            #logger.info(f"[EXEC] Branch=EXIT acct={ACCOUNT_SHORT_NAME} current_qty={qty}")
            cancel_all_open_orders(IB_INSTANCE,
                reason="exit_signal",
                contract=contract
            )
            if qty == 0:
                log_step( "No positions to exit for the exit signal")
                result.update({"ok": True, "action": "none_already_flat"})
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

            # Close existing position
            close_position(IB_INSTANCE, contract, qty)
            time.sleep(1)

            # Retry logic
            if not wait_until_flat(IB_INSTANCE, contract, settings):
                log_step( "Exit close not reflected — retrying")
                #close_position(contract, qty)
                time.sleep(1)

                result.update({"ok": False, "action": "exit_failed_after_retry"})
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            log_step( "Position closed successfully")
            result.update({"ok": True, "action": "exit_closed"})
            #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            #IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # SAME DIRECTION → NO-OP
        # ----------------------------------------------------------
        desired_dir = sig.desired_direction
        desired_qty = sig.desired_qty if sig.desired_qty > 0 else DEFAULT_QTY

        if qty != 0 and ((qty > 0 and desired_dir > 0) or (qty < 0 and desired_dir < 0)):
            log_step( "[EXEC] Same direction position already opened. Skipping execution")
            result.update({"ok": True, "action": "none_same_direction_already_open", "current_qty": qty})
            #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            #IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # REVERSAL (close ALWAYS, but entry obeys latency)
        # ----------------------------------------------------------
        if qty != 0 and ((qty > 0 and desired_dir < 0) or (qty < 0 and desired_dir > 0)):
            log_step( f"[EXEC] Opposite direction singal: Closing position and opening new one.")
            close_position(IB_INSTANCE,contract, qty)
            cancel_all_open_orders(IB_INSTANCE,reason="before_reversal_entry",  contract=contract)
            time.sleep(1)

            # Retry close
            if not wait_until_flat(IB_INSTANCE,contract, settings):
                log_step( "Reversal close not confirmed — not clear if existing postions were closed. Skipping execution")
                #close_position(contract, qty)
                time.sleep(1)
                result.update({"ok": False, "action": "reversal_close_not_confirmed"})
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result

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
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            
            if not sig.risk_valid:
                log_step( "[EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "reversal_closed_but_entry_skipped_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            
            #cancel_all_open_orders(reason="before_reversal_entry", acct=ACCOUNT_SHORT_NAME, contract=contract)
            # Fresh enough → open reversed position
            time.sleep(max(1, int(settings.delay_sec)))

            open_position_with_brackets(IB_INSTANCE,
                contract,
                desired_dir,
                desired_qty,
                sig.take_profit,
                sig.stop_loss,
                sig.target_percentage,
                ACCOUNT_SHORT_NAME    # <<< NEW
            )

            #logger.info(f"[EXEC] Entry order submitted acct={ACCOUNT_SHORT_NAME} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "reversal_opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            #IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # FLAT → NEW ENTRY
        # ----------------------------------------------------------
        if qty == 0:
            # Skip stale entries
            cancel_all_open_orders(IB_INSTANCE,reason="before_new_entry", contract=contract)
            if not allow_entry:
                result.update({
                    "ok": True,
                    "action": "entry_skipped_due_to_latency",
                    "latency_seconds": sig_age
                })
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result
            
            if desired_dir != 0 and not sig.risk_valid:
                log_step( "[EXEC] TP or SL not specified. Skipping execution")
                result.update({
                    "ok": True,
                    "action": "entry_ignored_no_risk_params",
                    "reason": "missing_takeprofit_or_stoploss"
                })
                #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
                #IB_INSTANCE.disconnect()
                flush_account_log("WEBHOOK_EXEC")
                return result


            # Fresh entry → open position
            #time.sleep(max(0, int(settings.execution_delay)))
            
            open_position_with_brackets(IB_INSTANCE,
                contract,
                desired_dir,
                desired_qty,
                sig.take_profit,
                sig.stop_loss,
                sig.target_percentage,
                ACCOUNT_SHORT_NAME    # <<< NEW
            )

            #logger.info(f"[EXEC] Entry order submitted acct={ACCOUNT_SHORT_NAME} dir={desired_dir} qty={desired_qty} symbol={sig.symbol}")

            result.update({
                "ok": True,
                "action": "opened",
                "opened_dir": desired_dir,
                "opened_qty": desired_qty
            })
            #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
            #IB_INSTANCE.disconnect()
            flush_account_log("WEBHOOK_EXEC")
            return result

        # ----------------------------------------------------------
        # Should never reach here
        # ----------------------------------------------------------
        result.update({"ok": False, "action": "ambiguous_state", "current_qty": qty})
        #logger.info(f"[IB] Disconnect acct={ACCOUNT_SHORT_NAME} port={acc.api_port} client_id={acc.client_id}")
        #IB_INSTANCE.disconnect()
        flush_account_log("WEBHOOK_EXEC")
        return result

    except Exception as e:
        #logger.info("error: {str(e)}")
        result.update({"ok": False, "error": str(e)})
        flush_account_log("WEBHOOK_EXEC")
        return result
    finally:
        flush_account_log("WEBHOOK_EXEC")

def background_scheduler_loop():
    IB_INSTANCE=None
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
                    #accounts = secrets_cache.get_accounts()
                    #logger.info(f"[HTTP] Accounts found={len(accounts)}")
                    #if accounts:
                    ensure_preclose_close_if_needed(IB_INSTANCE,settings)
                    last_preclose_run_day = market_day

            # ==========================================
            # POST-OPEN WINDOW — run ONCE per market day
            # ==========================================
            if reopen_dt <= now_local:
                if last_postopen_run_day != market_day:
                    logger.info("Triggering post-open ensure for market day %s", market_day)
                    #accounts = secrets_cache.get_accounts()
                    #logger.info(f"[HTTP] Accounts found={len(accounts)}")
                    #if accounts:
                    ensure_postopen_reopen_if_needed(IB_INSTANCE,settings)
                    last_postopen_run_day = market_day

        except Exception as e:
            logger.exception(f"Scheduler error: {e}")

        #time.sleep(20)
        

        


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
#start_ib_connection()  
# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)



@app.post("/webhook")
def webhook() -> Any:
    #global IB_INSTANCE
    logger.info("===HTTP /webhook received")
    ib = connect_ib_for_webhook()
    IB_INSTANCE=ib
    if ib is None or not ib.isConnected():
        logger.info("Cannot process webhook: IB is down")
        return jsonify({"ok": False, "error": "ib_down"}), 503
    try:
        payload = request.get_json(force=True, silent=False) or {}
    except Exception:
        payload = {}

    try:
        settings = settings_cache.get()
        sig = parse_signal(payload)
        logger.info(f"[HTTP] Received Tradin View alert={sig.raw_alert} symbol={sig.symbol} desired_dir={sig.desired_direction} desired_qty={sig.desired_qty} take_profit={sig.take_profit} stop_loss={sig.stop_loss}")

        # Ensure persistent global IB connection is alive
        if not IB_INSTANCE or not IB_INSTANCE.isConnected():
            logger.info("[ALARM] Global IB_INSTANCE is not connected")
            return jsonify({"ok": False, "error": "ib_not_connected"}), 503


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

        

        result = execute_signal_for_account(IB_INSTANCE, sig, settings)
        return jsonify({"ok": result["ok"], "result": result}), 200


    except Exception as e:
        logger.exception(f"Webhook handling failed. payload={payload} err={e}")
        return jsonify({"ok": False, "error": str(e)}), 400
    finally:
       # 🔥 CRITICAL — disconnect exactly once
       try:
           if IB_INSTANCE and IB_INSTANCE.isConnected():
               IB_INSTANCE.disconnect()
               logger.info("[IB] Clean disconnect after webhook")
       except Exception:
           pass

       IB_INSTANCE = None  # cleanup


# --------------------------------------------------------------------

# When running executor.py directly (not via waitress)
if __name__ == "__main__":
    logger.info("===STARTING: Executor initialized")
    ##start_scheduler()