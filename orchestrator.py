#!/usr/bin/env python3
"""
IBKR Multi-Account Orchestrator (Phase 1)

- Discovers all AWS Secrets Manager secrets whose Name contains "ibkr" (case-insensitive)
- Persists a fingerprint map locally and computes added/changed/removed each cycle
- For each account secret:
    - validates JSON schema (minimum required keys)
    - renders config.ini in /srv/ibkr/accounts/<account_id>/config.ini
    - starts /opt/ibc/gatewaystart.sh for newly added accounts
    - runs /opt/ibc/restart.sh for changed accounts
- Self-heals Xvfb (:1) if missing
- Health check:
    - If ibapi is installed: connect to TWS/Gateway API port and request current time (real API call)
    - Otherwise: checks TCP port(s) are listening (command server + API port)
- Logs to stdout/stderr (CloudWatch-friendly when run under systemd/agent)

NOTE:
You MUST ensure your instance has permission for ACCOUNTS_BASE (default /srv/ibkr/accounts).
If not, set --accounts-base to a user-writable path like ~/ibkr/accounts.
"""

from __future__ import annotations

import argparse
import boto3
import hashlib
import json
import logging
import os
import re
import signal
import socket
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Set
import subprocess
from pathlib import Path
from ib_insync import IB

# ----------------------------
# Defaults
# ----------------------------
HOME = Path.home()
DEFAULT_DISPLAY = ":1"
DEFAULT_XVFB_ARGS = ["Xvfb", DEFAULT_DISPLAY, "-screen", "0", "1024x768x16"]

DEFAULT_ACCOUNTS_BASE = "/srv/ibkr/accounts"
DEFAULT_STATE_FILE = os.path.expanduser("~/.ibkr/secrets_state.json")
DEFAULT_INTERVAL = 60

GATEWAY_START = "/opt/ibc/gatewaystart.sh"
GATEWAY_RESTART = "/opt/ibc/restart.sh"
GATEWAY_STOP = "/opt/ibc/stop.sh"

# ----------------------------
# Logging (CloudWatch-friendly)
# ----------------------------

logger = logging.getLogger("ibkr-orchestrator")


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )


def log_exception(msg: str, **kv: Any) -> None:
    # Emits stack trace + key/value context
    context = " ".join([f"{k}={v!r}" for k, v in kv.items()])
    logger.exception("%s %s", msg, context)


# ----------------------------
# File lock (prevent double orchestrators)
# ----------------------------

def acquire_lock(lock_path: str) -> int:
    """
    Exclusive lock so two orchestrators don't fight.
    Returns an open FD you must keep for the lifetime of the process.
    """
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)

    try:
        import fcntl  # linux only
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception as e:
        os.close(fd)
        raise RuntimeError(f"Could not acquire lock {lock_path}: {e}") from e

    os.write(fd, f"pid={os.getpid()}\n".encode())
    os.fsync(fd)
    return fd


# ----------------------------
# Helpers
# ----------------------------

def atomic_write_json(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except Exception:
            pass


def read_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fingerprint_secret(secret: dict) -> str:
    normalized = json.dumps(secret, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode()).hexdigest()



def ensure_dir_writable(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    testfile = os.path.join(path, ".write_test")
    with open(testfile, "w", encoding="utf-8") as f:
        f.write("ok")
    os.unlink(testfile)


# ----------------------------
# Xvfb self-healing
# ----------------------------

def ensure_xvfb(display: str, xvfb_args: list[str]) -> None:
    # pgrep returns 0 if found, 1 if not found, >1 on error
    pattern = rf"Xvfb\s+{re.escape(display)}\b"
    rc = subprocess.call(
        ["pgrep", "-f", pattern],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if rc == 0:
        return

    logger.warning("Xvfb %s not running; starting: %s", display, " ".join(xvfb_args))
    subprocess.Popen(
        xvfb_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    time.sleep(0.5)

    rc2 = subprocess.call(
        ["pgrep", "-f", pattern],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if rc2 != 0:
        raise RuntimeError(f"Failed to start Xvfb on {display}")


# ----------------------------
# Secret schema
# ----------------------------

@dataclass
class SecretSpec:
    name: str
    data: Dict[str, Any]
    fingerprint: str


def validate_secret_json(secret_name: str, d: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Minimal schema check:
      Required (case-sensitive):
        - IbLoginId (str)
        - IbPassword (str)
        - TradingMode (str: live|paper or similar)
        - CommandServerPort (int or str-int)
    Optional keys used by health check / runtime:
        - ApiPort (int) OR TWSApiPort (int) OR TwsApiPort (int)
        - Host (default 127.0.0.1)
        - ClientId (default 0)
    """
    required = ["account_number", "username", "password", "account_type"]
    missing = [k for k in required if k not in d]
    if missing:
        return False, f"Missing required keys: {missing}"

    if not str(d["account_number"]).strip().isdigit():
        return False, "account_number must be numeric"
    if not isinstance(d["username"], str) or not d["username"].strip():
        return False, "username must be a non-empty string"
    if not isinstance(d["password"], str) or not d["password"].strip():
        return False, "password must be a non-empty string"
    if not isinstance(d.get("account_type"), str) or d["account_type"].strip().lower() not in {"live", "paper"}:
        return False, "account_type must be 'live' or 'paper'"


    return True, "OK"


def render_config_ini(secret) -> str:
    """
    Render IBKR IBC config.ini exactly as required.
    """

    command_server_port = 7462 + int(secret["account_number"])
    twsapi_port = 4002 + int(secret["account_number"])

    return f"""IbLoginId={secret["username"]}
IbPassword={secret["password"]}
SecondFactorDevice=
ReloginAfterSecondFactorAuthenticationTimeout=yes
SecondFactorAuthenticationExitInterval=
SecondFactorAuthenticationTimeout=180
TradingMode={secret["account_type"]}
AcceptNonBrokerageAccountWarning=yes
LoginDialogDisplayTimeout=60
ExistingSessionDetectedAction=primary
ReadOnlyApi=no
BypassOrderPrecautions=yes
BypassBondWarning=yes
BypassNegativeYieldToWorstConfirmation=yes
BypassCalledBondWarning=yes
BypassSameActionPairTradeWarning=yes
BypassPriceBasedVolatilityRiskWarning=yes
BypassUSStocksMarketDataInSharesWarning=yes
BypassRedirectOrderWarning=yes
BypassNoOverfillProtectionPrecaution=yes
AutoRestartTime=10:00 PM
ColdRestartTime=07:00 PM
AcceptIncomingConnectionAction=reject
CommandServerPort={command_server_port}
OverrideTwsApiPort={twsapi_port}
"""



# ----------------------------
# AWS secrets reconciliation (single-pass list+fingerprint)
# ----------------------------

def reconcile_ibkr_secrets(
    region: str,
    state_file: str,
    name_filter_substring: str = "ibkr",
) -> Tuple[Set[str], Set[str], Set[str], Dict[str, dict]]:
    """
    Returns: (added, removed, changed, new_state_map)

    new_state_map is:
    {
        secret_name: {
            "fingerprint": str,
            "ibc_port": int
        }
    }
    """
    sm = boto3.client("secretsmanager", region_name=region)

    # old state
    if os.path.exists(state_file):
        try:
            old_state = read_json_file(state_file).get("secrets", {})
        except Exception:
            old_state = {}
    else:
        old_state = {}

    new_state: Dict[str, dict] = {}

    paginator = sm.get_paginator("list_secrets")
    for page in paginator.paginate():
        for s in page.get("SecretList", []):
            name = s.get("Name", "")
            if name_filter_substring.lower() not in name.lower():
                continue

            resp = sm.get_secret_value(SecretId=name)
            secret = json.loads(resp["SecretString"])

            # --- derive account_id and IBC port deterministically ---
            raw_account = secret["account_number"]
            if isinstance(raw_account, str):
                account_id = int("".join(filter(str.isdigit, raw_account)))
            else:
                account_id = int(raw_account)

            ibc_port = 7462 + account_id

            new_state[name] = {
                "fingerprint": fingerprint_secret(secret),
                "ibc_port": ibc_port,
            }

    old_keys = set(old_state.keys())
    new_keys = set(new_state.keys())

    added = new_keys - old_keys
    removed = old_keys - new_keys
    changed = {
        k
        for k in (old_keys & new_keys)
        if old_state[k]["fingerprint"] != new_state[k]["fingerprint"]
    }

    # persist new state atomically
    atomic_write_json(state_file, {"secrets": new_state})

    return added, removed, changed, new_state


def load_secret(region: str, secret_name: str) -> Dict[str, Any]:
    sm = boto3.client("secretsmanager", region_name=region)
    resp = sm.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


# ----------------------------
# Per-account runtime.json
# ----------------------------

def runtime_path(accounts_base: str, account_id: str) -> str:
    return os.path.join(accounts_base, account_id, "runtime.json")


def load_runtime(accounts_base: str, account_id: str) -> dict:
    p = runtime_path(accounts_base, account_id)
    if not os.path.exists(p):
        return {}
    try:
        return read_json_file(p)
    except Exception:
        return {}


def save_runtime(accounts_base: str, account_id: str, runtime: dict) -> None:
    p = runtime_path(accounts_base, account_id)
    atomic_write_json(p, runtime)


# ----------------------------
# Process control / scripts
# ----------------------------

def build_account_paths(accounts_base: str, account_id: str) -> dict:
    base_dir = os.path.join(accounts_base, account_id)
    return {
        "base_dir": base_dir,
        "config_ini": os.path.join(base_dir, "config.ini"),
        "logs_dir": os.path.join(base_dir, "logs"),
        "tws_settings": os.path.join(base_dir, "tws_settings"),
    }


def build_env(display: str, paths: dict) -> dict:
    env = os.environ.copy()
    env.update({
        "DISPLAY": display,
        "IBC_INI": paths["config_ini"],
        "LOG_PATH": paths["logs_dir"],
        "TWS_SETTINGS_PATH": paths["tws_settings"],
    })
    os.makedirs(paths["logs_dir"], exist_ok=True)
    os.makedirs(paths["tws_settings"], exist_ok=True)
    return env


def run_script(script_path: str, env: dict) -> subprocess.Popen:
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    return subprocess.Popen(
        [script_path],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def stop_account_best_effort(accounts_base: str, account_id: str) -> None:
    """
    Phase-1 safe-ish stop:
    - if we recorded a pid in runtime.json, send SIGTERM
    Otherwise just log. (IBC scripts vary; stopping cleanly is Phase 2.)
    """
    rt = load_runtime(accounts_base, account_id)
    pid = rt.get("pid")
    if not pid:
        logger.warning("No pid recorded for account_id=%s; cannot stop automatically", account_id)
        return
    try:
        os.kill(int(pid), signal.SIGTERM)
        logger.info("Sent SIGTERM to pid=%s account_id=%s", pid, account_id)
    except Exception as e:
        logger.warning("Failed to stop pid=%s account_id=%s: %s", pid, account_id, e)




def wait_for_ib_api(
    host: str,
    port: int,
    client_id: int,
    timeout: int = 5,
    max_wait: int = 20,
):
    """
    Wait up to max_wait seconds for IB Gateway API to become available.
    Returns (ok: bool, message: str)
    """
    deadline = time.time() + max_wait
    last_error = None
    time.sleep(max_wait)
    #while True:
    ib = IB()
    try:
        ib.connect(
            host=host,
            port=port,
            clientId=client_id,
            timeout=timeout,
        )

        # This is the REAL test: API + account access
        summary = ib.accountSummary()
        if summary:
            ib.disconnect()
            return True, "IB API reachable, account summary received"

        ib.disconnect()
        #break

    except Exception as e:
        last_error = str(e)

        time.sleep(2)

    return False, f"IB API not reachable after {max_wait}s ({last_error})"
# ----------------------------
# Account apply logic
# ----------------------------

def apply_account_added(args, secret_name: str) -> None:
    #account_id = account_id_from_secret_name(secret_name)
    secret = load_secret(args.region, secret_name)
    account_id = int(secret['account_number'])

    ok, reason = validate_secret_json(secret_name, secret)
    if not ok:
        logger.error("Invalid secret JSON; skipping start: %s reason=%s", secret_name, reason)
        return

    paths = build_account_paths(args.accounts_base, str(account_id))
    os.makedirs(paths["base_dir"], exist_ok=True)

    # Write config.ini
    cfg = render_config_ini(secret)
    with open(paths["config_ini"], "w", encoding="utf-8") as f:
        f.write(cfg)

    env = build_env(args.display, paths)

    # Start gateway
    p = run_script(GATEWAY_START, env)
    
    time.sleep(30)  # small initial delay so Java can start

    ok, msg = wait_for_ib_api(
        host="127.0.0.1",
        port=4002 + account_id,   # your rule
        client_id=1,
        max_wait=20,
    )
    
    if ok:
        logger.info("Gateway ready for %s: %s", secret_name, msg)
    else:
        logger.error("Gateway FAILED for %s: %s", secret_name, msg)
    


def apply_account_changed(args, secret_name: str, old_state: dict) -> None:
    #account_id = account_id_from_secret_name(secret_name)
    entry = old_state.get(secret_name)
    if not entry:
        logger.warning(
            "Secret changed: %s but no local state found – nothing to change",
            secret_name,
        )
        return
    secret = load_secret(args.region, secret_name)
    account_id = int(secret['account_number'])

    ok, reason = validate_secret_json(secret_name, secret)
    if not ok:
        logger.error("Invalid secret JSON; skipping restart: %s reason=%s", secret_name, reason)
        return

    paths = build_account_paths(args.accounts_base, str(account_id))
    os.makedirs(paths["base_dir"], exist_ok=True)

    # Rewrite config.ini
    cfg = render_config_ini(secret)
    with open(paths["config_ini"], "w", encoding="utf-8") as f:
        f.write(cfg)

    env = build_env(args.display, paths)

    # Restart gateway
    p = run_script(GATEWAY_RESTART, env)
    
    ibc_port = entry["ibc_port"]

    logger.warning(
        "Secret removed: %s -> stopping IBC on port %s (Phase 1)",
        secret_name,
        ibc_port,
    )

    env = os.environ.copy()
    env["COMMAND_SERVER_PORT"] = str(ibc_port)
    
    time.sleep(5)  # small initial delay so Java can start

    ok, msg = wait_for_ib_api(
        host="127.0.0.1",
        port=4002 + account_id,   # your rule
        client_id=1,
        max_wait=20,
    )
    
    if ok:
        logger.info("Gateway ready for %s: %s", secret_name, msg)
    else:
        logger.error("Gateway FAILED for %s: %s", secret_name, msg)

    


def apply_account_removed(args, secret_name: str, old_state: dict) -> None:
    entry = old_state.get(secret_name)

    if not entry:
        logger.warning(
            "Secret removed: %s but no local state found – nothing to stop",
            secret_name,
        )
        return

    ibc_port = entry["ibc_port"]

    logger.warning(
        "Secret removed: %s -> stopping IBC on port %s (Phase 1)",
        secret_name,
        ibc_port,
    )

    env = os.environ.copy()
    env["COMMAND_SERVER_PORT"] = str(ibc_port)

    subprocess.run(
        ["/opt/ibc/stop.sh"],
        env=env,
        check=False,
    )

def force_start_or_restart_all_secrets(args) -> None:
    sm = boto3.client("secretsmanager", region_name=args.region)

    paginator = sm.get_paginator("list_secrets")

    for page in paginator.paginate():
        for s in page.get("SecretList", []):
            secret_name = s.get("Name", "")

            if args.filter.lower() not in secret_name.lower():
                continue

            try:
                logger.info("Ensuring gateway for secret=%s", secret_name)

                secret = load_secret(args.region, secret_name)
                account_id = int(secret["account_number"])

                ok, reason = validate_secret_json(secret_name, secret)
                if not ok:
                    logger.error("Invalid secret %s: %s", secret_name, reason)
                    continue

                paths = build_account_paths(args.accounts_base, str(account_id))
                os.makedirs(paths["base_dir"], exist_ok=True)

                # Always rewrite config
                cfg = render_config_ini(secret)
                with open(paths["config_ini"], "w", encoding="utf-8") as f:
                    f.write(cfg)

                # Build FULL env (this is critical)
                env = build_env(args.display, paths)
                env["COMMAND_SERVER_PORT"] = str(7462 + account_id)

                # Try start
                p = subprocess.Popen(
                    [GATEWAY_START],
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    start_new_session=True,
                )

                out = (p.stdout or "").lower()

                logger.info(
                    "Gateway start triggered for %s (pid=%s)",
                    secret_name,
                    p.pid,
                )

                #     subprocess.run(
                #         [GATEWAY_RESTART],
                #         env=env,
                #         stdout=subprocess.DEVNULL,
                #         stderr=subprocess.DEVNULL,
                #         check=False,
                #     )
                # else:
                #     logger.info("Gateway start attempted for %s", secret_name)

            except Exception:
                log_exception("Failed force start/restart", secret=secret_name)
# ----------------------------
# Main
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    # 🔹 Make region OPTIONAL positional
    p.add_argument(
        'region',
        nargs='?',
        default=None,
        help="AWS region (defaults to boto3 resolved region)"
    )

    p.add_argument('--interval', type=int, default=DEFAULT_INTERVAL)
    p.add_argument('--state-file', default=DEFAULT_STATE_FILE)
    p.add_argument('--accounts-base', default=DEFAULT_ACCOUNTS_BASE)
    p.add_argument('--filter', default='ibkr')
    p.add_argument('--log-level', default='INFO')
    p.add_argument('--display', default=DEFAULT_DISPLAY)
    p.add_argument('--health-timeout', type=float, default=20.0)
    p.add_argument('--trusted-ip', default=None)
    p.add_argument('--lock-file', default=os.path.expanduser("~/.ibkr/orchestrator.lock"))

    args = p.parse_args()

    if args.region is None:
        session = boto3.Session()
        if not session.region_name:
            raise RuntimeError("AWS region could not be resolved from environment / metadata")
        args.region = session.region_name

    return args


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    # Lock
    try:
        _lock_fd = acquire_lock(args.lock_file)
    except Exception as e:
        logger.error("Another orchestrator may already be running: %s", e)
        sys.exit(2)

    # Ensure directories are writable
    try:
        os.makedirs(os.path.dirname(args.state_file), exist_ok=True)
    except Exception as e:
        logger.error("Cannot create state dir for %s: %s", args.state_file, e)
        sys.exit(2)

    try:
        ensure_dir_writable(args.accounts_base)
    except Exception:
        # If /srv is not writable, fail fast with a clear message
        logger.error(
            "accounts-base is not writable: %s. "
            "Either: sudo mkdir -p %s && sudo chown -R $USER:$USER %s "
            "OR run with --accounts-base ~/ibkr/accounts",
            args.accounts_base, args.accounts_base, args.accounts_base
        )
        sys.exit(2)

    xvfb_args = ["Xvfb", args.display, "-screen", "0", "1024x768x16"]

    # Fail-fast baseline
    try:
        ensure_xvfb(args.display, xvfb_args)
    except Exception:
        log_exception("Failed to ensure Xvfb baseline")
        sys.exit(2)

    logger.info("Orchestrator running region=%s filter=%r interval=%ss", args.region, args.filter, args.interval)
    logger.warning("Cold start detected → force start/restart all gateways")
    force_start_or_restart_all_secrets(args)

    while True:
        cycle_ok = True

        try:
            # Self-heal Xvfb each cycle
            ensure_xvfb(args.display, xvfb_args)
            if os.path.exists(args.state_file):
                old_state = read_json_file(args.state_file).get("secrets", {})
            else:
                old_state = {}
            added, removed, changed, _new_state = reconcile_ibkr_secrets(
                region=args.region,
                state_file=args.state_file,
                name_filter_substring=args.filter,
            )

            if added:
                logger.info("Secrets added: %s", sorted(list(added)))
            if changed:
                logger.info("Secrets changed: %s", sorted(list(changed)))
            if removed:
                logger.info("Secrets removed: %s", sorted(list(removed)))

            # Apply changes
            for sname in sorted(list(added)):
                try:
                    apply_account_added(args, sname)
                except Exception:
                    cycle_ok = False
                    log_exception("Failed handling added secret", secret=sname)

            for sname in sorted(list(changed)):
                try:
                    apply_account_changed(args, sname)
                except Exception:
                    cycle_ok = False
                    log_exception("Failed handling changed secret", secret=sname)

            for sname in sorted(list(removed)):
                try:
                    apply_account_removed(args, sname, old_state)
                except Exception:
                    cycle_ok = False
                    log_exception("Failed handling removed secret", secret=sname)

        except Exception:
            cycle_ok = False
            log_exception("Reconcile cycle failed")

        # Final “cycle” report:
        # CloudWatch will capture these logs if you're running under a service/agent.
        # if cycle_ok:
        #     logger.info("Reconcile cycle completed successfully (no unhandled errors)")
        # else:
        #     logger.error("Reconcile cycle completed with errors (see logs above)")

        time.sleep(max(5, args.interval))


if __name__ == "__main__":
    main()
