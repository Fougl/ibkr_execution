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
    required = ["active", "username", "password", "account_type", "2fa_time"]
    missing = [k for k in required if k not in d]
    if missing:
        return False, f"Missing required keys: {missing}"

    # active
    if str(d["active"]) not in {"0", "1"}:
        return False, "active must be '0' or '1'"

    # username
    if not isinstance(d["username"], str) or not d["username"].strip():
        return False, "username must be a non-empty string"

    # password
    if not isinstance(d["password"], str) or not d["password"].strip():
        return False, "password must be a non-empty string"

    # account_type
    if str(d["account_type"]).strip().lower() not in {"live", "paper"}:
        return False, "account_type must be 'live' or 'paper'"

    # 2fa_time format HH:MM
    if not isinstance(d["2fa_time"], str) or not re.match(r"^\d{2}:\d{2}$", d["2fa_time"]):
        return False, "2fa_time must be in HH:MM format"

    return True, "OK"


def derive_id_from_name(name: str) -> int:
    """
    Deterministically derive a small numeric ID from short_name.
    Stable across restarts.
    """
    h = hashlib.sha256(name.encode()).hexdigest()
    return int(h[:6], 16) % 1000

def render_config_ini(secret: Dict[str, Any], short_name: str) -> str:
    """
    Render IBKR IBC config.ini exactly as required.
    """
    derived_id = derive_id_from_name(short_name)

    command_server_port = 7462 + derived_id
    twsapi_port = 4002 + derived_id

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
AutoRestartTime={secret.get("2fa_time", "10:00 PM")}
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
            
            ok, reason = validate_secret_json(name, secret)
            if not ok:
                logger.error(
                    "Invalid secret JSON during reconcile; ignoring: %s reason=%s",
                    name,
                    reason,
                )
                continue  # <-- DO NOT add to state
            
            new_state[name] = {
                "fingerprint": fingerprint_secret(secret),
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




def apply_account_added(args, secret_name: str) -> None:
    secret = load_secret(args.region, secret_name)

    ok, reason = validate_secret_json(secret_name, secret)
    if not ok:
        logger.error(
            "Invalid secret JSON; skipping start: %s reason=%s",
            secret_name,
            reason,
        )
        return
    if str(secret["active"]) != "1":
        logger.info(
            "Secret %s inactive (active=0), skipping start",
            secret_name,
        )
        return


    short_name = short_name_from_secret_name(secret_name)

    paths = build_account_paths(args.accounts_base, short_name)
    os.makedirs(paths["base_dir"], exist_ok=True)

    # Write config.ini
    cfg = render_config_ini(secret, short_name)
    with open(paths["config_ini"], "w", encoding="utf-8") as f:
        f.write(cfg)

    # Derive port
    derived_id = derive_id_from_name(short_name)
    command_port = 7462 + derived_id

    env = build_env(args.display, paths)
    env["COMMAND_SERVER_PORT"] = str(command_port)

    run_script(GATEWAY_START, env)

    
    
    
def apply_account_changed(args, secret_name: str, old_state: dict) -> None:
    entry = old_state.get(secret_name)
    if not entry:
        logger.warning(
            "Secret changed: %s but no local state found – nothing to change",
            secret_name,
        )
        return
    

    # Extract short_name from secret name
    short_name = short_name_from_secret_name(secret_name)

    secret = load_secret(args.region, secret_name)
    
    

    ok, reason = validate_secret_json(secret_name, secret)
    if not ok:
        logger.error(
            "Invalid secret JSON; skipping restart: %s reason=%s",
            secret_name,
            reason,
        )
        return
    
    if str(secret["active"]) != "1":
        logger.info(
            "Secret %s inactive (active=0), stopping gateway",
            secret_name,
        )
    
        #short_name = short_name_from_secret_name(secret_name)
        derived_id = derive_id_from_name(short_name)
        command_port = 7462 + derived_id
    
        env = os.environ.copy()
        env["COMMAND_SERVER_PORT"] = str(command_port)
    
        subprocess.run(
            [GATEWAY_STOP],
            env=env,
            check=False,
        )
        return

    # Build paths using short_name (NOT account_number)
    paths = build_account_paths(args.accounts_base, short_name)
    os.makedirs(paths["base_dir"], exist_ok=True)

    # Rewrite config.ini
    cfg = render_config_ini(secret, short_name)
    with open(paths["config_ini"], "w", encoding="utf-8") as f:
        f.write(cfg)

    # Derive deterministic port (NOT from state)
    derived_id = derive_id_from_name(short_name)
    command_port = 7462 + derived_id

    logger.warning(
        "Secret changed: %s -> restarting IBC on port %s (Phase 1)",
        secret_name,
        command_port,
    )

    # Build env AND set COMMAND_SERVER_PORT BEFORE restart
    env = build_env(args.display, paths)
    env["COMMAND_SERVER_PORT"] = str(command_port)

    # Restart gateway
    run_script(GATEWAY_RESTART, env)

    
    
def short_name_from_secret_name(secret_name: str) -> str:
    """
    Extract short_name from secret name.
    Expected format: ibkr/short_name
    """
    parts = secret_name.split("/")
    if len(parts) != 2 or not parts[1]:
        raise ValueError(f"Invalid secret name format: {secret_name}. Expected ibkr/short_name")
    return parts[1]

    

def apply_account_removed(args, secret_name: str, old_state: dict) -> None:
    entry = old_state.get(secret_name)

    if not entry:
        logger.warning(
            "Secret removed: %s but no local state found – nothing to stop",
            secret_name,
        )
        return

    # Derive short_name from secret name
    short_name = short_name_from_secret_name(secret_name)

    # Deterministic port derivation (same logic as config)
    derived_id = derive_id_from_name(short_name)
    command_port = 7462 + derived_id  # <-- FIXED base port

    logger.warning(
        "Secret removed: %s -> stopping IBC on port %s (Phase 1)",
        secret_name,
        command_port,
    )

    env = os.environ.copy()
    env["COMMAND_SERVER_PORT"] = str(command_port)

    subprocess.run(
        [GATEWAY_STOP],
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

                ok, reason = validate_secret_json(secret_name, secret)
                if not ok:
                    logger.error("Invalid secret %s: %s", secret_name, reason)
                    continue

                # Respect active flag
                if str(secret["active"]) != "1":
                    logger.info(
                        "Secret %s inactive (active=0), skipping",
                        secret_name,
                    )
                    continue

                # Extract short_name from secret name
                short_name = short_name_from_secret_name(secret_name)

                # Build paths
                paths = build_account_paths(args.accounts_base, short_name)
                os.makedirs(paths["base_dir"], exist_ok=True)

                # Always rewrite config.ini
                cfg = render_config_ini(secret, short_name)
                with open(paths["config_ini"], "w", encoding="utf-8") as f:
                    f.write(cfg)

                # Derive deterministic port
                derived_id = derive_id_from_name(short_name)
                command_port = 7462 + derived_id

                # Build environment
                env = build_env(args.display, paths)
                env["COMMAND_SERVER_PORT"] = str(command_port)

                # Start gateway
                p = subprocess.Popen(
                    [GATEWAY_START],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )

                logger.info(
                    "Gateway start triggered for %s (pid=%s)",
                    secret_name,
                    p.pid,
                )

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
    logger.info("Orchestrator running region=%s filter=%r interval=%ss", args.region, args.filter, args.interval)
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
    #logger.warning("Cold start detected → force start/restart all gateways")
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
                    apply_account_changed(args, sname, old_state)
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
