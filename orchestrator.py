#!/usr/bin/env python3

import json
import subprocess
import time
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
import boto3
from botocore.exceptions import ClientError

# =========================
# PATHS (LOCKED)
# =========================

BASE_DIR = Path("/opt/ib")

# IBC install (from your setup.yaml)
IBC_DIR = Path("/opt/ib")
IBC_GATEWAY_SCRIPT = IBC_DIR / "gatewaystart.sh"

ACCOUNTS_DIR = BASE_DIR / "accounts"
LOGS_DIR = BASE_DIR / "logs"

BASE_API_PORT = 4001

# =========================
# LOGGING
# =========================

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("orchestrator")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    LOGS_DIR / "orchestrator.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5
)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)

handler.setFormatter(formatter)
logger.addHandler(handler)

# =========================
# HELPERS
# =========================

def load_account_secrets():
    logger.info("Discovering IBKR secrets from AWS Secrets Manager")

    client = boto3.client("secretsmanager", region_name="us-east-1")
    accounts = []

    paginator = client.get_paginator("list_secrets")

    for page in paginator.paginate():
        for secret in page.get("SecretList", []):
            name = secret["Name"]

            if "ibkr" not in name.lower():
                continue

            logger.info(f"Found IBKR secret: {name}")

            try:
                response = client.get_secret_value(SecretId=name)
            except ClientError as e:
                logger.error(f"Failed to read secret {name}: {e}")
                continue

            secret_string = response.get("SecretString")
            if not secret_string:
                logger.error(f"Secret {name} has no SecretString")
                continue

            data = json.loads(secret_string)

            for k in ("username", "password", "mode"):
                if k not in data:
                    raise RuntimeError(f"Secret {name} missing key: {k}")

            data["account_id"] = name
            accounts.append(data)

    logger.info(f"Loaded {len(accounts)} IBKR account(s)")
    return accounts


def write_config_ini(account, account_dir, api_port):
    config_path = account_dir / "config.ini"

    logger.info(
        f"Writing config.ini for {account['account_id']} "
        f"(port={api_port}, mode={account['mode']})"
    )

    content = f"""
[ibc]
GatewayOrTws=gateway
TradingMode={account['mode']}
IbLoginId={account['username']}
IbPassword={account['password']}

RemotePort={api_port}
LocalServerPort={api_port}

ReadOnlyApi=no
FixOrderIdDuplicates=yes
AcceptNonBrokerageAccounts=yes
LogLevel=INFO
"""

    with open(config_path, "w") as f:
        f.write(content.strip() + "\n")

    return config_path


def start_ibc(account_id, account_dir, config_path):
    log_dir = account_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = open(log_dir / "ibc.log", "a")

    cmd = [
        "xvfb-run",
        "-a",
        "/opt/ib/gatewaystart.sh",
        str(config_path),
    ]

    logger.info(f"Starting IBC (Gateway) for {account_id}")
    logger.info(f"Command: {' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=log_file,
        cwd=str(account_dir)
    )

    logger.info(f"IBC launcher started for {account_id} (pid={proc.pid})")
    return proc


# =========================
# MAIN
# =========================

def main():
    logger.info("========== ORCHESTRATOR START ==========")

    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)

    accounts = load_account_secrets()
    if not accounts:
        logger.error("No account secrets found — exiting")
        raise RuntimeError("No account secrets found")

    processes = []
    api_port = BASE_API_PORT

    for account in accounts:
        account_id = account["account_id"]
        logger.info(f"--- Account {account_id} ---")

        account_dir = ACCOUNTS_DIR / account_id
        account_dir.mkdir(parents=True, exist_ok=True)

        config_path = write_config_ini(account, account_dir, api_port)
        proc = start_ibc(account_id, account_dir, config_path)

        processes.append((account_id, proc, api_port))
        api_port += 1

        time.sleep(5)

    logger.info("All IBC processes started")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        logger.warning("Shutdown requested, terminating processes")
        for account_id, proc, _ in processes:
            logger.info(f"Stopping {account_id}")
            proc.terminate()

    logger.info("========== ORCHESTRATOR STOP ==========")


if __name__ == "__main__":
    main()
