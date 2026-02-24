#!/usr/bin/env python3
import requests
import sys
import time

account = sys.argv[1]  # short_name
port = sys.argv[2]     # executor port

url = f"http://127.0.0.1:{port}/flatten"

try:
    print(f"[prestop] Flattening positions for {account}...")
    resp = requests.post(url, json={"account": account}, timeout=10)
    print(f"[prestop] Response: {resp.status_code} {resp.text}")
except Exception as e:
    print(f"[prestop] ERROR: {e}")

# Sleep a few seconds before systemd kills the executor
# Gives time to flatten and cancel orders
time.sleep(5)