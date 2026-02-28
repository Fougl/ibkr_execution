#!/bin/bash

echo ">>> Checking for running WAITRESS"
WPID=$(ps aux | grep "[w]aitress-serve --host=0.0.0.0 --port=${EXECUTOR_PORT}" | awk '{print $2}' || true)

if [ -n "$WPID" ]; then
    echo ">>> Killing old Waitress PID: $WPID"
    kill "$WPID" || true
    sleep 2
else
    echo ">>> No running Waitress process found"
fi

echo ">>> There should be NO executor.py process anymore — removing this:"
EPID=$(ps aux | grep "[p]ython3 /opt/ibc/execution/executor.py" | awk '{print $2}' || true)
if [ -n "$EPID" ]; then
    kill "$EPID" || true
fi

echo ">>> Starting WAITRESS (this imports executor.py, starts scheduler, and serves webhooks)"
nohup /home/ubuntu/venv/bin/waitress-serve \
    --host=0.0.0.0 \
    --port=${EXECUTOR_PORT} \
    --call executor:create_app \
    > /opt/ibc/execution/waitress.log 2>&1 &

sleep 1
echo ""
echo ">>> RUNNING PROCESSES:"
ps aux | grep -E "waitress-serve" | grep -v grep

echo ">>> DONE"
tail -f /dev/null