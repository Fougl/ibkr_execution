echo ">>> Checking for running WAITRESS"
WPID=$(ps aux | grep "[w]aitress-serve --host=0.0.0.0 --port=${EXECUTOR_PORT} executor:app" | awk '{print $2}' || true)


if [ -n "$WPID" ]; then
    echo ">>> Killing old Waitress PID: $WPID"
    kill "$WPID" || true
    sleep 2
else
    echo ">>> No running Waitress process found"
fi

echo ">>> Checking for running EXECUTOR"
EPID=$(ps aux | grep "[p]ython3 /opt/ibc/execution/executor.py" | awk '{print $2}' || true)

if [ -n "$EPID" ]; then
    echo ">>> Killing old Executor PID: $EPID"
    kill "$EPID" || true
    sleep 2
else
    echo ">>> No running Executor process found"
fi

echo ">>> Starting EXECUTOR (scheduler + IBKR logic)"
nohup /home/ubuntu/venv/bin/python3 /opt/ibc/execution/executor.py > /opt/ibc/execution/executor.log 2>&1 &

echo ">>> Starting WAITRESS (webhook API only)"
nohup /home/ubuntu/venv/bin/waitress-serve --host=0.0.0.0 --port=${EXECUTOR_PORT} executor:app > /opt/ibc/execution/waitress.log 2>&1 &


sleep 1

echo ""
echo ">>> RUNNING PROCESSES:"
ps aux | grep -E "executor.py|waitress-serve" | grep -v grep

echo ""
echo ">>> DONE"
tail -f /dev/null