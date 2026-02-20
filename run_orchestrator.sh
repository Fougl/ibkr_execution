echo ">>> Checking for running WAITRESS (Orchestrator)"
WPID=$(ps aux | grep "[w]aitress-serve --host=0.0.0.0 --port=5001 orchestrator:app" | awk '{print $2}' || true)

if [ -n "$WPID" ]; then
    echo ">>> Killing old Waitress PID: $WPID"
    kill "$WPID" || true
    sleep 2
else
    echo ">>> No running Waitress process found"
fi

echo ">>> Checking for running ORCHESTRATOR"
OPID=$(ps aux | grep "[p]ython3 /opt/ibc/execution/orchestrator.py" | awk '{print $2}' || true)

if [ -n "$OPID" ]; then
    echo ">>> Killing old Orchestrator PID: $OPID"
    kill "$OPID" || true
    sleep 2
else
    echo ">>> No running Orchestrator process found"
fi

echo ">>> Starting ORCHESTRATOR (secret loop + gateway mgmt)"
nohup /home/ubuntu/venv/bin/python3 /opt/ibc/execution/orchestrator.py \
    > /opt/ibc/execution/orchestrator.log 2>&1 &

echo ">>> Starting WAITRESS (webhook listener on port 5001"
nohup /home/ubuntu/venv/bin/waitress-serve \
    --host=0.0.0.0 \
    --port=5001 \
    orchestrator:app \
    > /opt/ibc/execution/waitress.log 2>&1 &

sleep 1

echo ""
echo ">>> RUNNING PROCESSES:"
ps aux | grep -E "orchestrator.py|waitress-serve" | grep -v grep

echo ""
echo ">>> DONE"
tail -f /dev/null

