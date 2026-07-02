#!/bin/bash
# start_agent.sh — Starts the Axiom AI Agent (backend + orchestrator)

AGENT_DIR="/Users/mac/Library/Application Support/Claude/local-agent-mode-sessions/bc722310-6e7b-4b3b-8beb-a4386b65ca33/47ce6612-44b4-4158-af73-a1d68651e61f/local_ac2dd2c0-02f6-41cd-9e6a-71712897e02c/outputs/axiom-agent"

cd "$AGENT_DIR"

# Start FastAPI backend in background
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!
echo "Backend started (PID $BACKEND_PID)"

# Wait for backend to be ready
sleep 3

# Start orchestrator in background
python3 orchestrator.py &
ORCH_PID=$!
echo "Orchestrator started (PID $ORCH_PID)"

# Keep script running (so launchd knows it's alive)
wait
