# Dockerfile — Axiom AI Agent v2
FROM python:3.11-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY *.py ./

# Non-root user for security
# /data is the Railway persistent volume mount point
RUN useradd -m agent && \
    chown -R agent:agent /app && \
    mkdir -p /data && \
    chown -R agent:agent /data
USER agent

# Start the agent (not the FastAPI server)
CMD ["python", "-u", "orchestrator.py"]
