#!/bin/bash
set -e

# Change to the directory containing this script
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p ./logs

# Timestamp for log file
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
LOG_FILE="./logs/mcp-debug-$TIMESTAMP.log"

echo "Starting MCP server with debug logging to $LOG_FILE"

# Ensure the environment is loaded
if [ -f .env ]; then
  set -a
  source .env
  set +a
  echo "Loaded environment from .env" | tee -a "$LOG_FILE"
fi

# Force stdio mode regardless of .env setting
export TRANSPORT_MODE=stdio
export LOG_LEVEL=debug
export DEBUG=true

# Execute the MCP server with stdio transport and redirect stderr to the log file
# Note: We can't redirect stdout as it's needed for the stdio connection
./mcp-server -t stdio 2> "$LOG_FILE" 