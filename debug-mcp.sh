#!/bin/bash
set -e

# Store the original path of the config file if provided as an absolute path
CONFIG_FILE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -c|--config)
      CONFIG_FILE="$2"
      shift # past argument
      shift # past value
      ;;
    *)
      # unknown option
      shift
      ;;
  esac
done

# If config file is an absolute path, store it before changing directory
ABSOLUTE_CONFIG_PATH=""
if [[ -n "$CONFIG_FILE" && "$CONFIG_FILE" == /* ]]; then
  ABSOLUTE_CONFIG_PATH="$CONFIG_FILE"
fi

# Change to the directory containing this script
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p ./logs

# Timestamp for log file
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
LOG_FILE="./logs/mcp-debug-$TIMESTAMP.log"

echo "Starting MCP server with debug logging to $LOG_FILE"

# Use the absolute path if it was provided, otherwise use the relative path or default
if [[ -n "$ABSOLUTE_CONFIG_PATH" ]]; then
  CONFIG_FILE="$ABSOLUTE_CONFIG_PATH"
elif [ -z "$CONFIG_FILE" ]; then
  CONFIG_FILE="config.json"
fi

echo "Using config file: $CONFIG_FILE" | tee -a "$LOG_FILE"

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
./server -t stdio -c "$CONFIG_FILE" 2> "$LOG_FILE" 