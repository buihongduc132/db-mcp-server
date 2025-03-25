#!/bin/bash

set -e

# Set environment variables
export CURSOR_EDITOR=1

# Create logs directory if it doesn't exist
mkdir -p logs

# Generate a timestamp for the log filename
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
LOG_FILE="logs/cursor-mcp-$TIMESTAMP.log"

# Default config file
CONFIG_FILE="config.json"

# If the first argument is provided, use it as the config file
if [ -n "$1" ]; then
  CONFIG_FILE="$1"
fi

# Check if the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Config file '$CONFIG_FILE' not found." >&2
  echo "Usage: $0 [config_file]" >&2
  echo "  config_file: Path to JSON configuration file (default: config.json)" >&2
  exit 1
fi

# Display startup message
echo "Starting DB MCP Server in Cursor mode..." >&2
echo "Config file: $CONFIG_FILE" >&2
echo "Logs will be written to: $LOG_FILE" >&2

# Check if the server executable exists
if [ ! -f "server" ]; then
  echo "Server executable not found. Building..." >&2
  if ! go build -o server cmd/server/main.go; then
    echo "Error: Failed to build server. See error above." >&2
    exit 1
  fi
  echo "Server built successfully." >&2
fi

# Run the server in cursor mode with stdio transport
echo "Starting server..." >&2
exec ./server \
  --t stdio \
  --config "$CONFIG_FILE" \
  2> >(tee -a "$LOG_FILE" >&2) 