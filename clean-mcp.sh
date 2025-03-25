#!/bin/bash
set -e

# Change to the directory containing this script
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Create logs directory if it doesn't exist
mkdir -p "$DIR/logs" 2>/dev/null

# Timestamp for log file
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
LOG_FILE="$DIR/logs/clean-mcp-$TIMESTAMP.log"

# Any debug output goes to the log file or stderr, NEVER to stdout
{
  echo "Starting MCP server at $(date)"
  
  # Load environment without printing to stdout
  if [ -f .env ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment from .env"
  fi
  
  # Force settings for Cursor
  export TRANSPORT_MODE=stdio
  export LOG_LEVEL=debug
  
  echo "Executing MCP server with stdio transport"
} >> "$LOG_FILE" 2>&1

# Execute with clean stdout - ONLY the MCP server should write to stdout
exec "$DIR/mcp-server" -t stdio 2>> "$LOG_FILE" 