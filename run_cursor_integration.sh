#!/bin/bash

# Set environment variables for stdio mode
export TRANSPORT_MODE=stdio

echo "Starting MCP server in stdio mode for Cursor integration..." >&2
echo "Any debug output will appear on stderr, while JSON protocol messages go to stdout" >&2

# Ensure the directory is correct
cd "$(dirname "$0")"

# Run the server in stdio mode
# Using exec to replace the shell process with the server
# This ensures proper signal handling
exec go run cmd/server/main.go -transport stdio
