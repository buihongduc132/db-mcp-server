#!/bin/bash

# Set environment for building
export GO111MODULE=on

# Check for debug mode
if [ "$1" = "--debug" ]; then
  DEBUG=true
  echo "Running in DEBUG mode - detailed logs will be saved to mcp-debug.log" >&2
else
  DEBUG=false
fi

# Build the server
echo "Building MCP Server..." >&2
go build -o server ./cmd/server

# Set the TRANSPORT_MODE environment variable
export TRANSPORT_MODE=stdio

# Run the server with stdio mode explicitly set
echo "Starting MCP Server in stdio mode for Cursor integration..." >&2

if [ "$DEBUG" = "true" ]; then
  # Run with debug output captured to a file
  ./server --transport=stdio 2> mcp-debug.log
else
  # Normal run
  ./server --transport=stdio
fi

# Note: For debugging, you can use the following instead:
# ./server --transport=sse --port=9090
