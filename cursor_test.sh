#!/bin/bash

# This script simulates a Cursor client connecting to the MCP server
# It pipes a JSON-RPC request to the server and displays the response

# Path to the server executable
SERVER_SCRIPT="./run_cursor_integration.sh"

# Simulate Cursor by sending a request and capturing the response
echo "Simulating Cursor client connecting to MCP server..."

# First, run the server in the background with output captured
echo "Starting server in background..."
$SERVER_SCRIPT > server_output.txt &
SERVER_PID=$!

# Give the server a moment to start
sleep 2

# Check if server started successfully
if ! ps -p $SERVER_PID > /dev/null; then
  echo "Server failed to start. See server_output.txt for details."
  exit 1
fi

echo "Server started with PID $SERVER_PID"

# Capture the initial tools event message
TOOLS_EVENT=$(head -n 1 server_output.txt)
echo "Received tools event from server:"
echo "$TOOLS_EVENT" | jq .

# Send a test tool execution request
echo "Sending test tool execution request..."
TEST_REQUEST='{
  "jsonrpc": "2.0",
  "id": "test1",
  "method": "execute_tool",
  "params": {
    "name": "execute_query",
    "sql": "SELECT 1"
  }
}'

echo "$TEST_REQUEST" > request.json

# Send the request to the server's stdin and capture the response
RESPONSE=$(cat request.json | tail -f server_output.txt | head -n 2)
echo "Received response:"
echo "$RESPONSE"

# Clean up
echo "Stopping server..."
kill $SERVER_PID

echo "Test complete. Check server_output.txt for full server output." 