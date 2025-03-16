#!/bin/bash

# Build and start the server
echo "Building and starting MCP server..."
cd "$(dirname "$0")"
go build -o mcp-server ./cmd/server/
./mcp-server &
server_pid=$!

# Give it a moment to start up
sleep 2

# Use curl to test the SSE endpoint
echo "Testing SSE endpoint..."
curl -N http://localhost:9090/sse &
curl_pid=$!

# Wait 5 seconds to see the output
sleep 5

# Clean up
echo "Cleaning up..."
kill $curl_pid
kill $server_pid

echo "Test complete." 