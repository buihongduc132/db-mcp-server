#!/bin/bash

# Build the server
go build -o mcp-server cmd/server/main.go

# Run the server in SSE mode
PORT=9091
echo "Starting MCP server in SSE mode on port $PORT..."
echo "Configure Cursor to use: http://localhost:$PORT/mcp"
echo "Press Ctrl+C to stop the server"

# Start the server with SSE transport
./mcp-server --transport sse --port $PORT

echo "Server stopped." 