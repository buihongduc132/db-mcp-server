#!/bin/bash

# Kill any existing MCP servers
pkill -f "server -t"

# Export common environment variables
export MCP_SERVER_NAME="multidb"
export LOG_LEVEL="debug"

# Start the SSE server in the background
echo "Starting SSE server on port 9092..."
./server -t sse -port 9092 -c config.json &
SSE_PID=$!

# Wait a moment to ensure SSE server is up
sleep 2

# Print status
echo "MCP Servers started:"
echo "- SSE server (PID: $SSE_PID) running on http://127.0.0.1:9092/sse"
echo "- stdio server will be started by Cursor when needed"
echo
echo "Configuration in .cursor/mcp.json:"
echo "- mcp_multidb_stdio: for stdio transport"
echo "- mcp_multidb_sse: for SSE transport"
echo
echo "Available tools will be:"
echo "- mcp_multidb_query_cashflow"
echo "- mcp_multidb_execute_cashflow"
echo "- mcp_multidb_transaction_cashflow"
echo "- mcp_multidb_schema_cashflow"
echo "- mcp_multidb_performance_cashflow"
echo "- mcp_multidb_list_databases"
echo
echo "To stop servers:"
echo "kill $SSE_PID" 