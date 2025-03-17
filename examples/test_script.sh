#!/bin/bash

# MCP Server Test Script
# ---------------------
# This script sends direct HTTP requests to test the MCP server 
# without requiring Go dependencies

SERVER_URL=${1:-"http://localhost:9090"}
SESSION_ID="test-session-$(date +%s)"
MESSAGE_ENDPOINT="${SERVER_URL}/message?sessionId=${SESSION_ID}"

echo "Testing MCP Server at ${SERVER_URL}"
echo "Using session ID: ${SESSION_ID}"
echo "Message endpoint: ${MESSAGE_ENDPOINT}"

# Helper function to send a JSON-RPC request
send_request() {
    local id=$1
    local method=$2
    local params=$3
    
    echo -e "\n============================================="
    echo "Sending request: ${method} (ID: ${id})"
    echo "---------------------------------------------"
    
    # Construct the request
    local request="{\"jsonrpc\":\"2.0\",\"id\":${id},\"method\":\"${method}\""
    if [ -n "$params" ]; then
        request="${request},\"params\":${params}"
    fi
    request="${request}}"
    
    echo "Request: ${request}"
    echo "---------------------------------------------"
    
    # Send the request and capture the response
    local response=$(curl -s -X POST -H "Content-Type: application/json" -d "${request}" "${MESSAGE_ENDPOINT}")
    
    echo "Response: ${response}"
    echo "============================================="
}

# Initialize
echo -e "\n=== TESTING INITIALIZE ==="
send_request 1 "initialize" '{
    "protocolVersion": "1.0.0",
    "clientInfo": {
        "name": "Bash Test Client",
        "version": "1.0.0"
    },
    "capabilities": {
        "toolsSupported": true
    }
}'

# List tools
echo -e "\n=== TESTING TOOLS LIST ==="
send_request 2 "tools/list" ""

# Test echo tool
echo -e "\n=== TESTING ECHO TOOL ==="
send_request 3 "tools/execute" '{
    "tool": "echo",
    "input": {
        "message": "Hello from bash test script!"
    }
}'

# Test calculator tool
echo -e "\n=== TESTING CALCULATOR TOOL ==="
send_request 4 "tools/execute" '{
    "tool": "calculator",
    "input": {
        "operation": "add",
        "a": 10,
        "b": 5
    }
}'

# Test timestamp tool
echo -e "\n=== TESTING TIMESTAMP TOOL ==="
send_request 5 "tools/execute" '{
    "tool": "timestamp",
    "input": {
        "format": "rfc3339"
    }
}'

# Test random tool
echo -e "\n=== TESTING RANDOM TOOL ==="
send_request 6 "tools/execute" '{
    "tool": "random",
    "input": {
        "min": 1,
        "max": 100
    }
}'

# Test text tool
echo -e "\n=== TESTING TEXT TOOL ==="
send_request 7 "tools/execute" '{
    "tool": "text",
    "input": {
        "operation": "upper",
        "text": "this text will be converted to uppercase"
    }
}'

echo -e "\nAll tests completed!" 