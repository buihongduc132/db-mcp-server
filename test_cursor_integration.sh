#!/bin/bash

# Test script for verifying MCP server integration with Cursor

# Set the server URL
SERVER_URL="http://localhost:9090/cursor-mcp"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Testing MCP server integration with Cursor...${NC}"

# Test 1: Get tools list
echo -e "\n${YELLOW}Test 1: Getting tools list...${NC}"
TOOLS_RESPONSE=$(curl -s -X GET $SERVER_URL)
echo "Response:"
echo $TOOLS_RESPONSE | jq .

# Check if the response contains tools
if echo $TOOLS_RESPONSE | grep -q "tools"; then
    echo -e "${GREEN}✓ Tools list received successfully${NC}"
else
    echo -e "${RED}✗ Failed to get tools list${NC}"
    exit 1
fi

# Test 2: Execute a tool
echo -e "\n${YELLOW}Test 2: Executing a tool...${NC}"
TOOL_REQUEST='{
    "jsonrpc": "2.0",
    "id": "test-1",
    "method": "tools/call",
    "params": {
        "name": "execute_query",
        "arguments": {
            "sql": "SELECT 1 as test"
        }
    }
}'

echo "Request:"
echo $TOOL_REQUEST | jq .

TOOL_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$TOOL_REQUEST" $SERVER_URL)
echo "Response:"
echo $TOOL_RESPONSE | jq .

# Check if the response contains a result
if echo $TOOL_RESPONSE | grep -q "result"; then
    echo -e "${GREEN}✓ Tool executed successfully${NC}"
else
    echo -e "${RED}✗ Failed to execute tool${NC}"
    exit 1
fi

echo -e "\n${GREEN}All tests passed!${NC}"
echo -e "${YELLOW}Your MCP server should now be compatible with Cursor.${NC}"
echo -e "${YELLOW}Make sure to configure Cursor to use the MCP server at:${NC} $SERVER_URL" 