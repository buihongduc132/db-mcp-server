#!/bin/bash

# Mock Cursor client to simulate Cursor's interaction with the MCP server

# Set the server URL
SERVER_URL="http://localhost:9090/cursor-mcp"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Mock Cursor Client${NC}"
echo -e "${YELLOW}This script simulates Cursor's interaction with the MCP server${NC}"
echo -e "${YELLOW}Server URL: ${NC}$SERVER_URL\n"

# Function to get tools list
get_tools() {
    echo -e "${YELLOW}Getting tools list...${NC}"
    TOOLS_RESPONSE=$(curl -s -X GET $SERVER_URL)
    echo -e "${GREEN}Response:${NC}"
    echo $TOOLS_RESPONSE | jq .
    
    # Extract tools from response
    TOOLS=$(echo $TOOLS_RESPONSE | jq -r '.result.tools[] | .name')
    
    if [ -z "$TOOLS" ]; then
        echo -e "${RED}No tools found!${NC}"
        return 1
    else
        echo -e "${GREEN}Available tools:${NC}"
        echo "$TOOLS" | while read tool; do
            echo -e "  - ${BLUE}$tool${NC}"
        done
    fi
    
    return 0
}

# Function to execute a tool
execute_tool() {
    local tool_name=$1
    local args=$2
    
    echo -e "\n${YELLOW}Executing tool: ${BLUE}$tool_name${NC}"
    echo -e "${YELLOW}Arguments: ${NC}$args"
    
    # Create the request JSON
    TOOL_REQUEST=$(cat <<EOF
{
    "jsonrpc": "2.0",
    "id": "mock-cursor-$(date +%s)",
    "method": "tools/call",
    "params": {
        "name": "$tool_name",
        "arguments": $args
    }
}
EOF
)
    
    echo -e "${GREEN}Request:${NC}"
    echo $TOOL_REQUEST | jq .
    
    # Send the request
    TOOL_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" -d "$TOOL_REQUEST" $SERVER_URL)
    
    echo -e "${GREEN}Response:${NC}"
    echo $TOOL_RESPONSE | jq .
    
    # Check if the response contains an error
    ERROR=$(echo $TOOL_RESPONSE | jq -r '.error.message // empty')
    if [ ! -z "$ERROR" ]; then
        echo -e "${RED}Error: $ERROR${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Tool executed successfully${NC}"
    return 0
}

# Main menu
while true; do
    echo -e "\n${BLUE}=== Mock Cursor Client Menu ===${NC}"
    echo -e "1. ${YELLOW}Get tools list${NC}"
    echo -e "2. ${YELLOW}Execute 'execute_query' tool${NC}"
    echo -e "3. ${YELLOW}Execute 'insert_data' tool${NC}"
    echo -e "4. ${YELLOW}Execute 'update_data' tool${NC}"
    echo -e "5. ${YELLOW}Execute 'delete_data' tool${NC}"
    echo -e "q. ${RED}Quit${NC}"
    
    read -p "Enter your choice: " choice
    
    case $choice in
        1)
            get_tools
            ;;
        2)
            execute_tool "execute_query" '{"sql": "SELECT 1 as test"}'
            ;;
        3)
            execute_tool "insert_data" '{"table": "users", "data": {"name": "John Doe", "email": "john@example.com"}}'
            ;;
        4)
            execute_tool "update_data" '{"table": "users", "data": {"name": "Jane Doe"}, "condition": "id = 1"}'
            ;;
        5)
            execute_tool "delete_data" '{"table": "users", "condition": "id = 1"}'
            ;;
        q|Q)
            echo -e "${BLUE}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice${NC}"
            ;;
    esac
done 