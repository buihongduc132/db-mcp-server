#!/bin/bash

# Database MCP Server - Cursor Integration Script
# This script starts the database MCP server optimized for Cursor editor

# Configuration
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/cursor-mcp-$(date +%Y%m%d-%H%M%S).log"
CONFIG_FILE=${1:-""}

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Define colors for terminal output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# IMPORTANT: All status messages must go to stderr, not stdout
# Stdout is reserved exclusively for JSON protocol communication
echo -e "${GREEN}Starting Database MCP Server for Cursor...${NC}" >&2

# Ensure the server binary exists
if [ ! -f "./server" ]; then
    if [ ! -f "./mcp-server" ]; then
        echo -e "${YELLOW}Building server binary...${NC}" >&2
        go build -o server ./cmd/server/ >&2
        if [ $? -ne 0 ]; then
            echo -e "${RED}Failed to build server. Please check your Go environment.${NC}" >&2
            exit 1
        fi
        echo -e "${GREEN}Server built successfully.${NC}" >&2
    else
        echo -e "${YELLOW}Using existing mcp-server binary${NC}" >&2
    fi
fi

# Export environment variables for Cursor optimization
export CURSOR_EDITOR=1
export DEBUG=true
export LOG_LEVEL=debug

# Set up command arguments
CMD_ARGS="-t stdio"
if [ -n "$CONFIG_FILE" ]; then
    CMD_ARGS="$CMD_ARGS -config $CONFIG_FILE"
    echo -e "${GREEN}Using config file: ${YELLOW}$CONFIG_FILE${NC}" >&2
fi

# Log startup information
echo -e "${GREEN}Database MCP Server started in Cursor mode.${NC}" >&2
echo -e "${GREEN}Log file: ${YELLOW}$LOG_FILE${NC}" >&2
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}\n" >&2

# Start the server with stdio transport, optimized for Cursor
# All server output (including stderr) goes to the log file
# This keeps stdout completely clean for protocol communication
if [ -f "./server" ]; then
    ./server $CMD_ARGS 2> "$LOG_FILE"
elif [ -f "./mcp-server" ]; then
    ./mcp-server $CMD_ARGS 2> "$LOG_FILE"
else
    echo -e "${RED}Error: Server executable not found.${NC}" >&2
    exit 1
fi

# This will only execute if the server exits normally
echo -e "${GREEN}Database MCP Server stopped.${NC}" >&2 