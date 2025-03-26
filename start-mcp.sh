#!/bin/bash

# Script to start the database MCP server for use with Cursor or other clients

# Default values
TRANSPORT="stdio"
PORT=8080
CONFIG=""
CURSOR_MODE=0
DISABLE_LOGGING=0
LOG_FILE="logs/mcp-$(date +%Y%m%d-%H%M%S).log"

# Create logs directory if it doesn't exist
mkdir -p logs

# Display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -t <transport>   Transport mode: stdio (default) or sse"
    echo "  -p <port>        Server port for sse mode (default: 8080)"
    echo "  -c <config>      Path to database configuration file"
    echo "  -cursor          Optimize for Cursor editor (automatically uses stdio)"
    echo "  -no-log          Disable logging in transport (fixes JSON parsing errors)"
    echo "  -h               Display this help message"
    exit 1
}

# Process command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -t|--transport)
            TRANSPORT="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG="$2"
            shift 2
            ;;
        -cursor)
            CURSOR_MODE=1
            TRANSPORT="stdio"
            export CURSOR_EDITOR=1
            shift
            ;;
        -no-log|--no-log)
            DISABLE_LOGGING=1
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate transport mode
if [[ "$TRANSPORT" != "stdio" && "$TRANSPORT" != "sse" ]]; then
    echo "Error: Invalid transport mode. Must be 'stdio' or 'sse'."
    usage
fi

# Build command arguments
ARGS="-t $TRANSPORT"

if [[ "$TRANSPORT" == "sse" ]]; then
    ARGS="$ARGS -port $PORT"
fi

if [[ -n "$CONFIG" ]]; then
    ARGS="$ARGS -config $CONFIG"
fi

# Set disable logging environment variable if needed
if [[ $DISABLE_LOGGING -eq 1 ]]; then
    export DISABLE_LOGGING=true
    echo "Transport logging is disabled" >&2
fi

# Log startup message
if [[ $CURSOR_MODE -eq 1 ]]; then
    echo "Starting database MCP server in Cursor mode" >&2
else
    echo "Starting database MCP server with transport: $TRANSPORT" >&2
    echo "Logs will be stored in: $LOG_FILE" >&2
fi

# Execute the server
if [[ -f "./server" ]]; then
    # In Cursor mode, we don't need to redirect logs
    if [[ $CURSOR_MODE -eq 1 ]]; then
        exec ./server $ARGS
    else
        # For normal mode, redirect stderr to a log file for debugging
        exec ./server $ARGS 2> >(tee -a "$LOG_FILE" >&2)
    fi
else
    echo "Error: Server executable 'server' not found." >&2
    echo "Please build the server first with 'make build'" >&2
    exit 1
fi
