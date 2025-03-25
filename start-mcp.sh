#!/bin/bash

# Script to start the database MCP server for use with Cursor or other clients

# Default values
TRANSPORT="stdio"
PORT=8080
CONFIG=""
CURSOR_MODE=0

# Display usage information
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -t <transport>   Transport mode: stdio (default) or sse"
    echo "  -p <port>        Server port for sse mode (default: 8080)"
    echo "  -c <config>      Path to database configuration file"
    echo "  -cursor          Optimize for Cursor editor (automatically uses stdio)"
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

# Log startup message
if [[ $CURSOR_MODE -eq 1 ]]; then
    echo "Starting database MCP server in Cursor mode" >&2
else
    echo "Starting database MCP server with transport: $TRANSPORT" >&2
fi

# Execute the server
if [[ -f "./server" ]]; then
    exec ./server $ARGS
elif [[ -f "./mcp-server" ]]; then
    exec ./mcp-server $ARGS
else
    echo "Error: Server executable not found." >&2
    exit 1
fi
