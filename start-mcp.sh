#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Default values
CONFIG_FILE="config.json"
TRANSPORT_MODE="stdio"
SERVER_PORT=9092
SERVER_HOST="localhost"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--config)
      CONFIG_FILE="$2"
      shift 2
      ;;
    -t|--transport)
      TRANSPORT_MODE="$2"
      shift 2
      ;;
    -p|--port)
      SERVER_PORT="$2"
      shift 2
      ;;
    -h|--host)
      SERVER_HOST="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Print usage information
print_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -c, --config FILE       Specify the database configuration file (default: config.json)"
    echo "  -t, --transport MODE    Specify transport mode: stdio or sse (default: stdio)"
    echo "  -p, --port PORT         Specify server port for SSE mode (default: 9092)"
    echo "  -h, --host HOST         Specify server host for SSE mode (default: localhost)"
    echo ""
    echo "Example:"
    echo "  $0 -c ./config.json -t stdio"
    echo "  $0 -c ./config.json -t sse -p 9093 -h 0.0.0.0"
}

# Validate the configuration file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Configuration file '$CONFIG_FILE' not found!"
    echo ""
    print_usage
    exit 1
fi

# Build the command
CMD="./server -c $CONFIG_FILE -t $TRANSPORT_MODE"

# Add optional arguments
if [ "$TRANSPORT_MODE" = "sse" ]; then
    CMD="$CMD -p $SERVER_PORT -h $SERVER_HOST"
fi

# Display the command
echo "Starting MCP server with command:"
echo "$CMD"
echo ""

# Run the command
$CMD

# Check the exit status
if [ $? -ne 0 ]; then
    echo "Error: MCP server failed to start!"
    exit 1
fi
