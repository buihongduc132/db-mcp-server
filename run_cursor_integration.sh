#!/bin/bash

# Set environment variables for stdio mode
export TRANSPORT_MODE=stdio

# Run the server in stdio mode
cd ~/work/harvey/dev/FreePeak/mcp-server && go run cmd/server/main.go -transport stdio
