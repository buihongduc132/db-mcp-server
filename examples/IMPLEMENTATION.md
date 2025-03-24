# STDIO Transport Implementation for DB MCP Server

## Overview

The STDIO transport implementation enables the DB MCP Server to communicate through standard input and output streams. This allows for easier integration with command-line tools, text editors, IDEs, and other systems that can spawn processes and communicate through pipes.

## Implementation Details

The STDIO transport implementation consists of the following components:

1. **Transport Layer (`internal/transport/stdio.go`)**
   - Implements a transport that reads JSON-RPC requests from stdin
   - Writes JSON-RPC responses to stdout with a "MCPRPC:" prefix to distinguish from logs
   - Manages a session for the STDIO client

2. **Server Integration (`cmd/server/main.go`)**
   - Added support for STDIO transport mode
   - Modified logger initialization to redirect logs to stderr in STDIO mode
   - Added a SKIP_DB environment variable option for testing without database

3. **Test Scripts (`examples/*.py`)**
   - Provides example scripts for testing the STDIO transport
   - Demonstrates how to send requests and parse responses

## Communication Protocol

1. **Client to Server (Requests)**
   - JSON-RPC 2.0 requests sent as single lines to stdin
   - Each request must be a valid JSON object ending with a newline

2. **Server to Client (Responses)**
   - JSON-RPC 2.0 responses sent to stdout
   - Each response is prefixed with "MCPRPC:" to distinguish from log output
   - Logs are redirected to stderr

## Example Usage

```python
# Python example
import subprocess
import json

# Start the server
process = subprocess.Popen(
    ["./mcp-server", "-t", "stdio"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True
)

# Send a request
request = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "capabilities": {"tools": True}
    }
}
process.stdin.write(json.dumps(request) + "\n")
process.stdin.flush()

# Read response
while True:
    line = process.stdout.readline().strip()
    if line.startswith("MCPRPC:"):
        response = json.loads(line[7:])  # Remove "MCPRPC:" prefix
        print(response)
        break
```

## Benefits

1. **Cross-Language Integration**: Easily integrate with any language that can spawn processes
2. **Text Editor Integration**: Perfect for editor extensions that need database intelligence
3. **Command-Line Tools**: Build CLI tools that leverage the MCP server's database capabilities
4. **Scripting**: Automate database tasks using scripts with full context awareness

## Future Improvements

1. **Raw Mode**: Add a mode without the "MCPRPC:" prefix for simpler parsing
2. **Binary Protocol Option**: Support binary encoding for better performance
3. **Bidirectional Communication**: Add support for server-initiated events
4. **Improved Session Management**: Better handling of disconnections and reconnections

## Conclusion

The STDIO transport provides a flexible alternative to the HTTP/SSE transport, making the DB MCP Server more versatile and easier to integrate into various tools and environments. 