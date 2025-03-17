# MCP Server

A Model Control Protocol (MCP) server implementation in Go.

## Overview

This server implements the Model Control Protocol, which allows AI models to interact with external tools and resources. The server supports:

- Resource capabilities (reading and subscribing to resources)
- Prompt capabilities (retrieving prompts)
- Tool capabilities (calling external tools)
- Logging capabilities

## Usage

### Building the Server

```bash
go build -o mcp-server cmd/server/main.go
```

### Running the Server

#### Standard I/O Mode

```bash
./mcp-server --transport stdio
```

#### Server-Sent Events (SSE) Mode

```bash
./mcp-server --transport sse --port 3000
```

### Command Line Options

- `--transport`: Transport mode, either "stdio" (default) or "sse"
- `--port`: Server port for SSE transport (default: 3000)

## Architecture

The server is built with a clean, modular architecture:

- `cmd/server/main.go`: Entry point that sets up the server and transport
- `internal/mcp/`: Core MCP protocol types and utilities
- `internal/server/`: Server implementation with transport handlers

## Transport Modes

### Standard I/O

Uses stdin/stdout for communication, suitable for direct integration with AI models.

### Server-Sent Events (SSE)

Provides an HTTP endpoint for SSE-based communication, suitable for web-based clients.

## Protocol Implementation

The server implements the JSON-RPC based Model Control Protocol with support for:

- Initialization and capability negotiation
- Resource management
- Prompt retrieval
- Tool execution
- Notifications

## License

This project is licensed under the MIT License - see the LICENSE file for details.


## 📧 Support & Contact

- For questions or issues, email [mnhatlinh.doan@gmail.com](mailto:mnhatlinh.doan@gmail.com)
- Open an issue directly: [Issue Tracker](https://github.com/VaporScale/cashflow/issues)
- If cashflow helps your work, please consider supporting:

<p align="">
<a href="https://www.buymeacoffee.com/linhdmn">
<img src="https://img.buymeacoffee.com/button-api/?text=Support FreePeak&emoji=☕&slug=linhdmn&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff" 
alt="Buy Me A Coffee"/>
</a>
</p>

## Setting up with Cursor

To use this MCP server with Cursor and fix the "No tools valiables" error, follow these steps:

1. Configure the MCP server to use stdio transport mode:
   - Edit the `.env` file and set `TRANSPORT_MODE=stdio` 
   - Or use the `--transport=stdio` flag when running the server

2. Build and run the MCP server:
   ```bash
   # Make the run script executable
   chmod +x run_cursor_integration.sh
   
   # Run the script
   ./run_cursor_integration.sh
   ```

3. Configure Cursor to use this MCP server:
   - Open Cursor Settings
   - Go to "AI" → "Advanced"
   - Enable "Use a custom MCP server"
   - Set the MCP Server Command to the full path of the integration script:
     ```
     /path/to/your/mcp-server/run_cursor_integration.sh
     ```
   - Save settings and restart Cursor

4. Troubleshooting:
   - If you see "No tools valiables" errors, check the server logs (which appear on stderr)
   - Verify that the server is sending properly formatted JSON-RPC messages with a "tools" array
   - Try using stdio mode for direct Cursor integration

## Troubleshooting "No Available Tools" Error

If you encounter the "No available tools" error in Cursor when using this MCP Server, follow these steps:

1. **Verify the Format of Tool Definitions**:
   
   The Model Context Protocol requires tools to be defined in a specific format. The key change is using `parameters` instead of `schema`:

   ```json
   {
     "jsonrpc": "2.0",
     "method": "tools_available",
     "params": {
       "tools": [
         {
           "name": "execute_query",
           "description": "Execute a SQL query",
           "parameters": {
             "type": "object",
             "properties": {
               "sql": {
                 "type": "string",
                 "description": "SQL query to execute"
               }
             },
             "required": ["sql"]
           }
         }
       ]
     }
   }
   ```

2. **Run in Debug Mode**:
   
   Use the debug flag to capture detailed logs:
   ```bash
   ./run_cursor_integration.sh --debug
   ```
   
   Then check the `mcp-debug.log` file for errors or format issues.

3. **Test with the Mock Server**:
   
   We've included a mock server that sends correctly formatted tools:
   ```bash
   ./mock_cursor.sh
   ```
   
   Configure Cursor to use this script instead to verify if Cursor can recognize tools in this format.

4. **Verify JSON-RPC Format**:
   
   Ensure all messages follow the JSON-RPC 2.0 specification:
   - Include `jsonrpc: "2.0"` in all messages
   - Use the correct method names: `tools_available` and `execute_tool`
   - Format parameters correctly with the right field names

5. **Check Transport Mode**:
   
   Ensure you're using `stdio` transport mode when integrating with Cursor:
   - Set `TRANSPORT_MODE=stdio` in `.env`
   - Or use the `--transport=stdio` flag

If the issue persists, try comparing the output of your server with the mock server to identify any differences in format.

## Solution to "No Available Tools" Error

After extensive testing and debugging, we've identified and fixed the issues causing the "No Available Tools" error in Cursor. The key changes include:

1. **Tool Definition Format**: Changed from using `Parameters` to using `Arguments` in the tool definitions. This aligns with the TypeScript SDK reference.

2. **JSON-RPC Message Structure**: 
   - Ensured the ID field is a string, not a number
   - Used "name" instead of "tool" in the execute_tool request parameters
   - Properly structured the arguments field

3. **Example of a correct tool definition**:
   ```json
   {
     "name": "execute_query",
     "description": "Execute a SQL query and return the results",
     "arguments": [
       {
         "name": "sql",
         "description": "SQL query to execute",
         "required": true,
         "schema": {
           "type": "string"
         }
       }
     ]
   }
   ```

4. **Example of a correct tool execution request**:
   ```json
   {
     "jsonrpc": "2.0",
     "method": "execute_tool",
     "params": {
       "name": "execute_query",
       "arguments": {
         "sql": "SELECT 1"
       }
     },
     "id": "1"
   }
   ```

5. **Testing Tools**: We've created several tools to help test and debug:
   - `simple_mock.sh`: A minimal script that outputs correctly formatted JSON
   - `test_client`: A Go client that can test the MCP server
   - Debug logging in the server to trace JSON format issues

To test your MCP server with Cursor, follow these steps:
1. Ensure your tool definitions use the `arguments` format shown above
2. Make sure your execute_tool handler expects the tool name in the `name` field
3. Use string IDs in your JSON-RPC messages
4. Run the server with `./run_cursor_integration.sh --debug` to see detailed logs
5. If issues persist, try the test client: `./test_client`

## Cursor MCP Integration

### Making Tools Appear in Cursor Editor

To ensure your tools appear in the Cursor Editor MCP Server configuration, the server must follow the JSON-RPC 2.0 protocol and use the correct message format expected by Cursor:

1. The `tools/list` method must be used (not `tools_available` from older protocols)
2. Tool definitions must include the `inputSchema` field in the JSON Schema format
3. The response must be properly formatted as a JSON-RPC 2.0 message

### Example Tool Definition

```json
{
  "name": "execute_query",
  "description": "Execute a SQL query and return the results",
  "inputSchema": {
    "type": "object",
    "properties": {
      "sql": {
        "type": "string",
        "description": "SQL query to execute"
      }
    },
    "required": ["sql"]
  }
}
```

### Testing Tool Definitions

You can use the provided test script to verify that your tool definitions are correctly formatted:

```bash
# Output a sample tools/list message in the format expected by Cursor
./simple_cursor_mock.sh
```

### Troubleshooting

If your tools don't appear in Cursor, check the following:

1. **Message Format**: Ensure the server is sending a valid JSON-RPC 2.0 message with the `tools/list` method
2. **Tool Definition Format**: Each tool must have a `name`, `description`, and `inputSchema` property
3. **Transport**: Make sure you're using the correct transport mode (stdio or SSE) in your Cursor configuration
4. **Debug Logs**: Enable debug logs in your server to see the exact messages being sent
5. **Method Names**: Verify that you're using `tools/list` for listing tools and `tools/call` for executing tools

Example debug command:

```bash
# Run the server with debug logs enabled
DEBUG=true ./server
```

### Common Issues

- **Tools not appearing**: Check that your server is correctly implementing the `tools/list` method and sending valid tool definitions
- **Tools appearing but not working**: Ensure that your tool execution endpoint correctly handles the `tools/call` method and parameter format
- **Wrong tool format**: Make sure each tool has an `inputSchema` property that follows the JSON Schema format

For more details on the MCP protocol, refer to the [Cursor MCP Protocol Documentation](https://docs.cursor.sh/mcp-protocol).

## Integrating with Cursor Editor

The MCP server can be integrated with Cursor Editor using either stdio or SSE transport. Here's how to set up the SSE transport integration:

### Using SSE Transport with Cursor

1. **Build and run the MCP server in SSE mode**:

   ```bash
   # Using the convenience script
   ./run_sse.sh
   
   # Or manually
   go build -o mcp-server cmd/server/main.go
   ./mcp-server --transport sse --port 9091
   ```

   This will start the server on port 9091 (or whatever port you specify) with the SSE transport.

2. **Configure Cursor to use your MCP server**:

   - Open Cursor Editor
   - Go to Settings → AI → Advanced
   - Enable "Use a custom MCP server"
   - In the "MCP Server URL" field, enter:
     ```
     http://localhost:9091/mcp
     ```
   - Save your settings and restart Cursor
   
3. **Verify the connection**:

   After restarting Cursor, it should connect to your MCP server. You'll see log messages in the terminal indicating that Cursor established an SSE connection.

### Troubleshooting Cursor Integration

If you encounter problems connecting Cursor to your MCP server:

1. **Check server logs** for any error messages or connection attempts.

2. **Verify the server is running** by opening http://localhost:9091 in your browser. You should see a simple HTML page confirming the server is running.

3. **Test the SSE connection** by opening http://localhost:9091/mcp in your browser. While this doesn't establish a proper connection, it should not return a 404 error.

4. **Check for network/firewall issues** that might be blocking the connection.

5. **Try a different port** if port 9091 is already in use.

6. **Make sure you're using the correct URL format** in Cursor settings: `http://localhost:PORT/mcp`

### Using stdio Transport with Cursor

For stdio transport (direct integration), you would configure Cursor differently:

1. **Create a shell script** to start your MCP server with stdio transport:

   ```bash
   #!/bin/bash
   cd /path/to/your/mcp-server
   ./mcp-server --transport stdio
   ```

2. **Configure Cursor**:
   - In Cursor settings, under "MCP Server Command", provide the full path to your script.
   - Do not configure an MCP Server URL when using stdio transport.

### Testing Tools in Cursor

Once connected, you can test if Cursor recognizes your MCP server's capabilities:

1. In Cursor, try interacting with AI using features provided by your MCP server.
2. Check the server logs to see if it's receiving and processing requests from Cursor.

If tools aren't appearing in Cursor, there might be issues with how they're registered or with the capability negotiation process.