# MCP Server

MCP Server is a MySQL client proxy server that allows clients to connect via Server-Sent Events (SSE) and execute SQL operations. The server is built using clean architecture principles and can be extended to support multiple database systems.

## Features

- Clean architecture design
- Support for multiple database systems (MySQL, PostgreSQL)
- Server-Sent Events (SSE) for real-time updates
- JSON-based API for executing SQL operations
- Subscription-based change notifications
- **Integration with Cursor Editor's Model Context Protocol (MCP)**
- **Environment-based configuration with .env file support**
- **Support for both stdio and SSE transport modes for Cursor MCP**
- **JSON-RPC 2.0 compliant protocol for communication**

## Project Structure

```
mcp-server/
├── cmd/                     # Application entry points
│   └── server/              # Main server application
├── internal/                # Internal packages
│   ├── config/              # Configuration handling
│   ├── domain/              # Domain layer
│   │   ├── entities/        # Domain entities
│   │   └── repositories/    # Repository interfaces
│   ├── usecase/             # Use case layer
│   ├── interfaces/          # Interface adapters layer
│   │   └── api/             # HTTP API handlers
│   └── infrastructure/      # Infrastructure layer
│       ├── database/        # Database implementations
│       ├── transport/       # Transport implementations (stdio, SSE)
│       └── server/          # Server infrastructure
├── docs/                    # Documentation
│   └── cursor-integration.md # Guide for Cursor integration
├── .env.example             # Example environment configuration
└── Makefile                 # Build and run tasks
```

## Clean Architecture

The application follows clean architecture principles:

1. **Domain Layer**: Contains entities and repository interfaces
2. **Use Case Layer**: Implements application business logic
3. **Interface Adapters Layer**: Handles HTTP requests and responses
4. **Infrastructure Layer**: Provides concrete implementations of repositories

## Getting Started

### Prerequisites

- Go 1.22 or higher
- MySQL or PostgreSQL database
- Cursor Editor (for MCP integration)

### Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and configure your environment variables
3. Build the server with `make build`

### Running the Server

There are two primary modes for running the server:

1. **Standard SSE Mode** (for browser clients):
   ```bash
   make run-sse
   ```
   This runs the server on HTTP with SSE support.

2. **Stdio Mode** (for Cursor integration):
   ```bash
   make run-stdio
   ```
   This mode enables direct integration with the Cursor editor.

You can also run directly with environment variable overrides:
```bash
./server -port 9090 -transport sse -db-type mysql
```

## Cursor MCP Integration

The MCP Server integrates with Cursor's Model Context Protocol, allowing the AI to interact directly with your database.

### JSON-RPC 2.0 Protocol

As of the latest version, MCP Server implements the JSON-RPC 2.0 protocol for all communications with Cursor. This ensures compatibility and standardized error handling.

The protocol requires all messages to have the following format:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "method": "method-name",
  "params": { /* parameters */ }
}
```

Responses follow this format:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "result": { /* result data */ }
}
```

Or for errors:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "error": {
    "code": -32000,
    "message": "Error message",
    "data": { /* optional additional data */ }
  }
}
```

### Setting Up Cursor Integration

To set up integration with Cursor:

1. In your Cursor settings, configure MCP:
   - Set the transport to `stdio`
   - Set the command to the path of your `run_cursor_integration.sh` script

2. Ensure the script has execute permissions:
   ```bash
   chmod +x run_cursor_integration.sh
   ```

3. When using Cursor, the MCP server will start automatically and handle requests from the AI.

### Available Tools

The following tools are available through the MCP protocol:

1. **execute_query** - Execute SQL queries
   ```json
   {
     "name": "execute_query",
     "sql": "SELECT * FROM users LIMIT 10"
   }
   ```

2. **insert_data** - Insert data into a table
   ```json
   {
     "name": "insert_data",
     "table": "users",
     "data": {
       "name": "John Doe",
       "email": "john@example.com"
     }
   }
   ```

3. **update_data** - Update data in a table
   ```json
   {
     "name": "update_data",
     "table": "users",
     "data": {
       "name": "Jane Doe"
     },
     "condition": "id = 1"
   }
   ```

4. **delete_data** - Delete data from a table
   ```json
   {
     "name": "delete_data",
     "table": "users",
     "condition": "id = 1"
   }
   ```

## Transport Modes

The server supports two transport modes for Cursor MCP:

1. **stdio**: This mode is designed for direct integration with Cursor. The server reads requests from stdin and writes responses to stdout. All debug and log messages are written to stderr to avoid interfering with the protocol.

2. **SSE (Server-Sent Events)**: This mode exposes an HTTP endpoint for Cursor to connect to. It's useful for remote server deployments or when the server needs to be shared between multiple clients.

### Stdio Transport

To use stdio transport:

```bash
make run-stdio
```

Or use the integration script:

```bash
./run_cursor_integration.sh
```

### SSE Transport

To use SSE transport:

```bash
make run-sse
```

Then configure Cursor to connect to `http://localhost:9090/sse`.

## Troubleshooting

### JSON-RPC Format Errors

If you see errors like "Unexpected token 'T', 'transport' is not valid JSON", ensure:

1. All debug/log messages are being sent to stderr, not stdout
2. All JSON messages follow the JSON-RPC 2.0 format:
   - Include the `jsonrpc: "2.0"` field
   - Include an `id` field
   - Use `method` and `params` fields properly
   - Format error responses correctly with `code` and `message`

### Transport Errors

If you're having issues with transport:

1. For stdio transport, ensure your integration script is properly configured and has execute permissions
2. For SSE transport, check the server is running and accessible at the configured port
3. Verify there are no port conflicts with other services

### Testing Transport Modes

#### Testing Stdio Mode

You can test stdio mode by piping a JSON request:

```bash
echo '{"jsonrpc": "2.0", "id": "test1", "method": "execute_tool", "params": {"name": "execute_query", "sql": "SELECT 1"}}' | ./server -transport stdio
```

#### Testing SSE Mode

Visit `http://localhost:9090/test/sse` in your browser to test the SSE connection.

## Extending the Database Support

To add support for a new database system:

1. Create a new repository implementation in `internal/infrastructure/database/`
2. Update the database factory in `internal/infrastructure/database/factory.go`
3. Add any required dependencies to the `go.mod` file

## API Usage (Standard Mode)

### Connect to SSE Events

```
GET /events?client_id=<client_id>&subscribe=<table1,table2>
```

- `client_id`: Unique identifier for the client
- `subscribe`: Comma-separated list of tables to subscribe to for change notifications

### Execute Operations

```
POST /execute
Content-Type: application/json

{
  "client_id": "<client_id>",
  "method": "<method>",
  "params": { ... }
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

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

The MCP server can be integrated with Cursor Editor to provide custom tools. Follow these steps to set up the integration:

1. Start the MCP server:
   ```bash
   make run
   ```

2. In Cursor Editor, go to Settings > Extensions > MCP.

3. Add a new MCP server with the following URL:
   ```
   http://localhost:8080/cursor-mcp
   ```

4. Save the settings and restart Cursor Editor.

5. You should now see the tools provided by the MCP server in the Cursor Editor.

### Testing the Integration

You can test the integration using the provided test script:

```bash
./test_cursor_integration.sh
```

This script will:
1. Test getting the tools list from the server
2. Test executing a tool
3. Verify that the responses are correctly formatted

### Troubleshooting

If you see "No available tools" in Cursor Editor:

1. Make sure the MCP server is running
2. Check that the URL in Cursor settings is correct
3. Look at the server logs for any errors
4. Try restarting Cursor Editor
5. Verify that the server is responding correctly using the test script