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