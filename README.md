# MCP Server with SSE Transport

This is a Machine Communication Protocol (MCP) server that uses Server-Sent Events (SSE) as its transport mechanism. The server allows clients to connect, register tools, send requests, and receive responses through a persistent connection.

## Features

- HTTP server that listens on port 8080 (configurable)
- SSE endpoint for persistent client connections
- Message endpoint for JSON-RPC requests
- Multiple concurrent client connections with unique session IDs
- Session state management for each connected client
- Heartbeat mechanism to keep connections alive
- Comprehensive logging

## Architecture

The server follows a modular architecture:

- `cmd/server`: Main server entry point
- `internal/config`: Configuration management
- `internal/logger`: Logging utilities
- `internal/mcp`: MCP protocol handlers
- `internal/session`: Session management
- `internal/transport`: Transport implementations (SSE)
- `pkg/jsonrpc`: JSON-RPC 2.0 implementation
- `pkg/tools`: Tool registry and execution

## Communication Flow

1. **Server Initialization**: The server starts and listens on the configured port
2. **Client Connection**: Clients connect to the SSE endpoint (`/sse`)
3. **Initial Communication**: The server sends an initial SSE event with the message endpoint URL
4. **Request Processing**: Clients send JSON-RPC requests to the message endpoint (`/message`)
5. **Response Delivery**: The server sends responses as SSE events to the appropriate client
6. **Notification Handling**: Notifications are processed with 202 Accepted status codes

## Supported JSON-RPC Methods

- `initialize`: Set up a client connection with protocol version and capability negotiation
- `tools/list`: Return a list of available tools
- `notifications/initialized`: Client notification that initialization is complete

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Make (optional, for using the Makefile)

### Installation

1. Clone the repository
2. Install dependencies:

```bash
go mod download
```

### Configuration

Create a `.env` file based on the `.env.example` file:

```bash
cp .env.example .env
```

Edit the `.env` file to configure the server:

```
# Server Configuration
SERVER_PORT=8080
TRANSPORT_MODE=sse

# Database Configuration (if needed)
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=user
DB_PASSWORD=password
DB_NAME=dbname

# Logging configuration
LOG_LEVEL=info
```

### Running the Server

Using Go directly:

```bash
go run cmd/server/main.go
```

Or using the Makefile:

```bash
make run-sse
```

### Testing with the Example Client

The example client requires the `github.com/r3labs/sse/v2` package:

```bash
go get github.com/r3labs/sse/v2
```

Run the example client:

```bash
go run examples/client/client.go
```

## Adding New Tools

To add a new tool to the server, modify the `registerExampleTools` function in `cmd/server/main.go`:

```go
func registerExampleTools(mcpHandler *mcp.Handler) {
    // Add your tool here
    myTool := &tools.Tool{
        Name:        "my-tool",
        Description: "Description of my tool",
        InputSchema: map[string]interface{}{
            "type": "object",
            "properties": map[string]interface{}{
                "param1": map[string]interface{}{
                    "type":        "string",
                    "description": "Parameter description",
                },
            },
            "required": []string{"param1"},
        },
        Handler: func(params map[string]interface{}) (interface{}, error) {
            // Implement your tool logic here
            return map[string]interface{}{
                "result": "success",
            }, nil
        },
    }

    // Register the tool
    mcpHandler.RegisterTool(myTool)
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details. 