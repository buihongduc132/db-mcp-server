# MCP Server

A server implementation of the Message Communication Protocol (MCP), designed to be compatible with VS Code and Cursor extensions, as well as other MCP clients.

## Overview

The MCP (Message Communication Protocol) Server provides a standardized way for AI tools to communicate with client applications. It exposes a set of tools that clients can discover and invoke remotely.

## Key Features

- Server-Sent Events (SSE) transport layer
- JSON-RPC message format
- Tool registry for dynamic tool registration and discovery
- Editor integration support for VS Code and Cursor
- Structured error handling
- Session management

## Getting Started

### Prerequisites

- Go 1.18 or later

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/mcp-server.git
   cd mcp-server
   ```

2. Build the server:
   ```
   make build
   ```

3. Run the server:
   ```
   ./mcp-server
   ```

### Configuration

You can configure the server using environment variables or a `.env` file. See `.env.example` for available options.

## Tool System

The MCP Server includes a powerful tool system that allows clients to discover and invoke tools. Each tool has:

- A unique name
- A description
- A JSON Schema for input validation
- A handler function that executes the tool's logic

### Built-in Tools

The server comes with several built-in tools:

- `echo`: Simple echo tool that returns the input
- `calculator`: Performs basic arithmetic operations
- `timestamp`: Returns timestamps in various formats
- `random`: Generates random numbers
- `text`: Performs text operations (uppercase, lowercase, reverse, count)
- `getFileInfo`: Gets information about a file (for editor integration)
- `completeCode`: Provides code completion suggestions (for editor integration)
- `analyzeCode`: Analyzes code for issues and improvements (for editor integration)

### Creating Custom Tools

You can create custom tools by implementing the `Tool` interface and registering them with the server:

```go
// Create a new tool
myTool := &tools.Tool{
    Name:        "myTool",
    Description: "Does something awesome",
    InputSchema: tools.ToolInputSchema{
        Type: "object",
        Properties: map[string]interface{}{
            "param1": map[string]interface{}{
                "type":        "string",
                "description": "First parameter",
            },
        },
        Required: []string{"param1"},
    },
    Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
        // Tool implementation
        // ...
        return result, nil
    },
}

// Register the tool
toolRegistry.RegisterTool(myTool)
```

## MCP Protocol

The server implements the MCP protocol as defined in the [MCP specification](https://github.com/microsoft/mcp). Key aspects include:

### Methods

- `initialize`: Initializes the session and returns server capabilities
- `tools/list`: Lists available tools
- `tools/call` or `tools/execute`: Executes a tool
- `editor/context`: Receives editor context updates from the client
- `cancel`: Cancels a running request
- `notifications/initialized`: Notification sent by the client after initialization
- `notifications/tools/list_changed`: Notification sent by the server when tools change

### Tools Execution Flow

1. Client connects to the server via SSE
2. Client sends an `initialize` request
3. Server responds with its capabilities, including tools support
4. Client sends a `tools/list` request to discover available tools
5. Server responds with a list of available tools
6. Client sends a `tools/call` request to execute a tool
7. Server executes the tool and returns the result

## Extending Editor Integration

The server includes support for editor-specific features:

### Editor Context

Clients can send editor context information to the server using the `editor/context` method. This allows tools to be aware of the current state of the editor, such as:

- Current file
- Selected code
- Cursor position
- Open files
- Project structure

Tools can then use this context to provide more relevant results.

### Progress Reporting

Tools can report progress during execution using the `progressToken` mechanism. This allows clients to display progress indicators for long-running operations.

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
<img src="https://img.buymeacoffee.com/button-api/?text=Support cashflow&emoji=☕&slug=linhdmn&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff" 
alt="Buy Me A Coffee"/>
</a>