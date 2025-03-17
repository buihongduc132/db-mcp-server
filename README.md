<div align="center">

# DB MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/FreePeak/mcp-server)](https://goreportcard.com/report/github.com/FreePeak/mcp-server)
[![Go Reference](https://pkg.go.dev/badge/github.com/FreePeak/mcp-server.svg)](https://pkg.go.dev/github.com/FreePeak/mcp-server)
[![Contributors](https://img.shields.io/github/contributors/FreePeak/mcp-server)](https://github.com/FreePeak/mcp-server/graphs/contributors)

<h3>A robust implementation of the Message Communication Protocol (MCP)</h3>

[Features](#key-features) • [Installation](#installation) • [Usage](#usage) • [Documentation](#documentation) • [Contributing](#contributing) • [License](#license)

</div>

---

## 📋 Overview

The MCP Server is a high-performance, feature-rich implementation of the Message Communication Protocol designed to enable seamless integration between AI tools and client applications like VS Code and Cursor. It provides a standardized communication layer allowing clients to discover and invoke remote tools through a consistent, well-defined interface.

## ✨ Key Features

- **Flexible Transport**: Server-Sent Events (SSE) transport layer with robust connection handling
- **Standard Messaging**: JSON-RPC based message format for interoperability
- **Dynamic Tool Registry**: Register, discover, and invoke tools at runtime
- **Editor Integration**: First-class support for VS Code and Cursor extensions
- **Session Management**: Sophisticated session tracking and persistence
- **Structured Error Handling**: Comprehensive error reporting for better debugging
- **Performance Optimized**: Designed for high throughput and low latency

## 🚀 Installation

### Prerequisites

- Go 1.18 or later
- MySQL or PostgreSQL (optional, for persistent sessions)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/FreePeak/mcp-server.git
cd mcp-server

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your configuration

# Build the server
make build

# Run the server
./mcp-server
```

### Docker

```bash
# Build the Docker image
docker build -t mcp-server .

# Run the container
docker run -p 8080:8080 -d mcp-server
```

## 🔧 Configuration

MCP Server can be configured via environment variables or a `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `MCP_PORT` | Server port | `8080` |
| `MCP_HOST` | Server host | `localhost` |
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | `info` |
| `DB_DRIVER` | Database driver (mysql, postgres) | `none` |
| `DB_CONNECTION` | Database connection string | `""` |
| `SESSION_TTL` | Session time-to-live in seconds | `3600` |

See `.env.example` for more configuration options.

## 📖 Usage

### Basic Client Connection

```typescript
// TypeScript example
import { MCPClient } from 'mcp-client';

const client = new MCPClient('http://localhost:8080/mcp');

// Initialize connection
await client.initialize();

// List available tools
const tools = await client.listTools();
console.log(tools);

// Call a tool
const result = await client.callTool('calculator', {
  operation: 'add',
  a: 5,
  b: 3
});
console.log(result); // 8
```

### Custom Tool Registration (Server-side)

```go
// Go example
package main

import (
	"context"
	"mcpserver/internal/mcp"
)

func main() {
	// Create a custom tool
	calculatorTool := &mcp.Tool{
		Name:        "calculator",
		Description: "Performs basic arithmetic operations",
		InputSchema: mcp.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"operation": {
					"type":        "string",
					"description": "Operation to perform (add, subtract, multiply, divide)",
					"enum":        []string{"add", "subtract", "multiply", "divide"},
				},
				"a": {
					"type":        "number",
					"description": "First operand",
				},
				"b": {
					"type":        "number",
					"description": "Second operand",
				},
			},
			Required: []string{"operation", "a", "b"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// Implementation...
			return result, nil
		},
	}

	// Register the tool
	toolRegistry.RegisterTool(calculatorTool)
}
```

## 📚 Documentation

### MCP Protocol

The server implements the MCP protocol with the following key methods:

- **initialize**: Sets up the session and returns server capabilities
- **tools/list**: Discovers available tools
- **tools/call**: Executes a tool
- **editor/context**: Updates the server with editor context
- **cancel**: Cancels an in-progress operation

For full protocol documentation, visit the [MCP Specification](https://github.com/microsoft/mcp).

### Tool System

The MCP Server includes a powerful tool system that allows clients to discover and invoke tools. Each tool has:

- A unique name
- A description
- A JSON Schema for input validation
- A handler function that executes the tool's logic

### Built-in Tools

The server comes with several built-in tools:

| Tool | Description |
|------|-------------|
| `echo` | Returns the input (useful for testing) |
| `calculator` | Performs basic arithmetic operations |
| `timestamp` | Returns timestamps in various formats |
| `random` | Generates random numbers |
| `text` | Performs text operations (uppercase, lowercase, etc.) |
| `getFileInfo` | Gets information about a file (for editor integration) |
| `completeCode` | Provides code completion suggestions |
| `analyzeCode` | Analyzes code for issues and improvements |

### Editor Integration

The server includes support for editor-specific features through the `editor/context` method, enabling tools to be aware of:

- Current file
- Selected code
- Cursor position
- Open files
- Project structure

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b new-feature`
3. **Commit** your changes: `git commit -am 'Add new feature'` 
4. **Push** to the branch: `git push origin new-feature`
5. **Submit** a pull request

Please make sure your code follows our coding standards and includes appropriate tests.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📧 Support & Contact

- For questions or issues, email [mnhatlinh.doan@gmail.com](mailto:mnhatlinh.doan@gmail.com)
- Open an issue directly: [Issue Tracker](https://github.com/FreePeak/mcp-server/issues)
- If MCP Server helps your work, please consider supporting:

<p align="">
<a href="https://www.buymeacoffee.com/linhdmn">
<img src="https://img.buymeacoffee.com/button-api/?text=Support MCP Server&emoji=☕&slug=linhdmn&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff" 
alt="Buy Me A Coffee"/>
</a>
</p>