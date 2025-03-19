<div align="center">

# DB MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/FreePeak/db-mcp-server)](https://goreportcard.com/report/github.com/FreePeak/db-mcp-server)
[![Go Reference](https://pkg.go.dev/badge/github.com/FreePeak/db-mcp-server.svg)](https://pkg.go.dev/github.com/FreePeak/db-mcp-server)
[![Contributors](https://img.shields.io/github/contributors/FreePeak/db-mcp-server)](https://github.com/FreePeak/db-mcp-server/graphs/contributors)

<h3>A robust implementation of the Database Model Context Protocol (DB MCP)</h3>

[Features](#key-features) • [Installation](#installation) • [Usage](#usage) • [Documentation](#documentation) • [Contributing](#contributing) • [License](#license)

</div>

---

## 📋 Overview

The DB MCP Server is a high-performance, feature-rich implementation of the Database Model Context Protocol designed to enable seamless integration between database operations and client applications like VS Code and Cursor. It provides a standardized communication layer allowing clients to discover and invoke database operations through a consistent, well-defined interface, simplifying database access and management across different environments.

## ✨ Key Features

- **Flexible Transport**: Server-Sent Events (SSE) transport layer with robust connection handling
- **Standard Messaging**: JSON-RPC based message format for interoperability
- **Dynamic Tool Registry**: Register, discover, and invoke database tools at runtime
- **Editor Integration**: First-class support for VS Code and Cursor extensions
- **Session Management**: Sophisticated session tracking and persistence
- **Structured Error Handling**: Comprehensive error reporting for better debugging
- **Performance Optimized**: Designed for high throughput and low latency

## 🚀 Installation

### Prerequisites

- Go 1.18 or later
- MySQL or PostgreSQL (optional, for persistent sessions)
- Docker (optional, for containerized deployment)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/FreePeak/db-mcp-server.git
cd db-mcp-server

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your configuration

# Option 1: Build and run locally
make build
./mcp-server

# Option 2: Using Docker
docker build -t db-mcp-server .
docker run -p 9090:9090 db-mcp-server

# Option 3: Using Docker Compose (with MySQL)
docker-compose up -d
```

### Docker

```bash
# Build the Docker image
docker build -t db-mcp-server .

# Run the container
docker run -p 9090:9090 db-mcp-server

# Run with custom configuration
docker run -p 8080:8080 \
  -e SERVER_PORT=8080 \
  -e LOG_LEVEL=debug \
  -e DB_TYPE=mysql \
  -e DB_HOST=my-database-server \
  db-mcp-server
  
# Run with Docker Compose (includes MySQL database)
docker-compose up -d
```

## 🔧 Configuration

DB MCP Server can be configured via environment variables or a `.env` file:

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

### Integrating with Cursor Edit

DB MCP Server can be easily integrated with Cursor Edit by configuring the appropriate settings in your Cursor .configuration file `.cursor/mcp.json`: 

```json
{
    "mcpServers": {
        "db-mcp-server": {
            "url": "http://localhost:9090/sse"
        }
    }
}
```

To use this integration in Cursor:

1. Configure and start the DB MCP Server using one of the installation methods above
2. Add the configuration to your Cursor settings
3. Open Cursor and navigate to a SQL file
4. Use the database panel to connect to your database through the MCP server
5. Execute queries using Cursor's built-in database tools

The MCP Server will handle the database operations, providing enhanced capabilities beyond standard database connections:

- Better error reporting and validation
- Transaction management
- Parameter binding
- Security enhancements
- Performance monitoring

### Custom Tool Registration (Server-side)

```go
// Go example
package main

import (
	"context"
	"db-mcpserver/internal/mcp"
)

func main() {
	// Create a custom database tool
	queryTool := &mcp.Tool{
		Name:        "dbQuery",
		Description: "Executes read-only SQL queries with parameterized inputs",
		InputSchema: mcp.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"query": {
					"type":        "string",
					"description": "SQL query to execute",
				},
				"params": {
					"type":        "array",
					"description": "Query parameters",
					"items": map[string]interface{}{
						"type": "any",
					},
				},
				"timeout": {
					"type":        "integer",
					"description": "Query timeout in milliseconds (optional)",
				},
			},
			Required: []string{"query"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// Implementation...
			return result, nil
		},
	}

	// Register the tool
	toolRegistry.RegisterTool(queryTool)
}
```

## 📚 Documentation

### DB MCP Protocol

The server implements the DB MCP protocol with the following key methods:

- **initialize**: Sets up the session and returns server capabilities
- **tools/list**: Discovers available database tools
- **tools/call**: Executes a database tool
- **editor/context**: Updates the server with editor context
- **cancel**: Cancels an in-progress operation

For full protocol documentation, visit the [MCP Specification](https://github.com/microsoft/mcp) and our database-specific extensions.

### Tool System

The DB MCP Server includes a powerful tool system that allows clients to discover and invoke database tools. Each tool has:

- A unique name
- A description
- A JSON Schema for input validation
- A handler function that executes the tool's logic

### Built-in Tools

The server currently includes three core database tools:

| Tool | Description |
|------|-------------|
| `dbQuery` | Executes read-only SQL queries with parameterized inputs |
| `dbExecute` | Performs data modification operations (INSERT, UPDATE, DELETE) |
| `dbTransaction` | Manages SQL transactions with commit and rollback support |

### Editor Integration

The server includes support for editor-specific features through the `editor/context` method, enabling tools to be aware of:

- Current SQL file
- Selected query
- Cursor position
- Open database connections
- Database structure

## 🗺️ Roadmap

We're committed to expanding DB MCP Server's capabilities. Here's our planned development roadmap:

### Q2 2025
- **Schema Explorer** - Auto-discover database structure and relationships --> Completed
- **Query Builder** - Visual SQL query construction with syntax validation
- **Performance Analyzer** - Identify slow queries and optimization opportunities

### Q3 2025
- **Data Visualization** - Create charts and graphs from query results
- **Model Generator** - Auto-generate code models from database tables
- **Multi-DB Support** - Expanded support for NoSQL databases

### Q4 2025
- **Migration Manager** - Version-controlled database schema changes
- **Access Control** - Fine-grained permissions for database operations
- **Query History** - Track and recall previous queries with execution metrics

### Future Vision
- **AI-Assisted Query Optimization** - Smart recommendations for better performance
- **Cross-Database Operations** - Unified interface for heterogeneous database environments
- **Real-Time Collaboration** - Multi-user support for collaborative database work
- **Extended Plugin System** - Community-driven extension marketplace

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
- Open an issue directly: [Issue Tracker](https://github.com/FreePeak/db-mcp-server/issues)
- If DB MCP Server helps your work, please consider supporting:

<p align="">
<a href="https://www.buymeacoffee.com/linhdmn">
<img src="https://img.buymeacoffee.com/button-api/?text=Support DB MCP Server&emoji=☕&slug=linhdmn&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff" 
alt="Buy Me A Coffee"/>
</a>
</p>
