<div align="center">

<img src="assets/logo.svg" alt="DB MCP Server Logo" width="300" />

# Multi Database MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/FreePeak/db-mcp-server)](https://goreportcard.com/report/github.com/FreePeak/db-mcp-server)
[![Go Reference](https://pkg.go.dev/badge/github.com/FreePeak/db-mcp-server.svg)](https://pkg.go.dev/github.com/FreePeak/db-mcp-server)
[![Contributors](https://img.shields.io/github/contributors/FreePeak/db-mcp-server)](https://github.com/FreePeak/db-mcp-server/graphs/contributors)

<h3>A robust multi-database implementation of the Database Model Context Protocol (DB MCP)</h3>


</div>
A Clean Architecture implementation of a database server for [Model Context Protocol (MCP)](https://github.com/microsoft/mcp), providing AI assistants with structured access to multiple databases simultaneously.

## Overview

The DB MCP Server provides a standardized way for AI models to interact with databases, enabling them to execute SQL queries, manage transactions, explore schemas, and analyze performance across different database systems at the same time. Built on [FreePeak/cortex](https://github.com/FreePeak/cortex) framework, it follows Clean Architecture principles for maintainability and testability.

## Features

- **Simultaneous Multi-Database Support**: Connect to and interact with multiple MySQL and PostgreSQL databases concurrently
- **Database-Specific Tool Generation**: Auto-creates specialized tools for each connected database
- **Clean Architecture**: Modular design with clear separation of concerns
- **Dynamic Database Tools**: 
  - Execute SQL queries with parameters
  - Run data modification statements with proper error handling
  - Manage database transactions across sessions
  - Explore database schemas and relationships
  - Analyze query performance and receive optimization suggestions
- **Unified Interface**: Consistent interaction patterns across different database types
- **Connection Management**: Simple configuration for multiple database connections

## Currently Supported Databases

The DB MCP Server currently provides first-class support for:

| Database | Status | Features |
|----------|--------|----------|
| MySQL    | ✅ Full Support | Queries, Transactions, Schema Analysis, Performance Insights |
| PostgreSQL | ✅ Full Support | Queries, Transactions, Schema Analysis, Performance Insights |

## Roadmap

We're committed to expanding DB MCP Server to support a wide range of database systems. Here's our planned development roadmap:

### Q3 2025
- **MongoDB** - Support for document-oriented database operations, schema exploration, and query optimization
- **SQLite** - Lightweight embedded database integration with full transaction support
- **MariaDB** - Complete feature parity with MySQL implementation

### Q4 2025
- **Microsoft SQL Server** - Enterprise database support with specialized T-SQL capabilities
- **Oracle Database** - Enterprise-grade integration with Oracle-specific optimizations
- **Redis** - Key-value store operations and performance analysis

### 2026
- **Cassandra** - Distributed NoSQL database support for high-scale operations
- **Elasticsearch** - Specialized search and analytics capabilities
- **CockroachDB** - Distributed SQL database for global-scale applications
- **DynamoDB** - AWS-native NoSQL database integration
- **Neo4j** - Graph database support with specialized query capabilities
- **ClickHouse** - Analytics database support with column-oriented optimizations

Our goal is to provide a unified interface for AI assistants to work with any database while maintaining the specific features and optimizations of each database system.

## Installation

### Prerequisites

- Go 1.18 or later
- Supported databases:
  - MySQL
  - PostgreSQL

### Quick Start

```bash
# Clone the repository
git clone https://github.com/FreePeak/db-mcp-server.git
cd db-mcp-server

# Build the server
make build
```

## Configuration

The key advantage of DB MCP Server is the ability to connect to multiple databases simultaneously. Configure your database connections in a `config.json` file:

```json
{
  "connections": [
    {
      "id": "mysql1",
      "type": "mysql",
      "host": "localhost",
      "port": 13306,
      "name": "db1",
      "user": "user1",
      "password": "password1"
    },
    {
      "id": "mysql2",
      "type": "mysql",
      "host": "localhost",
      "port": 13307,
      "name": "db3",
      "user": "user3",
      "password": "password3"
    },
    {
      "id": "postgres1",
      "type": "postgres",
      "host": "localhost",
      "port": 15432,
      "name": "db2",
      "user": "user2",
      "password": "password2"
    }
  ]
}
```

Each database connection has a unique ID that is used to reference it when using database tools.

## Dynamic Database Tools

The DB MCP Server can dynamically generate database tools based on your database configuration. Instead of hardcoding tool names, the server will create tools with appropriate names based on the databases defined in your configuration.

### How It Works

1. The server reads database connections from:
   - Configuration file specified with `-c` or `-config` flags
   - Direct JSON configuration via `-db-config` flag
   - Environment variable `DB_CONFIG`
   - Individual environment variables (`DB_HOST`, `DB_PORT`, etc.)

2. For each database connection, the server automatically generates the following tools:
   - `query_<dbID>` - Execute SQL queries
   - `execute_<dbID>` - Execute SQL statements (INSERT, UPDATE, DELETE)
   - `transaction_<dbID>` - Manage database transactions
   - `performance_<dbID>` - Analyze query performance
   - `schema_<dbID>` - Get database schema information

3. Cursor-compatible aliases are also generated with the format:
   - `mcp_cashflow_db_mcp_server_sse_query_<dbID>`
   - `mcp_cashflow_db_mcp_server_sse_execute_<dbID>`
   - And so on...

### Configuration Example

Define multiple databases in `config.json`:

```json
{
  "connections": [
    {
      "id": "db1",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "name": "database1",
      "user": "user1",
      "password": "password1"
    },
    {
      "id": "db2",
      "type": "mysql",
      "host": "localhost",
      "port": 3307,
      "name": "database2",
      "user": "user2",
      "password": "password2"
    }
  ]
}
```

With this configuration, the server will automatically generate tools like:
- `query_db1`, `execute_db1`, `transaction_db1`, etc.
- `query_db2`, `execute_db2`, `transaction_db2`, etc.

### Command-Line Configuration

You can also provide database configuration directly via command line:

```bash
./db-mcp-server -db-config '{"connections":[{"id":"mydb","type":"mysql","host":"localhost","port":3306,"name":"mydb","user":"root","password":"password"}]}'
```

This will generate tools like `query_mydb`, `execute_mydb`, etc.

## Usage

The server supports two transport modes:

### SSE Mode (Server-Sent Events)

```bash
# Run with default host (localhost) and port (9092)
./server -t sse -config config.json

# Specify host and port for external access
./server -t sse -host example.com -port 8080 -config config.json
```

### STDIO Mode (for IDE integration)

```bash
# Run in STDIO mode (e.g., for Cursor integration)
<path-to-db-mcp-server>/server -t stdio -c <path-to-your-db-config>config.json
```

#### Example config for Cursor

```json
{
    "mcpServers": {
        "stdio-db-mcp-server": {
            "command": "<path-to-dir>/db-mcp-server/server",
            "args": [
                "-t",
                "stdio",
                "-c",
                "<your-dir>/database_config.json"
            ]
        }
    }
}
```

For Cursor integration, you can use the provided scripts:

```bash
# Start the server in Cursor
./cursor-mcp.sh config.json
```

## Available Tools

For each connected database, the server dynamically creates a set of database-specific tools. For example, if you have databases with IDs "mysql1", "mysql2", and "postgres1", the following tools will be available:

### Query Tools
- `query_mysql1` - Execute SQL queries on mysql1 database
- `query_mysql2` - Execute SQL queries on mysql2 database  
- `query_postgres1` - Execute SQL queries on postgres1 database

### Execute Tools
- `execute_mysql1` - Run data modification statements on mysql1
- `execute_mysql2` - Run data modification statements on mysql2
- `execute_postgres1` - Run data modification statements on postgres1

### Transaction Tools
- `transaction_mysql1` - Manage transactions on mysql1
- `transaction_mysql2` - Manage transactions on mysql2
- `transaction_postgres1` - Manage transactions on postgres1

### Performance Tools
- `performance_mysql1` - Analyze query performance on mysql1
- `performance_mysql2` - Analyze query performance on mysql2
- `performance_postgres1` - Analyze query performance on postgres1

### Schema Tools
- `schema_mysql1` - Explore database schema on mysql1
- `schema_mysql2` - Explore database schema on mysql2
- `schema_postgres1` - Explore database schema on postgres1

### Global Tools
- `list_databases` - Show all available database connections

This architecture enables AI assistants to work with multiple databases simultaneously while maintaining separation between them.

## Architecture

The server follows Clean Architecture principles with these layers:

1. **Domain Layer**: Core business entities and interfaces
2. **Repository Layer**: Data access implementations
3. **Use Case Layer**: Application business logic
4. **Delivery Layer**: External interfaces (MCP tools)

## License

This project is licensed under the MIT License - see the LICENSE file for details.


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

## Development Notes

### Dependencies

This project has a dependency on `github.com/FreePeak/cortex` which can be installed using the standard Go module system:

```bash
# Ensure the dependency is installed
go get github.com/FreePeak/cortex@v1.0.1
go mod tidy
```

# Database MCP Server

This server provides database tools for Cursor via the MCP (MultiLLM Communication Protocol).

## Features

- Execute SQL queries across multiple databases
- Execute statements (INSERT, UPDATE, DELETE)
- Manage transactions
- Analyze query performance
- Explore database schema
- List available databases

## Setup

1. Build the server:
   ```
   make build
   ```

2. Configure your database connections in `config.json`:
   ```json
   {
     "connections": [
       {
         "id": "mysql1",
         "type": "mysql",
         "host": "localhost",
         "port": 3306,
         "name": "db1",
         "user": "user",
         "password": "password"
       },
       {
         "id": "mysql2",
         "type": "mysql",
         "host": "localhost",
         "port": 3307,
         "name": "db3",
         "user": "user",
         "password": "password"
       }
     ]
   }
   ```

3. Start the server:
   ```
   ./restart-mcp.sh
   ```

4. Start the Cursor compatibility server (if needed):
   ```
   ./run-cursor-compat.sh
   ```

5. Test the tools:
   ```
   ./test-tools.sh
   ```

## Architecture

The server follows Clean Architecture principles:

- **cmd/server/main.go**: Entry point and HTTP handlers
- **internal/delivery/mcp**: Tool registration and handlers
- **internal/usecase**: Business logic
- **internal/repository**: Data access
- **pkg/dbtools**: Database utilities

## Tools Available in Cursor

The following tools are available for use in Cursor:

- `mcp_cashflow_db_mcp_server_sse_list_databases`: List all available databases
- `mcp_cashflow_db_mcp_server_sse_query_mysql1`: Execute SQL query on mysql1 database
- `mcp_cashflow_db_mcp_server_sse_query_mysql2`: Execute SQL query on mysql2 database
- `mcp_cashflow_db_mcp_server_sse_execute_mysql1`: Execute SQL statement on mysql1 database
- `mcp_cashflow_db_mcp_server_sse_execute_mysql2`: Execute SQL statement on mysql2 database
- `mcp_cashflow_db_mcp_server_sse_schema_mysql1`: Get schema of mysql1 database
- `mcp_cashflow_db_mcp_server_sse_schema_mysql2`: Get schema of mysql2 database
- `mcp_cashflow_db_mcp_server_sse_transaction_mysql1`: Manage transactions on mysql1 database
- `mcp_cashflow_db_mcp_server_sse_transaction_mysql2`: Manage transactions on mysql2 database
- `mcp_cashflow_db_mcp_server_sse_performance_mysql1`: Analyze query performance on mysql1 database
- `mcp_cashflow_db_mcp_server_sse_performance_mysql2`: Analyze query performance on mysql2 database

## Using with Cursor

### List Databases
```json
{
  "random_string": "dummy"
}
```

### Query Database
```json
{
  "query": "SELECT * FROM users LIMIT 10",
  "params": ["param1", "param2"]
}
```

### Execute Statement
```json
{
  "statement": "INSERT INTO users (name, email) VALUES (?, ?)",
  "params": ["John Doe", "john@example.com"]
}
```

### Get Schema
```json
{
  "random_string": "dummy"
}
```

### Transaction Management
```json
// Begin transaction
{
  "action": "begin",
  "readOnly": false
}

// Execute in transaction
{
  "action": "execute",
  "transactionId": "tx_id_from_begin",
  "statement": "INSERT INTO users (name) VALUES (?)",
  "params": ["Alice"]
}

// Commit transaction
{
  "action": "commit",
  "transactionId": "tx_id_from_begin"
}

// Rollback transaction
{
  "action": "rollback",
  "transactionId": "tx_id_from_begin"
}
```

### Performance Analysis
```json
{
  "action": "getSlowQueries",
  "limit": 10
}
```

## Common Issues

1. **Invalid session ID**: Make sure the Cursor compatibility server is running on port 9093.
2. **Unknown tool**: Check the tool name prefix and ensure the server is running.
3. **Connection errors**: Verify your database connection settings in config.json.

## Troubleshooting

1. Check the logs in the `logs` directory
2. Run the test script: `./test-tools.sh`
3. Stop all servers and restart using the provided scripts