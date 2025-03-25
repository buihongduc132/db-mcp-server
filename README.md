# DB MCP Server

A database management tool server that implements the MCP protocol for integration with AI coding assistants. This tool allows querying and manipulating databases through a standardized protocol interface.

## Features

- Connect to multiple database types (MySQL, PostgreSQL)
- Query databases and view results
- Execute database commands
- Explore database schema
- Integration with Cursor and other MCP compatible tools

## Installation

### Prerequisites

- Go 1.21 or later
- Access to a MySQL or PostgreSQL database

### Building from Source

```bash
# Clone the repository
git clone https://github.com/FreePeak/db-mcp-server.git
cd db-mcp-server

# Build the server
go build -o server cmd/server/main.go
```

## Configuration

Create a `config.json` file with your database connection details:

```json
{
  "connections": [
    {
      "id": "my_postgres",
      "type": "postgres",
      "host": "localhost",
      "port": 5432,
      "database": "mydatabase",
      "username": "postgres",
      "password": "yourpassword"
    },
    {
      "id": "my_mysql",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "database": "mydatabase",
      "username": "root",
      "password": "yourpassword"
    }
  ]
}
```

## Usage

### Running with Cursor

To use the server with Cursor:

```bash
./cursor-mcp.sh [config_file]
```

If no config file is specified, it will use the default `config.json` in the current directory.

### Running in SSE Mode

For web-based clients using Server-Sent Events:

```bash
./server --transport sse --port 3000 --config config.json
```

### Available Commands

The MCP server exposes the following tools:

- `dbList`: List all configured database connections
- `dbQuery`: Execute a SQL query that returns results
- `dbExecute`: Execute a SQL statement (INSERT, UPDATE, DELETE, etc.)
- `schema_[database_id]`: Get the schema of a specific database

## Development

### Project Structure

- `cmd/server/`: Server implementation
- `pkg/db/`: Database abstraction and connection management
- `pkg/dbtools/`: Database tools implementation
- `pkg/tools/`: Generic tools implementation

### Adding New Database Types

To add support for a new database type:

1. Update the `DatabaseType` enum in `pkg/dbtools/dbtools.go`
2. Add the appropriate driver import in `pkg/db/db.go`
3. Update the `NewDatabase` function to support the new connection string format

## License

[MIT License](LICENSE)