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

- Go 1.16 or later
- MySQL or PostgreSQL database

### Configuration

The application can be configured using environment variables or a `.env` file. Create a `.env` file in the root directory based on the `.env.example` template:

```ini
# Server Configuration
SERVER_PORT=9090

# Database Configuration
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database
```

### Building and Running

Build the application:

```bash
make build
```

Run the application (uses .env file configuration):

```bash
make run
```

Run with MySQL (using .env for database credentials):

```bash
make run-mysql
```

Add PostgreSQL support and run with PostgreSQL:

```bash
make run-postgres
```

Run with custom configuration (overrides .env):

```bash
./mcp-server -port 8080 -db-type mysql -db-config "user:password@tcp(localhost:3306)/dbname"
```

## API Usage

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

Available methods:

- `execute_query`: Execute a SQL query
  ```json
  {
    "sql": "SELECT * FROM users"
  }
  ```

- `insert_data`: Insert data into a table
  ```json
  {
    "table": "users",
    "data": {
      "name": "John Doe",
      "email": "john@example.com"
    }
  }
  ```

- `update_data`: Update data in a table
  ```json
  {
    "table": "users",
    "data": {
      "name": "Jane Doe"
    },
    "condition": "id = 1"
  }
  ```

- `delete_data`: Delete data from a table
  ```json
  {
    "table": "users",
    "condition": "id = 1"
  }
  ```

## Extending the Database Support

To add support for a new database system:

1. Create a new repository implementation in `internal/infrastructure/database/`
2. Update the database factory in `internal/infrastructure/database/factory.go`
3. Add any required dependencies to the `go.mod` file

## Cursor Integration

The server now supports integration with Cursor Editor through its Model Context Protocol (MCP). This allows using your database directly from Cursor's AI assistant.

### Cursor MCP Endpoint

The server exposes a dedicated endpoint for Cursor at:

```
http://localhost:9090/cursor-mcp
```

### Setup in Cursor

In Cursor, configure the external tool with:

```json
{
  "name": "Database Access",
  "description": "Provides tools to interact with a database",
  "transport": {
    "type": "sse",
    "serverUrl": "http://localhost:9090/cursor-mcp"
  }
}
```

For detailed instructions on Cursor integration, see [docs/cursor-integration.md](docs/cursor-integration.md).

## License

This project is licensed under the MIT License. 