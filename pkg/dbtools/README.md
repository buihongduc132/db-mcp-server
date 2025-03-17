# Database Tools Package

This package provides tools for interacting with databases in the MCP Server. It exposes database functionality as MCP tools that can be invoked by clients.

## Features

- Database query tool for executing SELECT statements
- Database execute tool for executing non-query statements (INSERT, UPDATE, DELETE)
- Transaction management tool for executing multiple statements atomically
- Support for both MySQL and PostgreSQL databases
- Parameterized queries to prevent SQL injection
- Connection pooling for optimal performance
- Timeouts for preventing long-running queries

## Available Tools

### 1. Database Query Tool (`dbQuery`)

Executes a SQL query and returns the results.

**Parameters:**
- `query` (string, required): SQL query to execute
- `params` (array): Parameters for prepared statements
- `timeout` (integer): Query timeout in milliseconds (default: 5000)

**Example:**
```json
{
  "query": "SELECT id, name, email FROM users WHERE status = ? AND created_at > ?",
  "params": ["active", "2023-01-01T00:00:00Z"],
  "timeout": 10000
}
```

**Returns:**
```json
{
  "rows": [
    {"id": 1, "name": "John", "email": "john@example.com"},
    {"id": 2, "name": "Jane", "email": "jane@example.com"}
  ],
  "count": 2,
  "query": "SELECT id, name, email FROM users WHERE status = ? AND created_at > ?",
  "params": ["active", "2023-01-01T00:00:00Z"]
}
```

### 2. Database Execute Tool (`dbExecute`)

Executes a SQL statement that doesn't return results (INSERT, UPDATE, DELETE).

**Parameters:**
- `statement` (string, required): SQL statement to execute
- `params` (array): Parameters for prepared statements
- `timeout` (integer): Execution timeout in milliseconds (default: 5000)

**Example:**
```json
{
  "statement": "INSERT INTO users (name, email, status) VALUES (?, ?, ?)",
  "params": ["Alice", "alice@example.com", "active"],
  "timeout": 10000
}
```

**Returns:**
```json
{
  "rowsAffected": 1,
  "lastInsertId": 3,
  "statement": "INSERT INTO users (name, email, status) VALUES (?, ?, ?)",
  "params": ["Alice", "alice@example.com", "active"]
}
```

### 3. Database Transaction Tool (`dbTransaction`)

Manages database transactions for executing multiple statements atomically.

**Parameters:**
- `action` (string, required): Action to perform (begin, commit, rollback, execute)
- `transactionId` (string): Transaction ID (returned from begin, required for all other actions)
- `statement` (string): SQL statement to execute (required for execute action)
- `params` (array): Parameters for the statement
- `readOnly` (boolean): Whether the transaction is read-only (for begin action)
- `timeout` (integer): Timeout in milliseconds (default: 30000)

**Example - Begin Transaction:**
```json
{
  "action": "begin",
  "readOnly": false,
  "timeout": 60000
}
```

**Returns:**
```json
{
  "transactionId": "tx-1625135848693",
  "readOnly": false,
  "status": "active"
}
```

**Example - Execute in Transaction:**
```json
{
  "action": "execute",
  "transactionId": "tx-1625135848693",
  "statement": "UPDATE accounts SET balance = balance - ? WHERE id = ?",
  "params": [100.00, 123]
}
```

**Example - Commit Transaction:**
```json
{
  "action": "commit",
  "transactionId": "tx-1625135848693"
}
```

**Returns:**
```json
{
  "transactionId": "tx-1625135848693",
  "status": "committed"
}
```

## Setup

To use these tools, initialize the database connection and register the tools:

```go
// Initialize database
err := dbtools.InitDatabase(config)
if err != nil {
    log.Fatalf("Failed to initialize database: %v", err)
}

// Register database tools
dbtools.RegisterDatabaseTools(toolRegistry)
```

## Error Handling

All tools return detailed error messages that indicate the specific issue. Common errors include:

- Database connection issues
- Invalid SQL syntax
- Transaction not found
- Timeout errors
- Permission errors

For transactions, always ensure you commit or rollback to avoid leaving transactions open. 