# Database Package

This package provides a unified database interface that works with both MySQL and PostgreSQL databases. It handles connection management, pooling, and query execution.

## Features

- Unified interface for MySQL and PostgreSQL
- Connection pooling with configurable parameters
- Context-aware query execution with timeout support
- Transaction support
- Proper error handling

## Usage

### Configuration

Configure the database connection using the `Config` struct:

```go
cfg := db.Config{
    Type:            "mysql", // or "postgres"
    Host:            "localhost",
    Port:            3306,
    User:            "user",
    Password:        "password",
    Name:            "dbname",
    MaxOpenConns:    25,
    MaxIdleConns:    5,
    ConnMaxLifetime: 5 * time.Minute,
    ConnMaxIdleTime: 5 * time.Minute,
}
```

### Connecting to the Database

```go
// Create a new database instance
database, err := db.NewDatabase(cfg)
if err != nil {
    log.Fatalf("Failed to create database instance: %v", err)
}

// Connect to the database
if err := database.Connect(); err != nil {
    log.Fatalf("Failed to connect to database: %v", err)
}
defer database.Close()
```

### Executing Queries

```go
// Context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Execute a query that returns rows
rows, err := database.Query(ctx, "SELECT id, name FROM users WHERE age > ?", 18)
if err != nil {
    log.Fatalf("Query failed: %v", err)
}
defer rows.Close()

// Process rows
for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        log.Printf("Failed to scan row: %v", err)
        continue
    }
    fmt.Printf("User: %d - %s\n", id, name)
}

if err = rows.Err(); err != nil {
    log.Printf("Error during row iteration: %v", err)
}
```

### Executing Statements

```go
// Execute a statement
result, err := database.Exec(ctx, "UPDATE users SET active = ? WHERE last_login < ?", true, time.Now().AddDate(0, -1, 0))
if err != nil {
    log.Fatalf("Statement execution failed: %v", err)
}

// Get affected rows
rowsAffected, err := result.RowsAffected()
if err != nil {
    log.Printf("Failed to get affected rows: %v", err)
}
fmt.Printf("Rows affected: %d\n", rowsAffected)
```

### Using Transactions

```go
// Start a transaction
tx, err := database.BeginTx(ctx, nil)
if err != nil {
    log.Fatalf("Failed to start transaction: %v", err)
}

// Execute statements within the transaction
_, err = tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")
if err != nil {
    tx.Rollback()
    log.Fatalf("Failed to execute statement in transaction: %v", err)
}

_, err = tx.ExecContext(ctx, "UPDATE user_stats SET user_count = user_count + 1")
if err != nil {
    tx.Rollback()
    log.Fatalf("Failed to execute statement in transaction: %v", err)
}

// Commit the transaction
if err := tx.Commit(); err != nil {
    log.Fatalf("Failed to commit transaction: %v", err)
}
```

## Error Handling

The package defines several common database errors:

- `ErrNotFound`: Record not found
- `ErrAlreadyExists`: Record already exists
- `ErrInvalidInput`: Invalid input parameters
- `ErrNotImplemented`: Functionality not implemented
- `ErrNoDatabase`: No database connection

These can be used for standardized error handling in your application. 