package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
)

// ExecuteQueryParams represents the parameters for executing a SQL query
type ExecuteQueryParams struct {
	Query string        `json:"query"`
	Args  []interface{} `json:"args,omitempty"`
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

// ExecuteResult represents the result of a SQL execution (non-query)
type ExecuteResult struct {
	RowsAffected int64 `json:"rowsAffected"`
	LastInsertID int64 `json:"lastInsertId"`
}

// HandleExecuteQuery executes a SQL query and returns the results
func HandleExecuteQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Parse parameters
	var queryParams ExecuteQueryParams
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parameters: %w", err)
	}

	if err := json.Unmarshal(paramsJSON, &queryParams); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	if queryParams.Query == "" {
		return nil, fmt.Errorf("query parameter is required")
	}

	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Execute query
	rows, err := db.ExecuteQuery(queryParams.Query, queryParams.Args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	return processQueryResult(rows)
}

// HandleExecuteNonQuery executes a SQL non-query (e.g., INSERT, UPDATE, DELETE)
func HandleExecuteNonQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Parse parameters
	var queryParams ExecuteQueryParams
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parameters: %w", err)
	}

	if err := json.Unmarshal(paramsJSON, &queryParams); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	if queryParams.Query == "" {
		return nil, fmt.Errorf("query parameter is required")
	}

	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Execute non-query
	result, err := db.ExecuteNonQuery(queryParams.Query, queryParams.Args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute non-query: %w", err)
	}

	// Get result stats
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("Failed to get rows affected: %v", err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		log.Printf("Failed to get last insert ID: %v", err)
	}

	return ExecuteResult{
		RowsAffected: rowsAffected,
		LastInsertID: lastInsertID,
	}, nil
}

// HandleGetTables gets a list of tables in the database
func HandleGetTables(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Get tables
	tables, err := db.GetTables()
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	return map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
	}, nil
}

// GetTableSchemaParams represents the parameters for getting a table schema
type GetTableSchemaParams struct {
	Table string `json:"table"`
}

// HandleGetTableSchema gets the schema of a table
func HandleGetTableSchema(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Parse parameters
	var schemaParams GetTableSchemaParams
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parameters: %w", err)
	}

	if err := json.Unmarshal(paramsJSON, &schemaParams); err != nil {
		return nil, fmt.Errorf("invalid parameters: %w", err)
	}

	if schemaParams.Table == "" {
		return nil, fmt.Errorf("table parameter is required")
	}

	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Get table schema
	schema, err := db.GetTableSchema(schemaParams.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	return map[string]interface{}{
		"table":   schemaParams.Table,
		"columns": schema,
		"count":   len(schema),
	}, nil
}

// processQueryResult processes SQL query result into a structured format
func processQueryResult(rows *sql.Rows) (QueryResult, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to get column names: %w", err)
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to get column types: %w", err)
	}

	// Create result structure
	result := QueryResult{
		Columns: columns,
		Rows:    [][]interface{}{},
	}

	// Create a slice of interface{} to hold the row values
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Loop through the rows
	for rows.Next() {
		// Scan the row values
		if err := rows.Scan(scanArgs...); err != nil {
			return QueryResult{}, fmt.Errorf("failed to scan row: %w", err)
		}

		// Process row values
		row := make([]interface{}, len(columns))
		for i, v := range values {
			// Handle MySQL data types
			switch columnTypes[i].DatabaseTypeName() {
			case "DECIMAL", "FLOAT", "DOUBLE":
				// Keep as float64
				row[i] = v
			case "BIGINT", "INT", "SMALLINT", "TINYINT":
				// Keep as int64
				row[i] = v
			case "BLOB", "BINARY", "VARBINARY":
				// Handle binary data
				if v == nil {
					row[i] = nil
				} else if b, ok := v.([]byte); ok {
					row[i] = fmt.Sprintf("<binary data: %d bytes>", len(b))
				} else {
					row[i] = v
				}
			default:
				// Handle string types and others
				if v == nil {
					row[i] = nil
				} else if b, ok := v.([]byte); ok {
					row[i] = string(b)
				} else {
					row[i] = v
				}
			}
		}

		// Add the row to the result
		result.Rows = append(result.Rows, row)
	}

	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("error iterating through rows: %w", err)
	}

	return result, nil
}

// HandlePingDatabase checks if the database connection is alive
func HandlePingDatabase(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Ping the database
	if err := db.GetDB().PingContext(ctx); err != nil {
		return map[string]interface{}{
			"status":  "error",
			"message": fmt.Sprintf("Database ping failed: %v", err),
		}, nil
	}

	return map[string]interface{}{
		"status":  "success",
		"message": "Database connection is alive",
	}, nil
}
