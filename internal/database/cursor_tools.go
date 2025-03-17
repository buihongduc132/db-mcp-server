package database

import (
	"context"
	"encoding/json"
	"fmt"
)

// CursorQueryParams represents the parameters for executing a SQL query specifically for Cursor
type CursorQueryParams struct {
	Query string        `json:"query"`
	Args  []interface{} `json:"args,omitempty"`
}

// HandleCursorQuery executes a SQL query with Cursor-friendly output formatting
func HandleCursorQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Parse parameters
	var queryParams CursorQueryParams
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

	// Process results with formatting for Cursor display
	queryResult, err := processQueryResult(rows)
	if err != nil {
		return nil, err
	}

	// Format the result as a Markdown table for better display in Cursor
	result := formatAsMarkdownTable(queryResult)
	return result, nil
}

// HandleGetDatabaseInfo returns information about the database for Cursor
func HandleGetDatabaseInfo(ctx context.Context, _ map[string]interface{}) (interface{}, error) {
	// Get database instance
	db, err := GetInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// Get database tables
	tables, err := db.GetTables()
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	// Get schema information for each table
	schemaInfo := make(map[string]interface{})
	for _, table := range tables {
		columns, err := db.GetTableSchema(table)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema for table %s: %w", table, err)
		}
		schemaInfo[table] = columns
	}

	result := map[string]interface{}{
		"connection": map[string]string{
			"database": db.Config.Database,
			"host":     db.Config.Host,
			"port":     fmt.Sprintf("%d", db.Config.Port),
		},
		"tables":  tables,
		"schemas": schemaInfo,
	}

	return result, nil
}

// formatAsMarkdownTable formats query results as a Markdown table for better display in Cursor
func formatAsMarkdownTable(queryResult QueryResult) map[string]interface{} {
	if len(queryResult.Columns) == 0 || len(queryResult.Rows) == 0 {
		return map[string]interface{}{
			"raw":      queryResult,
			"markdown": "No results found",
		}
	}

	// Create markdown table header
	markdownTable := "| "
	for _, col := range queryResult.Columns {
		markdownTable += fmt.Sprintf("%s | ", col)
	}
	markdownTable += "\n|"

	// Create header separator
	for range queryResult.Columns {
		markdownTable += " --- |"
	}
	markdownTable += "\n"

	// Add rows
	for _, row := range queryResult.Rows {
		markdownTable += "| "
		for _, cell := range row {
			var cellStr string
			if cell == nil {
				cellStr = "NULL"
			} else {
				cellStr = fmt.Sprintf("%v", cell)
			}
			markdownTable += fmt.Sprintf("%s | ", cellStr)
		}
		markdownTable += "\n"
	}

	return map[string]interface{}{
		"raw":      queryResult,
		"markdown": markdownTable,
		"rowCount": len(queryResult.Rows),
	}
}
