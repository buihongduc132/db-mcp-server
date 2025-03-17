package dbtools

import (
	"context"
	"fmt"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// createExecuteTool creates a tool for executing database statements that don't return rows
func createExecuteTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbExecute",
		Description: "Execute a database statement that doesn't return results (INSERT, UPDATE, DELETE, etc.)",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"statement": map[string]interface{}{
					"type":        "string",
					"description": "SQL statement to execute",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the statement (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Execution timeout in milliseconds (default: 5000)",
				},
			},
			Required: []string{"statement"},
		},
		Handler: handleExecute,
	}
}

// handleExecute handles the execute tool execution
func handleExecute(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database is initialized
	if dbInstance == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	// Extract parameters
	statement, ok := getStringParam(params, "statement")
	if !ok {
		return nil, fmt.Errorf("statement parameter is required")
	}

	// Extract timeout
	timeout := 5000 // Default timeout: 5 seconds
	if timeoutParam, ok := getIntParam(params, "timeout"); ok {
		timeout = timeoutParam
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Extract statement parameters
	var statementParams []interface{}
	if paramsArray, ok := getArrayParam(params, "params"); ok {
		statementParams = make([]interface{}, len(paramsArray))
		for i, param := range paramsArray {
			statementParams[i] = param
		}
	}

	// Execute statement
	result, err := dbInstance.Exec(timeoutCtx, statement, statementParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	// Get affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = -1 // Unable to determine
	}

	// Get last insert ID (if applicable)
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		lastInsertID = -1 // Unable to determine
	}

	// Return results
	return map[string]interface{}{
		"rowsAffected": rowsAffected,
		"lastInsertId": lastInsertID,
		"statement":    statement,
		"params":       statementParams,
	}, nil
}
