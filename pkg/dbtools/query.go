package dbtools

import (
	"context"
	"fmt"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// createQueryTool creates a tool for executing database queries that return results
func createQueryTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbQuery",
		Description: "Execute a database query that returns results",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"query": map[string]interface{}{
					"type":        "string",
					"description": "SQL query to execute",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the query (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Query timeout in milliseconds (default: 5000)",
				},
			},
			Required: []string{"query"},
		},
		Handler: handleQuery,
	}
}

// handleQuery handles the query tool execution
func handleQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database is initialized
	if dbInstance == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	// Extract parameters
	query, ok := getStringParam(params, "query")
	if !ok {
		return nil, fmt.Errorf("query parameter is required")
	}

	// Extract timeout
	timeout := 5000 // Default timeout: 5 seconds
	if timeoutParam, ok := getIntParam(params, "timeout"); ok {
		timeout = timeoutParam
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Extract query parameters
	var queryParams []interface{}
	if paramsArray, ok := getArrayParam(params, "params"); ok {
		queryParams = make([]interface{}, len(paramsArray))
		for i, param := range paramsArray {
			queryParams[i] = param
		}
	}

	// Execute query
	rows, err := dbInstance.Query(timeoutCtx, query, queryParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Convert rows to map
	results, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process query results: %w", err)
	}

	// Return results
	return map[string]interface{}{
		"rows":   results,
		"count":  len(results),
		"query":  query,
		"params": queryParams,
	}, nil
}
