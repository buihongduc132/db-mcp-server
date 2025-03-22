package dbtools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
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
		copy(queryParams, paramsArray)
	}

	// Execute query
	rows, err := dbInstance.Query(timeoutCtx, query, queryParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			logger.Error("Error closing rows: %v", closeErr)
		}
	}()

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

// createMockQueryTool creates a mock version of the query tool that works without database connection
func createMockQueryTool() *tools.Tool {
	// Create the tool using the same schema as the real query tool
	tool := createQueryTool()

	// Replace the handler with mock implementation
	tool.Handler = handleMockQuery

	return tool
}

// handleMockQuery is a mock implementation of the query handler
func handleMockQuery(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract parameters
	query, ok := getStringParam(params, "query")
	if !ok {
		return nil, fmt.Errorf("query parameter is required")
	}

	// Return mock data based on query
	var mockRows []map[string]interface{}

	// Simple pattern matching to generate relevant mock data
	if containsIgnoreCase(query, "user") {
		mockRows = []map[string]interface{}{
			{"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": time.Now().Add(-15 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "created_at": time.Now().Add(-7 * 24 * time.Hour).Format(time.RFC3339)},
		}
	} else if containsIgnoreCase(query, "order") {
		mockRows = []map[string]interface{}{
			{"id": 1001, "user_id": 1, "total_amount": "129.99", "status": "delivered", "created_at": time.Now().Add(-20 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 1002, "user_id": 2, "total_amount": "59.95", "status": "shipped", "created_at": time.Now().Add(-10 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 1003, "user_id": 1, "total_amount": "99.50", "status": "processing", "created_at": time.Now().Add(-2 * 24 * time.Hour).Format(time.RFC3339)},
		}
	} else if containsIgnoreCase(query, "product") {
		mockRows = []map[string]interface{}{
			{"id": 101, "name": "Smartphone", "price": "599.99", "created_at": time.Now().Add(-60 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 102, "name": "Laptop", "price": "999.99", "created_at": time.Now().Add(-45 * 24 * time.Hour).Format(time.RFC3339)},
			{"id": 103, "name": "Headphones", "price": "129.99", "created_at": time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339)},
		}
	} else {
		// Default mock data for other queries
		mockRows = []map[string]interface{}{
			{"result": "Mock data for query: " + query},
		}
	}

	// Extract any query parameters from the params
	var queryParams []interface{}
	if paramsArray, ok := getArrayParam(params, "params"); ok {
		queryParams = paramsArray
	}

	// Return the mock data in the same format as the real query tool
	return map[string]interface{}{
		"rows":   mockRows,
		"count":  len(mockRows),
		"query":  query,
		"params": queryParams,
	}, nil
}

// containsIgnoreCase checks if a string contains a substring (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
