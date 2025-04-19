package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetSampleDataTool handles retrieving sample data from a table
type GetSampleDataTool struct {
	BaseToolType
}

// NewGetSampleDataTool creates a new get sample data tool type
func NewGetSampleDataTool() *GetSampleDataTool {
	return &GetSampleDataTool{
		BaseToolType: BaseToolType{
			name:        "get_sample_data",
			description: "Retrieve a sample of data from a database table. This tool allows you to fetch a representative sample of rows from any table in the database, helping you understand the data structure, content, and patterns without retrieving the entire table. You can specify the number of rows to retrieve, apply filters, and sort the results. This is particularly useful for large tables where retrieving all data would be inefficient.",
		},
	}
}

// CreateTool creates a get sample data tool
func (t *GetSampleDataTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve a sample of data from a database table"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("table",
			tools.Description("Table name to get sample data from"),
			tools.Required(),
		),
		tools.WithNumber("limit",
			tools.Description("Maximum number of rows to retrieve (default: 10)"),
		),
		tools.WithString("where",
			tools.Description("WHERE clause to filter the data (optional)"),
		),
		tools.WithString("order_by",
			tools.Description("ORDER BY clause to sort the data (optional)"),
		),
		tools.WithBoolean("random",
			tools.Description("Whether to retrieve random rows (default: false)"),
		),
	)
}

// HandleRequest handles get sample data tool requests
func (t *GetSampleDataTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract table name
	tableName, ok := request.Parameters["table"].(string)
	if !ok {
		return nil, fmt.Errorf("table parameter must be a string")
	}

	// Extract limit (default to 10)
	limit := 10
	if request.Parameters["limit"] != nil {
		if limitParam, ok := request.Parameters["limit"].(float64); ok {
			limit = int(limitParam)
		}
	}

	// Extract where clause (optional)
	whereClause := ""
	if request.Parameters["where"] != nil {
		if whereParam, ok := request.Parameters["where"].(string); ok {
			whereClause = whereParam
		}
	}

	// Extract order by clause (optional)
	orderByClause := ""
	if request.Parameters["order_by"] != nil {
		if orderByParam, ok := request.Parameters["order_by"].(string); ok {
			orderByClause = orderByParam
		}
	}

	// Extract random flag
	random := false
	if request.Parameters["random"] != nil {
		if randomParam, ok := request.Parameters["random"].(bool); ok {
			random = randomParam
		}
	}

	logger.Info("Getting sample data for database %s, table %s, limit %d", targetDbID, tableName, limit)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Build the query based on parameters
	query := buildSampleDataQuery(dbType, tableName, limit, whereClause, orderByClause, random)

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample data: %w", err)
	}

	// Format the response
	var response strings.Builder
	response.WriteString(fmt.Sprintf("# Sample Data from Table %s in Database %s\n\n", tableName, targetDbID))
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// buildSampleDataQuery builds a query to retrieve sample data based on parameters
func buildSampleDataQuery(dbType, tableName string, limit int, whereClause, orderByClause string, random bool) string {
	// Sanitize table name based on database type
	var safeTableName string
	if strings.ToLower(dbType) == "postgres" {
		// For PostgreSQL, use double quotes for identifiers
		safeTableName = fmt.Sprintf("\"%s\"", strings.Replace(tableName, "\"", "\"\"", -1))
	} else {
		// For MySQL, use backticks for identifiers
		safeTableName = fmt.Sprintf("`%s`", strings.Replace(tableName, "`", "``", -1))
	}

	// Build the base query
	query := fmt.Sprintf("SELECT * FROM %s", safeTableName)

	// Add WHERE clause if provided
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add ORDER BY clause based on parameters
	if random {
		// Use database-specific random function
		if strings.ToLower(dbType) == "postgres" {
			query += " ORDER BY RANDOM()"
		} else {
			query += " ORDER BY RAND()"
		}
	} else if orderByClause != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderByClause)
	}

	// Add LIMIT clause
	query += fmt.Sprintf(" LIMIT %d", limit)

	return query
}
