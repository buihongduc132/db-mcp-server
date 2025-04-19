package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetUniqueValuesTool handles retrieving unique values from a column
type GetUniqueValuesTool struct {
	BaseToolType
}

// NewGetUniqueValuesTool creates a new get unique values tool type
func NewGetUniqueValuesTool() *GetUniqueValuesTool {
	return &GetUniqueValuesTool{
		BaseToolType: BaseToolType{
			name:        "get_unique_values",
			description: "Retrieve all unique values from a column in a database table. This tool allows you to analyze the distinct values present in any column, helping you understand data distributions, identify outliers, and discover patterns. You can limit the number of values returned, filter the results, and get additional statistics like value counts and percentages. This is particularly useful for categorical data and for understanding the domain of values in a specific column.",
		},
	}
}

// CreateTool creates a get unique values tool
func (t *GetUniqueValuesTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all unique values from a column in a database table"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("table",
			tools.Description("Table name containing the column"),
			tools.Required(),
		),
		tools.WithString("column",
			tools.Description("Column name to get unique values from"),
			tools.Required(),
		),
		tools.WithNumber("limit",
			tools.Description("Maximum number of unique values to retrieve (default: 100)"),
		),
		tools.WithString("where",
			tools.Description("WHERE clause to filter the data (optional)"),
		),
		tools.WithBoolean("include_counts",
			tools.Description("Whether to include counts for each unique value (default: true)"),
		),
		tools.WithBoolean("include_nulls",
			tools.Description("Whether to include NULL values (default: true)"),
		),
	)
}

// HandleRequest handles get unique values tool requests
func (t *GetUniqueValuesTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
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

	// Extract column name
	columnName, ok := request.Parameters["column"].(string)
	if !ok {
		return nil, fmt.Errorf("column parameter must be a string")
	}

	// Extract limit (default to 100)
	limit := 100
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

	// Extract include_counts flag (default to true)
	includeCounts := true
	if request.Parameters["include_counts"] != nil {
		if includeCountsParam, ok := request.Parameters["include_counts"].(bool); ok {
			includeCounts = includeCountsParam
		}
	}

	// Extract include_nulls flag (default to true)
	includeNulls := true
	if request.Parameters["include_nulls"] != nil {
		if includeNullsParam, ok := request.Parameters["include_nulls"].(bool); ok {
			includeNulls = includeNullsParam
		}
	}

	logger.Info("Getting unique values for database %s, table %s, column %s", targetDbID, tableName, columnName)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Build the query based on parameters
	query := buildUniqueValuesQuery(dbType, tableName, columnName, limit, whereClause, includeCounts, includeNulls)

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get unique values: %w", err)
	}

	// Format the response
	var response strings.Builder
	response.WriteString(fmt.Sprintf("# Unique Values in Column %s of Table %s in Database %s\n\n", columnName, tableName, targetDbID))
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// buildUniqueValuesQuery builds a query to retrieve unique values based on parameters
func buildUniqueValuesQuery(dbType, tableName, columnName string, limit int, whereClause string, includeCounts, includeNulls bool) string {
	// Sanitize identifiers based on database type
	var safeTableName, safeColumnName string
	if strings.ToLower(dbType) == "postgres" {
		// For PostgreSQL, use double quotes for identifiers
		safeTableName = fmt.Sprintf("\"%s\"", strings.Replace(tableName, "\"", "\"\"", -1))
		safeColumnName = fmt.Sprintf("\"%s\"", strings.Replace(columnName, "\"", "\"\"", -1))
	} else {
		// For MySQL, use backticks for identifiers
		safeTableName = fmt.Sprintf("`%s`", strings.Replace(tableName, "`", "``", -1))
		safeColumnName = fmt.Sprintf("`%s`", strings.Replace(columnName, "`", "``", -1))
	}

	// Build the base query
	var query string
	if includeCounts {
		// Query with counts
		query = fmt.Sprintf("SELECT %s, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM %s), 2) AS percentage FROM %s",
			safeColumnName, safeTableName, safeTableName)
	} else {
		// Query without counts
		query = fmt.Sprintf("SELECT DISTINCT %s FROM %s", safeColumnName, safeTableName)
	}

	// Add WHERE clause if provided
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	// Add NULL handling if needed
	if !includeNulls {
		if whereClause == "" {
			query += fmt.Sprintf(" WHERE %s IS NOT NULL", safeColumnName)
		} else {
			query += fmt.Sprintf(" AND %s IS NOT NULL", safeColumnName)
		}
	}

	// Add GROUP BY and ORDER BY for queries with counts
	if includeCounts {
		query += fmt.Sprintf(" GROUP BY %s ORDER BY COUNT(*) DESC", safeColumnName)
	} else {
		query += fmt.Sprintf(" ORDER BY %s", safeColumnName)
	}

	// Add LIMIT clause
	query += fmt.Sprintf(" LIMIT %d", limit)

	return query
}
