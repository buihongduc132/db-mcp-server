package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GenericSQLTool handles SQL operations on any database
type GenericSQLTool struct {
	BaseToolType
}

// NewGenericSQLTool creates a new generic SQL tool type
func NewGenericSQLTool() *GenericSQLTool {
	return &GenericSQLTool{
		BaseToolType: BaseToolType{
			name:        "sql",
			description: "Execute SQL on any database",
		},
	}
}

// CreateTool creates a generic SQL tool
func (t *GenericSQLTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Execute SQL queries or statements on any configured database"),
		tools.WithString("sql",
			tools.Description("SQL query or statement to execute"),
			tools.Required(),
		),
		tools.WithString("database",
			tools.Description("Database ID to execute the SQL on"),
			tools.Required(),
		),
		tools.WithArray("params",
			tools.Description("SQL parameters"),
			tools.Items(map[string]interface{}{"type": "string"}),
		),
		tools.WithBoolean("isQuery",
			tools.Description("Set to true for SELECT queries, false for statements (INSERT, UPDATE, DELETE)"),
		),
	)
}

// HandleRequest handles generic SQL tool requests
func (t *GenericSQLTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract SQL statement/query
	sql, ok := request.Parameters["sql"].(string)
	if !ok {
		return nil, fmt.Errorf("sql parameter must be a string")
	}

	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract parameters
	var sqlParams []interface{}
	if request.Parameters["params"] != nil {
		if paramsArr, ok := request.Parameters["params"].([]interface{}); ok {
			sqlParams = paramsArr
		}
	}

	// Determine if this is a query or a statement
	isQuery := false
	if request.Parameters["isQuery"] != nil {
		if isQueryParam, ok := request.Parameters["isQuery"].(bool); ok {
			isQuery = isQueryParam
		}
	} else {
		// Auto-detect if not specified
		sqlUpper := strings.TrimSpace(strings.ToUpper(sql))
		isQuery = strings.HasPrefix(sqlUpper, "SELECT") || 
			strings.HasPrefix(sqlUpper, "SHOW") || 
			strings.HasPrefix(sqlUpper, "DESCRIBE") || 
			strings.HasPrefix(sqlUpper, "EXPLAIN")
	}

	logger.Info("Executing SQL on database %s (isQuery: %v): %s", targetDbID, isQuery, sql)

	var result string
	var err error

	if isQuery {
		// Execute as a query (SELECT)
		result, err = useCase.ExecuteQuery(ctx, targetDbID, sql, sqlParams)
	} else {
		// Execute as a statement (INSERT, UPDATE, DELETE)
		result, err = useCase.ExecuteStatement(ctx, targetDbID, sql, sqlParams)
	}

	if err != nil {
		return nil, err
	}

	return createTextResponse(result), nil
}
