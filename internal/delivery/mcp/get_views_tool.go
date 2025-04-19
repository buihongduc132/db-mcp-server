package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetViewsTool handles retrieving all views from a database
type GetViewsTool struct {
	BaseToolType
}

// NewGetViewsTool creates a new get views tool type
func NewGetViewsTool() *GetViewsTool {
	return &GetViewsTool{
		BaseToolType: BaseToolType{
			name:        "get_views",
			description: "Retrieve all views from a database with their definitions. This tool provides comprehensive information about all views in the database, including view names, definitions (SQL queries), columns, and dependencies. Views are virtual tables based on the result of SQL statements that can simplify complex queries and provide an abstraction layer over the physical database schema. Use this tool to understand how data is being presented and transformed in the database.",
		},
	}
}

// CreateTool creates a get views tool
func (t *GetViewsTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all views from a database with their definitions"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("view",
			tools.Description("View name to get definition for (optional, leave empty for all views)"),
		),
		tools.WithBoolean("include_definition",
			tools.Description("Whether to include the full SQL definition of each view (default: true)"),
		),
	)
}

// HandleRequest handles get views tool requests
func (t *GetViewsTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract view name (optional)
	viewName := ""
	if request.Parameters["view"] != nil {
		if viewParam, ok := request.Parameters["view"].(string); ok {
			viewName = viewParam
		}
	}

	// Extract include_definition flag (default to true)
	includeDefinition := true
	if request.Parameters["include_definition"] != nil {
		if includeDefParam, ok := request.Parameters["include_definition"].(bool); ok {
			includeDefinition = includeDefParam
		}
	}

	logger.Info("Getting views for database %s, view %s, include_definition %v", targetDbID, viewName, includeDefinition)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define query based on database type
	var query string
	switch strings.ToLower(dbType) {
	case "postgres":
		query = getPostgresViewsQuery(viewName, includeDefinition)
	case "mysql":
		query = getMySQLViewsQuery(viewName, includeDefinition)
	default:
		return nil, fmt.Errorf("unsupported database type for views: %s", dbType)
	}

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get views: %w", err)
	}

	// Format the response
	var response strings.Builder
	if viewName == "" {
		response.WriteString(fmt.Sprintf("# All Views in Database %s\n\n", targetDbID))
	} else {
		response.WriteString(fmt.Sprintf("# View Definition for %s in Database %s\n\n", viewName, targetDbID))
	}
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// getPostgresViewsQuery returns a query for PostgreSQL views
func getPostgresViewsQuery(viewName string, includeDefinition bool) string {
	// Base query for PostgreSQL views
	baseQuery := `
SELECT 
    schemaname AS schema_name,
    viewname AS view_name,
    definition AS view_definition
FROM pg_catalog.pg_views
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`

	if viewName != "" {
		// Escape view name for safety
		safeViewName := strings.Replace(viewName, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND viewname = '%s'", safeViewName)
	}

	baseQuery += `
ORDER BY schemaname, viewname;`

	// If we don't want to include the definition, modify the query
	if !includeDefinition {
		baseQuery = `
SELECT 
    schemaname AS schema_name,
    viewname AS view_name,
    'Definition not included' AS view_definition
FROM pg_catalog.pg_views
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`

		if viewName != "" {
			// Escape view name for safety
			safeViewName := strings.Replace(viewName, "'", "''", -1)
			baseQuery += fmt.Sprintf(" AND viewname = '%s'", safeViewName)
		}

		baseQuery += `
ORDER BY schemaname, viewname;`
	}

	return baseQuery
}

// getMySQLViewsQuery returns a query for MySQL views
func getMySQLViewsQuery(viewName string, includeDefinition bool) string {
	// Base query for MySQL views
	baseQuery := `
SELECT 
    table_schema AS schema_name,
    table_name AS view_name`

	if includeDefinition {
		baseQuery += `,
    view_definition`
	} else {
		baseQuery += `,
    'Definition not included' AS view_definition`
	}

	baseQuery += `
FROM information_schema.views
WHERE table_schema = DATABASE()`

	if viewName != "" {
		// Escape view name for safety
		safeViewName := strings.Replace(viewName, "`", "``", -1)
		baseQuery += fmt.Sprintf(" AND table_name = '%s'", safeViewName)
	}

	baseQuery += `
ORDER BY table_schema, table_name;`

	return baseQuery
}
