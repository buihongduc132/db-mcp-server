package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetSchemasTool handles retrieving all schemas from a database
type GetSchemasTool struct {
	BaseToolType
}

// NewGetSchemasTool creates a new get schemas tool type
func NewGetSchemasTool() *GetSchemasTool {
	return &GetSchemasTool{
		BaseToolType: BaseToolType{
			name:        "get_schemas",
			description: "Retrieve all schemas from a database with detailed information. This tool provides information about database schemas, which are namespaces that contain database objects like tables, views, functions, and types. It shows schema names, owners, access privileges, and descriptions. Schemas help organize database objects and control access permissions. In PostgreSQL, schemas are extensively used, while in MySQL, schemas are equivalent to databases.",
		},
	}
}

// CreateTool creates a get schemas tool
func (t *GetSchemasTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all schemas from a database with detailed information"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("schema",
			tools.Description("Schema name to get information for (optional, leave empty for all schemas)"),
		),
		tools.WithBoolean("include_system_schemas",
			tools.Description("Whether to include system schemas like pg_catalog and information_schema"),
		),
	)
}

// HandleRequest handles get schemas tool requests
func (t *GetSchemasTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract schema name (optional)
	schemaName := ""
	if request.Parameters["schema"] != nil {
		if schemaParam, ok := request.Parameters["schema"].(string); ok {
			schemaName = schemaParam
		}
	}

	// Extract include_system_schemas flag
	includeSystemSchemas := false
	if request.Parameters["include_system_schemas"] != nil {
		if includeParam, ok := request.Parameters["include_system_schemas"].(bool); ok {
			includeSystemSchemas = includeParam
		}
	}

	logger.Info("Getting schemas for database %s, schema %s, include_system_schemas %v", targetDbID, schemaName, includeSystemSchemas)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define query based on database type
	var query string
	switch strings.ToLower(dbType) {
	case "postgres":
		query = getPostgresSchemasQuery(schemaName, includeSystemSchemas)
	case "mysql":
		query = getMySQLSchemasQuery(schemaName)
	default:
		return nil, fmt.Errorf("unsupported database type for schemas: %s", dbType)
	}

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get schemas: %w", err)
	}

	// Format the response
	var response strings.Builder
	if schemaName == "" {
		response.WriteString(fmt.Sprintf("# All Schemas in Database %s\n\n", targetDbID))
	} else {
		response.WriteString(fmt.Sprintf("# Schema Information for %s in Database %s\n\n", schemaName, targetDbID))
	}
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// getPostgresSchemasQuery returns a query for PostgreSQL schemas
func getPostgresSchemasQuery(schemaName string, includeSystemSchemas bool) string {
	// Base query for PostgreSQL schemas
	baseQuery := `
SELECT 
    n.nspname AS schema_name,
    pg_catalog.pg_get_userbyid(n.nspowner) AS owner,
    pg_catalog.array_to_string(n.nspacl, E'\n') AS access_privileges,
    pg_catalog.obj_description(n.oid, 'pg_namespace') AS description,
    (SELECT COUNT(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'r') AS tables_count,
    (SELECT COUNT(*) FROM pg_catalog.pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'v') AS views_count,
    (SELECT COUNT(*) FROM pg_catalog.pg_proc p WHERE p.pronamespace = n.oid) AS functions_count
FROM pg_catalog.pg_namespace n`

	if !includeSystemSchemas {
		baseQuery += `
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')`
	}

	if schemaName != "" {
		// Escape schema name for safety
		safeSchemaName := strings.Replace(schemaName, "'", "''", -1)
		if !includeSystemSchemas {
			baseQuery += fmt.Sprintf(" AND n.nspname = '%s'", safeSchemaName)
		} else {
			baseQuery += fmt.Sprintf(" WHERE n.nspname = '%s'", safeSchemaName)
		}
	}

	baseQuery += `
ORDER BY n.nspname;`

	return baseQuery
}

// getMySQLSchemasQuery returns a query for MySQL schemas (databases)
func getMySQLSchemasQuery(schemaName string) string {
	// In MySQL, schemas are equivalent to databases
	baseQuery := `
SELECT 
    schema_name,
    default_character_set_name AS character_set,
    default_collation_name AS collation,
    (SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name AND t.table_type = 'BASE TABLE') AS tables_count,
    (SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = s.schema_name AND t.table_type = 'VIEW') AS views_count,
    (SELECT COUNT(*) FROM information_schema.routines r WHERE r.routine_schema = s.schema_name) AS routines_count
FROM information_schema.schemata s`

	if schemaName != "" {
		// Escape schema name for safety
		safeSchemaName := strings.Replace(schemaName, "'", "''", -1)
		baseQuery += fmt.Sprintf(" WHERE schema_name = '%s'", safeSchemaName)
	}

	baseQuery += `
ORDER BY schema_name;`

	return baseQuery
}
