package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetTypesTool handles retrieving all custom data types from a database
type GetTypesTool struct {
	BaseToolType
}

// NewGetTypesTool creates a new get types tool type
func NewGetTypesTool() *GetTypesTool {
	return &GetTypesTool{
		BaseToolType: BaseToolType{
			name:        "get_types",
			description: "Retrieve all custom data types from a database. This tool provides information about user-defined data types, enumerated types, composite types, domain types, and range types in the database. It shows type names, categories, definitions, and related attributes. Custom data types are particularly important in PostgreSQL databases where they are commonly used to enforce data integrity and create more expressive data models.",
		},
	}
}

// CreateTool creates a get types tool
func (t *GetTypesTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all custom data types from a database"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("type_name",
			tools.Description("Type name to get definition for (optional, leave empty for all types)"),
		),
	)
}

// HandleRequest handles get types tool requests
func (t *GetTypesTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract type name (optional)
	typeName := ""
	if request.Parameters["type_name"] != nil {
		if typeParam, ok := request.Parameters["type_name"].(string); ok {
			typeName = typeParam
		}
	}

	logger.Info("Getting custom data types for database %s, type %s", targetDbID, typeName)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define query based on database type
	var query string
	switch strings.ToLower(dbType) {
	case "postgres":
		query = getPostgresTypesQuery(typeName)
	case "mysql":
		// MySQL doesn't have true custom types like PostgreSQL
		return createTextResponse("MySQL does not support custom data types in the same way as PostgreSQL. It only has built-in data types."), nil
	default:
		return nil, fmt.Errorf("unsupported database type for custom data types: %s", dbType)
	}

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get custom data types: %w", err)
	}

	// Format the response
	var response strings.Builder
	if typeName == "" {
		response.WriteString(fmt.Sprintf("# All Custom Data Types in Database %s\n\n", targetDbID))
	} else {
		response.WriteString(fmt.Sprintf("# Custom Data Type Definition for %s in Database %s\n\n", typeName, targetDbID))
	}
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// getPostgresTypesQuery returns a query for PostgreSQL custom data types
func getPostgresTypesQuery(typeName string) string {
	// Base query for PostgreSQL custom data types
	baseQuery := `
SELECT 
    n.nspname AS schema_name,
    t.typname AS type_name,
    CASE 
        WHEN t.typtype = 'e' THEN 'ENUM'
        WHEN t.typtype = 'c' THEN 'COMPOSITE'
        WHEN t.typtype = 'd' THEN 'DOMAIN'
        WHEN t.typtype = 'r' THEN 'RANGE'
        WHEN t.typtype = 'b' THEN 'BASE'
        ELSE t.typtype::text
    END AS type_category,
    CASE
        WHEN t.typtype = 'e' THEN 
            (SELECT string_agg(quote_literal(enumlabel), ', ' ORDER BY enumsortorder)
             FROM pg_enum
             WHERE enumtypid = t.oid)
        WHEN t.typtype = 'c' THEN 
            (SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod), ', ' ORDER BY attnum)
             FROM pg_attribute
             WHERE attrelid = t.typrelid AND attnum > 0 AND NOT attisdropped)
        WHEN t.typtype = 'd' THEN 
            format_type(t.typbasetype, t.typtypmod) || 
            CASE WHEN t.typnotnull THEN ' NOT NULL' ELSE '' END ||
            CASE WHEN t.typdefault IS NOT NULL THEN ' DEFAULT ' || t.typdefault ELSE '' END
        WHEN t.typtype = 'r' THEN 
            (SELECT format_type(rngsubtype, NULL) FROM pg_range WHERE rngtypid = t.oid)
        ELSE format_type(t.oid, NULL)
    END AS type_definition,
    pg_catalog.obj_description(t.oid, 'pg_type') AS description
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE (t.typtype IN ('e', 'c', 'd', 'r') OR (t.typtype = 'b' AND t.typname NOT LIKE '\\_%'))
AND n.nspname NOT IN ('pg_catalog', 'information_schema')`

	if typeName != "" {
		// Escape type name for safety
		safeTypeName := strings.Replace(typeName, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND t.typname = '%s'", safeTypeName)
	}

	baseQuery += `
ORDER BY n.nspname, t.typname;`

	return baseQuery
}
