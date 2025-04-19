package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetConstraintsTool handles retrieving all constraints from a database
type GetConstraintsTool struct {
	BaseToolType
}

// NewGetConstraintsTool creates a new get constraints tool type
func NewGetConstraintsTool() *GetConstraintsTool {
	return &GetConstraintsTool{
		BaseToolType: BaseToolType{
			name:        "get_constraints",
			description: "Retrieve all constraints from a database with detailed information. This tool provides comprehensive information about all constraints in the database, including primary keys, foreign keys, unique constraints, check constraints, and exclusion constraints. It shows constraint names, types, associated tables and columns, referenced tables and columns for foreign keys, and constraint definitions. Use this tool to understand data integrity rules and relationships between tables.",
		},
	}
}

// CreateTool creates a get constraints tool
func (t *GetConstraintsTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all constraints from a database with detailed information"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("table",
			tools.Description("Table name to get constraints for (optional, leave empty for all tables)"),
		),
		tools.WithString("constraint_type",
			tools.Description("Type of constraint to retrieve (optional: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, EXCLUSION)"),
		),
	)
}

// HandleRequest handles get constraints tool requests
func (t *GetConstraintsTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract table name (optional)
	tableName := ""
	if request.Parameters["table"] != nil {
		if tableParam, ok := request.Parameters["table"].(string); ok {
			tableName = tableParam
		}
	}

	// Extract constraint type (optional)
	constraintType := ""
	if request.Parameters["constraint_type"] != nil {
		if typeParam, ok := request.Parameters["constraint_type"].(string); ok {
			constraintType = typeParam
		}
	}

	logger.Info("Getting constraints for database %s, table %s, type %s", targetDbID, tableName, constraintType)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define query based on database type
	var query string
	switch strings.ToLower(dbType) {
	case "postgres":
		query = getPostgresConstraintsQuery(tableName, constraintType)
	case "mysql":
		query = getMySQLConstraintsQuery(tableName, constraintType)
	default:
		return nil, fmt.Errorf("unsupported database type for constraints: %s", dbType)
	}

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get constraints: %w", err)
	}

	// Format the response
	var response strings.Builder
	if tableName == "" {
		if constraintType == "" {
			response.WriteString(fmt.Sprintf("# All Constraints in Database %s\n\n", targetDbID))
		} else {
			response.WriteString(fmt.Sprintf("# %s Constraints in Database %s\n\n", constraintType, targetDbID))
		}
	} else {
		if constraintType == "" {
			response.WriteString(fmt.Sprintf("# All Constraints for Table %s in Database %s\n\n", tableName, targetDbID))
		} else {
			response.WriteString(fmt.Sprintf("# %s Constraints for Table %s in Database %s\n\n", constraintType, tableName, targetDbID))
		}
	}
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// getPostgresConstraintsQuery returns a query for PostgreSQL constraints
func getPostgresConstraintsQuery(tableName, constraintType string) string {
	// Base query for PostgreSQL constraints
	baseQuery := `
SELECT 
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    CASE 
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN ccu.table_name
        ELSE NULL
    END AS referenced_table,
    CASE 
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN 
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
        ELSE 
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position)
    END AS column_names,
    CASE 
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN 
            string_agg(ccu.column_name, ', ' ORDER BY kcu.ordinal_position)
        ELSE NULL
    END AS referenced_columns,
    CASE 
        WHEN tc.constraint_type = 'CHECK' THEN pgc.consrc
        ELSE NULL
    END AS check_definition
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
LEFT JOIN pg_constraint pgc
    ON pgc.conname = tc.constraint_name
LEFT JOIN pg_namespace nsp
    ON nsp.nspname = tc.table_schema
    AND pgc.connamespace = nsp.oid
WHERE tc.table_schema = 'public'`

	if tableName != "" {
		// Escape table name for safety
		safeTableName := strings.Replace(tableName, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND tc.table_name = '%s'", safeTableName)
	}

	if constraintType != "" {
		// Escape constraint type for safety
		safeConstraintType := strings.Replace(constraintType, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND tc.constraint_type = '%s'", safeConstraintType)
	}

	baseQuery += `
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, 
    CASE WHEN tc.constraint_type = 'FOREIGN KEY' THEN ccu.table_name ELSE NULL END,
    CASE WHEN tc.constraint_type = 'CHECK' THEN pgc.consrc ELSE NULL END
ORDER BY tc.table_name, tc.constraint_name;`

	return baseQuery
}

// getMySQLConstraintsQuery returns a query for MySQL constraints
func getMySQLConstraintsQuery(tableName, constraintType string) string {
	// Base query for MySQL constraints
	baseQuery := `
SELECT 
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    CASE 
        WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRIMARY KEY'
        WHEN tc.constraint_type = 'UNIQUE' THEN 'UNIQUE'
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN 'FOREIGN KEY'
        ELSE tc.constraint_type
    END AS constraint_type,
    GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position) AS column_names,
    kcu.referenced_table_name AS referenced_table,
    GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position) AS referenced_columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
    AND tc.table_name = kcu.table_name
WHERE tc.table_schema = DATABASE()`

	if tableName != "" {
		// Escape table name for safety
		safeTableName := strings.Replace(tableName, "`", "``", -1)
		baseQuery += fmt.Sprintf(" AND tc.table_name = '%s'", safeTableName)
	}

	if constraintType != "" {
		// Map constraint type to MySQL terminology
		var mysqlConstraintType string
		switch strings.ToUpper(constraintType) {
		case "PRIMARY KEY":
			mysqlConstraintType = "PRIMARY KEY"
		case "FOREIGN KEY":
			mysqlConstraintType = "FOREIGN KEY"
		case "UNIQUE":
			mysqlConstraintType = "UNIQUE"
		default:
			mysqlConstraintType = constraintType
		}
		// Escape constraint type for safety
		safeConstraintType := strings.Replace(mysqlConstraintType, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND tc.constraint_type = '%s'", safeConstraintType)
	}

	baseQuery += `
GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, tc.constraint_type, kcu.referenced_table_name
ORDER BY tc.table_name, tc.constraint_name;`

	return baseQuery
}
