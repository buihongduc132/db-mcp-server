package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// GetIndexesTool handles retrieving all indexes from a database
type GetIndexesTool struct {
	BaseToolType
}

// NewGetIndexesTool creates a new get indexes tool type
func NewGetIndexesTool() *GetIndexesTool {
	return &GetIndexesTool{
		BaseToolType: BaseToolType{
			name:        "get_indexes",
			description: "Retrieve all indexes from a database with detailed information. This tool provides comprehensive information about all indexes in the database, including index names, types, associated tables, indexed columns, uniqueness constraints, and other index properties. Use this tool to understand the indexing strategy of a database, identify missing or redundant indexes, and optimize query performance.",
		},
	}
}

// CreateTool creates a get indexes tool
func (t *GetIndexesTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve all indexes from a database with detailed information"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("table",
			tools.Description("Table name to get indexes for (optional, leave empty for all tables)"),
		),
		tools.WithBoolean("detailed",
			tools.Description("Whether to include detailed index information"),
		),
	)
}

// HandleRequest handles get indexes tool requests
func (t *GetIndexesTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
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

	// Extract detailed flag
	detailed := false
	if request.Parameters["detailed"] != nil {
		if detailedParam, ok := request.Parameters["detailed"].(bool); ok {
			detailed = detailedParam
		}
	}

	logger.Info("Getting indexes for database %s, table %s (detailed: %v)", targetDbID, tableName, detailed)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define query based on database type
	var query string
	switch strings.ToLower(dbType) {
	case "postgres":
		query = getPostgresIndexesQuery(tableName, detailed)
	case "mysql":
		query = getMySQLIndexesQuery(tableName, detailed)
	default:
		return nil, fmt.Errorf("unsupported database type for indexes: %s", dbType)
	}

	// Execute the query
	result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get indexes: %w", err)
	}

	// Format the response
	var response strings.Builder
	if tableName == "" {
		response.WriteString(fmt.Sprintf("# All Indexes in Database %s\n\n", targetDbID))
	} else {
		response.WriteString(fmt.Sprintf("# Indexes for Table %s in Database %s\n\n", tableName, targetDbID))
	}
	response.WriteString(result)

	return createTextResponse(response.String()), nil
}

// getPostgresIndexesQuery returns a query for PostgreSQL indexes
func getPostgresIndexesQuery(tableName string, detailed bool) string {
	// Base query for PostgreSQL indexes
	baseQuery := `
SELECT 
    t.relname AS table_name,
    i.relname AS index_name,
    a.amname AS index_type,
    CASE 
        WHEN ix.indisprimary THEN 'PRIMARY KEY'
        WHEN ix.indisunique THEN 'UNIQUE'
        ELSE 'INDEX'
    END AS constraint_type,
    array_to_string(array_agg(pg_get_indexdef(ix.indexrelid, k + 1, true)), ', ') AS column_names`

	if detailed {
		baseQuery += `,
    pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
    pg_get_indexdef(ix.indexrelid) AS index_definition,
    CASE WHEN ix.indpred IS NOT NULL THEN 'Yes' ELSE 'No' END AS is_partial,
    CASE WHEN a.amname = 'btree' AND ix.indoption[0] & 1 = 1 THEN 'DESC' ELSE 'ASC' END AS sort_order`
	}

	baseQuery += `
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_am a ON a.oid = i.relam,
generate_series(0, array_length(ix.indkey, 1) - 1) AS k
WHERE n.nspname = 'public'`

	if tableName != "" {
		// Escape table name for safety
		safeTableName := strings.Replace(tableName, "'", "''", -1)
		baseQuery += fmt.Sprintf(" AND t.relname = '%s'", safeTableName)
	}

	baseQuery += `
GROUP BY t.relname, i.relname, a.amname, ix.indisprimary, ix.indisunique`

	if detailed {
		baseQuery += `, i.oid, ix.indexrelid, ix.indpred, a.amname, ix.indoption`
	}

	baseQuery += `
ORDER BY t.relname, i.relname;`

	return baseQuery
}

// getMySQLIndexesQuery returns a query for MySQL indexes
func getMySQLIndexesQuery(tableName string, detailed bool) string {
	// Base query for MySQL indexes
	baseQuery := `
SELECT 
    table_name,
    index_name,
    GROUP_CONCAT(column_name ORDER BY seq_in_index) AS column_names,
    CASE 
        WHEN index_name = 'PRIMARY' THEN 'PRIMARY KEY'
        WHEN non_unique = 0 THEN 'UNIQUE'
        ELSE 'INDEX'
    END AS constraint_type,
    index_type`

	if detailed {
		baseQuery += `,
    CASE WHEN index_name = 'PRIMARY' THEN 'YES' ELSE 'NO' END AS is_primary,
    CASE WHEN non_unique = 0 THEN 'YES' ELSE 'NO' END AS is_unique,
    CASE WHEN index_type = 'FULLTEXT' THEN 'YES' ELSE 'NO' END AS is_fulltext,
    CASE WHEN index_comment != '' THEN index_comment ELSE NULL END AS comment`
	}

	baseQuery += `
FROM information_schema.statistics
WHERE table_schema = DATABASE()`

	if tableName != "" {
		// Escape table name for safety
		safeTableName := strings.Replace(tableName, "`", "``", -1)
		baseQuery += fmt.Sprintf(" AND table_name = '%s'", safeTableName)
	}

	baseQuery += `
GROUP BY table_name, index_name, non_unique, index_type`

	if detailed {
		baseQuery += `, index_comment`
	}

	baseQuery += `
ORDER BY table_name, index_name;`

	return baseQuery
}
