package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// DbStatsTool handles database statistics operations
type DbStatsTool struct {
	BaseToolType
}

// NewDbStatsTool creates a new database statistics tool type
func NewDbStatsTool() *DbStatsTool {
	return &DbStatsTool{
		BaseToolType: BaseToolType{
			name:        "db_stats",
			description: "Retrieve comprehensive database statistics and metrics. This tool provides detailed information about database performance, size, connections, and usage statistics. It helps you monitor database health, identify performance bottlenecks, and track resource utilization. Statistics include database size, number of connections, buffer usage, cache hit ratios, and more, depending on the database type.",
		},
	}
}

// CreateTool creates a database statistics tool
func (t *DbStatsTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve comprehensive database statistics and metrics"),
		tools.WithString("database",
			tools.Description("Database ID to get statistics for"),
			tools.Required(),
		),
		tools.WithBoolean("detailed",
			tools.Description("Whether to include detailed statistics (may be slower)"),
		),
	)
}

// HandleRequest handles database statistics tool requests
func (t *DbStatsTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// Extract database ID from parameters
	targetDbID, ok := request.Parameters["database"].(string)
	if !ok {
		return nil, fmt.Errorf("database parameter must be a string")
	}

	// Extract detailed flag
	detailed := false
	if request.Parameters["detailed"] != nil {
		if detailedParam, ok := request.Parameters["detailed"].(bool); ok {
			detailed = detailedParam
		}
	}

	logger.Info("Getting database statistics for %s (detailed: %v)", targetDbID, detailed)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define queries based on database type
	var queries []string
	switch strings.ToLower(dbType) {
	case "postgres":
		queries = getPostgresStatsQueries(detailed)
	case "mysql":
		queries = getMySQLStatsQueries(detailed)
	default:
		return nil, fmt.Errorf("unsupported database type for statistics: %s", dbType)
	}

	// Execute each query and combine results
	var results strings.Builder
	results.WriteString(fmt.Sprintf("# Database Statistics for %s (%s)\n\n", targetDbID, dbType))

	for _, query := range queries {
		// Execute the query
		result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
		if err != nil {
			// Log the error but continue with other queries
			logger.Warn("Error executing stats query: %v", err)
			results.WriteString(fmt.Sprintf("Error executing query: %s\n%v\n\n", query, err))
			continue
		}

		// Add the result
		results.WriteString(result)
		results.WriteString("\n\n")
	}

	return createTextResponse(results.String()), nil
}

// getPostgresStatsQueries returns queries for PostgreSQL statistics
func getPostgresStatsQueries(detailed bool) []string {
	// Basic queries
	queries := []string{
		// Database size
		`SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;`,
		
		// Connection statistics
		`SELECT 
			count(*) AS total_connections,
			sum(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active_connections,
			sum(CASE WHEN state = 'idle' THEN 1 ELSE 0 END) AS idle_connections
		FROM pg_stat_activity;`,
		
		// Table statistics
		`SELECT 
			schemaname, 
			relname AS table_name, 
			pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
			pg_size_pretty(pg_relation_size(relid)) AS table_size,
			pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_size,
			n_live_tup AS row_count
		FROM pg_stat_user_tables
		ORDER BY pg_total_relation_size(relid) DESC
		LIMIT 10;`,
	}

	// Add detailed queries if requested
	if detailed {
		detailedQueries := []string{
			// Index statistics
			`SELECT 
				schemaname,
				relname AS table_name,
				indexrelname AS index_name,
				idx_scan AS index_scans,
				idx_tup_read AS tuples_read,
				idx_tup_fetch AS tuples_fetched
			FROM pg_stat_user_indexes
			ORDER BY idx_scan DESC
			LIMIT 10;`,
			
			// Buffer cache statistics
			`SELECT 
				c.relname AS table_name,
				pg_size_pretty(count(*) * 8192) AS buffer_size,
				round(100.0 * count(*) / (SELECT setting::integer FROM pg_settings WHERE name = 'shared_buffers'), 2) AS buffer_percent
			FROM pg_class c
			INNER JOIN pg_buffercache b ON b.relfilenode = c.relfilenode
			INNER JOIN pg_database d ON (b.reldatabase = d.oid AND d.datname = current_database())
			WHERE c.relkind IN ('r', 't', 'm')
			GROUP BY c.relname
			ORDER BY count(*) DESC
			LIMIT 10;`,
			
			// Transaction statistics
			`SELECT 
				datname,
				xact_commit AS commits,
				xact_rollback AS rollbacks,
				blks_read,
				blks_hit,
				tup_returned,
				tup_fetched,
				tup_inserted,
				tup_updated,
				tup_deleted
			FROM pg_stat_database
			WHERE datname = current_database();`,
		}
		
		queries = append(queries, detailedQueries...)
	}

	return queries
}

// getMySQLStatsQueries returns queries for MySQL statistics
func getMySQLStatsQueries(detailed bool) []string {
	// Basic queries
	queries := []string{
		// Database size
		`SELECT 
			table_schema AS database_name,
			ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size_mb
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		GROUP BY table_schema;`,
		
		// Connection statistics
		`SHOW STATUS WHERE Variable_name IN ('Threads_connected', 'Threads_running', 'Max_used_connections');`,
		
		// Table statistics
		`SELECT 
			table_name,
			engine,
			table_rows,
			ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
			ROUND(data_length / 1024 / 1024, 2) AS data_size_mb,
			ROUND(index_length / 1024 / 1024, 2) AS index_size_mb
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		ORDER BY (data_length + index_length) DESC
		LIMIT 10;`,
	}

	// Add detailed queries if requested
	if detailed {
		detailedQueries := []string{
			// Buffer pool statistics
			`SHOW GLOBAL STATUS WHERE Variable_name LIKE 'Innodb_buffer_pool%';`,
			
			// Query cache statistics
			`SHOW GLOBAL STATUS WHERE Variable_name LIKE 'Qcache%';`,
			
			// Table I/O statistics
			`SELECT 
				table_schema,
				table_name,
				rows_read,
				rows_inserted,
				rows_updated,
				rows_deleted
			FROM information_schema.table_statistics
			WHERE table_schema = DATABASE()
			ORDER BY rows_read DESC
			LIMIT 10;`,
			
			// Index statistics
			`SELECT 
				table_schema,
				table_name,
				index_name,
				rows_read
			FROM information_schema.index_statistics
			WHERE table_schema = DATABASE()
			ORDER BY rows_read DESC
			LIMIT 10;`,
		}
		
		queries = append(queries, detailedQueries...)
	}

	return queries
}
