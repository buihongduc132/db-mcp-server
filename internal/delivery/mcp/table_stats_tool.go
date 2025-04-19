package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
	"github.com/FreePeak/db-mcp-server/internal/logger"
)

// TableStatsTool handles table statistics operations
type TableStatsTool struct {
	BaseToolType
}

// NewTableStatsTool creates a new table statistics tool type
func NewTableStatsTool() *TableStatsTool {
	return &TableStatsTool{
		BaseToolType: BaseToolType{
			name:        "table_stats",
			description: "Retrieve detailed statistics for a specific database table. This tool provides comprehensive information about a table's structure, size, usage patterns, and performance metrics. Statistics include row count, size on disk, index usage, read/write operations, and more. Use this tool to analyze table performance, identify optimization opportunities, and understand table usage patterns.",
		},
	}
}

// CreateTool creates a table statistics tool
func (t *TableStatsTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription("Retrieve detailed statistics for a specific database table"),
		tools.WithString("database",
			tools.Description("Database ID to use"),
			tools.Required(),
		),
		tools.WithString("table",
			tools.Description("Table name to get statistics for"),
			tools.Required(),
		),
		tools.WithBoolean("detailed",
			tools.Description("Whether to include detailed statistics (may be slower)"),
		),
	)
}

// HandleRequest handles table statistics tool requests
func (t *TableStatsTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
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

	// Extract detailed flag
	detailed := false
	if request.Parameters["detailed"] != nil {
		if detailedParam, ok := request.Parameters["detailed"].(bool); ok {
			detailed = detailedParam
		}
	}

	logger.Info("Getting table statistics for %s.%s (detailed: %v)", targetDbID, tableName, detailed)

	// Get database type to determine which queries to run
	dbType, err := useCase.GetDatabaseType(targetDbID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database type: %w", err)
	}

	// Define queries based on database type
	var queries []string
	switch strings.ToLower(dbType) {
	case "postgres":
		queries = getPostgresTableStatsQueries(tableName, detailed)
	case "mysql":
		queries = getMySQLTableStatsQueries(tableName, detailed)
	default:
		return nil, fmt.Errorf("unsupported database type for table statistics: %s", dbType)
	}

	// Execute each query and combine results
	var results strings.Builder
	results.WriteString(fmt.Sprintf("# Table Statistics for %s.%s\n\n", targetDbID, tableName))

	for _, query := range queries {
		// Execute the query
		result, err := useCase.ExecuteQuery(ctx, targetDbID, query, nil)
		if err != nil {
			// Log the error but continue with other queries
			logger.Warn("Error executing table stats query: %v", err)
			results.WriteString(fmt.Sprintf("Error executing query: %s\n%v\n\n", query, err))
			continue
		}

		// Add the result
		results.WriteString(result)
		results.WriteString("\n\n")
	}

	return createTextResponse(results.String()), nil
}

// getPostgresTableStatsQueries returns queries for PostgreSQL table statistics
func getPostgresTableStatsQueries(tableName string, detailed bool) []string {
	// Escape table name for safety
	safeTableName := strings.Replace(tableName, "'", "''", -1)

	// Basic queries
	queries := []string{
		// Table size and row count
		fmt.Sprintf(`SELECT 
			pg_size_pretty(pg_total_relation_size('%s')) AS total_size,
			pg_size_pretty(pg_relation_size('%s')) AS table_size,
			pg_size_pretty(pg_total_relation_size('%s') - pg_relation_size('%s')) AS index_size,
			n_live_tup AS row_count,
			n_dead_tup AS dead_tuples
		FROM pg_stat_user_tables
		WHERE relname = '%s';`, safeTableName, safeTableName, safeTableName, safeTableName, safeTableName),
		
		// Column information
		fmt.Sprintf(`SELECT 
			a.attname AS column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END AS nullable,
			CASE WHEN (
				SELECT COUNT(*) FROM pg_constraint
				WHERE conrelid = a.attrelid
				AND conkey[1] = a.attnum
				AND contype = 'p'
			) > 0 THEN 'PK' ELSE '' END AS is_primary_key
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
		JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		WHERE c.relname = '%s'
		AND a.attnum > 0
		AND NOT a.attisdropped
		AND n.nspname = 'public'
		ORDER BY a.attnum;`, safeTableName),
		
		// Index information
		fmt.Sprintf(`SELECT 
			i.relname AS index_name,
			pg_size_pretty(pg_relation_size(i.relname::regclass)) AS index_size,
			idx_scan AS index_scans,
			idx_tup_read AS tuples_read,
			idx_tup_fetch AS tuples_fetched,
			a.amname AS index_type,
			array_to_string(array_agg(pg_catalog.pg_get_indexdef(idx.indexrelid, k + 1, true)), ', ') AS column_names
		FROM pg_stat_user_indexes ui
		JOIN pg_index idx ON ui.indexrelid = idx.indexrelid
		JOIN pg_class i ON idx.indexrelid = i.oid
		JOIN pg_class c ON idx.indrelid = c.oid
		JOIN pg_am a ON i.relam = a.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid,
		generate_series(0, subarray(idx.indkey, 0, idx.indnkeyatts)::int[] - 1) AS k
		WHERE c.relname = '%s'
		AND n.nspname = 'public'
		GROUP BY i.relname, ui.idx_scan, ui.idx_tup_read, ui.idx_tup_fetch, a.amname
		ORDER BY i.relname;`, safeTableName),
	}

	// Add detailed queries if requested
	if detailed {
		detailedQueries := []string{
			// Table I/O statistics
			fmt.Sprintf(`SELECT 
				seq_scan AS sequential_scans,
				seq_tup_read AS sequential_tuples_read,
				idx_scan AS index_scans,
				idx_tup_fetch AS index_tuples_fetched,
				n_tup_ins AS tuples_inserted,
				n_tup_upd AS tuples_updated,
				n_tup_del AS tuples_deleted,
				n_tup_hot_upd AS hot_updates,
				n_live_tup AS live_tuples,
				n_dead_tup AS dead_tuples,
				vacuum_count,
				autovacuum_count,
				analyze_count,
				autoanalyze_count
			FROM pg_stat_user_tables
			WHERE relname = '%s';`, safeTableName),
			
			// Table bloat estimation
			fmt.Sprintf(`SELECT 
				current_database() AS db, schemaname, tblname, 
				bs*tblpages AS real_size,
				(tblpages-est_tblpages)*bs AS extra_size,
				CASE WHEN tblpages > 0
					THEN 100 * (tblpages-est_tblpages)/tblpages::float
					ELSE 0
				END AS extra_ratio, fillfactor,
				CASE WHEN tblpages > 0 AND tblpages-est_tblpages > 0
					THEN (bs*(tblpages-est_tblpages)/(tblpages)::float)
					ELSE 0
				END AS bloat_size,
				CASE WHEN tblpages > 0 AND tblpages-est_tblpages > 0
					THEN pg_size_pretty((bs*(tblpages-est_tblpages))::bigint)
					ELSE ''
				END AS bloat_size_pretty,
				is_na
			FROM (
				SELECT
					ceil(reltuples/((bs-page_hdr)/tpl_size)) + ceil(toasttuples/4) AS est_tblpages,
					tblpages, fillfactor, bs, tblid, schemaname, tblname, heappages, toastpages, is_na
				FROM (
					SELECT
						( 4 + tpl_hdr_size + tpl_data_size + (2*ma)
							- CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END
							- CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END
						) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages,
						toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, fillfactor, is_na
					FROM (
						SELECT
							tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,
							tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,
							coalesce(toast.reltuples, 0) AS toasttuples,
							coalesce(substring(
								array_to_string(tbl.reloptions, ' ')
								FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
							current_setting('block_size')::numeric AS bs,
							CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
							24 AS page_hdr,
							23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END
								+ CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
							sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size,
							bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na
						FROM pg_attribute AS att
							JOIN pg_class AS tbl ON att.attrelid = tbl.oid
							JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
							LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
								AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
							LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid
						WHERE NOT att.attisdropped
							AND tbl.relkind = 'r'
							AND ns.nspname = 'public'
							AND tbl.relname = '%s'
						GROUP BY 1,2,3,4,5,6,7,8,9,10
					) AS s
				) AS s2
			) AS s3;`, safeTableName),
		}
		
		queries = append(queries, detailedQueries...)
	}

	return queries
}

// getMySQLTableStatsQueries returns queries for MySQL table statistics
func getMySQLTableStatsQueries(tableName string, detailed bool) []string {
	// Escape table name for safety
	safeTableName := strings.Replace(tableName, "`", "``", -1)

	// Basic queries
	queries := []string{
		// Table size and row count
		fmt.Sprintf(`SELECT 
			table_name,
			engine,
			table_rows,
			avg_row_length,
			ROUND(data_length / 1024 / 1024, 2) AS data_size_mb,
			ROUND(index_length / 1024 / 1024, 2) AS index_size_mb,
			ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_size_mb
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		AND table_name = '%s';`, safeTableName),
		
		// Column information
		fmt.Sprintf(`SELECT 
			column_name,
			column_type,
			is_nullable,
			column_key,
			column_default,
			extra
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND table_name = '%s'
		ORDER BY ordinal_position;`, safeTableName),
		
		// Index information
		fmt.Sprintf(`SELECT 
			index_name,
			column_name,
			seq_in_index,
			non_unique,
			CASE 
				WHEN index_type = 'FULLTEXT' THEN 'FULLTEXT'
				WHEN index_name = 'PRIMARY' THEN 'PRIMARY'
				WHEN non_unique = 0 THEN 'UNIQUE'
				ELSE 'INDEX'
			END AS index_type
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		AND table_name = '%s'
		ORDER BY index_name, seq_in_index;`, safeTableName),
	}

	// Add detailed queries if requested
	if detailed {
		detailedQueries := []string{
			// Table I/O statistics
			fmt.Sprintf(`SHOW TABLE STATUS LIKE '%s';`, safeTableName),
			
			// Index usage statistics
			fmt.Sprintf(`SELECT 
				index_name,
				stat_name,
				stat_value
			FROM mysql.index_stats
			WHERE table_name = '%s'
			ORDER BY index_name, stat_name;`, safeTableName),
			
			// Table I/O statistics
			fmt.Sprintf(`SELECT 
				table_schema,
				table_name,
				rows_read,
				rows_inserted,
				rows_updated,
				rows_deleted
			FROM information_schema.table_statistics
			WHERE table_schema = DATABASE()
			AND table_name = '%s';`, safeTableName),
		}
		
		queries = append(queries, detailedQueries...)
	}

	return queries
}
