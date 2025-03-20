package dbtools

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/db"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// createSchemaExplorerTool creates a tool for exploring database schema
func createSchemaExplorerTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbSchema",
		Description: "Auto-discover database structure and relationships",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"component": map[string]interface{}{
					"type":        "string",
					"description": "Schema component to explore (tables, columns, relationships, or full)",
					"enum":        []string{"tables", "columns", "relationships", "full"},
				},
				"table": map[string]interface{}{
					"type":        "string",
					"description": "Table name (required when component is 'columns' and optional for 'relationships')",
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Query timeout in milliseconds (default: 10000)",
				},
			},
			Required: []string{"component"},
		},
		Handler: handleSchemaExplorer,
	}
}

// handleSchemaExplorer handles the schema explorer tool execution
func handleSchemaExplorer(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract parameters
	component, ok := getStringParam(params, "component")
	if !ok {
		return nil, fmt.Errorf("component parameter is required")
	}

	// Extract table parameter (optional depending on component)
	table, _ := getStringParam(params, "table")

	// Extract timeout
	timeout := 10000 // Default timeout: 10 seconds
	if timeoutParam, ok := getIntParam(params, "timeout"); ok {
		timeout = timeoutParam
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Force use of actual database and don't fall back to mock data
	log.Printf("dbSchema: Using component=%s, table=%s", component, table)
	log.Printf("dbSchema: DB instance nil? %v", dbInstance == nil)

	// Print database configuration
	if dbConfig != nil {
		log.Printf("dbSchema: DB Config - Type: %s, Host: %s, Port: %d, User: %s, Name: %s",
			dbConfig.Type, dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Name)
	} else {
		log.Printf("dbSchema: DB Config is nil")
	}

	if dbInstance == nil {
		log.Printf("dbSchema: Database connection not initialized, attempting to create one")
		// Try to initialize database if not already done
		if dbConfig == nil {
			return nil, fmt.Errorf("database not initialized: both dbInstance and dbConfig are nil")
		}

		// Connect to the database
		database, err := db.NewDatabase(*dbConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create database instance: %w", err)
		}

		if err := database.Connect(); err != nil {
			return nil, fmt.Errorf("failed to connect to database: %w", err)
		}

		dbInstance = database
		log.Printf("dbSchema: Connected to %s database at %s:%d/%s",
			dbConfig.Type, dbConfig.Host, dbConfig.Port, dbConfig.Name)
	}

	// Use actual database queries based on component type
	switch component {
	case "tables":
		return getTables(timeoutCtx)
	case "columns":
		if table == "" {
			return nil, fmt.Errorf("table parameter is required for columns component")
		}
		return getColumns(timeoutCtx, table)
	case "relationships":
		return getRelationships(timeoutCtx, table)
	case "full":
		return getFullSchema(timeoutCtx)
	default:
		return nil, fmt.Errorf("invalid component: %s", component)
	}
}

// getTables returns the list of tables from the actual database
func getTables(ctx context.Context) (interface{}, error) {
	var query string
	var args []interface{}

	log.Printf("dbSchema getTables: Database type: %s", dbConfig.Type)

	// Query depends on database type
	switch dbConfig.Type {
	case string(MySQL):
		query = `
			SELECT 
				TABLE_NAME as name,
				TABLE_TYPE as type,
				ENGINE as engine,
				TABLE_ROWS as estimated_row_count,
				CREATE_TIME as create_time,
				UPDATE_TIME as update_time
			FROM 
				information_schema.TABLES 
			WHERE 
				TABLE_SCHEMA = ?
			ORDER BY 
				TABLE_NAME
		`
		args = []interface{}{dbConfig.Name}
		log.Printf("dbSchema getTables: Using MySQL query with schema: %s", dbConfig.Name)

	case string(Postgres):
		query = `
			SELECT 
				table_name as name,
				table_type as type,
				'PostgreSQL' as engine,
				0 as estimated_row_count,
				NULL as create_time,
				NULL as update_time
			FROM 
				information_schema.tables 
			WHERE 
				table_schema = 'public'
			ORDER BY 
				table_name
		`
		log.Printf("dbSchema getTables: Using PostgreSQL query")

	default:
		// Fallback to a simple SHOW TABLES query
		log.Printf("dbSchema getTables: Using fallback SHOW TABLES query for unknown DB type: %s", dbConfig.Type)
		query = "SHOW TABLES"

		// Get the results
		rows, err := dbInstance.Query(ctx, query)
		if err != nil {
			log.Printf("dbSchema getTables: SHOW TABLES query failed: %v", err)
			return nil, fmt.Errorf("failed to query tables: %w", err)
		}
		defer rows.Close()

		// Convert to a list of tables
		var tables []map[string]interface{}
		var tableName string

		for rows.Next() {
			if err := rows.Scan(&tableName); err != nil {
				log.Printf("dbSchema getTables: Failed to scan row: %v", err)
				continue
			}

			tables = append(tables, map[string]interface{}{
				"name": tableName,
				"type": "BASE TABLE", // Default type
			})
		}

		if err := rows.Err(); err != nil {
			log.Printf("dbSchema getTables: Error during rows iteration: %v", err)
			return nil, fmt.Errorf("error iterating through tables: %w", err)
		}

		log.Printf("dbSchema getTables: Found %d tables using SHOW TABLES", len(tables))
		return map[string]interface{}{
			"tables": tables,
			"count":  len(tables),
			"type":   dbConfig.Type,
		}, nil
	}

	// Execute query
	log.Printf("dbSchema getTables: Executing query: %s with args: %v", query, args)
	rows, err := dbInstance.Query(ctx, query, args...)
	if err != nil {
		log.Printf("dbSchema getTables: Query failed: %v", err)
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	// Convert rows to map
	tables, err := rowsToMaps(rows)
	if err != nil {
		log.Printf("dbSchema getTables: Failed to process rows: %v", err)
		return nil, fmt.Errorf("failed to process query results: %w", err)
	}

	log.Printf("dbSchema getTables: Found %d tables", len(tables))
	return map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
		"type":   dbConfig.Type,
	}, nil
}

// getColumns returns the columns for a specific table from the actual database
func getColumns(ctx context.Context, table string) (interface{}, error) {
	var query string

	// Query depends on database type
	switch dbConfig.Type {
	case string(MySQL):
		query = `
			SELECT 
				COLUMN_NAME as name,
				COLUMN_TYPE as type,
				IS_NULLABLE as nullable,
				COLUMN_KEY as ` + "`key`" + `,
				EXTRA as extra,
				COLUMN_DEFAULT as default_value,
				CHARACTER_MAXIMUM_LENGTH as max_length,
				NUMERIC_PRECISION as numeric_precision,
				NUMERIC_SCALE as numeric_scale,
				COLUMN_COMMENT as comment
			FROM 
				information_schema.COLUMNS
			WHERE 
				TABLE_SCHEMA = ? AND TABLE_NAME = ?
			ORDER BY 
				ORDINAL_POSITION
		`
	case string(Postgres):
		query = `
			SELECT 
				column_name as name,
				data_type as type,
				is_nullable as nullable,
				CASE 
					WHEN EXISTS (
						SELECT 1 FROM information_schema.table_constraints tc
						JOIN information_schema.constraint_column_usage ccu
						ON tc.constraint_name = ccu.constraint_name
						WHERE tc.constraint_type = 'PRIMARY KEY'
						AND tc.table_name = c.table_name
						AND ccu.column_name = c.column_name
					) THEN 'PRI'
					WHEN EXISTS (
						SELECT 1 FROM information_schema.table_constraints tc
						JOIN information_schema.constraint_column_usage ccu
						ON tc.constraint_name = ccu.constraint_name
						WHERE tc.constraint_type = 'UNIQUE'
						AND tc.table_name = c.table_name
						AND ccu.column_name = c.column_name
					) THEN 'UNI'
					WHEN EXISTS (
						SELECT 1 FROM information_schema.table_constraints tc
						JOIN information_schema.constraint_column_usage ccu
						ON tc.constraint_name = ccu.constraint_name
						WHERE tc.constraint_type = 'FOREIGN KEY'
						AND tc.table_name = c.table_name
						AND ccu.column_name = c.column_name
					) THEN 'MUL'
					ELSE ''
				END as "key",
				'' as extra,
				column_default as default_value,
				character_maximum_length as max_length,
				numeric_precision as numeric_precision,
				numeric_scale as numeric_scale,
				'' as comment
			FROM 
				information_schema.columns c
			WHERE 
				table_schema = 'public' AND table_name = ?
			ORDER BY 
				ordinal_position
		`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.Type)
	}

	var args []interface{}
	if dbConfig.Type == string(MySQL) {
		args = []interface{}{dbConfig.Name, table}
	} else {
		args = []interface{}{table}
	}

	// Execute query
	rows, err := dbInstance.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for table %s: %w", table, err)
	}
	defer rows.Close()

	// Convert rows to map
	columns, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process query results: %w", err)
	}

	return map[string]interface{}{
		"table":   table,
		"columns": columns,
		"count":   len(columns),
		"type":    dbConfig.Type,
	}, nil
}

// getRelationships returns the foreign key relationships from the actual database
func getRelationships(ctx context.Context, table string) (interface{}, error) {
	var query string
	var args []interface{}

	// Query depends on database type
	switch dbConfig.Type {
	case string(MySQL):
		query = `
			SELECT 
				kcu.CONSTRAINT_NAME as constraint_name,
				kcu.TABLE_NAME as table_name,
				kcu.COLUMN_NAME as column_name,
				kcu.REFERENCED_TABLE_NAME as referenced_table,
				kcu.REFERENCED_COLUMN_NAME as referenced_column,
				rc.UPDATE_RULE as update_rule,
				rc.DELETE_RULE as delete_rule
			FROM 
				information_schema.KEY_COLUMN_USAGE kcu
			JOIN 
				information_schema.REFERENTIAL_CONSTRAINTS rc
				ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
				AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
			WHERE 
				kcu.TABLE_SCHEMA = ?
				AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		`

		args = []interface{}{dbConfig.Name}
		// If table is specified, add it to WHERE clause
		if table != "" {
			query += " AND (kcu.TABLE_NAME = ? OR kcu.REFERENCED_TABLE_NAME = ?)"
			args = append(args, table, table)
		}

	case string(Postgres):
		query = `
			SELECT
				tc.constraint_name,
				tc.table_name,
				kcu.column_name,
				ccu.table_name AS referenced_table,
				ccu.column_name AS referenced_column,
				'CASCADE' as update_rule, -- Postgres doesn't expose this in info schema
				'CASCADE' as delete_rule  -- Postgres doesn't expose this in info schema
			FROM 
				information_schema.table_constraints AS tc
			JOIN 
				information_schema.key_column_usage AS kcu
				ON tc.constraint_name = kcu.constraint_name
			JOIN 
				information_schema.constraint_column_usage AS ccu
				ON ccu.constraint_name = tc.constraint_name
			WHERE 
				tc.constraint_type = 'FOREIGN KEY'
				AND tc.table_schema = 'public'
		`

		// If table is specified, add it to WHERE clause
		if table != "" {
			query += " AND (tc.table_name = ? OR ccu.table_name = ?)"
			args = append(args, table, table)
		}

	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbConfig.Type)
	}

	// Execute query
	rows, err := dbInstance.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query relationships: %w", err)
	}
	defer rows.Close()

	// Convert rows to map
	relationships, err := rowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to process query results: %w", err)
	}

	return map[string]interface{}{
		"relationships": relationships,
		"count":         len(relationships),
		"type":          dbConfig.Type,
		"table":         table, // If specified
	}, nil
}

// getFullSchema returns complete schema information
func getFullSchema(ctx context.Context) (interface{}, error) {
	// Get tables
	tablesResult, err := getTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	// Get relationships
	relationshipsResult, err := getRelationships(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to get relationships: %w", err)
	}

	// Extract tables
	tables, ok := tablesResult.(map[string]interface{})["tables"].([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid table result format")
	}

	// For each table, get its columns
	var tablesWithColumns []map[string]interface{}
	for _, table := range tables {
		tableName, ok := table["name"].(string)
		if !ok {
			continue
		}

		columnsResult, err := getColumns(ctx, tableName)
		if err != nil {
			// Log error but continue
			log.Printf("Error getting columns for table %s: %v", tableName, err)
			table["columns"] = []map[string]interface{}{}
		} else {
			columns, ok := columnsResult.(map[string]interface{})["columns"].([]map[string]interface{})
			if ok {
				table["columns"] = columns
			} else {
				table["columns"] = []map[string]interface{}{}
			}
		}

		tablesWithColumns = append(tablesWithColumns, table)
	}

	return map[string]interface{}{
		"tables":        tablesWithColumns,
		"relationships": relationshipsResult.(map[string]interface{})["relationships"],
		"type":          dbConfig.Type,
	}, nil
}

// getMockTables returns mock table data
//
//nolint:unused // Mock function for testing/development
func getMockTables() (interface{}, error) {
	tables := []map[string]interface{}{
		{
			"name":                "users",
			"type":                "BASE TABLE",
			"engine":              "InnoDB",
			"estimated_row_count": 1500,
			"create_time":         time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339),
			"update_time":         time.Now().Add(-2 * 24 * time.Hour).Format(time.RFC3339),
		},
		{
			"name":                "orders",
			"type":                "BASE TABLE",
			"engine":              "InnoDB",
			"estimated_row_count": 8750,
			"create_time":         time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339),
			"update_time":         time.Now().Add(-1 * 24 * time.Hour).Format(time.RFC3339),
		},
		{
			"name":                "products",
			"type":                "BASE TABLE",
			"engine":              "InnoDB",
			"estimated_row_count": 350,
			"create_time":         time.Now().Add(-30 * 24 * time.Hour).Format(time.RFC3339),
			"update_time":         time.Now().Add(-5 * 24 * time.Hour).Format(time.RFC3339),
		},
	}

	return map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
		"type":   "mysql",
	}, nil
}

// getMockColumns returns mock column data for a given table
//
//nolint:unused // Mock function for testing/development
func getMockColumns(table string) (interface{}, error) {
	var columns []map[string]interface{}

	switch table {
	case "users":
		columns = []map[string]interface{}{
			{
				"name":              "id",
				"type":              "int(11)",
				"nullable":          "NO",
				"key":               "PRI",
				"extra":             "auto_increment",
				"default":           nil,
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     0,
				"comment":           "User unique identifier",
			},
			{
				"name":              "email",
				"type":              "varchar(255)",
				"nullable":          "NO",
				"key":               "UNI",
				"extra":             "",
				"default":           nil,
				"max_length":        255,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "User email address",
			},
			{
				"name":              "name",
				"type":              "varchar(100)",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           nil,
				"max_length":        100,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "User full name",
			},
			{
				"name":              "created_at",
				"type":              "timestamp",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "CURRENT_TIMESTAMP",
				"max_length":        nil,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "Creation timestamp",
			},
		}
	case "orders":
		columns = []map[string]interface{}{
			{
				"name":              "id",
				"type":              "int(11)",
				"nullable":          "NO",
				"key":               "PRI",
				"extra":             "auto_increment",
				"default":           nil,
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     0,
				"comment":           "Order ID",
			},
			{
				"name":              "user_id",
				"type":              "int(11)",
				"nullable":          "NO",
				"key":               "MUL",
				"extra":             "",
				"default":           nil,
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     0,
				"comment":           "User who placed the order",
			},
			{
				"name":              "total_amount",
				"type":              "decimal(10,2)",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "0.00",
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     2,
				"comment":           "Total order amount",
			},
			{
				"name":              "status",
				"type":              "enum('pending','processing','shipped','delivered')",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "pending",
				"max_length":        nil,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "Order status",
			},
			{
				"name":              "created_at",
				"type":              "timestamp",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "CURRENT_TIMESTAMP",
				"max_length":        nil,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "Order creation time",
			},
		}
	case "products":
		columns = []map[string]interface{}{
			{
				"name":              "id",
				"type":              "int(11)",
				"nullable":          "NO",
				"key":               "PRI",
				"extra":             "auto_increment",
				"default":           nil,
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     0,
				"comment":           "Product ID",
			},
			{
				"name":              "name",
				"type":              "varchar(255)",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           nil,
				"max_length":        255,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "Product name",
			},
			{
				"name":              "price",
				"type":              "decimal(10,2)",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "0.00",
				"max_length":        nil,
				"numeric_precision": 10,
				"numeric_scale":     2,
				"comment":           "Product price",
			},
			{
				"name":              "created_at",
				"type":              "timestamp",
				"nullable":          "NO",
				"key":               "",
				"extra":             "",
				"default":           "CURRENT_TIMESTAMP",
				"max_length":        nil,
				"numeric_precision": nil,
				"numeric_scale":     nil,
				"comment":           "Product creation time",
			},
		}
	default:
		return nil, fmt.Errorf("table %s not found", table)
	}

	return map[string]interface{}{
		"table":   table,
		"columns": columns,
		"count":   len(columns),
		"type":    "mysql",
	}, nil
}

// getMockRelationships returns mock relationship data for a given table
//
//nolint:unused // Mock function for testing/development
func getMockRelationships(table string) (interface{}, error) {
	relationships := []map[string]interface{}{
		{
			"constraint_name":        "fk_orders_users",
			"table_name":             "orders",
			"column_name":            "user_id",
			"referenced_table_name":  "users",
			"referenced_column_name": "id",
			"update_rule":            "CASCADE",
			"delete_rule":            "RESTRICT",
		},
		{
			"constraint_name":        "fk_order_items_orders",
			"table_name":             "order_items",
			"column_name":            "order_id",
			"referenced_table_name":  "orders",
			"referenced_column_name": "id",
			"update_rule":            "CASCADE",
			"delete_rule":            "CASCADE",
		},
		{
			"constraint_name":        "fk_order_items_products",
			"table_name":             "order_items",
			"column_name":            "product_id",
			"referenced_table_name":  "products",
			"referenced_column_name": "id",
			"update_rule":            "CASCADE",
			"delete_rule":            "RESTRICT",
		},
	}

	// Filter by table if provided
	if table != "" {
		filteredRelationships := make([]map[string]interface{}, 0)
		for _, r := range relationships {
			if r["table_name"] == table || r["referenced_table_name"] == table {
				filteredRelationships = append(filteredRelationships, r)
			}
		}
		relationships = filteredRelationships
	}

	return map[string]interface{}{
		"relationships": relationships,
		"count":         len(relationships),
		"type":          "mysql",
		"table":         table,
	}, nil
}

// getMockFullSchema returns a mock complete database schema
//
//nolint:unused // Mock function for testing/development
func getMockFullSchema() (interface{}, error) {
	tablesResult, _ := getMockTables()
	relationshipsResult, _ := getMockRelationships("")

	tables := tablesResult.(map[string]interface{})["tables"].([]map[string]interface{})
	tableDetails := make(map[string]interface{})

	for _, tableInfo := range tables {
		tableName := tableInfo["name"].(string)
		columnsResult, _ := getMockColumns(tableName)
		tableDetails[tableName] = columnsResult.(map[string]interface{})["columns"]
	}

	return map[string]interface{}{
		"tables":        tablesResult.(map[string]interface{})["tables"],
		"relationships": relationshipsResult.(map[string]interface{})["relationships"],
		"tableDetails":  tableDetails,
		"type":          "mysql",
	}, nil
}
