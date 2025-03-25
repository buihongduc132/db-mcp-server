package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/dbtools"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ToolRegistry manages the creation and registration of database tools
type ToolRegistry struct {
	server *server.MCPServer
}

// NewToolRegistry creates a new tool registry
func NewToolRegistry(mcpServer *server.MCPServer) *ToolRegistry {
	return &ToolRegistry{
		server: mcpServer,
	}
}

// RegisterAllTools registers all available database tools
func (tr *ToolRegistry) RegisterAllTools() {
	// Get available database connections
	dbIDs := dbtools.ListDatabases()

	// If no database connections are available, register mock tools
	if len(dbIDs) == 0 {
		log.Printf("No active database connections. Registering mock database tools.")
		tr.registerMockTools()
		return
	}

	// Register tools for each database
	for _, dbID := range dbIDs {
		tr.registerDatabaseTools(dbID)
	}

	// Register common tools (not specific to a database)
	tr.registerCommonTools()
}

// registerDatabaseTools registers all tools for a specific database
func (tr *ToolRegistry) registerDatabaseTools(dbID string) {
	// Register query tool
	tr.registerQueryTool(dbID)

	// Register execute tool
	tr.registerExecuteTool(dbID)

	// Register transaction tool
	tr.registerTransactionTool(dbID)

	// Register performance analyzer tool
	tr.registerPerformanceTool(dbID)

	// Register schema tool
	tr.registerSchemaTool(dbID)
}

// registerQueryTool registers a tool for executing SQL queries
func (tr *ToolRegistry) registerQueryTool(dbID string) {
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("query_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Execute SQL query on %s database", dbID)),
			mcp.WithString("query",
				mcp.Description("SQL query to execute"),
				mcp.Required(),
			),
			mcp.WithArray("params",
				mcp.Description("Query parameters"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			query, ok := request.Params.Arguments["query"].(string)
			if !ok {
				return nil, fmt.Errorf("query parameter must be a string")
			}

			var queryParams []interface{}
			if request.Params.Arguments["params"] != nil {
				if paramsArr, ok := request.Params.Arguments["params"].([]interface{}); ok {
					queryParams = paramsArr
				}
			}

			// Get database and execute query
			db, err := dbtools.GetDatabase(dbID)
			if err != nil {
				return nil, fmt.Errorf("failed to get database: %v", err)
			}

			// Execute query
			rows, err := db.Query(ctx, query, queryParams...)
			if err != nil {
				return nil, fmt.Errorf("query execution failed: %v", err)
			}
			defer rows.Close()

			// Process results into a readable format
			columns, err := rows.Columns()
			if err != nil {
				return nil, fmt.Errorf("failed to get column names: %v", err)
			}

			// Format results as text
			var resultText strings.Builder
			resultText.WriteString("Results:\n\n")
			resultText.WriteString(strings.Join(columns, "\t") + "\n")
			resultText.WriteString(strings.Repeat("-", 80) + "\n")

			// Prepare for scanning
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			// Process rows
			rowCount := 0
			for rows.Next() {
				rowCount++
				if err := rows.Scan(valuePtrs...); err != nil {
					return nil, fmt.Errorf("failed to scan row: %v", err)
				}

				// Convert to strings and print
				var rowText []string
				for i := range columns {
					val := values[i]
					if val == nil {
						rowText = append(rowText, "NULL")
					} else {
						switch v := val.(type) {
						case []byte:
							rowText = append(rowText, string(v))
						default:
							rowText = append(rowText, fmt.Sprintf("%v", v))
						}
					}
				}
				resultText.WriteString(strings.Join(rowText, "\t") + "\n")
			}

			if err = rows.Err(); err != nil {
				return nil, fmt.Errorf("error reading rows: %v", err)
			}

			resultText.WriteString(fmt.Sprintf("\nTotal rows: %d", rowCount))
			return mcp.NewToolResultText(resultText.String()), nil
		},
	)
}

// registerExecuteTool registers a tool for executing data modification statements
func (tr *ToolRegistry) registerExecuteTool(dbID string) {
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("execute_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Execute SQL statement on %s database", dbID)),
			mcp.WithString("statement",
				mcp.Description("SQL statement to execute (INSERT, UPDATE, DELETE, etc.)"),
				mcp.Required(),
			),
			mcp.WithArray("params",
				mcp.Description("Statement parameters"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			statement, ok := request.Params.Arguments["statement"].(string)
			if !ok {
				return nil, fmt.Errorf("statement parameter must be a string")
			}

			var statementParams []interface{}
			if request.Params.Arguments["params"] != nil {
				if paramsArr, ok := request.Params.Arguments["params"].([]interface{}); ok {
					statementParams = paramsArr
				}
			}

			// Get database
			db, err := dbtools.GetDatabase(dbID)
			if err != nil {
				return nil, fmt.Errorf("failed to get database: %v", err)
			}

			// Execute statement
			result, err := db.Exec(ctx, statement, statementParams...)
			if err != nil {
				return nil, fmt.Errorf("statement execution failed: %v", err)
			}

			// Get rows affected
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				rowsAffected = 0
			}

			// Get last insert ID (if applicable)
			lastInsertID, err := result.LastInsertId()
			if err != nil {
				lastInsertID = 0
			}

			return mcp.NewToolResultText(fmt.Sprintf("Statement executed successfully.\nRows affected: %d\nLast insert ID: %d", rowsAffected, lastInsertID)), nil
		},
	)
}

// registerTransactionTool registers a tool for managing database transactions
func (tr *ToolRegistry) registerTransactionTool(dbID string) {
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("transaction_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Manage transactions on %s database", dbID)),
			mcp.WithString("action",
				mcp.Description("Transaction action (begin, commit, rollback, execute)"),
				mcp.Required(),
			),
			mcp.WithString("transactionId",
				mcp.Description("Transaction ID (required for commit, rollback, execute)"),
			),
			mcp.WithString("statement",
				mcp.Description("SQL statement to execute within transaction (required for execute)"),
			),
			mcp.WithArray("params",
				mcp.Description("Statement parameters"),
			),
			mcp.WithBoolean("readOnly",
				mcp.Description("Whether the transaction is read-only (for begin)"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			action, ok := request.Params.Arguments["action"].(string)
			if !ok {
				return nil, fmt.Errorf("action parameter must be a string")
			}

			// Get database
			db, err := dbtools.GetDatabase(dbID)
			if err != nil {
				return nil, fmt.Errorf("failed to get database: %v", err)
			}

			// Handle based on action
			switch action {
			case "begin":
				// Extract read-only flag
				readOnly := false
				if readOnlyVal, ok := request.Params.Arguments["readOnly"].(bool); ok {
					readOnly = readOnlyVal
				}

				// Begin transaction
				tx, err := db.BeginTx(ctx, &sql.TxOptions{
					ReadOnly: readOnly,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to begin transaction: %v", err)
				}

				// Generate transaction ID and store it
				txID := fmt.Sprintf("tx-%s-%d", dbID, time.Now().UnixNano())

				// Store in a transaction map
				dbtools.StoreTransaction(txID, tx)

				return mcp.NewToolResultText(fmt.Sprintf("Transaction started. ID: %s", txID)), nil

			case "commit":
				// Extract transaction ID
				txID, ok := request.Params.Arguments["transactionId"].(string)
				if !ok || txID == "" {
					return nil, fmt.Errorf("transactionId parameter is required for commit")
				}

				// Get transaction
				tx, found := dbtools.GetTransaction(txID)
				if !found {
					return nil, fmt.Errorf("transaction not found: %s", txID)
				}

				// Commit
				if err := tx.Commit(); err != nil {
					return nil, fmt.Errorf("failed to commit transaction: %v", err)
				}

				// Remove from storage
				dbtools.RemoveTransaction(txID)

				return mcp.NewToolResultText(fmt.Sprintf("Transaction %s committed successfully", txID)), nil

			case "rollback":
				// Extract transaction ID
				txID, ok := request.Params.Arguments["transactionId"].(string)
				if !ok || txID == "" {
					return nil, fmt.Errorf("transactionId parameter is required for rollback")
				}

				// Get transaction
				tx, found := dbtools.GetTransaction(txID)
				if !found {
					return nil, fmt.Errorf("transaction not found: %s", txID)
				}

				// Rollback
				if err := tx.Rollback(); err != nil {
					return nil, fmt.Errorf("failed to rollback transaction: %v", err)
				}

				// Remove from storage
				dbtools.RemoveTransaction(txID)

				return mcp.NewToolResultText(fmt.Sprintf("Transaction %s rolled back successfully", txID)), nil

			case "execute":
				// Extract transaction ID
				txID, ok := request.Params.Arguments["transactionId"].(string)
				if !ok || txID == "" {
					return nil, fmt.Errorf("transactionId parameter is required for execute")
				}

				// Extract statement
				statement, ok := request.Params.Arguments["statement"].(string)
				if !ok || statement == "" {
					return nil, fmt.Errorf("statement parameter is required for execute")
				}

				// Get transaction
				tx, found := dbtools.GetTransaction(txID)
				if !found {
					return nil, fmt.Errorf("transaction not found: %s", txID)
				}

				// Extract parameters
				var statementParams []interface{}
				if request.Params.Arguments["params"] != nil {
					if paramsArr, ok := request.Params.Arguments["params"].([]interface{}); ok {
						statementParams = paramsArr
					}
				}

				// Check if this is a query or execute statement
				if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "SELECT") {
					// Execute query
					rows, err := tx.QueryContext(ctx, statement, statementParams...)
					if err != nil {
						return nil, fmt.Errorf("failed to execute query in transaction: %v", err)
					}
					defer rows.Close()

					// Process results
					columns, err := rows.Columns()
					if err != nil {
						return nil, fmt.Errorf("failed to get column names: %v", err)
					}

					// Format results as text
					var resultText strings.Builder
					resultText.WriteString(fmt.Sprintf("Query executed in transaction %s\n\n", txID))
					resultText.WriteString(strings.Join(columns, "\t") + "\n")
					resultText.WriteString(strings.Repeat("-", 80) + "\n")

					// Prepare for scanning
					values := make([]interface{}, len(columns))
					valuePtrs := make([]interface{}, len(columns))
					for i := range columns {
						valuePtrs[i] = &values[i]
					}

					// Process rows
					rowCount := 0
					for rows.Next() {
						rowCount++
						if err := rows.Scan(valuePtrs...); err != nil {
							return nil, fmt.Errorf("failed to scan row: %v", err)
						}

						// Convert to strings and print
						var rowText []string
						for i := range columns {
							val := values[i]
							if val == nil {
								rowText = append(rowText, "NULL")
							} else {
								switch v := val.(type) {
								case []byte:
									rowText = append(rowText, string(v))
								default:
									rowText = append(rowText, fmt.Sprintf("%v", v))
								}
							}
						}
						resultText.WriteString(strings.Join(rowText, "\t") + "\n")
					}

					if err = rows.Err(); err != nil {
						return nil, fmt.Errorf("error reading rows: %v", err)
					}

					resultText.WriteString(fmt.Sprintf("\nTotal rows: %d", rowCount))
					return mcp.NewToolResultText(resultText.String()), nil
				} else {
					// Execute statement
					result, err := tx.ExecContext(ctx, statement, statementParams...)
					if err != nil {
						return nil, fmt.Errorf("failed to execute statement in transaction: %v", err)
					}

					// Get rows affected
					rowsAffected, err := result.RowsAffected()
					if err != nil {
						rowsAffected = 0
					}

					return mcp.NewToolResultText(fmt.Sprintf("Statement executed in transaction %s\nRows affected: %d", txID, rowsAffected)), nil
				}
			default:
				return nil, fmt.Errorf("invalid action: %s", action)
			}
		},
	)
}

// registerPerformanceTool registers a tool for analyzing database performance
func (tr *ToolRegistry) registerPerformanceTool(dbID string) {
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("performance_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Analyze query performance on %s database", dbID)),
			mcp.WithString("action",
				mcp.Description("Action (getSlowQueries, getMetrics, analyzeQuery, reset, setThreshold)"),
				mcp.Required(),
			),
			mcp.WithString("query",
				mcp.Description("SQL query to analyze (required for analyzeQuery)"),
			),
			mcp.WithNumber("threshold",
				mcp.Description("Slow query threshold in milliseconds (required for setThreshold)"),
			),
			mcp.WithNumber("limit",
				mcp.Description("Maximum number of results to return"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			action, ok := request.Params.Arguments["action"].(string)
			if !ok {
				return nil, fmt.Errorf("action parameter must be a string")
			}

			// Get the performance analyzer
			analyzer := dbtools.GetPerformanceAnalyzer()

			// Handle different actions
			switch action {
			case "getSlowQueries":
				// Get slow queries
				metrics := analyzer.GetAllMetrics()

				// Filter to only show slow queries
				var slowQueries []*dbtools.QueryMetrics
				for _, m := range metrics {
					if m.AvgDuration >= analyzer.GetSlowThreshold() {
						slowQueries = append(slowQueries, m)
					}
				}

				// Format results
				var result strings.Builder
				result.WriteString(fmt.Sprintf("Slow queries (threshold: %s):\n\n", analyzer.GetSlowThreshold()))

				if len(slowQueries) == 0 {
					result.WriteString("No slow queries detected")
				} else {
					for i, q := range slowQueries {
						result.WriteString(fmt.Sprintf("%d. Query: %s\n", i+1, q.Query))
						result.WriteString(fmt.Sprintf("   Avg: %s, Min: %s, Max: %s, Count: %d\n\n",
							q.AvgDuration, q.MinDuration, q.MaxDuration, q.Count))
					}
				}

				return mcp.NewToolResultText(result.String()), nil

			case "getMetrics":
				// Get all metrics
				metrics := analyzer.GetAllMetrics()

				// Format results
				var result strings.Builder
				result.WriteString("Query performance metrics:\n\n")

				if len(metrics) == 0 {
					result.WriteString("No query metrics collected")
				} else {
					for i, q := range metrics {
						result.WriteString(fmt.Sprintf("%d. Query: %s\n", i+1, q.Query))
						result.WriteString(fmt.Sprintf("   Avg: %s, Min: %s, Max: %s, Count: %d\n\n",
							q.AvgDuration, q.MinDuration, q.MaxDuration, q.Count))
					}
				}

				return mcp.NewToolResultText(result.String()), nil

			case "analyzeQuery":
				// Extract query
				query, ok := request.Params.Arguments["query"].(string)
				if !ok || query == "" {
					return nil, fmt.Errorf("query parameter is required for analyzeQuery")
				}

				// Analyze query
				suggestions := dbtools.AnalyzeQuery(query)

				// Format results
				var result strings.Builder
				result.WriteString(fmt.Sprintf("Analysis for query: %s\n\n", query))

				if len(suggestions) == 0 {
					result.WriteString("No optimization suggestions found")
				} else {
					for i, s := range suggestions {
						result.WriteString(fmt.Sprintf("%d. %s\n", i+1, s))
					}
				}

				return mcp.NewToolResultText(result.String()), nil

			case "reset":
				// Reset metrics
				analyzer.Reset()
				return mcp.NewToolResultText("Performance metrics have been reset"), nil

			case "setThreshold":
				// Extract threshold
				thresholdValue, ok := request.Params.Arguments["threshold"].(float64)
				if !ok {
					return nil, fmt.Errorf("threshold parameter must be a number")
				}

				// Set threshold
				thresholdMs := int(thresholdValue)
				analyzer.SetSlowThreshold(time.Duration(thresholdMs) * time.Millisecond)

				return mcp.NewToolResultText(fmt.Sprintf("Slow query threshold set to %d ms", thresholdMs)), nil

			default:
				return nil, fmt.Errorf("invalid action: %s", action)
			}
		},
	)
}

// registerSchemaTool registers a tool for exploring database schema
func (tr *ToolRegistry) registerSchemaTool(dbID string) {
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("schema_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Get schema of %s database", dbID)),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Get database
			db, err := dbtools.GetDatabase(dbID)
			if err != nil {
				return nil, fmt.Errorf("failed to get database: %v", err)
			}

			// Query for schema information
			query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"

			// For MySQL, use a different query
			if db.DriverName() == "mysql" {
				query = "SHOW TABLES"
			}

			rows, err := db.Query(ctx, query)
			if err != nil {
				return nil, fmt.Errorf("failed to get schema: %v", err)
			}
			defer rows.Close()

			// Build a list of table names
			var tables []string
			for rows.Next() {
				var tableName string
				if err := rows.Scan(&tableName); err != nil {
					return nil, fmt.Errorf("failed to scan table name: %v", err)
				}
				tables = append(tables, tableName)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("error iterating tables: %v", err)
			}

			// Format the result
			schemaInfo := fmt.Sprintf("Database: %s\nTables: %d\n\n", dbID, len(tables))
			for i, table := range tables {
				schemaInfo += fmt.Sprintf("%d. %s\n", i+1, table)
			}

			return mcp.NewToolResultText(schemaInfo), nil
		},
	)
}

// registerCommonTools registers tools that are not specific to a database
func (tr *ToolRegistry) registerCommonTools() {
	// Register list databases tool
	tr.server.AddTool(
		mcp.NewTool(
			"list_databases",
			mcp.WithDescription("List all available databases"),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(fmt.Sprintf("Available databases: %v", dbtools.ListDatabases())), nil
		},
	)
}

// registerMockTools registers mock database tools when no real databases are available
func (tr *ToolRegistry) registerMockTools() {
	// Register mock query tool
	tr.server.AddTool(
		mcp.NewTool(
			"mock_query",
			mcp.WithDescription("Execute SQL query (mock)"),
			mcp.WithString("query",
				mcp.Description("SQL query to execute"),
				mcp.Required(),
			),
			mcp.WithArray("params",
				mcp.Description("Query parameters"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			query, ok := request.Params.Arguments["query"].(string)
			if !ok {
				return nil, fmt.Errorf("query parameter must be a string")
			}

			// Return mock data
			return mcp.NewToolResultText(fmt.Sprintf("Mock query execution: %s", query)), nil
		},
	)

	// Register mock execute tool
	tr.server.AddTool(
		mcp.NewTool(
			"mock_execute",
			mcp.WithDescription("Execute SQL statement (mock)"),
			mcp.WithString("statement",
				mcp.Description("SQL statement to execute"),
				mcp.Required(),
			),
			mcp.WithArray("params",
				mcp.Description("Statement parameters"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Extract parameters
			statement, ok := request.Params.Arguments["statement"].(string)
			if !ok {
				return nil, fmt.Errorf("statement parameter must be a string")
			}

			// Return mock data
			return mcp.NewToolResultText(fmt.Sprintf("Mock statement execution: %s\nAffected rows: 1", statement)), nil
		},
	)

	// Register mock schema tool
	tr.server.AddTool(
		mcp.NewTool(
			"mock_schema",
			mcp.WithDescription("Get database schema (mock)"),
			mcp.WithString("database",
				mcp.Description("Database name"),
			),
			mcp.WithString("table",
				mcp.Description("Table name"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// Return mock schema data
			return mcp.NewToolResultText("Mock Schema:\n1. users\n2. orders\n3. products"), nil
		},
	)

	// Register list databases tool (will return mock databases)
	tr.server.AddTool(
		mcp.NewTool(
			"list_databases",
			mcp.WithDescription("List all available databases"),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("Available databases: [mock_db] (MOCK)"), nil
		},
	)
}

func main() {
	// Parse command-line flags
	transportMode := flag.String("t", "stdio", "Transport mode (stdio or sse)")
	serverPort := flag.Int("port", 9092, "Server port for SSE transport")
	configFile := flag.String("c", "config.json", "Path to database configuration file")
	flag.Parse()

	// Set default transport mode if not specified
	if *transportMode == "" {
		*transportMode = "sse"
	}

	// Configure logging
	if *transportMode == "stdio" {
		// In STDIO mode, explicitly redirect logs to stderr to avoid mixing with JSON responses
		log.SetOutput(os.Stderr)
	}

	// Initialize database connections if not skipped
	if *transportMode == "stdio" && os.Getenv("SKIP_DB") == "true" {
		log.Printf("Skipping database initialization for STDIO mode with SKIP_DB=true")
	} else {
		// Initialize database connections (configuration is loaded inside InitDatabase)
		dbConfig := &dbtools.Config{
			ConfigFile: *configFile,
		}

		dbInitError := dbtools.InitDatabase(dbConfig)
		if dbInitError != nil {
			log.Printf("Warning: Failed to initialize database connections: %v", dbInitError)
		} else {
			// Verify database connections
			ctx := context.Background()
			dbIDs := dbtools.ListDatabases()
			if len(dbIDs) == 0 {
				log.Printf("Warning: No database connections configured")
			} else {
				for _, dbID := range dbtools.ListDatabases() {
					db, err := dbtools.GetDatabase(dbID)
					if err != nil {
						log.Printf("Warning: Failed to get database %s: %v", dbID, err)
						continue
					}
					if err := db.Ping(ctx); err != nil {
						log.Printf("Warning: Failed to ping database %s: %v", dbID, err)
					} else {
						log.Printf("Successfully connected to database %s", dbID)
					}
				}
			}
		}
	}

	// Configure hooks
	var hooks server.Hooks
	hooks.AddBeforeAny(func(id any, method mcp.MCPMethod, message any) {
		log.Printf("Request: %s, %v", method, id)
	})
	hooks.AddOnError(func(id any, method mcp.MCPMethod, message any, err error) {
		log.Printf("Error: %s, %v, %v", method, id, err)
	})

	// Create mcp-go server with hooks
	mcpServer := server.NewMCPServer(
		"DB MCP Server", // Server name
		"1.0.0",         // Server version
		server.WithHooks(&hooks),
	)

	// Register database tools using the tool registry
	toolRegistry := NewToolRegistry(mcpServer)
	toolRegistry.RegisterAllTools()

	// Handle transport mode
	switch *transportMode {
	case "sse":
		log.Printf("Starting SSE server on port %d", *serverPort)

		// Create SSE server
		sseServer := server.NewSSEServer(
			mcpServer,
			server.WithBaseURL(fmt.Sprintf("http://localhost:%d", *serverPort)),
		)

		// Start the server with graceful shutdown
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

		errCh := make(chan error, 1)
		go func() {
			errCh <- sseServer.Start(fmt.Sprintf(":%d", *serverPort))
		}()

		// Wait for interrupt or error
		select {
		case err := <-errCh:
			log.Fatalf("Server error: %v", err)
		case <-stop:
			log.Println("Shutting down server...")
			// No explicit shutdown needed, just exit
		}

	case "stdio":
		log.Printf("Starting STDIO server")
		// No graceful shutdown needed for stdio
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("STDIO server error: %v", err)
		}

	default:
		log.Fatalf("Invalid transport mode: %s", *transportMode)
	}
}
