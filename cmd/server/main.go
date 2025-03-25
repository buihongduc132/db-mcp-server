package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/FreePeak/db-mcp-server/pkg/dbtools"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	// Parse command line flags
	transportMode := flag.String("t", "", "Transport mode (sse or stdio)")
	serverPort := flag.Int("p", 8080, "Server port")
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

	// Setup hooks for logging
	hooks := server.Hooks{}
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

	// Register database tools
	registerDatabaseTools(mcpServer)

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

func registerDatabaseTools(mcpServer *server.MCPServer) {
	// Get available database connections
	dbIDs := dbtools.ListDatabases()

	// If no database connections are available, register mock tools
	if len(dbIDs) == 0 {
		log.Printf("No active database connections. Registering mock database tools.")
		// Register mock database tools
		mcpServer.AddTool(
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

		mcpServer.AddTool(
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

		mcpServer.AddTool(
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
		mcpServer.AddTool(
			mcp.NewTool(
				"list_databases",
				mcp.WithDescription("List all available databases"),
			),
			func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return mcp.NewToolResultText("Available databases: [mock_db] (MOCK)"), nil
			},
		)

		return
	}

	// Register real database tools
	for _, dbID := range dbIDs {
		// Create a closure to preserve the dbID
		func(id string) {
			// Register query tool
			mcpServer.AddTool(
				mcp.NewTool(
					fmt.Sprintf("query_%s", id),
					mcp.WithDescription(fmt.Sprintf("Execute SQL query on %s database", id)),
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
					db, err := dbtools.GetDatabase(id)
					if err != nil {
						return nil, fmt.Errorf("failed to get database: %v", err)
					}

					// Execute query
					result, err := db.Query(ctx, query, queryParams...)
					if err != nil {
						return nil, fmt.Errorf("query execution failed: %v", err)
					}

					return mcp.NewToolResultText(fmt.Sprintf("%v", result)), nil
				},
			)

			// Register schema tool
			mcpServer.AddTool(
				mcp.NewTool(
					fmt.Sprintf("schema_%s", id),
					mcp.WithDescription(fmt.Sprintf("Get schema of %s database", id)),
				),
				func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					// Get database
					db, err := dbtools.GetDatabase(id)
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
					schemaInfo := fmt.Sprintf("Database: %s\nTables: %d\n\n", id, len(tables))
					for i, table := range tables {
						schemaInfo += fmt.Sprintf("%d. %s\n", i+1, table)
					}

					return mcp.NewToolResultText(schemaInfo), nil
				},
			)
		}(dbID)
	}

	// Register list databases tool
	mcpServer.AddTool(
		mcp.NewTool(
			"list_databases",
			mcp.WithDescription("List all available databases"),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(fmt.Sprintf("Available databases: %v", dbtools.ListDatabases())), nil
		},
	)
}
