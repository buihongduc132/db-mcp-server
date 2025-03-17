package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"mcpserver/internal/database"
	"mcpserver/internal/mcp"
	"mcpserver/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Configure logging to use stderr instead of stdout to avoid interfering with JSON protocol
	log.SetOutput(os.Stderr)

	// Parse command line flags
	var (
		transportMode = flag.String("transport", "stdio", "Transport mode: stdio or sse")
		port          = flag.Int("port", 3000, "Server port (only used for SSE transport)")
	)
	flag.Parse()

	// Create MCP server with capabilities
	srv := server.NewMCPServer(
		"mcp-server",
		"1.0.0",
		server.WithResourceCapabilities(true, true),
		server.WithPromptCapabilities(true),
		server.WithToolCapabilities(true),
		server.WithLogging(),
	)

	// Register database tools
	registerDatabaseTools(srv)

	// Register Cursor-specific tools
	registerCursorTools(srv)

	// Create transport based on mode
	var transport server.Transport
	switch *transportMode {
	case "sse":
		transport = server.NewSSETransport(*port)
	default:
		transport = server.NewStdioTransport()
	}

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down server...")
		cancel()
	}()

	// Start server
	if err := transport.Serve(ctx, srv); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

// registerDatabaseTools registers all database-related tools with the server
func registerDatabaseTools(srv *server.MCPServer) {
	// Register database query tool
	srv.AddTool(
		mcp.Tool{
			Name:        "db-execute-query",
			Description: "Execute a SQL query and return the results",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {
						"query": {
							"type": "string",
							"description": "SQL query to execute"
						},
						"args": {
							"type": "array",
							"description": "Query arguments (for parameterized queries)",
							"items": {
								"type": "string"
							}
						}
					},
					"required": ["query"]
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleExecuteQuery(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Register database non-query tool (INSERT, UPDATE, DELETE)
	srv.AddTool(
		mcp.Tool{
			Name:        "db-execute-non-query",
			Description: "Execute a SQL non-query (INSERT, UPDATE, DELETE) and return affected rows",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {
						"query": {
							"type": "string",
							"description": "SQL non-query to execute"
						},
						"args": {
							"type": "array",
							"description": "Query arguments (for parameterized queries)",
							"items": {
								"type": "string"
							}
						}
					},
					"required": ["query"]
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleExecuteNonQuery(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Register get tables tool
	srv.AddTool(
		mcp.Tool{
			Name:        "db-get-tables",
			Description: "Get a list of tables in the database",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {}
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleGetTables(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Register get table schema tool
	srv.AddTool(
		mcp.Tool{
			Name:        "db-get-table-schema",
			Description: "Get the schema of a database table",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {
						"table": {
							"type": "string",
							"description": "Name of the table"
						}
					},
					"required": ["table"]
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleGetTableSchema(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Register database ping tool
	srv.AddTool(
		mcp.Tool{
			Name:        "db-ping",
			Description: "Check database connectivity",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {}
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandlePingDatabase(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)
}

// registerCursorTools registers tools specifically designed for Cursor integration
func registerCursorTools(srv *server.MCPServer) {
	// Register Cursor-specific query tool with formatted output
	srv.AddTool(
		mcp.Tool{
			Name:        "cursor-query",
			Description: "Execute a SQL query and return results formatted for Cursor display",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {
						"query": {
							"type": "string",
							"description": "SQL query to execute"
						},
						"args": {
							"type": "array",
							"description": "Query arguments (for parameterized queries)",
							"items": {
								"type": "string"
							}
						}
					},
					"required": ["query"]
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleCursorQuery(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Register database information tool for Cursor
	srv.AddTool(
		mcp.Tool{
			Name:        "cursor-get-database-info",
			Description: "Get comprehensive database information including tables and schemas",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {}
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			result, err := database.HandleGetDatabaseInfo(ctx, request.Params.Args)
			if err != nil {
				return nil, err
			}
			return &mcp.CallToolResult{
				Result: result,
			}, nil
		},
	)

	// Add a simple echo tool for testing Cursor connectivity
	srv.AddTool(
		mcp.Tool{
			Name:        "cursor-echo",
			Description: "Echo back the input message (for testing Cursor connectivity)",
			Properties: map[string]string{
				"schema": `{
					"type": "object",
					"properties": {
						"message": {
							"type": "string",
							"description": "Message to echo back"
						}
					},
					"required": ["message"]
				}`,
			},
		},
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var message string
			if msgVal, ok := request.Params.Args["message"]; ok {
				message = fmt.Sprintf("%v", msgVal)
			} else {
				message = "No message provided"
			}

			return &mcp.CallToolResult{
				Result: map[string]string{
					"echo": message,
				},
			}, nil
		},
	)
}
