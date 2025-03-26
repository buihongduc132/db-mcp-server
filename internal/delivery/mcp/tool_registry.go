package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/FreePeak/db-mcp-server/internal/usecase"
)

// ToolRegistry manages the creation and registration of database tools
type ToolRegistry struct {
	server          *server.MCPServer
	databaseUseCase *usecase.DatabaseUseCase
}

// NewToolRegistry creates a new tool registry
func NewToolRegistry(mcpServer *server.MCPServer, databaseUseCase *usecase.DatabaseUseCase) *ToolRegistry {
	return &ToolRegistry{
		server:          mcpServer,
		databaseUseCase: databaseUseCase,
	}
}

// RegisterAllTools registers all available database tools
func (tr *ToolRegistry) RegisterAllTools() {
	// Get available database connections
	dbIDs := tr.databaseUseCase.ListDatabases()

	// If no database connections are available, register mock tools
	if len(dbIDs) == 0 {
		// fmt.Println("No active database connections. Registering mock database tools.")
		tr.RegisterMockTools()
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

			// Execute query via use case
			result, err := tr.databaseUseCase.ExecuteQuery(ctx, dbID, query, queryParams)
			if err != nil {
				return nil, fmt.Errorf("query execution failed: %v", err)
			}

			return mcp.NewToolResultText(result), nil
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

			// Execute statement via use case
			result, err := tr.databaseUseCase.ExecuteStatement(ctx, dbID, statement, statementParams)
			if err != nil {
				return nil, fmt.Errorf("statement execution failed: %v", err)
			}

			return mcp.NewToolResultText(result), nil
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

			txID := ""
			if request.Params.Arguments["transactionId"] != nil {
				txID, _ = request.Params.Arguments["transactionId"].(string)
			}

			statement := ""
			if request.Params.Arguments["statement"] != nil {
				statement, _ = request.Params.Arguments["statement"].(string)
			}

			var params []interface{}
			if request.Params.Arguments["params"] != nil {
				if paramsArr, ok := request.Params.Arguments["params"].([]interface{}); ok {
					params = paramsArr
				}
			}

			readOnly := false
			if request.Params.Arguments["readOnly"] != nil {
				readOnly, _ = request.Params.Arguments["readOnly"].(bool)
			}

			// Execute transaction operation via use case
			result, extraData, err := tr.databaseUseCase.ExecuteTransaction(
				ctx, dbID, action, txID, statement, params, readOnly,
			)
			if err != nil {
				return nil, fmt.Errorf("transaction operation failed: %v", err)
			}

			if extraData != nil {
				// Convert extraData to JSON string
				return mcp.NewToolResultText(fmt.Sprintf("Transaction operation successful: %s", result)), nil
			}
			return mcp.NewToolResultText(result), nil
		},
	)
}

// registerPerformanceTool registers a tool for analyzing database performance
func (tr *ToolRegistry) registerPerformanceTool(dbID string) {
	// This is a placeholder - in a real implementation, we would need a performance use case
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
			mcp.WithNumber("limit",
				mcp.Description("Maximum number of results to return"),
			),
			mcp.WithNumber("threshold",
				mcp.Description("Slow query threshold in milliseconds (required for setThreshold)"),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// This is just a placeholder - in a real implementation we would call the performance use case
			return mcp.NewToolResultText("Performance analysis not implemented in this refactored version"), nil
		},
	)
}

// registerSchemaTool registers a tool for exploring database schema
func (tr *ToolRegistry) registerSchemaTool(dbID string) {
	// This is a placeholder - in a real implementation, we would need a schema use case
	tr.server.AddTool(
		mcp.NewTool(
			fmt.Sprintf("schema_%s", dbID),
			mcp.WithDescription(fmt.Sprintf("Get schema of %s database", dbID)),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// This is just a placeholder - in a real implementation we would call the schema use case
			return mcp.NewToolResultText("Schema information not implemented in this refactored version"), nil
		},
	)
}

// registerCommonTools registers tools that are not specific to a database
func (tr *ToolRegistry) registerCommonTools() {
	tr.server.AddTool(
		mcp.NewTool(
			"list_databases",
			mcp.WithDescription("List all available databases"),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			dbIDs := tr.databaseUseCase.ListDatabases()

			if len(dbIDs) == 0 {
				return mcp.NewToolResultText("No databases available"), nil
			}

			result := fmt.Sprintf("Available databases (%d):\n\n", len(dbIDs))
			for _, id := range dbIDs {
				result += fmt.Sprintf("- %s\n", id)
			}

			return mcp.NewToolResultText(result), nil
		},
	)
}

// RegisterMockTools registers mock database tools when no real connections are available
func (tr *ToolRegistry) RegisterMockTools() {
	// Register a mock query tool
	tr.server.AddTool(
		mcp.NewTool(
			"query_mock",
			mcp.WithDescription("Execute SQL query on mock database"),
			mcp.WithString("query",
				mcp.Description("SQL query to execute"),
				mcp.Required(),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			query, _ := request.Params.Arguments["query"].(string)

			// Generate mock response
			return mcp.NewToolResultText(fmt.Sprintf("Mock query executed: %s\n\nID\tName\tValue\n-------------------\n1\tTest1\t100\n2\tTest2\t200\n\nTotal rows: 2", query)), nil
		},
	)

	// Register a mock execute tool
	tr.server.AddTool(
		mcp.NewTool(
			"execute_mock",
			mcp.WithDescription("Execute SQL statement on mock database"),
			mcp.WithString("statement",
				mcp.Description("SQL statement to execute"),
				mcp.Required(),
			),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			statement, _ := request.Params.Arguments["statement"].(string)

			// Generate mock response
			return mcp.NewToolResultText(fmt.Sprintf("Mock statement executed: %s\n\nRows affected: 1\nLast insert ID: 42", statement)), nil
		},
	)
}
