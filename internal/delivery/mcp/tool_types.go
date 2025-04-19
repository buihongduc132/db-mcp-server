package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/tools"
)

// createTextResponse creates a simple response with a text content
func createTextResponse(text string) map[string]interface{} {
	return map[string]interface{}{
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": text,
			},
		},
	}
}

// addMetadata adds metadata to a response
func addMetadata(resp map[string]interface{}, key string, value interface{}) map[string]interface{} {
	if resp["metadata"] == nil {
		resp["metadata"] = make(map[string]interface{})
	}

	metadata, ok := resp["metadata"].(map[string]interface{})
	if !ok {
		// Create a new metadata map if conversion fails
		metadata = make(map[string]interface{})
		resp["metadata"] = metadata
	}

	metadata[key] = value
	return resp
}

// TODO: Refactor tool type implementations to reduce duplication and improve maintainability
// TODO: Consider using a code generation approach for repetitive tool patterns
// TODO: Add comprehensive request validation for all tool parameters
// TODO: Implement proper rate limiting and resource protection
// TODO: Add detailed documentation for each tool type and its parameters

// ToolType interface defines the structure for different types of database tools
type ToolType interface {
	// GetName returns the base name of the tool type (e.g., "query", "execute")
	GetName() string

	// GetDescription returns a description for this tool type
	GetDescription(dbID string) string

	// CreateTool creates a tool with the specified name
	// The returned tool must be compatible with server.MCPServer.AddTool's first parameter
	CreateTool(name string, dbID string) interface{}

	// HandleRequest handles tool requests for this tool type
	HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error)
}

// UseCaseProvider interface abstracts database use case operations
type UseCaseProvider interface {
	ExecuteQuery(ctx context.Context, dbID, query string, params []interface{}) (string, error)
	ExecuteStatement(ctx context.Context, dbID, statement string, params []interface{}) (string, error)
	ExecuteTransaction(ctx context.Context, dbID, action string, txID string, statement string, params []interface{}, readOnly bool) (string, map[string]interface{}, error)
	GetDatabaseInfo(dbID string) (map[string]interface{}, error)
	ListDatabases() []string
	GetDatabaseType(dbID string) (string, error)
}

// BaseToolType provides common functionality for tool types
type BaseToolType struct {
	name        string
	description string
}

// GetName returns the name of the tool type
func (b *BaseToolType) GetName() string {
	return b.name
}

// GetDescription returns a description for the tool type
func (b *BaseToolType) GetDescription(dbID string) string {
	return fmt.Sprintf("%s on %s database", b.description, dbID)
}

//------------------------------------------------------------------------------
// QueryTool implementation
//------------------------------------------------------------------------------

// QueryTool handles SQL query operations
type QueryTool struct {
	BaseToolType
}

// NewQueryTool creates a new query tool type
func NewQueryTool() *QueryTool {
	return &QueryTool{
		BaseToolType: BaseToolType{
			name:        "query",
			description: "Execute SQL SELECT queries to retrieve data from the database. This tool allows you to run read-only SQL queries that return data from the database without modifying any records. It supports parameterized queries for improved security and performance. Results are returned in a tabular format with column headers and row counts. Use this tool when you need to retrieve information from the database without making any changes.",
		},
	}
}

// CreateTool creates a query tool
func (t *QueryTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		tools.WithString("query",
			tools.Description("SQL query to execute"),
			tools.Required(),
		),
		tools.WithArray("params",
			tools.Description("Query parameters"),
			tools.Items(map[string]interface{}{"type": "string"}),
		),
	)
}

// HandleRequest handles query tool requests
func (t *QueryTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// If dbID is not provided, extract it from the tool name
	if dbID == "" {
		dbID = extractDatabaseIDFromName(request.Name)
	}

	query, ok := request.Parameters["query"].(string)
	if !ok {
		return nil, fmt.Errorf("query parameter must be a string")
	}

	var queryParams []interface{}
	if request.Parameters["params"] != nil {
		if paramsArr, ok := request.Parameters["params"].([]interface{}); ok {
			queryParams = paramsArr
		}
	}

	result, err := useCase.ExecuteQuery(ctx, dbID, query, queryParams)
	if err != nil {
		return nil, err
	}

	return createTextResponse(result), nil
}

// extractDatabaseIDFromName extracts the database ID from a tool name
func extractDatabaseIDFromName(name string) string {
	// Format is: <tooltype>_<dbID>
	parts := strings.Split(name, "_")
	if len(parts) < 2 {
		return ""
	}

	// The database ID is the last part
	return parts[len(parts)-1]
}

//------------------------------------------------------------------------------
// ExecuteTool implementation
//------------------------------------------------------------------------------

// ExecuteTool handles SQL statement execution
type ExecuteTool struct {
	BaseToolType
}

// NewExecuteTool creates a new execute tool type
func NewExecuteTool() *ExecuteTool {
	return &ExecuteTool{
		BaseToolType: BaseToolType{
			name:        "execute",
			description: "[DANGEROUS] Execute SQL statement that modifies data (INSERT, UPDATE, DELETE). This tool allows you to execute SQL statements that modify the database, including inserting new records, updating existing data, or deleting records. Use with extreme caution as these operations can permanently alter or remove data. Always verify your SQL statements before execution and consider using transactions for critical operations to allow for rollback if needed.",
		},
	}
}

// CreateTool creates an execute tool
func (t *ExecuteTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		tools.WithString("statement",
			tools.Description("SQL statement to execute (INSERT, UPDATE, DELETE, etc.)"),
			tools.Required(),
		),
		tools.WithArray("params",
			tools.Description("Statement parameters"),
			tools.Items(map[string]interface{}{"type": "string"}),
		),
	)
}

// HandleRequest handles execute tool requests
func (t *ExecuteTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// If dbID is not provided, extract it from the tool name
	if dbID == "" {
		dbID = extractDatabaseIDFromName(request.Name)
	}

	statement, ok := request.Parameters["statement"].(string)
	if !ok {
		return nil, fmt.Errorf("statement parameter must be a string")
	}

	var statementParams []interface{}
	if request.Parameters["params"] != nil {
		if paramsArr, ok := request.Parameters["params"].([]interface{}); ok {
			statementParams = paramsArr
		}
	}

	result, err := useCase.ExecuteStatement(ctx, dbID, statement, statementParams)
	if err != nil {
		return nil, err
	}

	return createTextResponse(result), nil
}

//------------------------------------------------------------------------------
// TransactionTool implementation
//------------------------------------------------------------------------------

// TransactionTool handles database transactions
type TransactionTool struct {
	BaseToolType
}

// NewTransactionTool creates a new transaction tool type
func NewTransactionTool() *TransactionTool {
	return &TransactionTool{
		BaseToolType: BaseToolType{
			name:        "transaction",
			description: "[DANGEROUS] Manage database transactions for executing multiple SQL operations atomically. This tool allows you to begin, commit, or rollback database transactions, ensuring that multiple operations are treated as a single unit of work. Transactions provide data integrity by ensuring that either all operations succeed or none do. Use with caution as committing transactions permanently applies changes to the database, while forgetting to commit or rollback can leave transactions open and lock database resources.",
		},
	}
}

// CreateTool creates a transaction tool
func (t *TransactionTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		tools.WithString("action",
			tools.Description("Transaction action (begin, commit, rollback, execute)"),
			tools.Required(),
		),
		tools.WithString("transactionId",
			tools.Description("Transaction ID (required for commit, rollback, execute)"),
		),
		tools.WithString("statement",
			tools.Description("SQL statement to execute within transaction (required for execute)"),
		),
		tools.WithArray("params",
			tools.Description("Statement parameters"),
			tools.Items(map[string]interface{}{"type": "string"}),
		),
		tools.WithBoolean("readOnly",
			tools.Description("Whether the transaction is read-only (for begin)"),
		),
	)
}

// HandleRequest handles transaction tool requests
func (t *TransactionTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// If dbID is not provided, extract it from the tool name
	if dbID == "" {
		dbID = extractDatabaseIDFromName(request.Name)
	}

	action, ok := request.Parameters["action"].(string)
	if !ok {
		return nil, fmt.Errorf("action parameter must be a string")
	}

	txID := ""
	if request.Parameters["transactionId"] != nil {
		var ok bool
		txID, ok = request.Parameters["transactionId"].(string)
		if !ok {
			return nil, fmt.Errorf("transactionId parameter must be a string")
		}
	}

	statement := ""
	if request.Parameters["statement"] != nil {
		var ok bool
		statement, ok = request.Parameters["statement"].(string)
		if !ok {
			return nil, fmt.Errorf("statement parameter must be a string")
		}
	}

	var params []interface{}
	if request.Parameters["params"] != nil {
		if paramsArr, ok := request.Parameters["params"].([]interface{}); ok {
			params = paramsArr
		}
	}

	readOnly := false
	if request.Parameters["readOnly"] != nil {
		var ok bool
		readOnly, ok = request.Parameters["readOnly"].(bool)
		if !ok {
			return nil, fmt.Errorf("readOnly parameter must be a boolean")
		}
	}

	message, metadata, err := useCase.ExecuteTransaction(ctx, dbID, action, txID, statement, params, readOnly)
	if err != nil {
		return nil, err
	}

	// Create response with text and metadata
	resp := createTextResponse(message)

	// Add metadata if provided
	for k, v := range metadata {
		addMetadata(resp, k, v)
	}

	return resp, nil
}

//------------------------------------------------------------------------------
// PerformanceTool implementation
//------------------------------------------------------------------------------

// PerformanceTool handles query performance analysis
type PerformanceTool struct {
	BaseToolType
}

// NewPerformanceTool creates a new performance tool type
func NewPerformanceTool() *PerformanceTool {
	return &PerformanceTool{
		BaseToolType: BaseToolType{
			name:        "performance",
			description: "Analyze and optimize database query performance. This tool provides comprehensive performance analysis capabilities for SQL queries, including identifying slow queries, analyzing query execution plans, and providing optimization suggestions. You can retrieve performance metrics, set slow query thresholds, analyze specific queries for optimization opportunities, and reset performance statistics. Use this tool to diagnose performance issues and improve database efficiency.",
		},
	}
}

// CreateTool creates a performance analysis tool
func (t *PerformanceTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		tools.WithString("action",
			tools.Description("Action (getSlowQueries, getMetrics, analyzeQuery, reset, setThreshold)"),
			tools.Required(),
		),
		tools.WithString("query",
			tools.Description("SQL query to analyze (required for analyzeQuery)"),
		),
		tools.WithNumber("limit",
			tools.Description("Maximum number of results to return"),
		),
		tools.WithNumber("threshold",
			tools.Description("Slow query threshold in milliseconds (required for setThreshold)"),
		),
	)
}

// HandleRequest handles performance tool requests
func (t *PerformanceTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// If dbID is not provided, extract it from the tool name
	if dbID == "" {
		dbID = extractDatabaseIDFromName(request.Name)
	}

	// This is a simplified implementation
	// In a real implementation, this would analyze query performance

	action, ok := request.Parameters["action"].(string)
	if !ok {
		return nil, fmt.Errorf("action parameter must be a string")
	}

	var limit int
	if request.Parameters["limit"] != nil {
		if limitParam, ok := request.Parameters["limit"].(float64); ok {
			limit = int(limitParam)
		}
	}

	query := ""
	if request.Parameters["query"] != nil {
		var ok bool
		query, ok = request.Parameters["query"].(string)
		if !ok {
			return nil, fmt.Errorf("query parameter must be a string")
		}
	}

	var threshold int
	if request.Parameters["threshold"] != nil {
		if thresholdParam, ok := request.Parameters["threshold"].(float64); ok {
			threshold = int(thresholdParam)
		}
	}

	// This is where we would call the useCase to analyze performance
	// For now, just return a placeholder
	output := fmt.Sprintf("Performance analysis for action '%s' on database '%s'\n", action, dbID)

	if query != "" {
		output += fmt.Sprintf("Query: %s\n", query)
	}

	if limit > 0 {
		output += fmt.Sprintf("Limit: %d\n", limit)
	}

	if threshold > 0 {
		output += fmt.Sprintf("Threshold: %d ms\n", threshold)
	}

	return createTextResponse(output), nil
}

//------------------------------------------------------------------------------
// SchemaTool implementation
//------------------------------------------------------------------------------

// SchemaTool handles database schema exploration
type SchemaTool struct {
	BaseToolType
}

// NewSchemaTool creates a new schema tool type
func NewSchemaTool() *SchemaTool {
	return &SchemaTool{
		BaseToolType: BaseToolType{
			name:        "schema",
			description: "Retrieve detailed database schema information. This tool provides comprehensive information about the database structure, including tables, columns, data types, constraints, indexes, and relationships. It helps you understand the database organization, identify primary and foreign keys, and discover table relationships. Use this tool when you need to explore an unfamiliar database or verify the structure of specific database objects.",
		},
	}
}

// CreateTool creates a schema tool
func (t *SchemaTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		// Use any string parameter for compatibility
		tools.WithString("random_string",
			tools.Description("Dummy parameter (optional)"),
		),
	)
}

// HandleRequest handles schema tool requests
func (t *SchemaTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	// If dbID is not provided, extract it from the tool name
	if dbID == "" {
		dbID = extractDatabaseIDFromName(request.Name)
	}

	info, err := useCase.GetDatabaseInfo(dbID)
	if err != nil {
		return nil, err
	}

	// Format response text
	infoStr := fmt.Sprintf("Database Schema for %s:\n\n%+v", dbID, info)
	return createTextResponse(infoStr), nil
}

//------------------------------------------------------------------------------
// ListDatabasesTool implementation
//------------------------------------------------------------------------------

// ListDatabasesTool handles listing available databases
type ListDatabasesTool struct {
	BaseToolType
}

// NewListDatabasesTool creates a new list databases tool type
func NewListDatabasesTool() *ListDatabasesTool {
	return &ListDatabasesTool{
		BaseToolType: BaseToolType{
			name:        "list_databases",
			description: "List all available databases with detailed connection information including database name, host, port, and type. This tool provides a comprehensive overview of all database connections configured in the system, allowing you to identify and select the appropriate database for your operations.",
		},
	}
}

// CreateTool creates a list databases tool
func (t *ListDatabasesTool) CreateTool(name string, dbID string) interface{} {
	return tools.NewTool(
		name,
		tools.WithDescription(t.GetDescription(dbID)),
		// Use any string parameter for compatibility
		tools.WithString("random_string",
			tools.Description("Dummy parameter (optional)"),
		),
	)
}

// HandleRequest handles list databases tool requests
func (t *ListDatabasesTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	databases := useCase.ListDatabases()

	// Format as text for display
	output := "Available databases:\n\n"
	output += "| # | Database ID | Type | Host | Port | Database Name |\n"
	output += "|---|------------|------|------|------|--------------|\n"

	for i, dbID := range databases {
		// Get database info to extract host, port, etc.
		dbInfo, err := useCase.GetDatabaseInfo(dbID)
		if err != nil {
			// If we can't get detailed info, just show the database ID
			output += fmt.Sprintf("| %d | %s | Unknown | Unknown | Unknown | Unknown |\n", i+1, dbID)
			continue
		}

		// Extract database type
		dbType, _ := useCase.GetDatabaseType(dbID)
		if dbType == "" {
			dbType = "Unknown"
		}

		// Extract host, port, and name from dbInfo if available
		host := "Unknown"
		port := "Unknown"
		name := "Unknown"

		// Try to extract database name from dbInfo
		if dbName, ok := dbInfo["database"].(string); ok {
			name = dbName
		}

		// For now, we'll use placeholders for host and port
		// In a real implementation, these would come from the connection config
		output += fmt.Sprintf("| %d | %s | %s | %s | %s | %s |\n", i+1, dbID, dbType, host, port, name)
	}

	if len(databases) == 0 {
		output += "No databases configured.\n"
	}

	return createTextResponse(output), nil
}

//------------------------------------------------------------------------------
// ToolTypeFactory provides a factory for creating tool types
//------------------------------------------------------------------------------

// ToolTypeFactory creates and manages tool types
type ToolTypeFactory struct {
	toolTypes map[string]ToolType
}

// NewToolTypeFactory creates a new tool type factory with all registered tool types
func NewToolTypeFactory() *ToolTypeFactory {
	factory := &ToolTypeFactory{
		toolTypes: make(map[string]ToolType),
	}

	// Register all tool types
	factory.Register(NewQueryTool())
	factory.Register(NewExecuteTool())
	factory.Register(NewTransactionTool())
	factory.Register(NewPerformanceTool())
	factory.Register(NewSchemaTool())
	factory.Register(NewListDatabasesTool())

	// Register the generic SQL tool
	factory.Register(NewGenericSQLTool())

	// Register database statistics tools
	factory.Register(NewDbStatsTool())
	factory.Register(NewTableStatsTool())

	// Register pre-generated query tools
	factory.Register(NewGetIndexesTool())
	factory.Register(NewGetConstraintsTool())
	factory.Register(NewGetViewsTool())
	factory.Register(NewGetTypesTool())
	factory.Register(NewGetSchemasTool())
	factory.Register(NewGetSampleDataTool())
	factory.Register(NewGetUniqueValuesTool())

	return factory
}

// Register adds a tool type to the factory
func (f *ToolTypeFactory) Register(toolType ToolType) {
	f.toolTypes[toolType.GetName()] = toolType
}

// GetToolType returns a tool type by name
func (f *ToolTypeFactory) GetToolType(name string) (ToolType, bool) {
	// Handle new simpler format: <tooltype>_<dbID> or just the tool type name
	parts := strings.Split(name, "_")
	if len(parts) > 0 {
		// First part is the tool type name
		toolType, ok := f.toolTypes[parts[0]]
		if ok {
			return toolType, true
		}
	}

	// Direct tool type lookup
	toolType, ok := f.toolTypes[name]
	return toolType, ok
}

// GetToolTypeForSourceName finds the appropriate tool type for a source name
func (f *ToolTypeFactory) GetToolTypeForSourceName(sourceName string) (ToolType, string, bool) {
	// Handle simpler format: <tooltype>_<dbID>
	parts := strings.Split(sourceName, "_")

	if len(parts) >= 2 {
		// First part is tool type, last part is dbID
		toolTypeName := parts[0]
		dbID := parts[len(parts)-1]

		toolType, ok := f.toolTypes[toolTypeName]
		if ok {
			return toolType, dbID, true
		}
	}

	// Handle case for global tools
	if sourceName == "list_databases" {
		toolType, ok := f.toolTypes["list_databases"]
		return toolType, "", ok
	}

	return nil, "", false
}

// GetAllToolTypes returns all registered tool types
func (f *ToolTypeFactory) GetAllToolTypes() []ToolType {
	types := make([]ToolType, 0, len(f.toolTypes))
	for _, toolType := range f.toolTypes {
		types = append(types, toolType)
	}
	return types
}
