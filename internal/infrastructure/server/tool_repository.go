package server

import (
	"context"
	"fmt"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/domain/repositories"
)

// DatabaseToolRepository implements the ToolRepository interface
type DatabaseToolRepository struct {
	dbRepo repositories.DBRepository
}

// NewDatabaseToolRepository creates a new database tool repository
func NewDatabaseToolRepository(dbRepo repositories.DBRepository) *DatabaseToolRepository {
	return &DatabaseToolRepository{
		dbRepo: dbRepo,
	}
}

// GetAllTools returns all available tools
func (r *DatabaseToolRepository) GetAllTools(ctx context.Context) ([]entities.MCPToolDefinition, error) {
	// Define the available tools
	tools := []entities.MCPToolDefinition{
		{
			Name:        "execute_query",
			Description: "Execute a SQL query and return the results",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"sql": map[string]interface{}{
						"type":        "string",
						"description": "SQL query to execute",
					},
				},
				"required": []string{"sql"},
			},
		},
		{
			Name:        "insert_data",
			Description: "Insert data into a table",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Table name",
					},
					"data": map[string]interface{}{
						"type":        "object",
						"description": "Data to insert as key-value pairs",
					},
				},
				"required": []string{"table", "data"},
			},
		},
		{
			Name:        "update_data",
			Description: "Update data in a table",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Table name",
					},
					"data": map[string]interface{}{
						"type":        "object",
						"description": "Data to update as key-value pairs",
					},
					"condition": map[string]interface{}{
						"type":        "string",
						"description": "WHERE condition",
					},
				},
				"required": []string{"table", "data", "condition"},
			},
		},
		{
			Name:        "delete_data",
			Description: "Delete data from a table",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"table": map[string]interface{}{
						"type":        "string",
						"description": "Table name",
					},
					"condition": map[string]interface{}{
						"type":        "string",
						"description": "WHERE condition",
					},
				},
				"required": []string{"table", "condition"},
			},
		},
	}

	return tools, nil
}

// ExecuteTool executes a tool and returns the result
func (r *DatabaseToolRepository) ExecuteTool(ctx context.Context, request entities.MCPToolRequest) (*entities.MCPToolResponse, error) {
	// Get arguments from params
	name, ok := request.Params["name"].(string)
	if !ok {
		return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Missing tool name"), nil
	}

	args, ok := request.Params["arguments"].(map[string]interface{})
	if !ok {
		// Arguments might be missing, which is fine for some tools
		args = make(map[string]interface{})
	}

	// Execute the appropriate tool
	switch name {
	case "execute_query":
		return r.executeQuery(ctx, request.ID, args)
	case "insert_data":
		return r.insertData(ctx, request.ID, args)
	case "update_data":
		return r.updateData(ctx, request.ID, args)
	case "delete_data":
		return r.deleteData(ctx, request.ID, args)
	default:
		return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeMethodNotFound, fmt.Sprintf("Unknown tool: %s", name)), nil
	}
}

// Helper to create a JSON-RPC error response
func createJsonRpcErrorResponse(id string, code int, message string) *entities.MCPToolResponse {
	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Error: &entities.MCPError{
			Code:    code,
			Message: message,
		},
	}
}

// executeQuery executes a SQL query
func (r *DatabaseToolRepository) executeQuery(ctx context.Context, id string, args map[string]interface{}) (*entities.MCPToolResponse, error) {
	sql, ok := args["sql"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'sql' must be a string"), nil
	}

	results, err := r.dbRepo.ExecuteQuery(ctx, sql)
	if err != nil {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Query error: %v", err)), nil
	}

	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Result:  results,
	}, nil
}

// insertData inserts data into a table
func (r *DatabaseToolRepository) insertData(ctx context.Context, id string, args map[string]interface{}) (*entities.MCPToolResponse, error) {
	// Extract table parameter
	table, ok := args["table"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
	}

	// Extract data parameter
	dataParam, ok := args["data"].(map[string]interface{})
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'data' must be an object"), nil
	}

	insertedID, err := r.dbRepo.InsertData(ctx, table, dataParam)
	if err != nil {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Insert error: %v", err)), nil
	}

	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Result:  map[string]int64{"inserted_id": insertedID},
	}, nil
}

// updateData updates data in a table
func (r *DatabaseToolRepository) updateData(ctx context.Context, id string, args map[string]interface{}) (*entities.MCPToolResponse, error) {
	// Extract table parameter
	table, ok := args["table"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
	}

	// Extract data parameter
	dataParam, ok := args["data"].(map[string]interface{})
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'data' must be an object"), nil
	}

	// Extract condition parameter
	condition, ok := args["condition"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'condition' must be a string"), nil
	}

	affected, err := r.dbRepo.UpdateData(ctx, table, dataParam, condition)
	if err != nil {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Update error: %v", err)), nil
	}

	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Result:  map[string]int64{"rows_affected": affected},
	}, nil
}

// deleteData deletes data from a table
func (r *DatabaseToolRepository) deleteData(ctx context.Context, id string, args map[string]interface{}) (*entities.MCPToolResponse, error) {
	// Extract table parameter
	table, ok := args["table"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
	}

	// Extract condition parameter
	condition, ok := args["condition"].(string)
	if !ok {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeInvalidParams, "Parameter 'condition' must be a string"), nil
	}

	affected, err := r.dbRepo.DeleteData(ctx, table, condition)
	if err != nil {
		return createJsonRpcErrorResponse(id, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Delete error: %v", err)), nil
	}

	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Result:  map[string]int64{"rows_affected": affected},
	}, nil
}
