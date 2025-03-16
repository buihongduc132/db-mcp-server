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
			Schema: entities.MCPParameterSchema{
				Type: "object",
				Properties: map[string]entities.MCPPropertySchema{
					"sql": {
						Type:        "string",
						Description: "SQL query to execute",
					},
				},
				Required: []string{"sql"},
			},
		},
		{
			Name:        "insert_data",
			Description: "Insert data into a table",
			Schema: entities.MCPParameterSchema{
				Type: "object",
				Properties: map[string]entities.MCPPropertySchema{
					"table": {
						Type:        "string",
						Description: "Table name",
					},
					"data": {
						Type:        "object",
						Description: "Data to insert as key-value pairs",
					},
				},
				Required: []string{"table", "data"},
			},
		},
		{
			Name:        "update_data",
			Description: "Update data in a table",
			Schema: entities.MCPParameterSchema{
				Type: "object",
				Properties: map[string]entities.MCPPropertySchema{
					"table": {
						Type:        "string",
						Description: "Table name",
					},
					"data": {
						Type:        "object",
						Description: "Data to update as key-value pairs",
					},
					"condition": {
						Type:        "string",
						Description: "WHERE condition",
					},
				},
				Required: []string{"table", "data", "condition"},
			},
		},
		{
			Name:        "delete_data",
			Description: "Delete data from a table",
			Schema: entities.MCPParameterSchema{
				Type: "object",
				Properties: map[string]entities.MCPPropertySchema{
					"table": {
						Type:        "string",
						Description: "Table name",
					},
					"condition": {
						Type:        "string",
						Description: "WHERE condition",
					},
				},
				Required: []string{"table", "condition"},
			},
		},
	}

	return tools, nil
}

// ExecuteTool executes a tool and returns the result
func (r *DatabaseToolRepository) ExecuteTool(ctx context.Context, request entities.MCPToolRequest) (*entities.MCPToolResponse, error) {
	response := &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      request.ID,
		Result:  nil,
	}

	// Extract the tool name from the parameters
	toolName, ok := request.Parameters["name"].(string)
	if !ok {
		return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Missing or invalid 'name' parameter"), nil
	}

	switch toolName {
	case "execute_query":
		sql, ok := request.Parameters["sql"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'sql' must be a string"), nil
		}

		results, err := r.dbRepo.ExecuteQuery(ctx, sql)
		if err != nil {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Query error: %v", err)), nil
		}

		response.Result = results

	case "insert_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
		}

		dataParam, ok := request.Parameters["data"].(map[string]interface{})
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'data' must be an object"), nil
		}

		id, err := r.dbRepo.InsertData(ctx, table, dataParam)
		if err != nil {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Insert error: %v", err)), nil
		}

		response.Result = map[string]int64{"inserted_id": id}

	case "update_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
		}

		dataParam, ok := request.Parameters["data"].(map[string]interface{})
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'data' must be an object"), nil
		}

		condition, ok := request.Parameters["condition"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'condition' must be a string"), nil
		}

		affected, err := r.dbRepo.UpdateData(ctx, table, dataParam, condition)
		if err != nil {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Update error: %v", err)), nil
		}

		response.Result = map[string]int64{"rows_affected": affected}

	case "delete_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'table' must be a string"), nil
		}

		condition, ok := request.Parameters["condition"].(string)
		if !ok {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeInvalidParams, "Parameter 'condition' must be a string"), nil
		}

		affected, err := r.dbRepo.DeleteData(ctx, table, condition)
		if err != nil {
			return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeToolExecutionFailed, fmt.Sprintf("Delete error: %v", err)), nil
		}

		response.Result = map[string]int64{"rows_affected": affected}

	default:
		return createJsonRpcErrorResponse(request.ID, entities.ErrorCodeMethodNotFound, fmt.Sprintf("Unknown tool: %s", toolName)), nil
	}

	return response, nil
}

// createJsonRpcErrorResponse creates a JSON-RPC 2.0 error response
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
