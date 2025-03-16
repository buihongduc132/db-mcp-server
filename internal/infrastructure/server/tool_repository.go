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
		ID:     request.ID,
		Status: "success",
	}

	switch request.Name {
	case "execute_query":
		sql, ok := request.Parameters["sql"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'sql' must be a string"), nil
		}

		results, err := r.dbRepo.ExecuteQuery(ctx, sql)
		if err != nil {
			return createErrorResponse(request.ID, fmt.Sprintf("Query error: %v", err)), nil
		}

		response.Result = results

	case "insert_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'table' must be a string"), nil
		}

		dataParam, ok := request.Parameters["data"].(map[string]interface{})
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'data' must be an object"), nil
		}

		id, err := r.dbRepo.InsertData(ctx, table, dataParam)
		if err != nil {
			return createErrorResponse(request.ID, fmt.Sprintf("Insert error: %v", err)), nil
		}

		response.Result = map[string]int64{"inserted_id": id}

	case "update_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'table' must be a string"), nil
		}

		dataParam, ok := request.Parameters["data"].(map[string]interface{})
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'data' must be an object"), nil
		}

		condition, ok := request.Parameters["condition"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'condition' must be a string"), nil
		}

		affected, err := r.dbRepo.UpdateData(ctx, table, dataParam, condition)
		if err != nil {
			return createErrorResponse(request.ID, fmt.Sprintf("Update error: %v", err)), nil
		}

		response.Result = map[string]int64{"rows_affected": affected}

	case "delete_data":
		table, ok := request.Parameters["table"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'table' must be a string"), nil
		}

		condition, ok := request.Parameters["condition"].(string)
		if !ok {
			return createErrorResponse(request.ID, "Parameter 'condition' must be a string"), nil
		}

		affected, err := r.dbRepo.DeleteData(ctx, table, condition)
		if err != nil {
			return createErrorResponse(request.ID, fmt.Sprintf("Delete error: %v", err)), nil
		}

		response.Result = map[string]int64{"rows_affected": affected}

	default:
		return createErrorResponse(request.ID, fmt.Sprintf("Unknown tool: %s", request.Name)), nil
	}

	return response, nil
}

// createErrorResponse creates an error response for a tool request
func createErrorResponse(id, errorMsg string) *entities.MCPToolResponse {
	return &entities.MCPToolResponse{
		ID:     id,
		Status: "error",
		Error:  errorMsg,
	}
}
