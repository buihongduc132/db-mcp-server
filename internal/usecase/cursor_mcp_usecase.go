package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/domain/repositories"
)

// CursorMCPUseCase handles Cursor MCP protocol operations
type CursorMCPUseCase struct {
	toolRepo repositories.ToolRepository
}

// NewCursorMCPUseCase creates a new Cursor MCP use case
func NewCursorMCPUseCase(toolRepo repositories.ToolRepository) *CursorMCPUseCase {
	return &CursorMCPUseCase{
		toolRepo: toolRepo,
	}
}

// GetToolsEvent gets the tools event to send to Cursor
func (uc *CursorMCPUseCase) GetToolsEvent(ctx context.Context) (*entities.MCPToolsEvent, error) {
	tools, err := uc.toolRepo.GetAllTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tools: %w", err)
	}

	// Create a tools event following JSON-RPC 2.0 format
	event := &entities.MCPToolsEvent{
		JsonRPC: entities.JSONRPCVersion,
		Method:  entities.MethodToolsAvailable,
		Params: entities.MCPToolsEventParams{
			Tools: tools,
		},
	}

	return event, nil
}

// ExecuteTool executes a tool and returns a response
func (uc *CursorMCPUseCase) ExecuteTool(ctx context.Context, toolRequest *entities.MCPToolRequest) (*entities.MCPToolResponse, error) {
	// Extract tool name from parameters
	toolName, ok := toolRequest.Parameters["name"].(string)
	if !ok {
		return createErrorResponse(toolRequest.ID, entities.ErrorCodeInvalidParams, "Missing or invalid tool name"), nil
	}

	// Create a tool request for the repository
	repoRequest := entities.MCPToolRequest{
		ID:         toolRequest.ID,
		Method:     entities.MethodExecuteTool,
		Parameters: toolRequest.Parameters,
	}

	response, err := uc.toolRepo.ExecuteTool(ctx, repoRequest)
	if err != nil {
		// Return a properly formatted error response
		return createErrorResponse(
			toolRequest.ID,
			entities.ErrorCodeToolExecutionFailed,
			fmt.Sprintf("Failed to execute tool '%s': %v", toolName, err),
		), nil
	}

	// Return successful response
	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      toolRequest.ID,
		Result:  response.Result,
	}, nil
}

// Helper function to create error responses
func createErrorResponse(id string, code int, message string) *entities.MCPToolResponse {
	return &entities.MCPToolResponse{
		JsonRPC: entities.JSONRPCVersion,
		ID:      id,
		Error: &entities.MCPError{
			Code:    code,
			Message: message,
		},
	}
}

// ParseToolRequest parses a JSON string into a tool request
func (uc *CursorMCPUseCase) ParseToolRequest(requestJSON string) (*entities.MCPToolRequest, error) {
	var request entities.MCPToolRequest
	err := json.Unmarshal([]byte(requestJSON), &request)
	if err != nil {
		return nil, fmt.Errorf("invalid tool request JSON: %w", err)
	}

	// Validate JSON-RPC 2.0 request
	if request.JsonRPC != entities.JSONRPCVersion {
		return nil, fmt.Errorf("invalid JSON-RPC version: expected %s", entities.JSONRPCVersion)
	}

	if request.ID == "" {
		return nil, fmt.Errorf("missing request ID")
	}

	if request.Method == "" {
		return nil, fmt.Errorf("missing request method")
	}

	return &request, nil
}
