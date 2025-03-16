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
func (uc *CursorMCPUseCase) GetToolsEvent(ctx context.Context) (*entities.MCPEvent, error) {
	tools, err := uc.toolRepo.GetAllTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tools: %w", err)
	}

	event := &entities.MCPEvent{
		Type: "tools",
		Payload: entities.MCPToolsEvent{
			Tools: tools,
		},
	}

	return event, nil
}

// ExecuteTool executes a tool and returns a response event
func (uc *CursorMCPUseCase) ExecuteTool(ctx context.Context, toolRequest *entities.MCPToolRequest) (*entities.MCPEvent, error) {
	response, err := uc.toolRepo.ExecuteTool(ctx, *toolRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to execute tool: %w", err)
	}

	event := &entities.MCPEvent{
		Type:    "tool_response",
		Payload: response,
	}

	return event, nil
}

// ParseToolRequest parses a JSON string into a tool request
func (uc *CursorMCPUseCase) ParseToolRequest(requestJSON string) (*entities.MCPToolRequest, error) {
	var request entities.MCPToolRequest
	err := json.Unmarshal([]byte(requestJSON), &request)
	if err != nil {
		return nil, fmt.Errorf("invalid tool request JSON: %w", err)
	}
	return &request, nil
}
