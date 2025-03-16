package repositories

import (
	"context"
	"mcpserver/internal/domain/entities"
)

// ToolRepository defines the interface for tool operations
type ToolRepository interface {
	// GetAllTools returns all available tools
	GetAllTools(ctx context.Context) ([]entities.MCPToolDefinition, error)

	// ExecuteTool executes a tool and returns the result
	ExecuteTool(ctx context.Context, request entities.MCPToolRequest) (*entities.MCPToolResponse, error)
}
