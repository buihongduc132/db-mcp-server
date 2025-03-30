package mcp

import (
	"context"
	"log"

	"github.com/FreePeak/cortex/pkg/server"
	"github.com/FreePeak/cortex/pkg/types"
)

// ServerWrapper provides a wrapper around server.MCPServer to handle type assertions
type ServerWrapper struct {
	mcpServer *server.MCPServer
}

// NewServerWrapper creates a new ServerWrapper
func NewServerWrapper(mcpServer *server.MCPServer) *ServerWrapper {
	return &ServerWrapper{
		mcpServer: mcpServer,
	}
}

// AddTool adds a tool to the server
func (sw *ServerWrapper) AddTool(ctx context.Context, tool interface{}, handler func(ctx context.Context, request server.ToolCallRequest) (interface{}, error)) error {
	// Log the operation for debugging
	log.Printf("Adding tool: %T", tool)

	// Cast the tool to the expected type (*types.Tool)
	typedTool, ok := tool.(*types.Tool)
	if !ok {
		log.Printf("Warning: Tool is not of type *types.Tool: %T", tool)
		return nil
	}

	// Pass the tool to the MCPServer's AddTool method
	return sw.mcpServer.AddTool(ctx, typedTool, handler)
}
