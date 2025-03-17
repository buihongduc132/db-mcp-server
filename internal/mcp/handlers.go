package mcp

import (
	"encoding/json"
	"fmt"
	"mcpserver/internal/logger"
	"mcpserver/internal/session"
	"mcpserver/pkg/jsonrpc"
	"mcpserver/pkg/tools"
)

// Handler handles MCP requests
type Handler struct {
	toolRegistry *tools.Registry
}

// NewHandler creates a new MCP handler
func NewHandler() *Handler {
	return &Handler{
		toolRegistry: tools.NewRegistry(),
	}
}

// RegisterTool registers a tool with the handler
func (h *Handler) RegisterTool(tool *tools.Tool) {
	h.toolRegistry.RegisterTool(tool)
}

// Initialize handles the initialize method
func (h *Handler) Initialize(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Handling initialize request")

	// Parse the params
	var params struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if err := json.Unmarshal(req.Params, &params); err != nil {
		logger.Error("Failed to parse initialize params: %v", err)
		return nil, jsonrpc.InvalidParamsError(err.Error())
	}

	// Store client capabilities in the session
	sess.SetCapabilities(params.Capabilities)

	// Return server capabilities
	return map[string]interface{}{
		"protocolVersion": "1.0.0",
		"serverInfo": map[string]string{
			"name":    "MCP SSE Server",
			"version": "1.0.0",
		},
		"capabilities": map[string]interface{}{
			"toolsSupported": true,
			"notifications":  true,
		},
	}, nil
}

// ListTools handles the tools/list method
func (h *Handler) ListTools(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Handling tools/list request")

	// Get tools from the registry
	allTools := h.toolRegistry.GetAllTools()

	// Convert to a response format
	toolsResponse := make([]map[string]interface{}, 0, len(allTools))
	for _, tool := range allTools {
		toolsResponse = append(toolsResponse, map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": tool.InputSchema,
		})
	}

	return toolsResponse, nil
}

// HandleInitialized handles the notifications/initialized notification
func (h *Handler) HandleInitialized(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Received initialized notification from client: %s", sess.ID)
	// This is a notification, so no response is required
	return nil, nil
}

// ExecuteTool handles the tools/execute method
func (h *Handler) ExecuteTool(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Handling tools/execute request")

	// Parse the params
	var params struct {
		Tool  string                 `json:"tool"`
		Input map[string]interface{} `json:"input"`
	}

	if err := json.Unmarshal(req.Params, &params); err != nil {
		logger.Error("Failed to parse tools/execute params: %v", err)
		return nil, jsonrpc.InvalidParamsError(err.Error())
	}

	// Check if the tool exists
	tool, exists := h.toolRegistry.GetTool(params.Tool)
	if !exists {
		logger.Error("Tool not found: %s", params.Tool)
		return nil, jsonrpc.InvalidParamsError(fmt.Sprintf("Tool not found: %s", params.Tool))
	}

	// Execute the tool
	result, err := tool.Handler(params.Input)
	if err != nil {
		logger.Error("Tool execution error: %v", err)
		return nil, jsonrpc.InternalError(err.Error())
	}

	return result, nil
}

// GetAllMethodHandlers returns all method handlers
func (h *Handler) GetAllMethodHandlers() map[string]func(*jsonrpc.Request, *session.Session) (interface{}, *jsonrpc.Error) {
	return map[string]func(*jsonrpc.Request, *session.Session) (interface{}, *jsonrpc.Error){
		"initialize":                h.Initialize,
		"tools/list":                h.ListTools,
		"tools/execute":             h.ExecuteTool,
		"notifications/initialized": h.HandleInitialized,
	}
}
