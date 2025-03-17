package mcp

import (
	"encoding/json"
	"fmt"
	"mcpserver/internal/logger"
	"mcpserver/internal/session"
	"mcpserver/pkg/jsonrpc"
	"mcpserver/pkg/tools"
)

const (
	// Latest protocol version supported
	ProtocolVersion = "2024-01-01"
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

	// Log the full request for debugging
	reqJSON, _ := json.Marshal(req)
	logger.Debug("initialize request data: %s", string(reqJSON))

	// Parse the params
	var params struct {
		ProtocolVersion string                 `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if err := json.Unmarshal(req.Params.(json.RawMessage), &params); err != nil {
		logger.Error("Failed to parse initialize params: %v", err)
		return nil, jsonrpc.InvalidParamsError(err.Error())
	}

	// Log client info
	logger.Info("Client connected: %s %s", params.ClientInfo.Name, params.ClientInfo.Version)
	logger.Debug("Client capabilities: %v", params.Capabilities)

	// Store client capabilities in the session
	sess.SetCapabilities(params.Capabilities)

	// Get all registered tools for tool capabilities
	allTools := h.toolRegistry.GetAllTools()
	toolsList := make([]map[string]interface{}, 0, len(allTools))

	for _, tool := range allTools {
		toolsList = append(toolsList, map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"schema":      tool.InputSchema,
		})
	}

	// Create response with server capabilities
	response := map[string]interface{}{
		"protocolVersion": ProtocolVersion,
		"serverInfo": map[string]string{
			"name":    "MCP SSE Server",
			"version": "1.0.0",
		},
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{
				"listChanged": false,
			},
			"supportedMethods": []string{
				"initialize",
				"tools/list",
				"tools/call",
				"notifications/initialized",
			},
		},
	}

	// Log response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("Initialize response: %s", string(responseJSON))

	return response, nil
}

// ListTools handles the tools/list method
func (h *Handler) ListTools(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Handling tools/list request")
	logger.Debug("tools/list request data: %+v", req)

	// Get tools from the registry
	allTools := h.toolRegistry.GetAllTools()

	// Convert to a response format
	toolsResponse := make([]map[string]interface{}, 0, len(allTools))
	for _, tool := range allTools {
		toolsResponse = append(toolsResponse, map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"schema":      tool.InputSchema,
		})
	}

	// Format as per the mcp-go ListToolsResult format
	response := map[string]interface{}{
		"tools": toolsResponse,
	}

	logger.Debug("tools/list response: %d tools found", len(toolsResponse))
	return response, nil
}

// HandleInitialized handles the notifications/initialized notification
func (h *Handler) HandleInitialized(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Received initialized notification from client: %s", sess.ID)
	// This is a notification, so no response is required
	return nil, nil
}

// ExecuteTool handles the tools/call method
func (h *Handler) ExecuteTool(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Info("Handling tools/call request")

	// Log the full request for debugging
	reqJSON, _ := json.Marshal(req)
	logger.Debug("tools/call request data: %s", string(reqJSON))

	// Parse the params
	var params struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}

	paramsBytes, _ := json.Marshal(req.Params)
	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		logger.Error("Failed to parse tools/call params: %v", err)
		return nil, jsonrpc.InvalidParamsError(err.Error())
	}

	logger.Debug("Tool requested: %s with arguments: %v", params.Name, params.Arguments)

	// Check if the tool exists
	tool, exists := h.toolRegistry.GetTool(params.Name)
	if !exists {
		logger.Error("Tool not found: %s", params.Name)
		logger.Debug("Available tools: %v", h.listAvailableTools())
		return nil, jsonrpc.InvalidParamsError(fmt.Sprintf("Tool not found: %s", params.Name))
	}

	logger.Info("Executing tool: %s", params.Name)

	// Execute the tool
	result, err := tool.Handler(params.Arguments)
	if err != nil {
		logger.Error("Tool execution error: %v", err)

		// Return an error result but not as a JSON-RPC error
		errorResult := map[string]interface{}{
			"content": []tools.Content{
				tools.NewTextContent(fmt.Sprintf("Error: %v", err)),
			},
			"isError": true,
		}

		return errorResult, nil
	}

	// Log the result for debugging
	resultJSON, _ := json.Marshal(result)
	logger.Debug("Tool execution result: %s", string(resultJSON))

	// Format the result to match the expected CallToolResult structure
	var content []tools.Content
	content = append(content, tools.NewTextContent(fmt.Sprintf("%v", result)))

	response := map[string]interface{}{
		"content": content,
		"isError": false,
	}

	return response, nil
}

// Helper function to list available tools
func (h *Handler) listAvailableTools() []string {
	tools := h.toolRegistry.GetAllTools()
	var names []string
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

// GetAllMethodHandlers returns all method handlers
func (h *Handler) GetAllMethodHandlers() map[string]func(*jsonrpc.Request, *session.Session) (interface{}, *jsonrpc.Error) {
	return map[string]func(*jsonrpc.Request, *session.Session) (interface{}, *jsonrpc.Error){
		"initialize":                h.Initialize,
		"tools/list":                h.ListTools,
		"tools/call":                h.ExecuteTool,
		"notifications/initialized": h.HandleInitialized,
	}
}
