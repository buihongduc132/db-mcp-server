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
	toolRegistry   *tools.Registry
	methodHandlers map[string]MethodHandler
}

// MethodHandler is a function that handles a method
type MethodHandler func(*jsonrpc.Request, *session.Session) (interface{}, *jsonrpc.Error)

// NewHandler creates a new Handler
func NewHandler(toolRegistry *tools.Registry) *Handler {
	h := &Handler{
		toolRegistry:   toolRegistry,
		methodHandlers: make(map[string]MethodHandler),
	}

	// Register method handlers
	h.methodHandlers = map[string]MethodHandler{
		"initialize":                h.Initialize,
		"tools/list":                h.ListTools,
		"tools/execute":             h.ExecuteTool,
		"notifications/initialized": h.HandleInitialized,
	}

	return h
}

// RegisterTool registers a tool with the handler
func (h *Handler) RegisterTool(tool *tools.Tool) {
	h.toolRegistry.RegisterTool(tool)
}

// Initialize handles the initialize request
func (h *Handler) Initialize(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling initialize request")

	// Log the full request for debugging
	reqData, _ := json.Marshal(req)
	logger.Debug("Initialize request data: %s", string(reqData))

	// Create a struct to hold the parsed parameters
	params := struct {
		ProtocolVersion *string                `json:"protocolVersion"`
		Capabilities    map[string]interface{} `json:"capabilities"`
		ClientInfo      map[string]interface{} `json:"clientInfo"`
	}{}

	// Handle different types of Params
	if req.Params == nil {
		logger.Warn("Initialize request has no params")
	} else if paramsMap, ok := req.Params.(map[string]interface{}); ok {
		// If params is already a map, use it directly
		logger.Debug("Params is already a map, using directly")

		if pv, ok := paramsMap["protocolVersion"]; ok {
			if pvStr, ok := pv.(string); ok {
				params.ProtocolVersion = &pvStr
			}
		}

		if caps, ok := paramsMap["capabilities"]; ok {
			if capsMap, ok := caps.(map[string]interface{}); ok {
				params.Capabilities = capsMap
			}
		}

		if clientInfo, ok := paramsMap["clientInfo"]; ok {
			if clientInfoMap, ok := clientInfo.(map[string]interface{}); ok {
				params.ClientInfo = clientInfoMap
			}
		}
	} else {
		// Try to unmarshal from JSON
		logger.Debug("Trying to unmarshal params from JSON")
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			logger.Error("Failed to marshal params: %v", err)
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
		}

		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			logger.Error("Failed to unmarshal params: %v", err)
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
		}
	}

	// Log client info and capabilities
	if params.ClientInfo != nil {
		clientInfoJSON, _ := json.Marshal(params.ClientInfo)
		logger.Debug("Client info: %s", string(clientInfoJSON))
	}

	if params.Capabilities != nil {
		capsJSON, _ := json.Marshal(params.Capabilities)
		logger.Debug("Client capabilities: %s", string(capsJSON))
	}

	// Store client capabilities in session
	if params.Capabilities != nil {
		sess.SetCapabilities(params.Capabilities)
	}

	// Create response
	response := map[string]interface{}{
		"protocolVersion": "0.1.0",
		"serverInfo": map[string]interface{}{
			"name":    "MCP Server",
			"version": "0.1.0",
		},
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{
				"supportsTool": h.toolRegistry.GetAllTools() != nil && len(h.toolRegistry.GetAllTools()) > 0,
			},
			"methods": []string{
				"tools/list",
				"tools/execute",
			},
		},
	}

	// Log the response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("Initialize response: %s", string(responseJSON))

	return response, nil
}

// ListTools handles the tools/list request
func (h *Handler) ListTools(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling tools/list request")

	// Log the full request for debugging
	reqData, _ := json.Marshal(req)
	logger.Debug("ListTools request data: %s", string(reqData))

	// Get all tools from the registry
	tools := h.toolRegistry.GetAllTools()

	// Format tools according to the ListToolsResult format
	toolsData := make([]map[string]interface{}, 0, len(tools))
	for _, tool := range tools {
		toolData := map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"schema":      tool.InputSchema,
		}
		toolsData = append(toolsData, toolData)
	}

	// Create the response
	response := map[string]interface{}{
		"tools": toolsData,
	}

	logger.Debug("Found %d tools in response", len(toolsData))

	// Log the response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("ListTools response: %s", string(responseJSON))

	return response, nil
}

// HandleInitialized handles the notification/initialized request
func (h *Handler) HandleInitialized(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling notifications/initialized request")

	// Log the full request for debugging
	reqData, _ := json.Marshal(req)
	logger.Debug("HandleInitialized request data: %s", string(reqData))

	// Create the response (empty success response for notifications)
	response := map[string]interface{}{}

	// Log the response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("HandleInitialized response: %s", string(responseJSON))

	return response, nil
}

// ExecuteTool handles the tools/execute request
func (h *Handler) ExecuteTool(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling tools/execute request")

	// Log the full request for debugging
	reqData, _ := json.Marshal(req)
	logger.Debug("ExecuteTool request data: %s", string(reqData))

	// Create a struct to hold the parsed parameters
	params := struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
	}{
		// Initialize Arguments to avoid nil map
		Arguments: make(map[string]interface{}),
	}

	// Handle different types of Params
	if req.Params == nil {
		logger.Warn("ExecuteTool request has no params")
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Missing tool parameters",
		}
	} else if paramsMap, ok := req.Params.(map[string]interface{}); ok {
		// If params is already a map, use it directly
		logger.Debug("Params is already a map, using directly")

		if name, ok := paramsMap["name"].(string); ok {
			params.Name = name
		}

		if args, ok := paramsMap["arguments"].(map[string]interface{}); ok {
			params.Arguments = args
		} else if args, ok := paramsMap["arguments"]; ok {
			// If arguments is not nil but not a map, try to convert
			argsJSON, err := json.Marshal(args)
			if err != nil {
				logger.Error("Failed to marshal arguments: %v", err)
			} else {
				var argsMap map[string]interface{}
				if err := json.Unmarshal(argsJSON, &argsMap); err == nil {
					params.Arguments = argsMap
				}
			}
		}
	} else {
		// Try to unmarshal from JSON
		logger.Debug("Trying to unmarshal params from JSON")
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			logger.Error("Failed to marshal params: %v", err)
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
		}

		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			logger.Error("Failed to unmarshal params: %v", err)
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
		}
	}

	// Validate required parameters
	if params.Name == "" {
		logger.Error("Missing tool name")
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Missing tool name",
		}
	}

	logger.Debug("Executing tool: %s with arguments: %v", params.Name, params.Arguments)

	// Get the tool from the registry
	tool, exists := h.toolRegistry.GetTool(params.Name)
	if !exists {
		logger.Error("Tool not found: %s", params.Name)
		// Debug log to show available tools
		availableTools := h.listAvailableTools()
		logger.Debug("Available tools: %v", availableTools)

		return nil, &jsonrpc.Error{
			Code:    jsonrpc.MethodNotFoundCode,
			Message: fmt.Sprintf("Tool not found: %s", params.Name),
		}
	}

	// Execute the tool with the provided arguments
	result, err := tool.Handler(params.Arguments)
	if err != nil {
		logger.Error("Tool execution error: %v", err)
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.InternalErrorCode,
			Message: fmt.Sprintf("Tool execution error: %v", err),
		}
	}

	// Create the response
	response := map[string]interface{}{
		"result": result,
	}

	// Log the response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("ExecuteTool response: %s", string(responseJSON))

	return response, nil
}

// listAvailableTools returns a list of available tool names
func (h *Handler) listAvailableTools() []string {
	tools := h.toolRegistry.GetAllTools()
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

// GetMethodHandler returns a method handler for the given method
func (h *Handler) GetMethodHandler(method string) (MethodHandler, bool) {
	handler, ok := h.methodHandlers[method]
	return handler, ok
}

// GetAllMethodHandlers returns all method handlers
func (h *Handler) GetAllMethodHandlers() map[string]MethodHandler {
	return h.methodHandlers
}
