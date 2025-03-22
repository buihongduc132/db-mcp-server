package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/internal/session"
	"github.com/FreePeak/db-mcp-server/pkg/jsonrpc"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

const (
	// ProtocolVersion is the latest protocol version supported
	ProtocolVersion = "2024-11-05"
)

// Helper function to log request and response together
func logRequestResponse(method string, req *jsonrpc.Request, sess *session.Session, response interface{}, err *jsonrpc.Error) {
	// Marshal request and response to JSON for logging
	reqJSON, _ := json.Marshal(req)

	var respJSON []byte
	if err != nil {
		respJSON, _ = json.Marshal(err)
	} else {
		respJSON, _ = json.Marshal(response)
	}

	// Get request ID for correlation
	requestID := "null"
	if req.ID != nil {
		requestIDBytes, _ := json.Marshal(req.ID)
		requestID = string(requestIDBytes)
	}

	// Get session ID if available
	sessionID := "unknown"
	if sess != nil {
		sessionID = sess.ID
	}

	// Log using the RequestResponseLog function
	logger.RequestResponseLog(
		fmt.Sprintf("%s [ID:%s]", method, requestID),
		sessionID,
		string(reqJSON),
		string(respJSON),
	)
}

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
		"initialize":                       h.Initialize,
		"tools/list":                       h.ListTools,
		"tools/call":                       h.ExecuteTool,
		"tools/execute":                    h.ExecuteTool, // Alias for tools/call to support more clients
		"notifications/initialized":        h.HandleInitialized,
		"notifications/tools/list_changed": h.HandleToolsListChanged,
		"editor/context":                   h.HandleEditorContext, // New method for editor context
		"cancel":                           h.HandleCancel,
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

	// Log client info and capabilities at a high level
	if params.ClientInfo != nil {
		logger.Info("Client connected: %s v%s",
			params.ClientInfo["name"],
			params.ClientInfo["version"])
	}

	// Store client capabilities in session
	if params.Capabilities != nil {
		sess.SetCapabilities(params.Capabilities)

		// Log all capabilities for debugging
		capsJSON, _ := json.Marshal(params.Capabilities)
		logger.Debug("Client capabilities: %s", string(capsJSON))
	}

	// Get all registered tools
	tools := h.toolRegistry.GetAllTools()
	hasTools := len(tools) > 0

	// Log available tools
	if hasTools {
		logger.Info("Available tools: %s", h.ListAvailableTools())
	} else {
		logger.Warn("No tools available in registry")
	}

	// Check if the client supports tools
	clientSupportsTools := false
	if params.Capabilities != nil {
		if toolsCap, ok := params.Capabilities["tools"]; ok {
			// Client indicates it supports tools
			if toolsBool, ok := toolsCap.(bool); ok && toolsBool {
				clientSupportsTools = true
				logger.Info("Client indicates support for tools")
			} else {
				logger.Info("Client does not support tools: %v", toolsCap)
			}
		} else {
			logger.Info("Client did not specify tool capabilities")
		}
	}

	// Create response with the capabilities in the format expected by clients
	response := map[string]interface{}{
		"protocolVersion": ProtocolVersion,
		"serverInfo": map[string]interface{}{
			"name":    "MCP Server",
			"version": "1.0.0",
		},
		"capabilities": map[string]interface{}{
			"logging":   map[string]interface{}{},
			"prompts":   map[string]interface{}{"listChanged": true},
			"resources": map[string]interface{}{"subscribe": true, "listChanged": true},
			"tools":     map[string]interface{}{},
		},
	}

	// If client supports tools and we have tools, update the tools capability
	if clientSupportsTools && hasTools {
		// Send the notification after a brief delay
		go func() {
			// Wait a short time for client to process initialization
			time.Sleep(100 * time.Millisecond)

			// Use the new notification method
			h.NotifyToolsChanged(sess)
		}()
	}

	// Mark session as initialized
	sess.SetInitialized(true)

	// Log the request and response together
	logRequestResponse("initialize", req, sess, response, nil)

	return response, nil
}

// SendNotificationToClient sends a notification to the client via the session
func (h *Handler) SendNotificationToClient(sess *session.Session, method string, params map[string]interface{}) error {
	// Create a proper JSON-RPC notification
	notification := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}

	// Marshal to JSON
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		logger.Error("Failed to marshal notification: %v", err)
		return err
	}

	logger.Debug("Sending notification: %s", string(notificationJSON))

	// Send the event to the client
	return sess.SendEvent("message", notificationJSON)
}

// ListTools handles the tools/list request
func (h *Handler) ListTools(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling tools/list request")

	// Log request parameters for debugging
	if req.Params != nil {
		paramsJSON, _ := json.Marshal(req.Params)
		logger.Debug("ListTools params: %s", string(paramsJSON))
	}

	// Get all tools from the registry
	allTools := h.toolRegistry.GetAllTools()

	// Format tools according to the ListToolsResult format
	toolsData := make([]map[string]interface{}, 0, len(allTools))
	for _, tool := range allTools {
		// Format the tool data exactly as expected by the client
		toolData := map[string]interface{}{
			"name":        tool.Name,
			"description": tool.Description,
			"inputSchema": map[string]interface{}{
				"type":       tool.InputSchema.Type,
				"properties": tool.InputSchema.Properties,
				"required":   tool.InputSchema.Required,
			},
		}
		toolsData = append(toolsData, toolData)
	}

	// Create the response matching the expected format
	response := map[string]interface{}{
		"tools": toolsData,
	}

	// Log each tool being returned
	for i, tool := range allTools {
		logger.Debug("Tool %d: %s - %s", i+1, tool.Name, tool.Description)
	}

	logger.Info("Returning %d tools: %s", len(toolsData), h.ListAvailableTools())

	// Log the full response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("ListTools response: %s", string(responseJSON))

	// Log the request and response together
	logRequestResponse("tools/list", req, sess, response, nil)

	return response, nil
}

// HandleInitialized handles the notification/initialized request
func (h *Handler) HandleInitialized(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling notifications/initialized request")

	// Create the response (empty success response for notifications)
	response := map[string]interface{}{}

	// Log the request and response together
	logRequestResponse("notifications/initialized", req, sess, response, nil)

	return response, nil
}

// ExecuteTool handles the tools/call request
func (h *Handler) ExecuteTool(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling tools execution request: %s", req.Method)

	// Create a struct to hold the parsed parameters
	params := struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments"`
		Meta      *struct {
			ProgressToken string `json:"progressToken,omitempty"`
		} `json:"_meta,omitempty"`
	}{
		// Initialize Arguments to avoid nil map
		Arguments: make(map[string]interface{}),
	}

	// Handle different types of Params
	if req.Params == nil {
		logger.Warn("ExecuteTool request has no params")
		jsonRPCErr := &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Missing tool parameters",
		}
		logRequestResponse(req.Method, req, sess, nil, jsonRPCErr)
		return nil, jsonRPCErr
	} else if paramsMap, ok := req.Params.(map[string]interface{}); ok {
		// If params is already a map, use it directly
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

		// Check for meta information
		if metaMap, ok := paramsMap["_meta"].(map[string]interface{}); ok {
			meta := struct {
				ProgressToken string `json:"progressToken,omitempty"`
			}{}
			if pt, ok := metaMap["progressToken"].(string); ok {
				meta.ProgressToken = pt
			}
			params.Meta = &meta
		}
	} else {
		// Try to unmarshal from JSON
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			logger.Error("Failed to marshal params: %v", err)
			jsonRPCErr := &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
			logRequestResponse(req.Method, req, sess, nil, jsonRPCErr)
			return nil, jsonRPCErr
		}

		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			logger.Error("Failed to unmarshal params: %v", err)
			jsonRPCErr := &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
			logRequestResponse(req.Method, req, sess, nil, jsonRPCErr)
			return nil, jsonRPCErr
		}
	}

	// Log the full request for debugging
	reqJSON, _ := json.Marshal(req)
	logger.Debug("Tool execution request: %s", string(reqJSON))

	// Validate required parameters
	if params.Name == "" {
		logger.Error("Missing tool name")
		jsonRPCErr := &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Missing tool name",
		}
		logRequestResponse(req.Method, req, sess, nil, jsonRPCErr)
		return nil, jsonRPCErr
	}

	logger.Info("Executing tool: %s", params.Name)

	// Get the tool from the registry
	tool, exists := h.toolRegistry.GetTool(params.Name)
	if !exists {
		logger.Error("Tool not found: %s", params.Name)
		// Debug log to show available tools
		availableTools := h.ListAvailableTools()
		logger.Debug("Available tools: %s", availableTools)

		jsonRPCErr := &jsonrpc.Error{
			Code:    jsonrpc.MethodNotFoundCode,
			Message: fmt.Sprintf("Tool not found: %s", params.Name),
		}
		logRequestResponse(req.Method, req, sess, nil, jsonRPCErr)
		return nil, jsonRPCErr
	}

	// Log tool arguments for debugging
	argsJSON, _ := json.Marshal(params.Arguments)
	logger.Debug("Tool arguments: %s", string(argsJSON))

	// Validate tool input
	if err := h.toolRegistry.ValidateToolInput(params.Name, params.Arguments); err != nil {
		logger.Error("Tool input validation error: %v", err)

		// For input validation errors, return a structured error response
		response := map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": fmt.Sprintf("Error: %v", err),
				},
			},
			"isError": true,
		}

		logRequestResponse(req.Method, req, sess, response, nil)
		return response, nil
	}

	// Create context with timeout and cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Default timeout
	defer cancel()

	// Store request ID in context for cancellation
	type requestIDKey struct{}
	if req.ID != nil {
		idBytes, _ := json.Marshal(req.ID)
		ctx = context.WithValue(ctx, requestIDKey{}, string(idBytes))
	}

	// Add progress notification if requested
	var progressChan chan float64
	if params.Meta != nil && params.Meta.ProgressToken != "" {
		progressChan = make(chan float64)
		progressToken := params.Meta.ProgressToken

		// Start goroutine to handle progress updates
		go func() {
			for progress := range progressChan {
				// Create a properly formatted progress notification
				progressNotification := map[string]interface{}{
					"jsonrpc": "2.0",
					"method":  "notifications/progress",
					"params": map[string]interface{}{
						"progressToken": progressToken,
						"progress":      progress,
					},
				}

				notificationJSON, _ := json.Marshal(progressNotification)

				// Send directly as a message event
				if err := sess.SendEvent("message", notificationJSON); err != nil {
					logger.Error("Failed to send progress event: %v", err)
				}
			}
		}()
	}

	// Execute the tool with the provided arguments
	result, err := tool.Handler(ctx, params.Arguments)
	if progressChan != nil {
		close(progressChan)
	}

	if err != nil {
		logger.Error("Tool execution error: %v", err)

		// For tool execution errors, return a structured error response per the MCP spec
		// This lets the LLM see and handle the error
		response := map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": fmt.Sprintf("Error: %v", err),
				},
			},
			"isError": true,
		}

		// Log the full response for debugging
		responseJSON, _ := json.Marshal(response)
		logger.Debug("Tool error response: %s", string(responseJSON))

		logRequestResponse(req.Method, req, sess, response, nil)
		return response, nil
	}

	// Format content based on the result type
	var content []map[string]interface{}

	switch typedResult := result.(type) {
	case string:
		// If result is a string, use it directly
		content = append(content, map[string]interface{}{
			"type": "text",
			"text": typedResult,
		})
	case []tools.Content:
		// If the result is already a Content array, use it directly
		for _, c := range typedResult {
			content = append(content, map[string]interface{}{
				"type": c.Type,
				"text": c.Text,
			})
		}
	case tools.Result:
		// If the result is a Result object, use its content
		for _, c := range typedResult.Content {
			content = append(content, map[string]interface{}{
				"type": c.Type,
				"text": c.Text,
			})
		}

		// Create the response with the isError flag
		response := map[string]interface{}{
			"content": content,
			"isError": typedResult.IsError,
		}

		// Log the full response for debugging
		responseJSON, _ := json.Marshal(response)
		logger.Debug("Tool result response: %s", string(responseJSON))

		logRequestResponse(req.Method, req, sess, response, nil)
		return response, nil
	default:
		// For other types, convert to JSON
		resultJSON, err := json.Marshal(result)
		if err != nil {
			logger.Error("Failed to marshal result: %v", err)
			content = append(content, map[string]interface{}{
				"type": "text",
				"text": fmt.Sprintf("%v", result),
			})
		} else {
			content = append(content, map[string]interface{}{
				"type": "text",
				"text": string(resultJSON),
			})
		}
	}

	// Create the response in the correct format for CallToolResult
	response := map[string]interface{}{
		"content": content,
		"isError": false,
	}

	// Log the full response for debugging
	responseJSON, _ := json.Marshal(response)
	logger.Debug("Tool success response: %s", string(responseJSON))

	// Log the request and response together
	logRequestResponse(req.Method, req, sess, response, nil)

	return response, nil
}

// HandleEditorContext handles editor context updates from the client
func (h *Handler) HandleEditorContext(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling editor/context request")

	// Parse editor context from request
	var editorContext map[string]interface{}

	if req.Params == nil {
		logger.Warn("Editor context request has no params")
		return map[string]interface{}{}, nil
	}

	// Try to convert params to a map
	if contextMap, ok := req.Params.(map[string]interface{}); ok {
		editorContext = contextMap
	} else {
		// Try to unmarshal from JSON
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			logger.Error("Failed to marshal editor context params: %v", err)
			return map[string]interface{}{}, nil
		}

		if err := json.Unmarshal(paramsJSON, &editorContext); err != nil {
			logger.Error("Failed to unmarshal editor context: %v", err)
			return map[string]interface{}{}, nil
		}
	}

	// Store editor context in session
	sess.SetData("editorContext", editorContext)

	// Log the context update (sanitized for privacy/size)
	var keys []string
	for k := range editorContext {
		keys = append(keys, k)
	}
	logger.Info("Updated editor context with fields: %s", strings.Join(keys, ", "))

	// Return empty success response
	return map[string]interface{}{}, nil
}

// ListAvailableTools returns a list of available tool names as a comma-separated string
func (h *Handler) ListAvailableTools() string {
	tools := h.toolRegistry.GetAllTools()
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}

	if len(names) == 0 {
		return "none"
	}

	return strings.Join(names, ", ")
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

// HandleCancel handles the cancel request
func (h *Handler) HandleCancel(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling cancel request")

	// Parse the request to get the ID to cancel
	var params struct {
		ID interface{} `json:"id"`
	}

	// Handle different types of Params
	if req.Params == nil {
		logger.Warn("Cancel request has no params")
		jsonRPCErr := &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Missing cancel parameters",
		}
		logRequestResponse("cancel", req, sess, nil, jsonRPCErr)
		return nil, jsonRPCErr
	} else if paramsMap, ok := req.Params.(map[string]interface{}); ok {
		// If params is already a map, use it directly
		if id, ok := paramsMap["id"]; ok {
			params.ID = id
		}
	} else {
		// Try to unmarshal from JSON
		paramsJSON, err := json.Marshal(req.Params)
		if err != nil {
			logger.Error("Failed to marshal params: %v", err)
			jsonRPCErr := &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
			logRequestResponse("cancel", req, sess, nil, jsonRPCErr)
			return nil, jsonRPCErr
		}

		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			logger.Error("Failed to unmarshal params: %v", err)
			jsonRPCErr := &jsonrpc.Error{
				Code:    jsonrpc.ParseErrorCode,
				Message: "Invalid params",
			}
			logRequestResponse("cancel", req, sess, nil, jsonRPCErr)
			return nil, jsonRPCErr
		}
	}

	// Log the cancellation request
	logger.Info("Received cancellation request for ID: %v", params.ID)

	// Create an empty response (for now, we just acknowledge the cancellation)
	// In a real implementation, you'd want to actually cancel any ongoing operations
	response := map[string]interface{}{}

	// Log the request and response together
	logRequestResponse("cancel", req, sess, response, nil)

	return response, nil
}

// HandleToolsListChanged handles the notifications/tools/list_changed notification
func (h *Handler) HandleToolsListChanged(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error) {
	logger.Debug("Handling notifications/tools/list_changed request")

	// This is a notification, so no response is expected
	// But we'll log the available tools for debugging purposes
	tools := h.ListAvailableTools()
	logger.Info("Tools list changed notification received. Available tools: %s", tools)

	// Create the response (empty success response for notifications)
	response := map[string]interface{}{}

	// Log the request and response together
	logRequestResponse("notifications/tools/list_changed", req, sess, response, nil)

	return response, nil
}

// NotifyToolsChanged sends a tools/list_changed notification to the client
// if the session has been initialized. This matches the behavior in mcp-go.
func (h *Handler) NotifyToolsChanged(sess *session.Session) {
	// Only send notification if session is initialized
	if !sess.IsInitialized() {
		logger.Debug("Not sending tools changed notification - session not initialized")
		return
	}

	// Create a formal notification format for tools/list_changed
	notification := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/tools/list_changed",
		"params":  map[string]interface{}{},
	}

	// Convert to JSON
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		logger.Error("Failed to marshal tools/list_changed notification: %v", err)
		return
	}

	logger.Info("Sending tools/list_changed notification")
	logger.Debug("Notification payload: %s", string(notificationJSON))

	// Send directly as a message event
	if err := sess.SendEvent("message", notificationJSON); err != nil {
		logger.Error("Failed to send tools/list_changed notification: %v", err)
	}
}
