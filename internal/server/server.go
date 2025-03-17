package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcpserver/internal/mcp"
	"sort"
	"sync"
	"sync/atomic"
)

// MCPServer implements a Model Control Protocol server
type MCPServer struct {
	mu                   sync.RWMutex
	name                 string
	version              string
	instructions         string
	resources            map[string]resourceEntry
	resourceTemplates    map[string]resourceTemplateEntry
	prompts              map[string]mcp.Prompt
	promptHandlers       map[string]PromptHandlerFunc
	tools                map[string]ServerTool
	notificationHandlers map[string]NotificationHandlerFunc
	capabilities         serverCapabilities
	notifications        chan ServerNotification
	clientMu             sync.Mutex
	currentClient        NotificationContext
	initialized          atomic.Bool
}

type resourceEntry struct {
	resource mcp.Resource
	handler  ResourceHandlerFunc
}

type resourceTemplateEntry struct {
	template mcp.ResourceTemplate
	handler  ResourceTemplateHandlerFunc
}

type ServerTool struct {
	Tool    mcp.Tool
	Handler ToolHandlerFunc
}

type NotificationContext struct {
	ClientID  string
	SessionID string
}

type ServerNotification struct {
	Context      NotificationContext
	Notification mcp.JSONRPCNotification
}

type ResourceHandlerFunc func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error)
type ResourceTemplateHandlerFunc func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error)
type PromptHandlerFunc func(ctx context.Context, request mcp.GetPromptRequest) (*mcp.GetPromptResult, error)
type ToolHandlerFunc func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)
type NotificationHandlerFunc func(ctx context.Context, notification mcp.JSONRPCNotification)

type serverCapabilities struct {
	tools     *toolCapabilities
	resources *resourceCapabilities
	prompts   *promptCapabilities
	logging   bool
}

type resourceCapabilities struct {
	subscribe   bool
	listChanged bool
}

type promptCapabilities struct {
	listChanged bool
}

type toolCapabilities struct {
	listChanged bool
}

// ServerOption is a function that configures an MCPServer
type ServerOption func(*MCPServer)

// WithResourceCapabilities configures resource-related server capabilities
func WithResourceCapabilities(subscribe, listChanged bool) ServerOption {
	return func(s *MCPServer) {
		s.capabilities.resources = &resourceCapabilities{
			subscribe:   subscribe,
			listChanged: listChanged,
		}
	}
}

// WithPromptCapabilities configures prompt-related server capabilities
func WithPromptCapabilities(listChanged bool) ServerOption {
	return func(s *MCPServer) {
		s.capabilities.prompts = &promptCapabilities{
			listChanged: listChanged,
		}
	}
}

// WithToolCapabilities configures tool-related server capabilities
func WithToolCapabilities(listChanged bool) ServerOption {
	return func(s *MCPServer) {
		s.capabilities.tools = &toolCapabilities{
			listChanged: listChanged,
		}
	}
}

// WithLogging enables logging capabilities for the server
func WithLogging() ServerOption {
	return func(s *MCPServer) {
		s.capabilities.logging = true
	}
}

// WithInstructions sets the server instructions
func WithInstructions(instructions string) ServerOption {
	return func(s *MCPServer) {
		s.instructions = instructions
	}
}

// NewMCPServer creates a new MCP server instance
func NewMCPServer(name, version string, opts ...ServerOption) *MCPServer {
	s := &MCPServer{
		name:                 name,
		version:              version,
		resources:            make(map[string]resourceEntry),
		resourceTemplates:    make(map[string]resourceTemplateEntry),
		prompts:              make(map[string]mcp.Prompt),
		promptHandlers:       make(map[string]PromptHandlerFunc),
		tools:                make(map[string]ServerTool),
		notificationHandlers: make(map[string]NotificationHandlerFunc),
		notifications:        make(chan ServerNotification, 100),
		capabilities: serverCapabilities{
			tools:     nil,
			resources: nil,
			prompts:   nil,
			logging:   false,
		},
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// HandleMessage processes an incoming JSON-RPC message
func (s *MCPServer) HandleMessage(ctx context.Context, message json.RawMessage) mcp.JSONRPCMessage {
	var baseMessage struct {
		JSONRPC string      `json:"jsonrpc"`
		Method  string      `json:"method"`
		ID      interface{} `json:"id,omitempty"`
	}

	if err := json.Unmarshal(message, &baseMessage); err != nil {
		return createErrorResponse(nil, -32700, "Parse error")
	}

	// Handle notifications (no ID field)
	if baseMessage.ID == nil {
		var notification mcp.JSONRPCNotification
		if err := json.Unmarshal(message, &notification); err != nil {
			return createErrorResponse(nil, -32700, "Parse error")
		}
		return s.handleNotification(ctx, notification)
	}

	// Handle method calls
	switch baseMessage.Method {
	case "initialize":
		var request mcp.InitializeRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return createErrorResponse(baseMessage.ID, -32700, "Parse error")
		}
		return s.handleInitialize(ctx, baseMessage.ID, request)

	case "ping":
		var request mcp.PingRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return createErrorResponse(baseMessage.ID, -32700, "Parse error")
		}
		return s.handlePing(ctx, baseMessage.ID, request)

	case "tools/list":
		var request mcp.ListToolsRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return createErrorResponse(baseMessage.ID, -32700, "Parse error")
		}
		return s.handleListTools(ctx, baseMessage.ID, request)

	case "tools/call":
		var request mcp.CallToolRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return createErrorResponse(baseMessage.ID, -32700, "Parse error")
		}
		return s.handleToolCall(ctx, baseMessage.ID, request)

	// Add other method handlers here...

	default:
		return createErrorResponse(baseMessage.ID, -32601, fmt.Sprintf("Method not found: %s", baseMessage.Method))
	}
}

func (s *MCPServer) handleInitialize(ctx context.Context, id interface{}, request mcp.InitializeRequest) mcp.JSONRPCMessage {
	if s.initialized.Load() {
		return createErrorResponse(id, -32600, "Server already initialized")
	}

	// Store client information for notifications
	s.clientMu.Lock()
	s.currentClient = NotificationContext{
		ClientID:  request.Params.ClientID,
		SessionID: request.Params.SessionID,
	}
	s.clientMu.Unlock()

	s.initialized.Store(true)

	result := mcp.InitializeResult{
		Name:         s.name,
		Version:      s.version,
		Instructions: s.instructions,
		Capabilities: mcp.ServerCapabilities{
			Resources: s.capabilities.resources != nil,
			Prompts:   s.capabilities.prompts != nil,
			Tools:     s.capabilities.tools != nil,
			Logging:   s.capabilities.logging,
		},
	}

	// Send tool list notification after successful initialization
	go func() {
		if s.capabilities.tools != nil {
			s.sendToolListChangedNotification()
		}
	}()

	return createResponse(id, result)
}

func (s *MCPServer) handlePing(ctx context.Context, id interface{}, request mcp.PingRequest) mcp.JSONRPCMessage {
	return createResponse(id, mcp.PingResult{})
}

func (s *MCPServer) handleNotification(ctx context.Context, notification mcp.JSONRPCNotification) mcp.JSONRPCMessage {
	if handler, ok := s.notificationHandlers[notification.Notification.Method]; ok {
		handler(ctx, notification)
	}
	return mcp.JSONRPCMessage{}
}

func createResponse(id interface{}, result interface{}) mcp.JSONRPCMessage {
	return mcp.JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
}

func createErrorResponse(id interface{}, code int, message string) mcp.JSONRPCMessage {
	return mcp.JSONRPCMessage{
		JSONRPC: "2.0",
		ID:      id,
		Error: &mcp.JSONRPCError{
			Code:    code,
			Message: message,
		},
	}
}

// AddTool registers a new tool and its handler
func (s *MCPServer) AddTool(tool mcp.Tool, handler ToolHandlerFunc) {
	s.AddTools(ServerTool{Tool: tool, Handler: handler})
}

// AddTools registers multiple tools at once
func (s *MCPServer) AddTools(tools ...ServerTool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, tool := range tools {
		s.tools[tool.Tool.Name] = tool
	}

	// Send notification if capabilities are enabled and server is initialized
	if s.capabilities.tools != nil && s.capabilities.tools.listChanged && s.initialized.Load() {
		go s.sendToolListChangedNotification()
	}
}

// sendToolList sends the current list of tools to the client
func (s *MCPServer) sendToolList() {
	s.mu.RLock()
	tools := make([]mcp.Tool, 0, len(s.tools))
	for _, t := range s.tools {
		tools = append(tools, t.Tool)
	}
	s.mu.RUnlock()

	// Sort tools by name for consistent order
	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	// Log the tools being sent
	log.Printf("Sending tool list to client: %d tools available", len(tools))
	for i, tool := range tools {
		log.Printf("Tool %d: %s - %s", i+1, tool.Name, tool.Description)
	}
}

// handleListTools handles a request to list available tools
func (s *MCPServer) handleListTools(
	ctx context.Context,
	id interface{},
	request mcp.ListToolsRequest,
) mcp.JSONRPCMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert tools to a slice
	tools := make([]mcp.Tool, 0, len(s.tools))
	for _, t := range s.tools {
		tools = append(tools, t.Tool)
	}

	// Sort tools by name for consistent order
	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	// Convert to cursor format
	cursorResult := mcp.ConvertToolListToCursor(tools)

	return createResponse(id, cursorResult)
}

// sendToolListChangedNotification sends a notification to the client that the tool list has changed
func (s *MCPServer) sendToolListChangedNotification() {
	s.mu.RLock()
	tools := make([]mcp.Tool, 0, len(s.tools))
	for _, t := range s.tools {
		tools = append(tools, t.Tool)
	}
	s.mu.RUnlock()

	// Sort tools by name for consistent order
	sort.Slice(tools, func(i, j int) bool {
		return tools[i].Name < tools[j].Name
	})

	// Create properly formatted notification using the adapter
	notificationData := mcp.MarshalToolListChangedNotification(tools)

	// Parse the notification back into our struct
	var notification mcp.JSONRPCNotification
	if err := json.Unmarshal(notificationData, &notification); err != nil {
		log.Printf("Error unmarshaling notification: %v", err)
		return
	}

	// Queue the notification to be sent to the client
	s.clientMu.Lock()
	context := s.currentClient
	s.clientMu.Unlock()

	if context.ClientID != "" {
		s.notifications <- ServerNotification{
			Context:      context,
			Notification: notification,
		}
	}
}

// handleToolCall handles a request to call a tool
func (s *MCPServer) handleToolCall(
	ctx context.Context,
	id interface{},
	request mcp.CallToolRequest,
) mcp.JSONRPCMessage {
	s.mu.RLock()
	tool, ok := s.tools[request.Params.Name]
	s.mu.RUnlock()

	if !ok {
		return createErrorResponse(
			id,
			-32602, // Invalid params
			fmt.Sprintf("Tool not found: %s", request.Params.Name),
		)
	}

	result, err := tool.Handler(ctx, request)
	if err != nil {
		return createErrorResponse(id, -32603, err.Error()) // Internal error
	}

	return createResponse(id, result)
}
