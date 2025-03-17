package mcp

const JSONRPC_VERSION = "2.0"

// JSONRPCMessage represents a JSON-RPC message
type JSONRPCMessage struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      interface{}   `json:"id,omitempty"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC error
type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// JSONRPCNotification represents a JSON-RPC notification
type JSONRPCNotification struct {
	JSONRPC      string       `json:"jsonrpc"`
	Notification Notification `json:"notification"`
}

// Notification represents a notification message
type Notification struct {
	Method string             `json:"method"`
	Params NotificationParams `json:"params"`
}

// NotificationParams represents notification parameters
type NotificationParams struct {
	AdditionalFields map[string]interface{} `json:"-"`
}

// InitializeRequest represents an initialize request
type InitializeRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  struct {
		ClientID  string `json:"clientId"`
		SessionID string `json:"sessionId"`
	} `json:"params"`
}

// InitializeResult represents an initialize response
type InitializeResult struct {
	Name         string             `json:"name"`
	Version      string             `json:"version"`
	Instructions string             `json:"instructions,omitempty"`
	Capabilities ServerCapabilities `json:"capabilities"`
}

// ServerCapabilities represents server capabilities
type ServerCapabilities struct {
	Resources bool `json:"resources"`
	Prompts   bool `json:"prompts"`
	Tools     bool `json:"tools"`
	Logging   bool `json:"logging"`
}

// PingRequest represents a ping request
type PingRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
}

// PingResult represents a ping response
type PingResult struct{}

// Resource represents a resource
type Resource struct {
	URI         string            `json:"uri"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Type        string            `json:"type"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// ResourceTemplate represents a resource template
type ResourceTemplate struct {
	Template    string            `json:"template"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Type        string            `json:"type"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// ResourceContents represents resource contents
type ResourceContents struct {
	URI      string                 `json:"uri"`
	Contents map[string]interface{} `json:"contents"`
}

// ReadResourceRequest represents a read resource request
type ReadResourceRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  struct {
		URI string `json:"uri"`
	} `json:"params"`
}

// ReadResourceResult represents a read resource result
type ReadResourceResult struct {
	Resources []ResourceContents `json:"resources"`
}

// Prompt represents a prompt definition
type Prompt struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Arguments   map[string]Argument `json:"arguments,omitempty"`
}

// Argument represents a prompt argument
type Argument struct {
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// GetPromptRequest represents a get prompt request
type GetPromptRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  struct {
		Name      string                 `json:"name"`
		Arguments map[string]interface{} `json:"arguments,omitempty"`
	} `json:"params"`
}

// GetPromptResult represents a get prompt result
type GetPromptResult struct {
	Content string `json:"content"`
}

// Tool represents a tool definition
type Tool struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// CallToolRequest represents a call tool request
type CallToolRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  struct {
		Name string                 `json:"name"`
		Args map[string]interface{} `json:"arguments,omitempty"`
	} `json:"params"`
}

// CallToolResult represents a call tool result
type CallToolResult struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// ListToolsRequest represents a request to list tools
type ListToolsRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  struct {
		Cursor string `json:"cursor,omitempty"`
		Limit  int    `json:"limit,omitempty"`
	} `json:"params"`
}

// ListToolsResult represents the result of a list tools request
type ListToolsResult struct {
	Tools []Tool `json:"tools"`
	// Pagination fields
	NextCursor string `json:"nextCursor,omitempty"`
	HasMore    bool   `json:"hasMore,omitempty"`
}
