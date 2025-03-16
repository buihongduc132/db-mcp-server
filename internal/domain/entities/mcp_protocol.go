package entities

// MCPToolDefinition defines a tool that can be used by Cursor
type MCPToolDefinition struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	InputSchema interface{} `json:"inputSchema"`
}

// MCPToolArgument represents a named argument for a tool
type MCPToolArgument struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Required    bool        `json:"required"`
	Schema      interface{} `json:"schema,omitempty"`
}

// MCPParameterSchema defines the schema for tool parameters
type MCPParameterSchema struct {
	Type       string                       `json:"type"`
	Properties map[string]MCPPropertySchema `json:"properties"`
	Required   []string                     `json:"required,omitempty"`
}

// MCPPropertySchema defines the schema for a property in a tool parameter
type MCPPropertySchema struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

// MCPToolRequest defines a request from Cursor to execute a tool
type MCPToolRequest struct {
	JsonRPC string                 `json:"jsonrpc"` // Must be "2.0"
	ID      string                 `json:"id"`
	Method  string                 `json:"method"` // Should be "tools/call"
	Params  map[string]interface{} `json:"params"` // Following JSON-RPC spec, this should be "params" not "parameters"
}

// MCPToolResponse defines a response to a tool execution request
type MCPToolResponse struct {
	JsonRPC string                 `json:"jsonrpc"` // Must be "2.0"
	ID      string                 `json:"id"`
	Result  map[string]interface{} `json:"result,omitempty"` // Should be a map with content or other data
	Error   *MCPError              `json:"error,omitempty"`
}

// MCPError defines the error structure for JSON-RPC 2.0
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// MCPToolsEvent is sent to inform Cursor about available tools
// Note on JSON-RPC 2.0 format:
// - For notifications (no response expected): use Method and Params fields (no ID)
// - For responses to requests: use ID and Result fields (no Method)
// Never mix Method+Result in the same message as it violates the JSON-RPC 2.0 spec
type MCPToolsEvent struct {
	JsonRPC string                 `json:"jsonrpc"`          // Must be "2.0"
	ID      string                 `json:"id,omitempty"`     // Only for responses
	Method  string                 `json:"method,omitempty"` // Only for notifications/requests
	Params  map[string]interface{} `json:"params,omitempty"` // Used with Method for notifications
	Result  map[string]interface{} `json:"result,omitempty"` // Used with ID for responses
}

// MCPListToolsRequest defines a request from Cursor to list tools
type MCPListToolsRequest struct {
	JsonRPC string                 `json:"jsonrpc"` // Must be "2.0"
	ID      string                 `json:"id"`
	Method  string                 `json:"method"` // Should be "tools/list"
	Params  map[string]interface{} `json:"params,omitempty"`
}

// Constants for method names and jsonrpc version
const (
	JSONRPCVersion  = "2.0"
	MethodToolsList = "tools/list"
	MethodToolsCall = "tools/call"
)

// Error codes according to JSON-RPC 2.0 spec
const (
	ErrorCodeParseError     = -32700
	ErrorCodeInvalidRequest = -32600
	ErrorCodeMethodNotFound = -32601
	ErrorCodeInvalidParams  = -32602
	ErrorCodeInternalError  = -32603
	// -32000 to -32099 reserved for implementation-defined server errors
	ErrorCodeToolExecutionFailed = -32000
)
