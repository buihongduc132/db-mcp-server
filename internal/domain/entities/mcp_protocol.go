package entities

// MCPToolDefinition defines a tool that can be used by Cursor
type MCPToolDefinition struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Schema      MCPParameterSchema `json:"schema"`
}

// MCPParameterSchema defines the schema for tool parameters
type MCPParameterSchema struct {
	Type       string                       `json:"type"`
	Properties map[string]MCPPropertySchema `json:"properties"`
	Required   []string                     `json:"required"`
}

// MCPPropertySchema defines the schema for a property in a tool parameter
type MCPPropertySchema struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

// MCPToolRequest defines a request from Cursor to execute a tool
type MCPToolRequest struct {
	JsonRPC    string                 `json:"jsonrpc"` // Must be "2.0"
	ID         string                 `json:"id"`
	Method     string                 `json:"method"` // Should be "execute_tool"
	Parameters map[string]interface{} `json:"params"` // Note: changed from "parameters" to "params"
}

// MCPToolResponse defines a response to a tool execution request
type MCPToolResponse struct {
	JsonRPC string      `json:"jsonrpc"` // Must be "2.0"
	ID      string      `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *MCPError   `json:"error,omitempty"`
}

// MCPError defines the error structure for JSON-RPC 2.0
type MCPError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// MCPToolsEvent is sent to inform Cursor about available tools
type MCPToolsEvent struct {
	JsonRPC string              `json:"jsonrpc"` // Must be "2.0"
	Method  string              `json:"method"`  // Should be "tools_available"
	Params  MCPToolsEventParams `json:"params"`
}

// MCPToolsEventParams contains the parameters for a tools event
type MCPToolsEventParams struct {
	Tools []MCPToolDefinition `json:"tools"`
}

// Constants for method names and jsonrpc version
const (
	JSONRPCVersion       = "2.0"
	MethodToolsAvailable = "tools_available"
	MethodExecuteTool    = "execute_tool"
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
