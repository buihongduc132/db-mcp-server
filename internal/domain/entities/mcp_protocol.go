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
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Parameters map[string]interface{} `json:"parameters"`
}

// MCPToolResponse defines a response to a tool execution request
type MCPToolResponse struct {
	ID     string      `json:"id"`
	Status string      `json:"status"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// MCPEvent represents an event in the MCP protocol
type MCPEvent struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// MCPToolsEvent is sent to inform Cursor about available tools
type MCPToolsEvent struct {
	Tools []MCPToolDefinition `json:"tools"`
}
