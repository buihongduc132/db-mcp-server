package mcp

// Tool is an MCP tool
type Tool struct {
	name        string
	description string
	parameters  []Parameter
}

// Parameter is a tool parameter
type Parameter struct {
	name        string
	description string
	required    bool
	paramType   string
}

// NewTool creates a new MCP tool
func NewTool(name string, options ...func(*Tool)) *Tool {
	tool := &Tool{
		name:        name,
		description: "",
		parameters:  []Parameter{},
	}

	for _, option := range options {
		option(tool)
	}

	return tool
}

// WithDescription sets the tool description
func WithDescription(description string) func(*Tool) {
	return func(t *Tool) {
		t.description = description
	}
}

// WithString adds a string parameter to the tool
func WithString(name string, options ...func(*Parameter)) func(*Tool) {
	return func(t *Tool) {
		param := Parameter{
			name:      name,
			paramType: "string",
		}

		for _, option := range options {
			option(&param)
		}

		t.parameters = append(t.parameters, param)
	}
}

// WithArray adds an array parameter to the tool
func WithArray(name string, options ...func(*Parameter)) func(*Tool) {
	return func(t *Tool) {
		param := Parameter{
			name:      name,
			paramType: "array",
		}

		for _, option := range options {
			option(&param)
		}

		t.parameters = append(t.parameters, param)
	}
}

// WithBoolean adds a boolean parameter to the tool
func WithBoolean(name string, options ...func(*Parameter)) func(*Tool) {
	return func(t *Tool) {
		param := Parameter{
			name:      name,
			paramType: "boolean",
		}

		for _, option := range options {
			option(&param)
		}

		t.parameters = append(t.parameters, param)
	}
}

// WithNumber adds a numeric parameter to the tool
func WithNumber(name string, options ...func(*Parameter)) func(*Tool) {
	return func(t *Tool) {
		param := Parameter{
			name:      name,
			paramType: "number",
		}

		for _, option := range options {
			option(&param)
		}

		t.parameters = append(t.parameters, param)
	}
}

// Description sets the parameter description
func Description(description string) func(*Parameter) {
	return func(p *Parameter) {
		p.description = description
	}
}

// Required marks the parameter as required
func Required() func(*Parameter) {
	return func(p *Parameter) {
		p.required = true
	}
}

// CallToolRequest represents a request to call a tool
type CallToolRequest struct {
	Params struct {
		Arguments map[string]interface{}
	}
}

// CallToolResult represents the result of a tool call
type CallToolResult struct {
	Content string
}

// NewToolResultText creates a new text result
func NewToolResultText(content string) *CallToolResult {
	return &CallToolResult{
		Content: content,
	}
}
