package tools

import (
	"fmt"
	"sync"
)

// Tool represents a tool that can be executed by the MCP server
type Tool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	InputSchema map[string]interface{} `json:"schema"`
	Handler     ToolHandler
}

// Result represents a tool execution result
type Result struct {
	Result  interface{} `json:"result,omitempty"`
	Content []Content   `json:"content,omitempty"`
	IsError bool        `json:"isError,omitempty"`
}

// Content represents content in a tool execution result
type Content struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// NewTextContent creates a new text content
func NewTextContent(text string) Content {
	return Content{
		Type: "text",
		Text: text,
	}
}

// ToolHandler is a function that handles a tool execution
type ToolHandler func(params map[string]interface{}) (interface{}, error)

// Registry is a registry of tools
type Registry struct {
	tools map[string]*Tool
	mu    sync.RWMutex
}

// NewRegistry creates a new tool registry
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]*Tool),
	}
}

// RegisterTool registers a tool with the registry
func (r *Registry) RegisterTool(tool *Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[tool.Name] = tool
}

// GetTool gets a tool by name
func (r *Registry) GetTool(name string) (*Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	tool, ok := r.tools[name]
	return tool, ok
}

// GetAllTools returns all registered tools
func (r *Registry) GetAllTools() []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tools := make([]*Tool, 0, len(r.tools))
	for _, tool := range r.tools {
		tools = append(tools, tool)
	}
	return tools
}

// ExecuteTool executes a tool with the given name and parameters
func (r *Registry) ExecuteTool(name string, params map[string]interface{}) (interface{}, error) {
	tool, ok := r.GetTool(name)
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}

	return tool.Handler(params)
}

// ErrToolNotFound is returned when a tool is not found
var ErrToolNotFound = &ToolError{
	Code:    "tool_not_found",
	Message: "Tool not found",
}

// ToolError represents an error that occurred while executing a tool
type ToolError struct {
	Code    string
	Message string
	Data    interface{}
}

// Error returns a string representation of the error
func (e *ToolError) Error() string {
	return e.Message
}
