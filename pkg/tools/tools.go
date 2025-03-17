package tools

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Tool represents a tool that can be executed by the MCP server
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema ToolInputSchema `json:"inputSchema"`
	Handler     ToolHandler
	// Optional metadata for the tool
	Category  string      `json:"-"` // Category for grouping tools
	CreatedAt time.Time   `json:"-"` // When the tool was registered
	RawSchema interface{} `json:"-"` // Alternative to InputSchema for complex schemas
}

// ToolInputSchema represents the schema for tool input parameters
type ToolInputSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties,omitempty"`
	Required   []string               `json:"required,omitempty"`
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
// Enhanced to use context for cancellation and timeouts
type ToolHandler func(ctx context.Context, params map[string]interface{}) (interface{}, error)

// ToolExecutionOptions provides options for tool execution
type ToolExecutionOptions struct {
	Timeout     time.Duration
	ProgressCB  func(progress float64, message string) // Optional progress callback
	TraceID     string                                 // For tracing/logging
	UserContext map[string]interface{}                 // User-specific context
}

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

	// Set creation time if not already set
	if tool.CreatedAt.IsZero() {
		tool.CreatedAt = time.Now()
	}

	r.tools[tool.Name] = tool
}

// DeregisterTool removes a tool from the registry
func (r *Registry) DeregisterTool(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, exists := r.tools[name]
	if exists {
		delete(r.tools, name)
		return true
	}
	return false
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

// GetToolsByCategory returns tools filtered by category
func (r *Registry) GetToolsByCategory(category string) []*Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var tools []*Tool
	for _, tool := range r.tools {
		if tool.Category == category {
			tools = append(tools, tool)
		}
	}
	return tools
}

// ExecuteTool executes a tool with the given name and parameters
func (r *Registry) ExecuteTool(ctx context.Context, name string, params map[string]interface{}) (interface{}, error) {
	tool, ok := r.GetTool(name)
	if !ok {
		return nil, fmt.Errorf("tool not found: %s", name)
	}

	// Execute with context
	return tool.Handler(ctx, params)
}

// ExecuteToolWithTimeout executes a tool with timeout
func (r *Registry) ExecuteToolWithTimeout(name string, params map[string]interface{}, timeout time.Duration) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return r.ExecuteTool(ctx, name, params)
}

// ValidateToolInput validates the input parameters against the tool's schema
func (r *Registry) ValidateToolInput(name string, params map[string]interface{}) error {
	tool, ok := r.GetTool(name)
	if !ok {
		return fmt.Errorf("tool not found: %s", name)
	}

	// Check required parameters
	for _, required := range tool.InputSchema.Required {
		if _, exists := params[required]; !exists {
			return fmt.Errorf("missing required parameter: %s", required)
		}
	}

	// TODO: Implement full JSON Schema validation if needed
	return nil
}

// ErrToolNotFound is returned when a tool is not found
var ErrToolNotFound = &ToolError{
	Code:    "tool_not_found",
	Message: "Tool not found",
}

// ErrToolExecutionFailed is returned when a tool execution fails
var ErrToolExecutionFailed = &ToolError{
	Code:    "tool_execution_failed",
	Message: "Tool execution failed",
}

// ErrInvalidToolInput is returned when the input parameters are invalid
var ErrInvalidToolInput = &ToolError{
	Code:    "invalid_tool_input",
	Message: "Invalid tool input",
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
