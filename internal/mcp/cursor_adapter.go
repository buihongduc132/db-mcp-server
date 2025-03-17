package mcp

import (
	"encoding/json"
	"log"
)

// CursorTool represents a tool in the format expected by Cursor
type CursorTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// ConvertToolToCursor converts a Tool to the format expected by Cursor
func ConvertToolToCursor(tool Tool) CursorTool {
	inputSchema := []byte("{}")

	// If the tool has a schema property, use it as inputSchema
	if schema, ok := tool.Properties["schema"]; ok && schema != "" {
		inputSchema = []byte(schema)
	}

	return CursorTool{
		Name:        tool.Name,
		Description: tool.Description,
		InputSchema: inputSchema,
	}
}

// CursorToolsListResult represents the response for a tools/list request in Cursor format
type CursorToolsListResult struct {
	Tools []CursorTool `json:"tools"`
}

// ConvertToolListToCursor converts a list of tools to Cursor format
func ConvertToolListToCursor(tools []Tool) CursorToolsListResult {
	cursorTools := make([]CursorTool, 0, len(tools))

	for _, tool := range tools {
		cursorTools = append(cursorTools, ConvertToolToCursor(tool))
	}

	return CursorToolsListResult{
		Tools: cursorTools,
	}
}

// MarshalToolListChangedNotification creates a properly formatted tool list changed notification
func MarshalToolListChangedNotification(tools []Tool) []byte {
	// Convert tools to cursor format
	cursorTools := make([]CursorTool, 0, len(tools))
	for _, tool := range tools {
		cursorTools = append(cursorTools, ConvertToolToCursor(tool))
	}

	// Create notification in the format expected by Cursor
	notification := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/tools/list_changed",
		"params": map[string]interface{}{
			"tools": cursorTools,
		},
	}

	data, err := json.Marshal(notification)
	if err != nil {
		log.Printf("Error marshaling tool list notification: %v", err)
		return []byte("{}")
	}

	return data
}
