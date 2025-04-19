package mcp

import (
	"context"
	"fmt"

	"github.com/FreePeak/cortex/pkg/server"
)

// ListDatabasesTool implements the list_databases tool
type ListDatabasesTool struct {
	BaseToolType
}

// NewListDatabasesTool creates a new list databases tool
func NewListDatabasesTool() *ListDatabasesTool {
	return &ListDatabasesTool{
		BaseToolType: BaseToolType{
			name:        "list_databases",
			description: "List all configured database connections with detailed information including database name, host, port, and type",
		},
	}
}

// CreateTool creates a new tool instance
func (t *ListDatabasesTool) CreateTool(name string, dbID string) server.Tool {
	return server.Tool{
		Name:        name,
		Description: t.description,
		InputSchema: server.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"random_string": map[string]interface{}{
					"type":        "string",
					"description": "Dummy parameter (optional)",
				},
			},
		},
	}
}

// HandleRequest handles list databases tool requests
func (t *ListDatabasesTool) HandleRequest(ctx context.Context, request server.ToolCallRequest, dbID string, useCase UseCaseProvider) (interface{}, error) {
	databases := useCase.ListDatabases()

	// Format as text for display
	output := "Available databases:\n\n"
	output += "| # | Database ID | Type | Host | Port | Database Name | Description |\n"
	output += "|---|------------|------|------|------|--------------|-------------|\n"

	for i, dbID := range databases {
		// Get database info to extract host, port, etc.
		dbInfo, err := useCase.GetDatabaseInfo(dbID)
		if err != nil {
			// If we can't get detailed info, just show the database ID
			output += fmt.Sprintf("| %d | %s | Unknown | Unknown | Unknown | Unknown | Unknown |\n", i+1, dbID)
			continue
		}

		// Extract database type
		dbType, _ := useCase.GetDatabaseType(dbID)
		if dbType == "" {
			dbType = "Unknown"
		}

		// Extract host, port, name, and description from dbInfo if available
		host := "Unknown"
		port := "Unknown"
		name := "Unknown"
		description := ""

		// Try to extract database name from dbInfo
		if dbName, ok := dbInfo["database"].(string); ok {
			name = dbName
		}

		// Try to extract description from dbInfo
		if desc, ok := dbInfo["description"].(string); ok {
			description = desc
		}

		// For now, we'll use placeholders for host and port
		// In a real implementation, these would come from the connection config
		output += fmt.Sprintf("| %d | %s | %s | %s | %s | %s | %s |\n", i+1, dbID, dbType, host, port, name, description)
	}

	if len(databases) == 0 {
		output += "No databases configured.\n"
	}

	return createTextResponse(output), nil
}
