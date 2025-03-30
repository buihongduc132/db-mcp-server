package mcp

// TODO: Refactor tool registration to reduce code duplication
// TODO: Implement better error handling with error types instead of generic errors
// TODO: Add metrics collection for tool usage and performance
// TODO: Improve logging with structured logs and log levels
// TODO: Consider implementing tool discovery mechanism to avoid hardcoded tool lists

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/FreePeak/cortex/pkg/server"
)

// DefaultToolPrefix is the default prefix for Cursor-compatible tool names
const DefaultToolPrefix = "mcp_cashflow_db_mcp_server_sse_"

// ToolRegistry structure to handle tool registration
type ToolRegistry struct {
	server          *ServerWrapper
	mcpServer       *server.MCPServer
	databaseUseCase UseCaseProvider
	factory         *ToolTypeFactory
}

// NewToolRegistry creates a new tool registry
func NewToolRegistry(mcpServer *server.MCPServer) *ToolRegistry {
	factory := NewToolTypeFactory()
	return &ToolRegistry{
		server:    NewServerWrapper(mcpServer),
		mcpServer: mcpServer,
		factory:   factory,
	}
}

// RegisterAllTools registers all tools with the server
func (tr *ToolRegistry) RegisterAllTools(ctx context.Context, useCase UseCaseProvider) error {
	tr.databaseUseCase = useCase

	// Get available databases
	dbList := useCase.ListDatabases()
	log.Printf("Found %d database connections for tool registration: %v", len(dbList), dbList)

	if len(dbList) == 0 {
		log.Printf("No databases available, registering mock tools")
		return tr.RegisterMockTools(ctx)
	}

	// Register database-specific tools
	registrationErrors := 0
	for _, dbID := range dbList {
		if err := tr.registerDatabaseTools(ctx, dbID); err != nil {
			log.Printf("Error registering tools for database %s: %v", dbID, err)
			registrationErrors++
		} else {
			log.Printf("Successfully registered tools for database %s", dbID)
		}
	}

	// Register common tools
	tr.registerCommonTools(ctx)

	// Only register cursor tools if we're not skipping them and the prefix isn't already configured
	// to be the cursor-compatible one
	if os.Getenv("MCP_SKIP_CURSOR_TOOLS") != "true" && getToolNamePrefix() != DefaultToolPrefix {
		log.Printf("Registering cursor-compatible tool aliases")
		if err := tr.RegisterCursorCompatibleTools(ctx); err != nil {
			log.Printf("Error registering cursor-compatible tools: %v", err)
			registrationErrors++
		}
	} else {
		log.Printf("Skipping cursor-compatible tool aliases - using direct naming")
	}

	if registrationErrors > 0 {
		return fmt.Errorf("errors occurred while registering tools for %d databases", registrationErrors)
	}
	return nil
}

// registerDatabaseTools registers all tools for a specific database
func (tr *ToolRegistry) registerDatabaseTools(ctx context.Context, dbID string) error {
	// Get all tool types from the factory
	toolTypeNames := []string{
		"query", "execute", "transaction", "performance", "schema",
	}

	log.Printf("Registering tools for database %s", dbID)

	// Check if this database actually exists
	dbInfo, err := tr.databaseUseCase.GetDatabaseInfo(dbID)
	if err != nil {
		return fmt.Errorf("failed to get database info for %s: %w", dbID, err)
	}

	log.Printf("Database %s info: %+v", dbID, dbInfo)

	// Register each tool type for this database
	registrationErrors := 0
	for _, typeName := range toolTypeNames {
		toolName := fmt.Sprintf("%s_%s", typeName, dbID)
		if err := tr.registerTool(ctx, typeName, toolName, dbID); err != nil {
			log.Printf("Error registering tool %s: %v", toolName, err)
			registrationErrors++
		} else {
			log.Printf("Successfully registered tool %s", toolName)
		}
	}

	if registrationErrors > 0 {
		return fmt.Errorf("errors occurred while registering %d tools", registrationErrors)
	}

	log.Printf("Completed registering tools for database %s", dbID)
	return nil
}

// registerTool registers a tool with the server
func (tr *ToolRegistry) registerTool(ctx context.Context, toolTypeName string, name string, dbID string) error {
	log.Printf("Registering tool '%s' of type '%s' (database: %s)", name, toolTypeName, dbID)

	toolTypeImpl, ok := tr.factory.GetToolType(toolTypeName)
	if !ok {
		return fmt.Errorf("failed to get tool type for '%s'", toolTypeName)
	}

	tool := toolTypeImpl.CreateTool(name, dbID)

	return tr.server.AddTool(ctx, tool, func(ctx context.Context, request server.ToolCallRequest) (interface{}, error) {
		response, err := toolTypeImpl.HandleRequest(ctx, request, dbID, tr.databaseUseCase)
		return FormatResponse(response, err)
	})
}

// registerCommonTools registers tools that are not specific to a database
func (tr *ToolRegistry) registerCommonTools(ctx context.Context) {
	// Register the list_databases tool
	_, ok := tr.factory.GetToolType("list_databases")
	if ok {
		if err := tr.registerTool(ctx, "list_databases", "list_databases", ""); err != nil {
			log.Printf("Error registering list_databases tool: %v", err)
		}
	}
}

// createToolAlias creates an alias for an existing tool
func (tr *ToolRegistry) createToolAlias(ctx context.Context, toolTypeName string, existingName string, aliasName string) error {
	log.Printf("Creating alias '%s' for tool '%s' of type '%s'", aliasName, existingName, toolTypeName)

	toolTypeImpl, ok := tr.factory.GetToolType(toolTypeName)
	if !ok {
		return fmt.Errorf("failed to get tool type for '%s'", toolTypeName)
	}

	// For aliases that apply to a specific database, extract the dbID from the existing name
	// This is a simplification - in a real implementation we'd have to look up the actual dbID
	dbID := ""

	// Create a new tool with the alias name but delegate to the original handler
	tool := toolTypeImpl.CreateTool(aliasName, dbID)

	return tr.server.AddTool(ctx, tool, func(ctx context.Context, request server.ToolCallRequest) (interface{}, error) {
		response, err := toolTypeImpl.HandleRequest(ctx, request, dbID, tr.databaseUseCase)
		return FormatResponse(response, err)
	})
}

// getToolNamePrefix returns the prefix to use for Cursor-compatible tool names
func getToolNamePrefix() string {
	// Check if we should disable using the cortex prefix completely
	if os.Getenv("MCP_DISABLE_CORTEX_PREFIX") == "true" {
		return DefaultToolPrefix
	}

	// Check if a custom prefix is defined in environment variable
	customPrefix := os.Getenv("MCP_TOOL_PREFIX")
	if customPrefix != "" {
		return customPrefix
	}

	// Use a consistent default that matches what Cursor expects
	return DefaultToolPrefix
}

// RegisterMockTools registers mock tools with the server when no db connections available
func (tr *ToolRegistry) RegisterMockTools(ctx context.Context) error {
	log.Printf("Registering mock tools")

	// For each tool type, register a mock tool
	for toolTypeName := range tr.factory.toolTypes {
		mockToolName := fmt.Sprintf("mock_%s", toolTypeName)

		toolTypeImpl, ok := tr.factory.GetToolType(toolTypeName)
		if !ok {
			log.Printf("Failed to get tool type for '%s'", toolTypeName)
			continue
		}

		tool := toolTypeImpl.CreateTool(mockToolName, "mock")

		err := tr.server.AddTool(ctx, tool, func(ctx context.Context, request server.ToolCallRequest) (interface{}, error) {
			response, err := toolTypeImpl.HandleRequest(ctx, request, "mock", tr.databaseUseCase)
			return FormatResponse(response, err)
		})

		if err != nil {
			log.Printf("Failed to register mock tool '%s': %v", mockToolName, err)
			continue
		}

		// Create cursor-compatible alias for the mock tool
		cursorName := mockToolName
		if strings.HasPrefix(toolTypeName, "database_") {
			cursorName = strings.TrimPrefix(toolTypeName, "database_")
		}

		if cursorName != mockToolName {
			err = tr.createToolAlias(ctx, toolTypeName, mockToolName, cursorName)
			if err != nil {
				log.Printf("Failed to create cursor alias for mock tool '%s': %v", mockToolName, err)
			}
		}
	}

	return nil
}

// RegisterCursorCompatibleTools registers aliases for all tools with Cursor-compatible naming
func (tr *ToolRegistry) RegisterCursorCompatibleTools(ctx context.Context) error {
	// Check if we should skip cursor tool registration
	if os.Getenv("MCP_SKIP_CURSOR_TOOLS") == "true" {
		log.Printf("Skipping cursor tool registration due to MCP_SKIP_CURSOR_TOOLS=true")
		return nil
	}

	prefix := getToolNamePrefix()

	// If prefix is already the default, we don't need to create aliases
	if prefix == DefaultToolPrefix {
		log.Printf("Using standard prefix '%s', skipping duplicate tool registration", prefix)
		return nil
	}

	// Get all registered databases
	databases := tr.databaseUseCase.ListDatabases()
	log.Printf("Creating Cursor-compatible aliases with prefix '%s' for %d databases", prefix, len(databases))

	// For each database and tool type, create a cursor-compatible alias
	for _, dbID := range databases {
		for _, toolType := range []string{"query", "execute", "transaction", "performance", "schema"} {
			sourceName := fmt.Sprintf("%s_%s", toolType, dbID)
			targetName := fmt.Sprintf("%s%s_%s", prefix, dbID, toolType)

			// Skip if the target name already starts with the standard prefix to avoid duplicates
			if strings.HasPrefix(targetName, DefaultToolPrefix) {
				log.Printf("Skipping duplicate tool alias: %s", targetName)
				continue
			}

			// Register the alias tool
			if err := tr.createToolAlias(ctx, toolType, sourceName, targetName); err != nil {
				log.Printf("Warning: failed to create cursor-compatible alias '%s': %v", targetName, err)
			} else {
				log.Printf("Created cursor-compatible alias '%s' -> '%s'", targetName, sourceName)
			}
		}
	}

	// Don't forget the list_databases tool
	listDbSource := "list_databases"
	listDbTarget := fmt.Sprintf("%slist_databases", prefix)

	// Skip if the target name already starts with the standard prefix to avoid duplicates
	if !strings.HasPrefix(listDbTarget, DefaultToolPrefix) {
		if err := tr.createToolAlias(ctx, "list_databases", listDbSource, listDbTarget); err != nil {
			log.Printf("Warning: failed to create list_databases alias '%s': %v", listDbTarget, err)
		} else {
			log.Printf("Created list_databases alias '%s' -> '%s'", listDbTarget, listDbSource)
		}
	}

	log.Printf("Registered cursor-compatible aliases with prefix '%s' for all tools", prefix)
	return nil
}
