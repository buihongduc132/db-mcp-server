package dbtools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/db"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// DatabaseType represents a supported database type
type DatabaseType string

const (
	// MySQL database type
	MySQL DatabaseType = "mysql"
	// Postgres database type
	Postgres DatabaseType = "postgres"
)

// Config represents database configuration
type Config struct {
	ConfigFile  string
	Connections []ConnectionConfig
}

// ConnectionConfig represents a single database connection configuration
type ConnectionConfig struct {
	ID       string       `json:"id"`
	Type     DatabaseType `json:"type"`
	Host     string       `json:"host"`
	Port     int          `json:"port"`
	Name     string       `json:"name"`
	User     string       `json:"user"`
	Password string       `json:"password"`
}

// MultiDBConfig represents configuration for multiple database connections
type MultiDBConfig struct {
	Connections []ConnectionConfig `json:"connections"`
}

// Database connection manager (singleton)
var (
	dbManager *db.Manager
)

// DatabaseConnectionInfo represents detailed information about a database connection
type DatabaseConnectionInfo struct {
	ID      string       `json:"id"`
	Type    DatabaseType `json:"type"`
	Host    string       `json:"host"`
	Port    int          `json:"port"`
	Name    string       `json:"name"`
	Status  string       `json:"status"`
	Latency string       `json:"latency,omitempty"`
}

// InitDatabase initializes the database connections
func InitDatabase(cfg *Config) error {
	// Create database manager
	dbManager = db.NewDBManager()

	var multiDBConfig *MultiDBConfig

	// If config file is provided, load it
	if cfg != nil && cfg.ConfigFile != "" {
		// Read config file
		configData, err := os.ReadFile(cfg.ConfigFile)
		if err != nil {
			return fmt.Errorf("failed to read config file: %w", err)
		}

		// Parse config
		multiDBConfig = &MultiDBConfig{}
		if err := json.Unmarshal(configData, multiDBConfig); err != nil {
			return fmt.Errorf("failed to parse config file: %w", err)
		}
	} else if cfg != nil && len(cfg.Connections) > 0 {
		// Use connections from config
		multiDBConfig = &MultiDBConfig{
			Connections: cfg.Connections,
		}
	} else {
		// Try to load from environment variable
		dbConfigJSON := os.Getenv("DB_CONFIG")
		if dbConfigJSON != "" {
			multiDBConfig = &MultiDBConfig{}
			if err := json.Unmarshal([]byte(dbConfigJSON), multiDBConfig); err != nil {
				return fmt.Errorf("failed to parse DB_CONFIG environment variable: %w", err)
			}
		} else {
			return fmt.Errorf("no database configuration provided")
		}
	}

	// Load configurations (multiDBConfig will always be non-nil at this point)
	// Convert config to JSON for loading
	configJSON, err := json.Marshal(multiDBConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal database config: %w", err)
	}

	if err := dbManager.LoadConfig(configJSON); err != nil {
		return fmt.Errorf("failed to load database config: %w", err)
	}

	// Connect to all databases
	if err := dbManager.Connect(); err != nil {
		return fmt.Errorf("failed to connect to databases: %w", err)
	}

	// Log connected databases
	dbs := dbManager.ListDatabases()
	log.Printf("Connected to %d databases: %v", len(dbs), dbs)

	return nil
}

// CloseDatabase closes all database connections
func CloseDatabase() error {
	if dbManager == nil {
		return nil
	}
	return dbManager.Close()
}

// GetDatabase returns a database instance by ID
func GetDatabase(id string) (db.Database, error) {
	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}
	return dbManager.GetDB(id)
}

// ListDatabases returns a list of available database connections
func ListDatabases() []string {
	if dbManager == nil {
		return nil
	}
	return dbManager.ListDatabases()
}

// showConnectedDatabases returns information about all connected databases
func showConnectedDatabases(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	var connections []DatabaseConnectionInfo
	dbIDs := ListDatabases()

	for _, dbID := range dbIDs {
		database, err := GetDatabase(dbID)
		if err != nil {
			continue
		}

		// Get connection details
		connInfo := DatabaseConnectionInfo{
			ID: dbID,
		}

		// Check connection status and measure latency
		start := time.Now()
		err = database.Ping(ctx)
		latency := time.Since(start)

		if err != nil {
			connInfo.Status = "disconnected"
			connInfo.Latency = "n/a"
		} else {
			connInfo.Status = "connected"
			connInfo.Latency = latency.String()
		}

		connections = append(connections, connInfo)
	}

	return connections, nil
}

// RegisterDatabaseTools registers all database tools with the provided registry
func RegisterDatabaseTools(registry *tools.Registry) error {
	// Register schema explorer tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbSchema",
		Description: "Auto-discover database structure and relationships",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database name to explore (optional, leave empty for all databases)",
				},
				"table": map[string]interface{}{
					"type":        "string",
					"description": "Table name to explore (optional, leave empty for all tables)",
				},
			},
		},
		Handler: handleSchemaExplorer,
	})

	// Register query tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbQuery",
		Description: "Execute a database query that returns results",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"query": map[string]interface{}{
					"type":        "string",
					"description": "SQL query to execute",
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to query (optional if only one database is configured)",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the query (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Query timeout in milliseconds (default: 5000)",
				},
			},
			Required: []string{"query"},
		},
		Handler: handleQuery,
	})

	// Register execute tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbExecute",
		Description: "Execute a database statement that doesn't return results (INSERT, UPDATE, DELETE, etc.)",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"statement": map[string]interface{}{
					"type":        "string",
					"description": "SQL statement to execute",
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to query (optional if only one database is configured)",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the statement (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Statement timeout in milliseconds (default: 5000)",
				},
			},
			Required: []string{"statement"},
		},
		Handler: handleExecute,
	})

	// Register list databases tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbList",
		Description: "List all available database connections",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"showStatus": map[string]interface{}{
					"type":        "boolean",
					"description": "Show connection status and latency",
				},
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// Show connection status?
			showStatus, ok := params["showStatus"].(bool)
			if ok && showStatus {
				return showConnectedDatabases(ctx, params)
			}

			// Just list database IDs
			return ListDatabases(), nil
		},
	})

	// Register query builder tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbQueryBuilder",
		Description: "Build and execute a query using an object-oriented approach",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to query",
				},
				"table": map[string]interface{}{
					"type":        "string",
					"description": "Table name to query",
				},
				"select": map[string]interface{}{
					"type":        "array",
					"description": "Columns to select",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"where": map[string]interface{}{
					"type":        "object",
					"description": "Where conditions",
				},
				"orderBy": map[string]interface{}{
					"type":        "array",
					"description": "Order by columns",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"limit": map[string]interface{}{
					"type":        "integer",
					"description": "Limit results",
				},
				"offset": map[string]interface{}{
					"type":        "integer",
					"description": "Offset results",
				},
			},
			Required: []string{"database", "table"},
		},
		Handler: handleQueryBuilder,
	})

	return nil
}

// PingDatabase pings a database to check the connection
func PingDatabase(db *sql.DB) error {
	return db.Ping()
}

// rowsToMaps converts sql.Rows to a slice of maps
func rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Make a slice for the values
	values := make([]interface{}, len(columns))

	// Create references for the values
	valueRefs := make([]interface{}, len(columns))
	for i := range columns {
		valueRefs[i] = &values[i]
	}

	// Create the slice to store results
	var results []map[string]interface{}

	// Fetch rows
	for rows.Next() {
		// Scan the result into the pointers
		err := rows.Scan(valueRefs...)
		if err != nil {
			return nil, err
		}

		// Create a map for this row
		result := make(map[string]interface{})
		for i, column := range columns {
			val := values[i]

			// Handle null values
			if val == nil {
				result[column] = nil
				continue
			}

			// Convert bytes to string for easier JSON serialization
			if b, ok := val.([]byte); ok {
				result[column] = string(b)
			} else {
				result[column] = val
			}
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// getStringParam safely extracts a string parameter from the params map
func getStringParam(params map[string]interface{}, key string) (string, bool) {
	if val, ok := params[key].(string); ok {
		return val, true
	}
	return "", false
}

// getIntParam safely extracts an int parameter from the params map
func getIntParam(params map[string]interface{}, key string) (int, bool) {
	switch v := params[key].(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	case int64:
		return int(v), true
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i), true
		}
	}
	return 0, false
}

// getArrayParam safely extracts an array parameter from the params map
func getArrayParam(params map[string]interface{}, key string) ([]interface{}, bool) {
	if val, ok := params[key].([]interface{}); ok {
		return val, true
	}
	return nil, false
}
