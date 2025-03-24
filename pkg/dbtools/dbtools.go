package dbtools

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/config"
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

// Database connection manager (singleton)
var (
	dbManager *db.Manager
)

// DatabaseConnectionInfo represents detailed information about a database connection
type DatabaseConnectionInfo struct {
	ID       string       `json:"id"`
	Type     DatabaseType `json:"type"`
	Host     string       `json:"host"`
	Port     int          `json:"port"`
	Database string       `json:"database"`
	Status   string       `json:"status"`
	Latency  string       `json:"latency,omitempty"`
}

// InitDatabase initializes the database connections
func InitDatabase(cfg *config.Config) error {
	// Create database manager
	dbManager = db.NewDBManager()

	// Load configurations
	if cfg.MultiDBConfig != nil {
		// Convert config to JSON for loading
		configJSON, err := json.Marshal(cfg.MultiDBConfig)
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
	} else {
		return fmt.Errorf("no database configuration provided")
	}

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
					"description": "Database ID to use (optional if only one database is configured)",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the statement (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
			},
			Required: []string{"statement"},
		},
		Handler: handleExecute,
	})

	// Register transaction tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbTransaction",
		Description: "Manage database transactions (begin, commit, rollback, execute within transaction)",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"action": map[string]interface{}{
					"type":        "string",
					"description": "Transaction action: begin, commit, rollback, or execute",
					"enum":        []string{"begin", "commit", "rollback", "execute"},
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to use (optional if only one database is configured)",
				},
				"transactionId": map[string]interface{}{
					"type":        "string",
					"description": "Transaction ID (required for commit, rollback, and execute actions)",
				},
				"statement": map[string]interface{}{
					"type":        "string",
					"description": "SQL statement to execute (required for execute action)",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the statement (for prepared statements, used with execute action)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
			},
			Required: []string{"action"},
		},
		Handler: handleTransaction,
	})

	// Register performance analyzer tool
	registry.RegisterTool(&tools.Tool{
		Name:        "dbPerformanceAnalyzer",
		Description: "Identify slow queries and optimization opportunities",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"query": map[string]interface{}{
					"type":        "string",
					"description": "SQL query to analyze",
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to use (optional if only one database is configured)",
				},
			},
			Required: []string{"query"},
		},
		Handler: handlePerformanceAnalyzer,
	})

	// Register showConnectedDatabases tool
	registry.RegisterTool(&tools.Tool{
		Name:        "showConnectedDatabases",
		Description: "Shows information about all connected databases",
		InputSchema: tools.ToolInputSchema{
			Type:       "object",
			Properties: map[string]interface{}{},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			return showConnectedDatabases(ctx, params)
		},
	})

	return nil
}

// PingDatabase tests the connection to a database
func PingDatabase(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// Helper function to convert rows to a slice of maps
func rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	var results []map[string]interface{}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		// Create a map for this row
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Handle NULL values
			if val == nil {
				row[col] = nil
				continue
			}

			// Convert byte slices to strings for JSON compatibility
			switch v := val.(type) {
			case []byte:
				row[col] = string(v)
			case time.Time:
				row[col] = v.Format(time.RFC3339)
			default:
				row[col] = v
			}
		}

		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Helper function to extract string parameter
func getStringParam(params map[string]interface{}, key string) (string, bool) {
	value, ok := params[key].(string)
	return value, ok
}

// Helper function to extract float64 parameter and convert to int
func getIntParam(params map[string]interface{}, key string) (int, bool) {
	value, ok := params[key].(float64)
	if !ok {
		// Try to convert from JSON number
		if num, ok := params[key].(json.Number); ok {
			if v, err := num.Int64(); err == nil {
				return int(v), true
			}
		}
		return 0, false
	}
	return int(value), true
}

// Helper function to extract array of interface{} parameters
func getArrayParam(params map[string]interface{}, key string) ([]interface{}, bool) {
	value, ok := params[key].([]interface{})
	return value, ok
}
