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
	dbManager *db.DBManager
	dbConfig  *db.Config
)

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

// RegisterDatabaseTools registers all database tools with the provided registry
func RegisterDatabaseTools(registry *tools.Registry) error {
	// Register tools that work with multiple databases
	registry.RegisterTool(&tools.Tool{
		Name:        "dbSchema",
		Description: "Auto-discover database structure and relationships",
		Handler:     handleSchemaExplorer,
	})

	registry.RegisterTool(&tools.Tool{
		Name:        "dbQuery",
		Description: "Execute a database query that returns results",
		Handler:     handleQuery,
	})

	registry.RegisterTool(&tools.Tool{
		Name:        "dbExecute",
		Description: "Execute a database statement that doesn't return results (INSERT, UPDATE, DELETE, etc.)",
		Handler:     handleExecute,
	})

	registry.RegisterTool(&tools.Tool{
		Name:        "dbTransaction",
		Description: "Manage database transactions (begin, commit, rollback, execute within transaction)",
		Handler:     handleTransaction,
	})

	registry.RegisterTool(&tools.Tool{
		Name:        "dbPerformanceAnalyzer",
		Description: "Identify slow queries and optimization opportunities",
		Handler:     handlePerformanceAnalyzer,
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
