package dbtools

import (
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

// Database connection instance (singleton)
var (
	dbInstance db.Database
	dbConfig   *db.Config
)

// InitDatabase initializes the database connection
func InitDatabase(cfg *config.Config) error {
	// Create database config from app config
	dbConfig = &db.Config{
		Type:            cfg.DBConfig.Type,
		Host:            cfg.DBConfig.Host,
		Port:            cfg.DBConfig.Port,
		User:            cfg.DBConfig.User,
		Password:        cfg.DBConfig.Password,
		Name:            cfg.DBConfig.Name,
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	// Create database instance
	database, err := db.NewDatabase(*dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create database instance: %w", err)
	}

	// Connect to the database
	if err := database.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	dbInstance = database
	log.Printf("Connected to %s database at %s:%d/%s",
		dbConfig.Type, dbConfig.Host, dbConfig.Port, dbConfig.Name)

	return nil
}

// CloseDatabase closes the database connection
func CloseDatabase() error {
	if dbInstance == nil {
		return nil
	}
	return dbInstance.Close()
}

// GetDatabase returns the database instance
func GetDatabase() db.Database {
	return dbInstance
}

// RegisterDatabaseTools registers all database tools with the provided registry
func RegisterDatabaseTools(registry *tools.Registry) {
	// Register query tool
	registry.RegisterTool(createQueryTool())

	// Register execute tool
	registry.RegisterTool(createExecuteTool())

	// Register transaction tool
	registry.RegisterTool(createTransactionTool())
	
	// Register schema explorer tool
	registry.RegisterTool(createSchemaExplorerTool())
}

// RegisterSchemaExplorerTool registers only the schema explorer tool
// This is useful when database connection fails but we still want to provide schema exploration
func RegisterSchemaExplorerTool(registry *tools.Registry) {
	registry.RegisterTool(createSchemaExplorerTool())
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
