package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"

	"github.com/FreePeak/db-mcp-server/pkg/db"
)

// Config holds all server configuration
type Config struct {
	ServerPort    int
	TransportMode string
	LogLevel      string
	DBConfig      DatabaseConfig    // Legacy single database config
	MultiDBConfig *db.MultiDBConfig // New multi-database config
	DBConfigFile  string            // Path to database configuration file
}

// DatabaseConfig holds database configuration (legacy support)
type DatabaseConfig struct {
	Type     string
	Host     string
	Port     int
	User     string
	Password string
	Name     string
}

// LoadConfig loads the configuration from environment variables and optional JSON config
func LoadConfig() (*Config, error) {
	// Load .env file if it exists
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: .env file not found, using environment variables only")
	} else {
		log.Printf("Loaded configuration from .env file")
	}

	port, _ := strconv.Atoi(getEnv("SERVER_PORT", "9090"))
	dbPort, _ := strconv.Atoi(getEnv("DB_PORT", "3306"))
	dbConfigFile := getEnv("DB_CONFIG_FILE", "config/databases.json")

	config := &Config{
		ServerPort:    port,
		TransportMode: getEnv("TRANSPORT_MODE", "sse"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		DBConfigFile:  dbConfigFile,
		DBConfig: DatabaseConfig{
			Type:     getEnv("DB_TYPE", "mysql"),
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", ""),
			Password: getEnv("DB_PASSWORD", ""),
			Name:     getEnv("DB_NAME", ""),
		},
	}

	// Try to load multi-database configuration from JSON file
	if _, err := os.Stat(config.DBConfigFile); err == nil {
		configData, err := os.ReadFile(config.DBConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read database config file: %w", err)
		}

		var multiDBConfig db.MultiDBConfig
		if err := json.Unmarshal(configData, &multiDBConfig); err != nil {
			return nil, fmt.Errorf("failed to parse database config file: %w", err)
		}

		config.MultiDBConfig = &multiDBConfig
	} else {
		// If no JSON config found, create a single connection config from environment variables
		config.MultiDBConfig = &db.MultiDBConfig{
			Connections: []db.DatabaseConnectionConfig{
				{
					ID:       "default",
					Type:     config.DBConfig.Type,
					Host:     config.DBConfig.Host,
					Port:     config.DBConfig.Port,
					User:     config.DBConfig.User,
					Password: config.DBConfig.Password,
					Name:     config.DBConfig.Name,
				},
			},
		}
	}

	return config, nil
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
