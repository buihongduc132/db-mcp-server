package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// TransportMode defines the MCP transport protocol
type TransportMode string

const (
	// StdioTransport is used for local development with stdio
	StdioTransport TransportMode = "stdio"

	// SSETransport is used for production with Server-Sent Events
	SSETransport TransportMode = "sse"
)

// DBConfig represents database configuration
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	Type     string
}

// Config represents application configuration
type Config struct {
	ServerPort    int
	DB            DBConfig
	TransportMode TransportMode
}

// LoadConfig loads configuration from environment variables with .env file support
func LoadConfig() (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load() // We don't return error if .env file doesn't exist

	// Initialize config with default values
	config := &Config{
		ServerPort:    9090,         // Default server port
		TransportMode: SSETransport, // Default to SSE transport
		DB: DBConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "user",
			Password: "password",
			DBName:   "dbname",
			Type:     "mysql", // Default database type
		},
	}

	// Override with environment variables if they exist
	if port := os.Getenv("SERVER_PORT"); port != "" {
		if portNum, err := strconv.Atoi(port); err == nil {
			config.ServerPort = portNum
		}
	}

	// Transport mode configuration
	if mode := os.Getenv("TRANSPORT_MODE"); mode != "" {
		switch mode {
		case string(StdioTransport):
			config.TransportMode = StdioTransport
		case string(SSETransport):
			config.TransportMode = SSETransport
		default:
			// Invalid transport mode, use default
			fmt.Printf("Warning: Invalid transport mode '%s', using default 'sse'\n", mode)
		}
	}

	// Database configuration
	if dbType := os.Getenv("DB_TYPE"); dbType != "" {
		config.DB.Type = dbType
	}

	if dbHost := os.Getenv("DB_HOST"); dbHost != "" {
		config.DB.Host = dbHost
	}

	if dbPort := os.Getenv("DB_PORT"); dbPort != "" {
		if portNum, err := strconv.Atoi(dbPort); err == nil {
			config.DB.Port = portNum
		}
	}

	if dbUser := os.Getenv("DB_USER"); dbUser != "" {
		config.DB.User = dbUser
	}

	if dbPassword := os.Getenv("DB_PASSWORD"); dbPassword != "" {
		config.DB.Password = dbPassword
	}

	if dbName := os.Getenv("DB_NAME"); dbName != "" {
		config.DB.DBName = dbName
	}

	return config, nil
}

// GetDSN returns the database connection string (DSN) based on the database type
func (c *DBConfig) GetDSN() string {
	switch c.Type {
	case "mysql":
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DBName)
	case "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", c.Host, c.Port, c.User, c.Password, c.DBName)
	default:
		// Default to MySQL format
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.User, c.Password, c.Host, c.Port, c.DBName)
	}
}
