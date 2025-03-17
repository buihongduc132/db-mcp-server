package config

import (
	"os"
	"strconv"
)

// Config holds all server configuration
type Config struct {
	ServerPort    int
	TransportMode string
	LogLevel      string
	DBConfig      DatabaseConfig
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Type     string
	Host     string
	Port     int
	User     string
	Password string
	Name     string
}

// LoadConfig loads the configuration from environment variables
func LoadConfig() *Config {
	port, _ := strconv.Atoi(getEnv("SERVER_PORT", "8080"))
	dbPort, _ := strconv.Atoi(getEnv("DB_PORT", "3306"))

	return &Config{
		ServerPort:    port,
		TransportMode: getEnv("TRANSPORT_MODE", "sse"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		DBConfig: DatabaseConfig{
			Type:     getEnv("DB_TYPE", "mysql"),
			Host:     getEnv("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnv("DB_USER", ""),
			Password: getEnv("DB_PASSWORD", ""),
			Name:     getEnv("DB_NAME", ""),
		},
	}
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
