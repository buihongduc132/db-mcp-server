package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEnv(t *testing.T) {
	// Setup
	os.Setenv("TEST_ENV_VAR", "test_value")
	defer os.Unsetenv("TEST_ENV_VAR")

	// Test with existing env var
	value := getEnv("TEST_ENV_VAR", "default_value")
	assert.Equal(t, "test_value", value)

	// Test with non-existing env var
	value = getEnv("NON_EXISTING_VAR", "default_value")
	assert.Equal(t, "default_value", value)
}

func TestLoadConfig(t *testing.T) {
	// Clear any environment variables that might affect the test
	os.Unsetenv("SERVER_PORT")
	os.Unsetenv("TRANSPORT_MODE")
	os.Unsetenv("LOG_LEVEL")
	os.Unsetenv("DB_TYPE")
	os.Unsetenv("DB_HOST")
	os.Unsetenv("DB_PORT")
	os.Unsetenv("DB_USER")
	os.Unsetenv("DB_PASSWORD")
	os.Unsetenv("DB_NAME")
	
	// Get current working directory and handle .env file
	cwd, _ := os.Getwd()
	envPath := filepath.Join(cwd, ".env")
	tempPath := filepath.Join(cwd, ".env.bak")
	
	// Save existing .env if it exists
	envExists := false
	if _, err := os.Stat(envPath); err == nil {
		envExists = true
		err = os.Rename(envPath, tempPath)
		if err != nil {
			t.Fatalf("Failed to rename .env file: %v", err)
		}
		// Restore at the end
		defer func() {
			if envExists {
				os.Rename(tempPath, envPath)
			}
		}()
	}
	
	// Test with default values (no .env file and no environment variables)
	config := LoadConfig()
	assert.Equal(t, 9090, config.ServerPort)
	assert.Equal(t, "sse", config.TransportMode)
	assert.Equal(t, "info", config.LogLevel)
	assert.Equal(t, "mysql", config.DBConfig.Type)
	assert.Equal(t, "localhost", config.DBConfig.Host)
	assert.Equal(t, 3306, config.DBConfig.Port)
	assert.Equal(t, "", config.DBConfig.User)
	assert.Equal(t, "", config.DBConfig.Password)
	assert.Equal(t, "", config.DBConfig.Name)

	// Test with custom environment variables
	os.Setenv("SERVER_PORT", "8080")
	os.Setenv("TRANSPORT_MODE", "stdio")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("DB_TYPE", "postgres")
	os.Setenv("DB_HOST", "db.example.com")
	os.Setenv("DB_PORT", "5432")
	os.Setenv("DB_USER", "testuser")
	os.Setenv("DB_PASSWORD", "testpass")
	os.Setenv("DB_NAME", "testdb")
	defer func() {
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("TRANSPORT_MODE")
		os.Unsetenv("LOG_LEVEL")
		os.Unsetenv("DB_TYPE")
		os.Unsetenv("DB_HOST")
		os.Unsetenv("DB_PORT")
		os.Unsetenv("DB_USER")
		os.Unsetenv("DB_PASSWORD")
		os.Unsetenv("DB_NAME")
	}()

	config = LoadConfig()
	assert.Equal(t, 8080, config.ServerPort)
	assert.Equal(t, "stdio", config.TransportMode)
	assert.Equal(t, "debug", config.LogLevel)
	assert.Equal(t, "postgres", config.DBConfig.Type)
	assert.Equal(t, "db.example.com", config.DBConfig.Host)
	assert.Equal(t, 5432, config.DBConfig.Port)
	assert.Equal(t, "testuser", config.DBConfig.User)
	assert.Equal(t, "testpass", config.DBConfig.Password)
	assert.Equal(t, "testdb", config.DBConfig.Name)
}
