package dbtools

import (
	"fmt"
	"sync"
)

// DatabaseConnectionConfig represents a database connection configuration
type DatabaseConnectionConfig struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	User        string `json:"user"`
	Password    string `json:"password"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

var (
	configMutex sync.RWMutex
	configs     = make(map[string]DatabaseConnectionConfig)
)

// RegisterDatabaseConfig registers a database configuration
func RegisterDatabaseConfig(config DatabaseConnectionConfig) {
	configMutex.Lock()
	defer configMutex.Unlock()
	configs[config.ID] = config
}

// GetDatabaseConfig returns a database configuration by ID
func GetDatabaseConfig(id string) (DatabaseConnectionConfig, error) {
	configMutex.RLock()
	defer configMutex.RUnlock()
	
	config, ok := configs[id]
	if !ok {
		return DatabaseConnectionConfig{}, fmt.Errorf("database configuration not found for ID: %s", id)
	}
	
	return config, nil
}
