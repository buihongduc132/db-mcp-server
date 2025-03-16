package database

import (
	"fmt"
	"mcpserver/internal/domain/repositories"
)

// Factory manages the creation of database repositories
type Factory struct{}

// NewFactory creates a new database factory
func NewFactory() *Factory {
	return &Factory{}
}

// CreateRepository creates a database repository based on the database type
func (f *Factory) CreateRepository(dbType, connectionString string) (repositories.DBRepository, error) {
	switch dbType {
	case "mysql":
		return NewMySQLRepository(connectionString)
	case "postgres":
		// Note: This requires the lib/pq package to be added to go.mod
		// Uncomment after adding the dependency
		// return NewPostgresRepository(connectionString)
		return nil, fmt.Errorf("postgres support requires adding github.com/lib/pq dependency")
	// Add other database types here as needed
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}
