package repositories

import (
	"context"
	"mcpserver/internal/domain/entities"
)

// ClientRepository defines the interface for client operations
type ClientRepository interface {
	// AddClient adds a new client to the repository
	AddClient(ctx context.Context, client *entities.Client) error

	// RemoveClient removes a client from the repository
	RemoveClient(ctx context.Context, clientID string) error

	// GetClient gets a client by ID
	GetClient(ctx context.Context, clientID string) (*entities.Client, error)

	// BroadcastToSubscribers broadcasts an event to all clients subscribed to a table
	BroadcastToSubscribers(ctx context.Context, table string, event string) error
}
