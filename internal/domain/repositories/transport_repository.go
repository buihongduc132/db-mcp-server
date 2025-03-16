package repositories

import (
	"context"
	"mcpserver/internal/domain/entities"
)

// TransportRepository defines the interface for different transport mechanisms (stdio or SSE)
type TransportRepository interface {
	// Start starts the transport mechanism
	Start(ctx context.Context) error

	// Stop stops the transport mechanism
	Stop(ctx context.Context) error

	// Send sends an event to the client
	Send(event *entities.MCPEvent) error

	// Receive receives events from the client (asynchronously)
	Receive() (<-chan *entities.MCPToolRequest, <-chan error)
}
