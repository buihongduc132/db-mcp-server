package repositories

import (
	"context"
)

// TransportRepository defines the interface for different transport mechanisms (stdio or SSE)
type TransportRepository interface {
	// Start starts the transport mechanism
	Start(ctx context.Context) error

	// Stop stops the transport mechanism
	Stop(ctx context.Context) error

	// Send sends an event to the client (legacy format)
	// Deprecated: Use SendRaw instead with properly formatted JSON-RPC 2.0 messages
	Send(event interface{}) error

	// SendRaw sends a raw JSON string to the client
	// This should be used for sending JSON-RPC 2.0 formatted messages
	SendRaw(jsonStr string) error

	// Receive receives events from the client (asynchronously)
	Receive() (<-chan interface{}, <-chan error)
}
