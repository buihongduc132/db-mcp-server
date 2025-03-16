package transport

import (
	"fmt"
	"mcpserver/internal/config"
	"mcpserver/internal/domain/repositories"
	"net/http"
)

// Factory creates transport instances based on configuration
type Factory struct{}

// NewFactory creates a new transport factory
func NewFactory() *Factory {
	return &Factory{}
}

// CreateTransport creates a transport implementation based on the configuration
func (f *Factory) CreateTransport(mode config.TransportMode, w http.ResponseWriter, r *http.Request) (repositories.TransportRepository, error) {
	switch mode {
	case config.StdioTransport:
		return NewStdioTransport(), nil
	case config.SSETransport:
		return NewSSETransport(w, r), nil
	default:
		return nil, fmt.Errorf("unsupported transport mode: %s", mode)
	}
}
