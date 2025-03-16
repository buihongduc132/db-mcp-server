package server

import (
	"context"
	"errors"
	"mcpserver/internal/domain/entities"
	"sync"
)

// InMemoryClientRepository is an in-memory implementation of the ClientRepository interface
type InMemoryClientRepository struct {
	clients map[string]*entities.Client
	mutex   sync.RWMutex
}

// NewInMemoryClientRepository creates a new in-memory client repository
func NewInMemoryClientRepository() *InMemoryClientRepository {
	return &InMemoryClientRepository{
		clients: make(map[string]*entities.Client),
	}
}

// AddClient adds a new client to the repository
func (r *InMemoryClientRepository) AddClient(ctx context.Context, client *entities.Client) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.clients[client.ID]; exists {
		return errors.New("client already exists")
	}

	r.clients[client.ID] = client
	return nil
}

// RemoveClient removes a client from the repository
func (r *InMemoryClientRepository) RemoveClient(ctx context.Context, clientID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.clients[clientID]; !exists {
		return errors.New("client not found")
	}

	delete(r.clients, clientID)
	return nil
}

// GetClient gets a client by ID
func (r *InMemoryClientRepository) GetClient(ctx context.Context, clientID string) (*entities.Client, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	client, exists := r.clients[clientID]
	if !exists {
		return nil, errors.New("client not found")
	}

	return client, nil
}

// BroadcastToSubscribers broadcasts an event to all clients subscribed to a table
func (r *InMemoryClientRepository) BroadcastToSubscribers(ctx context.Context, table string, event string) error {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	for _, client := range r.clients {
		if client.IsSubscribedTo(table) {
			// Non-blocking send to prevent deadlocks
			select {
			case client.EventChan <- event:
				// Event sent successfully
			default:
				// Client's event channel is full or closed, ignore
			}
		}
	}

	return nil
}
