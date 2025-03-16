package usecase

import (
	"context"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/domain/repositories"
)

// ClientUseCase handles client-related operations
type ClientUseCase struct {
	clientRepo repositories.ClientRepository
}

// NewClientUseCase creates a new client use case
func NewClientUseCase(clientRepo repositories.ClientRepository) *ClientUseCase {
	return &ClientUseCase{
		clientRepo: clientRepo,
	}
}

// RegisterClient registers a new client with the given ID and subscriptions
func (uc *ClientUseCase) RegisterClient(ctx context.Context, clientID string, subscribedTables []string) (*entities.Client, error) {
	client := entities.NewClient(clientID, subscribedTables)
	err := uc.clientRepo.AddClient(ctx, client)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// UnregisterClient removes a client from the system
func (uc *ClientUseCase) UnregisterClient(ctx context.Context, clientID string) error {
	return uc.clientRepo.RemoveClient(ctx, clientID)
}

// GetClient gets a client by ID
func (uc *ClientUseCase) GetClient(ctx context.Context, clientID string) (*entities.Client, error) {
	return uc.clientRepo.GetClient(ctx, clientID)
}

// NotifySubscribers notifies all clients subscribed to a table about a change
func (uc *ClientUseCase) NotifySubscribers(ctx context.Context, table string, event string) error {
	return uc.clientRepo.BroadcastToSubscribers(ctx, table, event)
}
