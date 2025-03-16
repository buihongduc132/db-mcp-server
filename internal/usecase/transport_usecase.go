package usecase

import (
	"context"
	"fmt"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/domain/repositories"
)

// TransportUseCase handles transport-related operations
type TransportUseCase struct {
	transport   repositories.TransportRepository
	toolUseCase *CursorMCPUseCase
}

// NewTransportUseCase creates a new transport use case
func NewTransportUseCase(transport repositories.TransportRepository, toolUseCase *CursorMCPUseCase) *TransportUseCase {
	return &TransportUseCase{
		transport:   transport,
		toolUseCase: toolUseCase,
	}
}

// Start starts the transport and handles requests
func (u *TransportUseCase) Start(ctx context.Context) error {
	// Start the transport
	if err := u.transport.Start(ctx); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Get tools and send initial tools event
	toolsEvent, err := u.toolUseCase.GetToolsEvent(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tools: %w", err)
	}

	// Send tools event
	if err := u.transport.Send(toolsEvent); err != nil {
		return fmt.Errorf("failed to send tools event: %w", err)
	}

	// Start handling requests
	go u.handleRequests(ctx)

	return nil
}

// Stop stops the transport
func (u *TransportUseCase) Stop(ctx context.Context) error {
	return u.transport.Stop(ctx)
}

// handleRequests handles incoming requests from the transport
func (u *TransportUseCase) handleRequests(ctx context.Context) {
	requestChan, errorChan := u.transport.Receive()

	for {
		select {
		case request := <-requestChan:
			// Handle the request
			responseEvent, err := u.toolUseCase.ExecuteTool(ctx, request)
			if err != nil {
				u.transport.Send(&entities.MCPEvent{
					Type: "error",
					Payload: map[string]string{
						"error": err.Error(),
					},
				})
				continue
			}

			// Send the response
			if err := u.transport.Send(responseEvent); err != nil {
				fmt.Printf("Error sending response: %v\n", err)
			}

		case err := <-errorChan:
			// Handle transport errors
			fmt.Printf("Transport error: %v\n", err)

		case <-ctx.Done():
			// Context canceled, stop handling requests
			return
		}
	}
}
