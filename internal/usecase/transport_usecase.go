package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	// Log the tools event for debugging
	log.Printf("Sending tools event with %d tools", len(toolsEvent.Result.Tools))

	// Convert to JSON for more reliable transmission
	toolsJSON, err := json.Marshal(toolsEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal tools event: %w", err)
	}

	// Send the tools event as raw JSON
	if err := u.transport.SendRaw(string(toolsJSON)); err != nil {
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
		case requestObj := <-requestChan:
			// Type assert the request to the correct type
			mcpRequest, ok := requestObj.(*entities.MCPToolRequest)
			if !ok {
				log.Printf("Error: received request is not an MCPToolRequest: %T", requestObj)
				continue
			}

			// Handle the request with the correct type
			response, err := u.toolUseCase.ExecuteTool(ctx, mcpRequest)
			if err != nil {
				// Create an error response in JSON-RPC 2.0 format
				errorResponse := &entities.MCPToolResponse{
					JsonRPC: entities.JSONRPCVersion,
					ID:      mcpRequest.ID,
					Error: &entities.MCPError{
						Code:    entities.ErrorCodeInternalError,
						Message: err.Error(),
					},
				}

				// Marshal and send the error response
				errorJSON, err := json.Marshal(errorResponse)
				if err != nil {
					log.Printf("Error marshaling error response: %v", err)
					continue
				}

				if err := u.transport.SendRaw(string(errorJSON)); err != nil {
					log.Printf("Error sending error response: %v", err)
				}
				continue
			}

			// Marshal and send the response
			responseJSON, err := json.Marshal(response)
			if err != nil {
				log.Printf("Error marshaling response: %v", err)
				continue
			}

			if err := u.transport.SendRaw(string(responseJSON)); err != nil {
				log.Printf("Error sending response: %v", err)
			}

		case err := <-errorChan:
			// Handle transport errors
			log.Printf("Transport error: %v", err)

		case <-ctx.Done():
			// Context canceled, stop handling requests
			return
		}
	}
}
