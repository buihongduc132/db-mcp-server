package api

import (
	"context"
	"fmt"
	"mcpserver/internal/usecase"
	"net/http"
	"strings"
)

// EventHandler handles SSE event requests
type EventHandler struct {
	clientUseCase *usecase.ClientUseCase
}

// NewEventHandler creates a new event handler
func NewEventHandler(clientUseCase *usecase.ClientUseCase) *EventHandler {
	return &EventHandler{
		clientUseCase: clientUseCase,
	}
}

// ServeHTTP handles the HTTP request for SSE connections
func (h *EventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract client_id and subscriptions from query parameters
	clientID := r.URL.Query().Get("client_id")
	subscribe := r.URL.Query().Get("subscribe")
	if clientID == "" || subscribe == "" {
		http.Error(w, "Missing client_id or subscribe", http.StatusBadRequest)
		return
	}

	// Parse subscribed tables
	subscribedTables := strings.Split(subscribe, ",")

	// Register client
	client, err := h.clientUseCase.RegisterClient(r.Context(), clientID, subscribedTables)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to register client: %v", err), http.StatusInternalServerError)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Keep connection alive and send events
	for {
		select {
		case event := <-client.EventChan:
			fmt.Fprintf(w, "data: %s\n\n", event)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			// Client disconnected
			ctx := context.Background()
			h.clientUseCase.UnregisterClient(ctx, clientID)
			return
		}
	}
}
