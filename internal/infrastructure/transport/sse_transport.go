package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcpserver/internal/domain/entities"
	"net/http"
	"sync"
)

// SSETransport implements the transport repository interface for Server-Sent Events
type SSETransport struct {
	eventChan      chan *entities.MCPEvent
	requestChan    chan *entities.MCPToolRequest
	errorChan      chan error
	responseWriter http.ResponseWriter
	request        *http.Request
	mu             sync.Mutex
	started        bool
}

// NewSSETransport creates a new SSE transport
func NewSSETransport(w http.ResponseWriter, r *http.Request) *SSETransport {
	return &SSETransport{
		eventChan:      make(chan *entities.MCPEvent),
		requestChan:    make(chan *entities.MCPToolRequest),
		errorChan:      make(chan error),
		responseWriter: w,
		request:        r,
		started:        false,
	}
}

// Start starts the SSE transport
func (t *SSETransport) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started {
		return fmt.Errorf("transport already started")
	}

	// Set SSE headers
	t.responseWriter.Header().Set("Content-Type", "text/event-stream")
	t.responseWriter.Header().Set("Cache-Control", "no-cache")
	t.responseWriter.Header().Set("Connection", "keep-alive")
	t.responseWriter.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

	// Start goroutine to handle incoming events
	go t.handleEvents(ctx)

	// Start goroutine to handle POST requests as tool requests
	if t.request.Method == http.MethodPost {
		go t.handleToolRequests()
	}

	t.started = true
	return nil
}

// Stop stops the SSE transport
func (t *SSETransport) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started {
		return nil
	}

	close(t.eventChan)
	close(t.requestChan)
	close(t.errorChan)

	t.started = false
	return nil
}

// Send sends an event to the client
func (t *SSETransport) Send(event *entities.MCPEvent) error {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		return fmt.Errorf("transport not started")
	}
	t.mu.Unlock()

	t.eventChan <- event
	return nil
}

// Receive receives events from the client
func (t *SSETransport) Receive() (<-chan *entities.MCPToolRequest, <-chan error) {
	return t.requestChan, t.errorChan
}

// handleEvents writes events to the response
func (t *SSETransport) handleEvents(ctx context.Context) {
	for {
		select {
		case event := <-t.eventChan:
			if err := t.writeEvent(event); err != nil {
				t.errorChan <- err
				return
			}
		case <-ctx.Done():
			log.Println("Context done, stopping SSE transport")
			return
		case <-t.request.Context().Done():
			log.Println("Client disconnected")
			return
		}
	}
}

// handleToolRequests handles tool requests from the client
func (t *SSETransport) handleToolRequests() {
	decoder := json.NewDecoder(t.request.Body)
	defer t.request.Body.Close()

	var toolRequest entities.MCPToolRequest
	if err := decoder.Decode(&toolRequest); err != nil {
		t.errorChan <- fmt.Errorf("error decoding tool request: %w", err)
		return
	}

	t.requestChan <- &toolRequest
}

// writeEvent writes an event to the response
func (t *SSETransport) writeEvent(event *entities.MCPEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	// Write the event to the response
	fmt.Fprintf(t.responseWriter, "data: %s\n\n", string(eventJSON))
	if flusher, ok := t.responseWriter.(http.Flusher); ok {
		flusher.Flush()
	}

	return nil
}
