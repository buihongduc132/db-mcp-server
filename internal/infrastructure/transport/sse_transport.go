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

	// Validate we have the response writer and request
	if t.responseWriter == nil || t.request == nil {
		return fmt.Errorf("SSE transport requires response writer and request")
	}

	log.Printf("Starting SSE transport for client: %s", t.request.RemoteAddr)

	// Set SSE headers
	t.responseWriter.Header().Set("Content-Type", "text/event-stream")
	t.responseWriter.Header().Set("Cache-Control", "no-cache")
	t.responseWriter.Header().Set("Connection", "keep-alive")
	t.responseWriter.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

	// Send an initial comment to establish the connection
	fmt.Fprint(t.responseWriter, ": SSE connection established\n\n")
	if flusher, ok := t.responseWriter.(http.Flusher); ok {
		flusher.Flush()
	} else {
		return fmt.Errorf("response writer does not support flushing")
	}

	// Start goroutine to handle incoming events
	go t.handleEvents(ctx)

	// Start goroutine to handle POST requests as tool requests
	// Only handle POST requests with a request body
	if t.request.Method == http.MethodPost && t.request.ContentLength > 0 {
		go t.handleToolRequests()
	} else {
		log.Printf("Request is not a POST or has no body: %s %s", t.request.Method, t.request.URL.Path)
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

	log.Printf("Stopping SSE transport for client: %s", t.request.RemoteAddr)

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
		case event, ok := <-t.eventChan:
			if !ok {
				// Channel closed
				log.Println("Event channel closed, stopping SSE event handler")
				return
			}
			if err := t.writeEvent(event); err != nil {
				log.Printf("Error writing SSE event: %v", err)
				t.errorChan <- err
				return // Stop on write error
			}
		case <-ctx.Done():
			log.Println("Context done, stopping SSE event handler")
			return
		case <-t.request.Context().Done():
			log.Println("Client disconnected, stopping SSE event handler")
			return
		}
	}
}

// handleToolRequests handles tool requests from the client
func (t *SSETransport) handleToolRequests() {
	if t.request.Body == nil {
		log.Println("Request body is nil, cannot handle tool requests")
		t.errorChan <- fmt.Errorf("request body is nil")
		return
	}

	log.Printf("Processing tool request from %s", t.request.RemoteAddr)

	decoder := json.NewDecoder(t.request.Body)
	defer t.request.Body.Close()

	var toolRequest entities.MCPToolRequest
	if err := decoder.Decode(&toolRequest); err != nil {
		log.Printf("Error decoding tool request: %v", err)
		t.errorChan <- fmt.Errorf("error decoding tool request: %w", err)
		return
	}

	log.Printf("Received tool request: %s (ID: %s)", toolRequest.Name, toolRequest.ID)
	t.requestChan <- &toolRequest
}

// writeEvent writes an event to the response
func (t *SSETransport) writeEvent(event *entities.MCPEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	// Write the event to the response in SSE format
	_, err = fmt.Fprintf(t.responseWriter, "data: %s\n\n", string(eventJSON))
	if err != nil {
		return fmt.Errorf("error writing to response: %w", err)
	}

	// Flush the response to ensure the event is sent immediately
	if flusher, ok := t.responseWriter.(http.Flusher); ok {
		flusher.Flush()
		log.Printf("Sent SSE event: %s", string(eventJSON))
	} else {
		return fmt.Errorf("response writer does not support flushing")
	}

	return nil
}
