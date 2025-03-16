package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcpserver/internal/domain/entities"
	"net/http"
	"os"
	"strings"
	"sync"
)

// SSETransport implements the transport repository interface for Server-Sent Events
type SSETransport struct {
	eventChan      chan interface{}
	requestChan    chan interface{}
	errorChan      chan error
	responseWriter http.ResponseWriter
	request        *http.Request
	mu             sync.Mutex
	started        bool
}

// NewSSETransport creates a new SSE transport
func NewSSETransport(w http.ResponseWriter, r *http.Request) *SSETransport {
	return &SSETransport{
		eventChan:      make(chan interface{}),
		requestChan:    make(chan interface{}),
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
		go t.handleToolRequests(ctx)
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

// Send sends an event to the client (legacy method)
func (t *SSETransport) Send(event interface{}) error {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		return fmt.Errorf("transport not started")
	}
	t.mu.Unlock()

	t.eventChan <- event
	return nil
}

// SendRaw sends a raw JSON string to the client as an SSE event
func (t *SSETransport) SendRaw(jsonStr string) error {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		return fmt.Errorf("transport not started")
	}
	t.mu.Unlock()

	// Write the event to the response in SSE format
	_, err := fmt.Fprintf(t.responseWriter, "data: %s\n\n", jsonStr)
	if err != nil {
		return fmt.Errorf("error writing raw JSON to SSE: %w", err)
	}

	// Flush the response to ensure the event is sent immediately
	if flusher, ok := t.responseWriter.(http.Flusher); ok {
		flusher.Flush()
		log.Printf("Sent SSE raw JSON: %s", jsonStr)
	} else {
		return fmt.Errorf("response writer does not support flushing")
	}

	return nil
}

// Receive receives events from the client
func (t *SSETransport) Receive() (<-chan interface{}, <-chan error) {
	return t.requestChan, t.errorChan
}

// handleEvents writes events to the response
func (t *SSETransport) handleEvents(ctx context.Context) {
	// Recover from any panics
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in handleEvents: %v", r)
		}
	}()

	for {
		select {
		case event, ok := <-t.eventChan:
			if !ok {
				// Channel closed
				log.Println("Event channel closed, stopping events handler")
				return
			}

			// Handle the event and log any errors
			if err := t.writeEvent(event); err != nil {
				log.Printf("Error writing event: %v", err)
				// Send the error to the error channel but don't panic
				select {
				case t.errorChan <- err:
					// Error sent successfully
				default:
					// Error channel full or closed, just log
					log.Printf("Could not send error to channel: %v", err)
				}

				// Check if we should stop due to a connection error
				if isConnectionError(err) {
					log.Printf("Connection error detected, stopping events handler: %v", err)
					return
				}
			}
		case <-ctx.Done():
			log.Println("Context done, stopping SSE events handler")
			return
		}
	}
}

// isConnectionError checks if an error is related to a connection issue
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection closed") ||
		strings.Contains(errStr, "response writer is nil") ||
		strings.Contains(errStr, "transport not started")
}

// handleToolRequests handles tool requests coming from the client
func (t *SSETransport) handleToolRequests(ctx context.Context) {
	// For SSE, we only get one request body
	// Parse it as a JSON-RPC 2.0 request
	var request entities.MCPToolRequest
	decoder := json.NewDecoder(t.request.Body)
	defer t.request.Body.Close()

	if err := decoder.Decode(&request); err != nil {
		log.Printf("Error parsing tool request: %v", err)
		t.errorChan <- fmt.Errorf("error parsing tool request: %w", err)

		// Send a properly formatted JSON-RPC error response
		errorResponse := &entities.MCPToolResponse{
			JsonRPC: entities.JSONRPCVersion,
			ID:      "null", // We don't know the ID
			Error: &entities.MCPError{
				Code:    entities.ErrorCodeParseError,
				Message: fmt.Sprintf("Invalid JSON: %v", err),
			},
		}
		errorJSON, _ := json.Marshal(errorResponse)
		t.SendRaw(string(errorJSON))
		return
	}

	// Validate JSON-RPC 2.0 format
	if request.JsonRPC != entities.JSONRPCVersion {
		log.Printf("Invalid JSON-RPC version: %s", request.JsonRPC)
		errorResponse := &entities.MCPToolResponse{
			JsonRPC: entities.JSONRPCVersion,
			ID:      request.ID,
			Error: &entities.MCPError{
				Code:    entities.ErrorCodeInvalidRequest,
				Message: fmt.Sprintf("Invalid JSON-RPC version, expected %s", entities.JSONRPCVersion),
			},
		}
		errorJSON, _ := json.Marshal(errorResponse)
		t.SendRaw(string(errorJSON))
		return
	}

	log.Printf("Received tool request: %s (ID: %s)", request.Method, request.ID)
	t.requestChan <- &request
}

// writeEvent writes an event to the response
func (t *SSETransport) writeEvent(event interface{}) error {
	var jsonBytes []byte
	var err error

	// Log type of event for debugging
	eventType := fmt.Sprintf("%T", event)
	fmt.Fprintf(os.Stderr, "Writing SSE event of type: %s\n", eventType)

	// Special handling for tools event to ensure correct format
	if toolsEvent, ok := event.(*entities.MCPToolsEvent); ok {
		// Log for debugging
		fmt.Fprintf(os.Stderr, "Processing SSE tools event with result: %v\n", toolsEvent.Result)

		// Ensure the event is properly formatted for Cursor
		jsonBytes, err = json.Marshal(toolsEvent)
		if err != nil {
			return fmt.Errorf("error marshaling tools event: %w", err)
		}

		// Log the JSON for debugging
		fmt.Fprintf(os.Stderr, "SSE Tools event JSON: %s\n", string(jsonBytes))
	} else {
		// For other event types
		jsonBytes, err = json.Marshal(event)
		if err != nil {
			return fmt.Errorf("error marshaling event: %w", err)
		}
	}

	// Lock to protect responseWriter access
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if responseWriter is nil before writing
	if t.responseWriter == nil {
		return fmt.Errorf("response writer is nil, connection may have been closed")
	}

	// Check if the transport is still started
	if !t.started {
		return fmt.Errorf("transport not started or already stopped")
	}

	// Write the event to the response in SSE format
	if _, err := fmt.Fprintf(t.responseWriter, "data: %s\n\n", string(jsonBytes)); err != nil {
		return fmt.Errorf("error writing SSE event: %w", err)
	}

	// Safely flush the response
	// Wrap the flusher check in a recover to handle any panics
	var flushErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic in writeEvent flush: %v", r)
				flushErr = fmt.Errorf("panic during flush: %v", r)
			}
		}()

		// Try to flush, but don't panic if it fails
		if flusher, ok := t.responseWriter.(http.Flusher); ok {
			if flusher != nil {
				flusher.Flush()
			} else {
				flushErr = fmt.Errorf("flusher is nil")
			}
		} else {
			log.Printf("ResponseWriter does not support http.Flusher interface")
		}
	}()

	if flushErr != nil {
		return flushErr
	}

	return nil
}
