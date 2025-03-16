package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mcpserver/internal/domain/entities"
	"os"
	"strings"
	"sync"
)

// StdioTransport implements the transport repository interface for stdio
type StdioTransport struct {
	eventChan   chan *entities.MCPEvent
	requestChan chan *entities.MCPToolRequest
	errorChan   chan error
	reader      *bufio.Reader
	writer      io.Writer
	mu          sync.Mutex
	started     bool
}

// NewStdioTransport creates a new stdio transport
func NewStdioTransport() *StdioTransport {
	return &StdioTransport{
		eventChan:   make(chan *entities.MCPEvent),
		requestChan: make(chan *entities.MCPToolRequest),
		errorChan:   make(chan error),
		reader:      bufio.NewReader(os.Stdin),
		writer:      os.Stdout,
		started:     false,
	}
}

// Start starts the stdio transport
func (t *StdioTransport) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.started {
		return fmt.Errorf("transport already started")
	}

	// Start goroutine to handle outgoing events (writing to stdout)
	go t.handleEvents(ctx)

	// Start goroutine to handle incoming requests (reading from stdin)
	go t.handleRequests(ctx)

	t.started = true
	log.Println("Started stdio transport")
	return nil
}

// Stop stops the stdio transport
func (t *StdioTransport) Stop(ctx context.Context) error {
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
func (t *StdioTransport) Send(event *entities.MCPEvent) error {
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
func (t *StdioTransport) Receive() (<-chan *entities.MCPToolRequest, <-chan error) {
	return t.requestChan, t.errorChan
}

// handleEvents writes events to stdout
func (t *StdioTransport) handleEvents(ctx context.Context) {
	for {
		select {
		case event := <-t.eventChan:
			if err := t.writeEvent(event); err != nil {
				t.errorChan <- err
			}
		case <-ctx.Done():
			log.Println("Context done, stopping stdio transport")
			return
		}
	}
}

// handleRequests reads requests from stdin
func (t *StdioTransport) handleRequests(ctx context.Context) {
	log.Println("Started reading requests from stdin")

	for {
		select {
		case <-ctx.Done():
			log.Println("Context done, stopping request handler")
			return
		default:
			// Read a line from stdin
			line, err := t.reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					// EOF means stdin was closed, which is a normal shutdown
					log.Println("EOF received, closing request handler")
					return
				}
				log.Printf("Error reading from stdin: %v", err)
				t.errorChan <- fmt.Errorf("error reading from stdin: %w", err)
				continue
			}

			// Trim any whitespace (including newlines) from the input
			line = strings.TrimSpace(line)

			// Skip empty lines
			if line == "" {
				continue
			}

			log.Printf("Received request: %s", line)

			// Parse the request
			var toolRequest entities.MCPToolRequest
			if err := json.Unmarshal([]byte(line), &toolRequest); err != nil {
				log.Printf("Error parsing request: %v, input: %s", err, line)
				t.errorChan <- fmt.Errorf("error parsing request: %w", err)
				continue
			}

			log.Printf("Parsed tool request: %s", toolRequest.Name)

			// Send the request to the channel
			t.requestChan <- &toolRequest
		}
	}
}

// writeEvent writes an event to the writer (stdout)
func (t *StdioTransport) writeEvent(event *entities.MCPEvent) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("error marshaling event: %w", err)
	}

	// Write the event to stdout without any extra formatting
	// This ensures Cursor can parse it correctly
	_, err = fmt.Fprint(t.writer, string(eventJSON))
	if err != nil {
		return err
	}

	// Add a newline after the JSON to flush the output
	_, err = fmt.Fprintln(t.writer)
	return err
}
