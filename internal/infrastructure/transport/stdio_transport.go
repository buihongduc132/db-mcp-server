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

	log.Println("Starting stdio transport...")

	// Immediately send a diagnostic message to stderr (won't interfere with protocol)
	fmt.Fprintln(os.Stderr, "Stdio transport started and waiting for input...")

	// Start goroutine to handle outgoing events (writing to stdout)
	go t.handleEvents(ctx)

	// Start goroutine to handle incoming requests (reading from stdin)
	go t.handleRequests(ctx)

	t.started = true
	return nil
}

// Stop stops the stdio transport
func (t *StdioTransport) Stop(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.started {
		return nil
	}

	log.Println("Stopping stdio transport...")

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
		case event, ok := <-t.eventChan:
			if !ok {
				// Channel closed
				return
			}
			if err := t.writeEvent(event); err != nil {
				// Log to stderr so it doesn't interfere with protocol
				fmt.Fprintf(os.Stderr, "Error writing event: %v\n", err)
				t.errorChan <- err
			}
		case <-ctx.Done():
			// Log to stderr so it doesn't interfere with protocol
			fmt.Fprintln(os.Stderr, "Context done, stopping stdio events handler")
			return
		}
	}
}

// handleRequests reads requests from stdin
func (t *StdioTransport) handleRequests(ctx context.Context) {
	// Log to stderr so it doesn't interfere with protocol
	fmt.Fprintln(os.Stderr, "Started reading requests from stdin...")

	for {
		select {
		case <-ctx.Done():
			// Log to stderr so it doesn't interfere with protocol
			fmt.Fprintln(os.Stderr, "Context done, stopping request handler")
			return
		default:
			// Read a line from stdin
			line, err := t.reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					// EOF means stdin was closed, which is a normal shutdown
					// Log to stderr so it doesn't interfere with protocol
					fmt.Fprintln(os.Stderr, "EOF received, closing request handler")
					return
				}
				// Log to stderr so it doesn't interfere with protocol
				fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
				t.errorChan <- fmt.Errorf("error reading from stdin: %w", err)
				continue
			}

			// Trim any whitespace (including newlines) from the input
			line = strings.TrimSpace(line)

			// Skip empty lines
			if line == "" {
				continue
			}

			// Log to stderr so it doesn't interfere with protocol
			fmt.Fprintf(os.Stderr, "Received request: %s\n", line)

			// Parse the request
			var toolRequest entities.MCPToolRequest
			if err := json.Unmarshal([]byte(line), &toolRequest); err != nil {
				// Log to stderr so it doesn't interfere with protocol
				fmt.Fprintf(os.Stderr, "Error parsing request: %v, input: %s\n", err, line)
				t.errorChan <- fmt.Errorf("error parsing request: %w", err)
				continue
			}

			// Log to stderr so it doesn't interfere with protocol
			fmt.Fprintf(os.Stderr, "Parsed tool request: %s\n", toolRequest.Name)

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
	_, err = fmt.Fprintln(t.writer, string(eventJSON))
	if err != nil {
		return err
	}

	// Log to stderr for debugging (won't interfere with protocol)
	fmt.Fprintf(os.Stderr, "Sent event: %s\n", string(eventJSON))

	return nil
}
