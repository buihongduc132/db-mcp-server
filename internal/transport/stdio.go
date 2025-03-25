package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/internal/mcp"
	"github.com/FreePeak/db-mcp-server/internal/session"
	"github.com/FreePeak/db-mcp-server/pkg/jsonrpc"
)

// StdioTransport implements the STDIO transport for the MCP server
type StdioTransport struct {
	sessionManager *session.Manager
	methodHandlers map[string]mcp.MethodHandler
	mu             sync.RWMutex
	running        bool
	done           chan struct{}
	currentSession *session.Session
	isCursor       bool
	bufWriter      *bufio.Writer
}

// NewStdioTransport creates a new STDIO transport
func NewStdioTransport(sessionManager *session.Manager) *StdioTransport {
	// Check if we're running in Cursor
	isCursor := false
	if cursorEnv := os.Getenv("CURSOR_EDITOR"); cursorEnv != "" {
		isCursor = true
		logger.Info("STDIO transport optimized for Cursor editor")
	}

	// Create buffered writer for stdout for better performance
	bufWriter := bufio.NewWriterSize(os.Stdout, 32*1024) // 32KB buffer

	return &StdioTransport{
		sessionManager: sessionManager,
		methodHandlers: make(map[string]mcp.MethodHandler),
		done:           make(chan struct{}),
		isCursor:       isCursor,
		bufWriter:      bufWriter,
	}
}

// RegisterMethodHandler registers a method handler
func (t *StdioTransport) RegisterMethodHandler(method string, handler mcp.MethodHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.methodHandlers[method] = handler
}

// GetMethodHandler gets a method handler by name
func (t *StdioTransport) GetMethodHandler(method string) (mcp.MethodHandler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	handler, ok := t.methodHandlers[method]
	return handler, ok
}

// Start starts the STDIO transport
func (t *StdioTransport) Start() error {
	if t.running {
		return fmt.Errorf("STDIO transport already running")
	}

	// Create a session for the STDIO client with a buffer for notifications
	sess := t.sessionManager.CreateSession()
	t.currentSession = sess
	logger.Info("Created new STDIO session %s", sess.ID)

	// Set up event callback for sending responses
	sess.EventCallback = func(event string, data []byte) error {
		// For STDIO, we only care about message events
		if event == "message" {
			// Write to buffered stdout for better performance
			t.mu.Lock()
			defer t.mu.Unlock()

			// For Cursor compatibility, we need to ensure clean protocol output
			// No debug messages, no color codes, just pure JSON

			// Write the exact JSON data with no formatting or modification
			_, writeErr := t.bufWriter.Write(data)
			if writeErr != nil {
				logger.Error("Error writing to stdout buffer: %v", writeErr)
				return writeErr
			}

			// Add newline after JSON
			if _, err := t.bufWriter.WriteString("\n"); err != nil {
				logger.Error("Error writing newline to stdout buffer: %v", err)
				return err
			}

			// Immediately flush after each message to ensure delivery
			if flushErr := t.bufWriter.Flush(); flushErr != nil {
				logger.Error("Error flushing stdout buffer: %v", flushErr)
				return flushErr
			}

			// For Cursor compatibility, also perform a sync on the underlying file
			if t.isCursor {
				if syncErr := os.Stdout.Sync(); syncErr != nil {
					logger.Error("Error syncing stdout: %v", syncErr)
					return syncErr
				}
			}
		}
		return nil
	}

	// Mark the session as connected
	sess.Connected = true
	t.running = true

	// Create a context that can be canceled when Stop is called
	ctx, cancel := context.WithCancel(context.Background())

	// Set up cleanup when Stop is called
	go func() {
		<-t.done
		cancel()
	}()

	// Start reading from stdin
	go t.readStdin(ctx, sess)

	return nil
}

// Stop stops the STDIO transport
func (t *StdioTransport) Stop() {
	if !t.running {
		return
	}

	logger.Info("Stopping STDIO transport...")
	t.running = false
	close(t.done)

	// Flush any remaining data in the buffer
	t.mu.Lock()
	if t.bufWriter != nil {
		if err := t.bufWriter.Flush(); err != nil {
			logger.Error("Error flushing buffer on shutdown: %v", err)
		}
	}
	t.mu.Unlock()

	// Mark session as disconnected and clean up
	if t.currentSession != nil {
		t.currentSession.Connected = false
		t.sessionManager.RemoveSession(t.currentSession.ID)
		t.currentSession = nil
	}

	logger.Info("STDIO transport stopped")
}

// readStdin reads JSON-RPC requests from stdin and processes them
func (t *StdioTransport) readStdin(ctx context.Context, sess *session.Session) {
	// Different buffer size for Cursor vs other clients
	bufferSize := 4096
	if t.isCursor {
		bufferSize = 65536 // 64KB for Cursor which might send larger messages
	}

	reader := bufio.NewReaderSize(os.Stdin, bufferSize)

	for {
		select {
		case <-ctx.Done():
			logger.Info("STDIO reader stopped: context canceled")
			return
		default:
			// Create a cancellable read operation
			lineChan := make(chan string, 1)
			errChan := make(chan error, 1)

			go func() {
				// Read a line from stdin
				line, err := reader.ReadString('\n')
				if err != nil {
					errChan <- err
					return
				}
				lineChan <- line
			}()

			// Wait for either a line, an error, or context cancellation
			select {
			case <-ctx.Done():
				return
			case err := <-errChan:
				if err == io.EOF {
					logger.Info("Received EOF on stdin, shutting down")
					t.Stop()
					return
				}
				logger.Error("Error reading from stdin: %v", err)
				// Continue trying to read unless it's a fatal error
				if err != io.EOF && !strings.Contains(err.Error(), "closed") {
					continue
				}
				t.Stop()
				return
			case line := <-lineChan:
				// Process the line
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				// Cursor may send large messages, log input size rather than content
				lineLength := len(line)
				if lineLength > 500 {
					logger.Debug("Received large request (length: %d bytes)", lineLength)
				} else {
					logger.Debug("Received request: %s", line)
				}

				// Parse the line as a JSON-RPC request
				var req jsonrpc.Request
				if err := json.Unmarshal([]byte(line), &req); err != nil {
					logger.Error("Failed to parse JSON-RPC request: %v", err)
					t.sendErrorResponse(sess, nil, &jsonrpc.Error{
						Code:    jsonrpc.ParseErrorCode,
						Message: "Invalid JSON: " + err.Error(),
					})
					continue
				}

				// Log the method and ID for tracing
				logger.Info("Processing request: method=%s, id=%v", req.Method, req.ID)

				// Process the request with timeout to prevent hanging
				// Use longer timeout for Cursor which might have longer-running operations
				timeout := 30 * time.Second
				if t.isCursor {
					timeout = 2 * time.Minute
				}

				reqCtx, cancel := context.WithTimeout(ctx, timeout)
				go func() {
					defer cancel()
					t.processRequest(reqCtx, sess, &req)
				}()
			}
		}
	}
}

// processRequest processes a JSON-RPC request
func (t *StdioTransport) processRequest(ctx context.Context, sess *session.Session, req *jsonrpc.Request) {
	startTime := time.Now()

	// Find the handler for the method
	handler, ok := t.GetMethodHandler(req.Method)
	if !ok {
		logger.Error("Method not found: %s", req.Method)
		t.sendErrorResponse(sess, req.ID, &jsonrpc.Error{
			Code:    jsonrpc.MethodNotFoundCode,
			Message: fmt.Sprintf("Method not found: %s", req.Method),
		})
		return
	}

	// Call the handler with context and request
	result, jsonRPCErr := handler(req, sess)

	// Check if this is a notification (no ID)
	isNotification := req.ID == nil

	// Send the response for non-notification requests
	if jsonRPCErr != nil {
		logger.Debug("Method handler error: %v", jsonRPCErr)
		t.sendErrorResponse(sess, req.ID, jsonRPCErr)
	} else if !isNotification {
		// Only send response for non-notifications
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			logger.Error("Failed to marshal response: %v", err)
			t.sendErrorResponse(sess, req.ID, &jsonrpc.Error{
				Code:    jsonrpc.InternalErrorCode,
				Message: "Failed to marshal response",
			})
			return
		}

		// Log response size rather than content for large responses
		responseSize := len(responseJSON)
		if responseSize > 500 {
			logger.Debug("Sending large response (size: %d bytes)", responseSize)
		} else {
			logger.Debug("Sending response: %s", string(responseJSON))
		}

		// Log execution time
		elapsed := time.Since(startTime)
		if elapsed > 500*time.Millisecond {
			logger.Info("Method %s completed in %v", req.Method, elapsed)
		}

		// Send the response through the session
		if err := sess.SendEvent("message", responseJSON); err != nil {
			logger.Error("Failed to send response: %v", err)
		}
	}
}

// sendErrorResponse sends a JSON-RPC error response
func (t *StdioTransport) sendErrorResponse(sess *session.Session, id interface{}, err *jsonrpc.Error) {
	response := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   err,
	}

	responseJSON, jsonErr := json.Marshal(response)
	if jsonErr != nil {
		logger.Error("Failed to marshal error response: %v", jsonErr)
		return
	}

	logger.Debug("Sending error response: %s", string(responseJSON))
	if sendErr := sess.SendEvent("message", responseJSON); sendErr != nil {
		logger.Error("Failed to send error response: %v", sendErr)
	}
}
