package transport

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

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
}

// NewStdioTransport creates a new STDIO transport
func NewStdioTransport(sessionManager *session.Manager) *StdioTransport {
	return &StdioTransport{
		sessionManager: sessionManager,
		methodHandlers: make(map[string]mcp.MethodHandler),
		done:           make(chan struct{}),
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

	// Create a session for the STDIO client
	sess := t.sessionManager.CreateSession()
	logger.Info("Created new STDIO session %s", sess.ID)

	// Set up event callback for sending responses
	sess.EventCallback = func(event string, data []byte) error {
		// For STDIO, we only care about message events
		if event == "message" {
			// Write the message to stdout with a specific prefix and newline
			// This helps the client distinguish between JSON responses and log messages
			_, writeErr := fmt.Fprintf(os.Stdout, "MCPRPC:%s\n", string(data))
			if writeErr != nil {
				logger.Error("Error writing to stdout: %v", writeErr)
				return writeErr
			}
			// Force flush stdout to ensure immediate delivery
			if syncErr := os.Stdout.Sync(); syncErr != nil {
				logger.Error("Error syncing stdout: %v", syncErr)
				return syncErr
			}
		}
		return nil
	}

	// Mark the session as connected
	sess.Connected = true

	// Start reading from stdin
	t.running = true

	go t.readStdin(sess)

	return nil
}

// Stop stops the STDIO transport
func (t *StdioTransport) Stop() {
	if !t.running {
		return
	}

	t.running = false
	close(t.done)
}

// readStdin reads JSON-RPC requests from stdin and processes them
func (t *StdioTransport) readStdin(sess *session.Session) {
	reader := bufio.NewReader(os.Stdin)

	for t.running {
		// Read a line from stdin
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Received EOF on stdin, shutting down")
				t.Stop()
				return
			}
			logger.Error("Error reading from stdin: %v", err)
			continue
		}

		// Trim whitespace
		line = strings.TrimSpace(line)
		if line == "" {
			continue
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

		// Process the request
		go t.processRequest(sess, &req)
	}
}

// processRequest processes a JSON-RPC request
func (t *StdioTransport) processRequest(sess *session.Session, req *jsonrpc.Request) {
	// Log the received request
	reqJSON, _ := json.Marshal(req)
	logger.Debug("Received request: %s", string(reqJSON))
	logger.Info("Processing request: method=%s, id=%v", req.Method, req.ID)

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

	// Call the handler
	result, jsonRPCErr := handler(req, sess)

	// Check if this is a notification (no ID)
	isNotification := req.ID == nil

	// Send the response
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

		logger.Debug("Sending response: %s", string(responseJSON))

		// Send the response
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
		"error": map[string]interface{}{
			"code":    err.Code,
			"message": err.Message,
		},
	}

	// If the error has data, include it
	if err.Data != nil {
		response["error"].(map[string]interface{})["data"] = err.Data
	}

	responseJSON, jsonErr := json.Marshal(response)
	if jsonErr != nil {
		logger.Error("Failed to marshal error response: %v", jsonErr)
		return
	}

	logger.Debug("Sending error response: %s", string(responseJSON))

	// Send the error response
	if err := sess.SendEvent("message", responseJSON); err != nil {
		logger.Error("Failed to send error response: %v", err)
	}
}
