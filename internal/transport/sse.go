package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/internal/mcp"
	"github.com/FreePeak/db-mcp-server/internal/session"
	"github.com/FreePeak/db-mcp-server/pkg/jsonrpc"
)

const (
	// SSE Headers
	headerContentType               = "Content-Type"
	headerCacheControl              = "Cache-Control"
	headerConnection                = "Connection"
	headerAccessControlAllowOrigin  = "Access-Control-Allow-Origin"
	headerAccessControlAllowHeaders = "Access-Control-Allow-Headers"
	headerAccessControlAllowMethods = "Access-Control-Allow-Methods"

	// SSE Content type
	contentTypeEventStream = "text/event-stream"

	// Heartbeat interval in seconds
	heartbeatInterval = 30
)

// SSETransport implements the Server-Sent Events transport
type SSETransport struct {
	sessionManager *session.Manager
	methodHandlers map[string]mcp.MethodHandler
	mu             sync.RWMutex
	basePath       string
	activeRequests map[string]*sseRequest
	requestMu      sync.Mutex
}

// sseRequest holds information about an active SSE request
type sseRequest struct {
	session       *session.Session
	ctx           context.Context
	cancel        context.CancelFunc
	writer        http.ResponseWriter
	flusher       http.Flusher
	lastHeartbeat time.Time
}

// NewSSETransport creates a new SSE transport
func NewSSETransport(sessionManager *session.Manager, basePath string) *SSETransport {
	return &SSETransport{
		sessionManager: sessionManager,
		methodHandlers: make(map[string]mcp.MethodHandler),
		basePath:       basePath,
		activeRequests: make(map[string]*sseRequest),
	}
}

// RegisterMethodHandler registers a method handler
func (t *SSETransport) RegisterMethodHandler(method string, handler mcp.MethodHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.methodHandlers[method] = handler
}

// GetMethodHandler gets a method handler by name
func (t *SSETransport) GetMethodHandler(method string) (mcp.MethodHandler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	handler, ok := t.methodHandlers[method]
	return handler, ok
}

// HandleSSE handles SSE connections
func (t *SSETransport) HandleSSE(w http.ResponseWriter, r *http.Request) {
	// Check if the request accepts text/event-stream
	if r.Header.Get("Accept") != "text/event-stream" {
		http.Error(w, "This endpoint requires Accept: text/event-stream", http.StatusBadRequest)
		return
	}

	// Get or create a session
	var sess *session.Session
	sessionID := r.URL.Query().Get("sessionId")

	if sessionID != "" {
		var ok bool
		sess, ok = t.sessionManager.GetSession(sessionID)
		if !ok {
			// Session not found, create a new one
			sess = t.sessionManager.CreateSession()
			sessionID = sess.ID
			logger.Info("Session not found, created new session: %s", sessionID)
		} else {
			logger.Info("Using existing session: %s", sessionID)
		}
	} else {
		// No session ID provided, create a new session
		sess = t.sessionManager.CreateSession()
		sessionID = sess.ID
		logger.Info("No session ID provided, created new session: %s", sessionID)
	}

	// Check if the response writer supports flushing
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported by server", http.StatusInternalServerError)
		return
	}

	// Set headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

	// Set up the SSE connection
	ctx, cancel := context.WithCancel(r.Context())

	// Store the request information
	t.requestMu.Lock()
	t.activeRequests[sessionID] = &sseRequest{
		session:       sess,
		ctx:           ctx,
		cancel:        cancel,
		writer:        w,
		flusher:       flusher,
		lastHeartbeat: time.Now(),
	}
	t.requestMu.Unlock()

	// Register event callback for sending events
	sess.EventCallback = func(event string, data []byte) error {
		t.requestMu.Lock()
		req, ok := t.activeRequests[sessionID]
		t.requestMu.Unlock()

		if !ok {
			return fmt.Errorf("no active request for session %s", sessionID)
		}

		// Check if the context is done
		select {
		case <-req.ctx.Done():
			return fmt.Errorf("request context done")
		default:
			// Continue with sending the event
		}

		// Write the event data
		fmt.Fprintf(req.writer, "data: %s\n\n", data)
		req.flusher.Flush()

		// Update last heartbeat time
		req.lastHeartbeat = time.Now()

		return nil
	}

	// Mark the session as connected
	sess.Connected = true

	// Send the initial connection event
	initialEvent := map[string]interface{}{
		"event": "connection",
		"data": map[string]interface{}{
			"sessionId": sessionID,
			"status":    "connected",
		},
	}
	initialEventJSON, _ := json.Marshal(initialEvent)
	fmt.Fprintf(w, "data: %s\n\n", initialEventJSON)
	flusher.Flush()

	// Start a heartbeat goroutine
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.requestMu.Lock()
				req, ok := t.activeRequests[sessionID]
				t.requestMu.Unlock()

				if !ok {
					return
				}

				// Send a heartbeat event
				heartbeatEvent := map[string]interface{}{
					"event": "heartbeat",
					"data": map[string]interface{}{
						"time": time.Now().Unix(),
					},
				}
				heartbeatJSON, _ := json.Marshal(heartbeatEvent)
				fmt.Fprintf(req.writer, "data: %s\n\n", heartbeatJSON)
				req.flusher.Flush()

				// Update last heartbeat time
				req.lastHeartbeat = time.Now()
			}
		}
	}()

	// Wait for the connection to close
	<-ctx.Done()

	// Clean up
	t.requestMu.Lock()
	delete(t.activeRequests, sessionID)
	t.requestMu.Unlock()

	// Mark session as disconnected
	sess.Connected = false
	logger.Info("SSE connection closed for session %s", sessionID)
}

// HandleMessage handles JSON-RPC message requests
func (t *SSETransport) HandleMessage(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Only accept POST requests
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the session ID from the query parameter
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		http.Error(w, "Missing sessionId parameter", http.StatusBadRequest)
		return
	}

	// Get the session
	sess, ok := t.sessionManager.GetSession(sessionID)
	if !ok {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	// Update session last active time
	sess.UpdateLastActive()

	// Parse the JSON-RPC request
	var req jsonrpc.Request
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		jsonErr := &jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: fmt.Sprintf("Invalid JSON: %v", err),
		}
		t.sendJSONRPCResponse(w, nil, jsonErr)
		return
	}

	// Find the method handler
	handler, ok := t.GetMethodHandler(req.Method)
	if !ok {
		jsonErr := &jsonrpc.Error{
			Code:    jsonrpc.MethodNotFoundCode,
			Message: fmt.Sprintf("Method not found: %s", req.Method),
		}
		t.sendJSONRPCResponse(w, req.ID, jsonErr)
		return
	}

	// Execute the handler
	result, jsonErr := handler(&req, sess)

	// Send the response if this is not a notification (has an ID)
	if req.ID != nil {
		t.sendJSONRPCResponse(w, req.ID, jsonErr, result)
	} else if jsonErr != nil {
		// Log error even for notifications
		logger.Error("Error handling notification %s: %v", req.Method, jsonErr)
	}
}

// sendJSONRPCResponse sends a JSON-RPC response
func (t *SSETransport) sendJSONRPCResponse(w http.ResponseWriter, id interface{}, jsonErr *jsonrpc.Error, result ...interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var response map[string]interface{}

	if jsonErr != nil {
		// Error response
		response = map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      id,
			"error": map[string]interface{}{
				"code":    jsonErr.Code,
				"message": jsonErr.Message,
			},
		}

		// Include error data if present
		if jsonErr.Data != nil {
			response["error"].(map[string]interface{})["data"] = jsonErr.Data
		}
	} else if len(result) > 0 {
		// Success response with result
		response = map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      id,
			"result":  result[0],
		}
	} else {
		// Success response without result
		response = map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      id,
			"result":  nil,
		}
	}

	// Marshal and send
	respJSON, err := json.Marshal(response)
	if err != nil {
		logger.Error("Failed to marshal JSON-RPC response: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Write(respJSON)
}
