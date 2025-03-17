package transport

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"mcpserver/internal/logger"
	"mcpserver/internal/mcp"
	"mcpserver/internal/session"
	"mcpserver/pkg/jsonrpc"
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

// SSETransport implements the SSE transport for the MCP server
type SSETransport struct {
	sessionManager *session.Manager
	methodHandlers map[string]mcp.MethodHandler
	basePath       string
	mu             sync.RWMutex
}

// NewSSETransport creates a new SSE transport
func NewSSETransport(sessionManager *session.Manager, basePath string) *SSETransport {
	return &SSETransport{
		sessionManager: sessionManager,
		methodHandlers: make(map[string]mcp.MethodHandler),
		basePath:       basePath,
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

// HandleSSE handles SSE connection requests
func (t *SSETransport) HandleSSE(w http.ResponseWriter, r *http.Request) {
	// Check if the request method is GET
	if r.Method != http.MethodGet {
		logger.Error("Method not allowed: %s, expected GET", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Log request details
	logger.Debug("SSE connection request from: %s", r.RemoteAddr)
	logger.Debug("User-Agent: %s", r.UserAgent())
	logger.Debug("Query parameters: %v", r.URL.Query())

	// Get or create a session
	sessionID := r.URL.Query().Get("sessionId")
	var sess *session.Session
	var err error

	if sessionID != "" {
		// Try to get an existing session
		sess, err = t.sessionManager.GetSession(sessionID)
		if err != nil {
			logger.Info("Session %s not found, creating new session", sessionID)
			sess = t.sessionManager.CreateSession()
		} else {
			logger.Info("Reconnecting to session %s", sessionID)
		}
	} else {
		// Create a new session
		sess = t.sessionManager.CreateSession()
		logger.Info("Created new session %s", sess.ID)
	}

	// Set SSE headers
	w.Header().Set(headerContentType, contentTypeEventStream)
	w.Header().Set(headerCacheControl, "no-cache")
	w.Header().Set(headerConnection, "keep-alive")
	w.Header().Set(headerAccessControlAllowOrigin, "*")
	w.Header().Set(headerAccessControlAllowHeaders, "Content-Type")
	w.Header().Set(headerAccessControlAllowMethods, "GET, OPTIONS")
	w.WriteHeader(http.StatusOK)

	// Set event callback
	sess.EventCallback = func(event string, data []byte) error {
		// Log the event
		logger.SSEEventLog(event, sess.ID, string(data))

		// Format the event according to SSE specification with consistent formatting
		eventText := fmt.Sprintf("event: %s\ndata: %s\n\n", event, string(data))

		// Write the event
		_, err := fmt.Fprint(w, eventText)
		if err != nil {
			logger.Error("Error writing event to client: %v", err)
			return err
		}

		// Flush the response writer
		sess.Flusher.Flush()
		logger.Debug("Event sent to client: %s", sess.ID)
		return nil
	}

	// Connect the session
	err = sess.Connect(w, r)
	if err != nil {
		logger.Error("Failed to connect session: %v", err)
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	// Send initial message with the message endpoint
	messageEndpoint := fmt.Sprintf("%s/message?sessionId=%s", t.basePath, sess.ID)
	logger.Info("Setting message endpoint to: %s", messageEndpoint)

	// Format and send the endpoint event directly as specified in mcp-go
	initialEvent := fmt.Sprintf("event: endpoint\ndata: %s\n\n", messageEndpoint)
	logger.Info("Sending initial endpoint event to client")
	logger.Debug("Endpoint event data: %s", initialEvent)

	// Write directly to the response writer instead of using SendEvent
	_, err = fmt.Fprint(w, initialEvent)
	if err != nil {
		logger.Error("Failed to send initial endpoint event: %v", err)
		return
	}

	// Flush to ensure the client receives the event immediately
	sess.Flusher.Flush()

	// Start heartbeat in a separate goroutine
	go t.startHeartbeat(sess)

	// Wait for the client to disconnect
	<-sess.Context().Done()
	logger.Info("Client disconnected: %s", sess.ID)
}

// HandleMessage handles a JSON-RPC message
func (t *SSETransport) HandleMessage(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set(headerAccessControlAllowOrigin, "*")
	w.Header().Set(headerAccessControlAllowHeaders, "Content-Type")
	w.Header().Set(headerAccessControlAllowMethods, "POST, OPTIONS")
	w.Header().Set(headerContentType, "application/json")

	// Handle preflight requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Check request method
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get session ID from query parameter
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		http.Error(w, "Missing sessionId parameter", http.StatusBadRequest)
		return
	}

	// Get session
	sess, err := t.sessionManager.GetSession(sessionID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid session: %v", err), http.StatusBadRequest)
		return
	}

	// Session is automatically updated when GetSession is called

	// Parse JSON-RPC request
	var req jsonrpc.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("Failed to parse JSON-RPC request: %v", err)
		jsonResponse := &jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			Error:   jsonrpc.ParseError(err),
		}
		json.NewEncoder(w).Encode(jsonResponse)
		return
	}

	// Log the request
	logger.Debug("Received request: %+v", req)

	// Get method handler
	handler, ok := t.GetMethodHandler(req.Method)
	if !ok {
		logger.Error("Method not found: %s", req.Method)
		jsonResponse := &jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Error:   jsonrpc.MethodNotFoundError(req.Method),
		}
		json.NewEncoder(w).Encode(jsonResponse)
		return
	}

	// Process the request
	result, jsonRPCErr := t.processRequest(&req, sess, handler)

	// Create response
	var jsonResponse *jsonrpc.Response
	if jsonRPCErr != nil {
		jsonResponse = &jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Error:   jsonRPCErr,
		}
	} else {
		jsonResponse = &jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Result:  result,
		}
	}

	// Log the response
	responseJSON, _ := json.Marshal(jsonResponse)
	logger.Debug("Sending response: %s", string(responseJSON))

	// Write response
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(jsonResponse); err != nil {
		logger.Error("Failed to encode JSON-RPC response: %v", err)
	}
}

// processRequest processes a JSON-RPC request and returns the result or error
func (t *SSETransport) processRequest(req *jsonrpc.Request, sess *session.Session, handler mcp.MethodHandler) (interface{}, *jsonrpc.Error) {
	// Log the request
	logger.Info("Processing request: method=%s, id=%v", req.Method, req.ID)

	// Handle the params type conversion properly
	// We'll keep the params as they are, and let each handler deal with the type conversion
	// This avoids any incorrect type assertions

	// Call the method handler
	result, jsonRPCErr := handler(req, sess)

	if jsonRPCErr != nil {
		logger.Error("Method handler error: %v", jsonRPCErr)
		return nil, jsonRPCErr
	}

	// Log the result for debugging
	resultJSON, _ := json.Marshal(result)
	logger.Debug("Method handler result: %s", string(resultJSON))

	return result, nil
}

// startHeartbeat sends periodic heartbeat events to keep the connection alive
func (t *SSETransport) startHeartbeat(sess *session.Session) {
	ticker := time.NewTicker(heartbeatInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if the session is still connected
			if !sess.Connected {
				return
			}

			// Format the heartbeat timestamp
			timestamp := time.Now().Format(time.RFC3339)

			// Use the existing SendEvent method which handles thread safety internally
			err := sess.SendEvent("heartbeat", []byte(timestamp))
			if err != nil {
				logger.Error("Failed to send heartbeat: %v", err)
				sess.Disconnect()
				return
			}

			logger.Debug("Heartbeat sent to client: %s", sess.ID)

		case <-sess.Context().Done():
			// Session is closed
			return
		}
	}
}
