package transport

import (
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

	// Log detailed request information
	logger.Debug("SSE connection request from: %s", r.RemoteAddr)
	logger.Debug("User-Agent: %s", r.UserAgent())
	logger.Debug("Query parameters: %v", r.URL.Query())

	// Log all headers for debugging
	logger.Debug("------ REQUEST HEADERS ------")
	for name, values := range r.Header {
		for _, value := range values {
			logger.Debug("  %s: %s", name, value)
		}
	}
	logger.Debug("----------------------------")

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
		// Ensure exact format: "event: message\ndata: {...}\n\n"
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
	// Use the exact format expected: "event: endpoint\ndata: URL\n\n"
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

	// Parse request body as JSON-RPC request
	var req jsonrpc.Request
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&req)
	if err != nil {
		logger.Error("Failed to decode JSON-RPC request: %v", err)
		errorResponse := jsonrpc.Error{
			Code:    jsonrpc.ParseErrorCode,
			Message: "Invalid JSON: " + err.Error(),
		}
		t.sendErrorResponse(w, nil, &errorResponse)
		return
	}

	// Log received request
	reqJSON, _ := json.Marshal(req)
	logger.Debug("Received request: %s", string(reqJSON))
	logger.Info("Processing request: method=%s, id=%v", req.Method, req.ID)

	// Find handler for the method
	handler, ok := t.GetMethodHandler(req.Method)
	if !ok {
		logger.Error("Method not found: %s", req.Method)
		errorResponse := jsonrpc.Error{
			Code:    jsonrpc.MethodNotFoundCode,
			Message: fmt.Sprintf("Method not found: %s", req.Method),
		}
		t.sendErrorResponse(w, req.ID, &errorResponse)
		return
	}

	// Process the request with the handler
	result, jsonRpcErr := t.processRequest(&req, sess, handler)

	// Check if this is a notification (no ID)
	isNotification := req.ID == nil

	// Send the response back to the client
	if jsonRpcErr != nil {
		logger.Debug("Method handler error: %v", jsonRpcErr)
		t.sendErrorResponse(w, req.ID, jsonRpcErr)
	} else if isNotification {
		// For notifications, return 202 Accepted without a response body
		logger.Debug("Notification processed successfully")
		w.WriteHeader(http.StatusAccepted)
	} else {
		resultJSON, _ := json.Marshal(result)
		logger.Debug("Method handler result: %s", string(resultJSON))

		// Ensure consistent response format for all methods
		response := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  result,
		}

		responseJSON, err := json.Marshal(response)
		if err != nil {
			logger.Error("Failed to marshal response: %v", err)
			errorResponse := jsonrpc.Error{
				Code:    jsonrpc.InternalErrorCode,
				Message: "Failed to marshal response",
			}
			t.sendErrorResponse(w, req.ID, &errorResponse)
			return
		}

		logger.Debug("Sending response: %s", string(responseJSON))

		// Queue the response to be sent as an event
		if err := sess.SendEvent("message", responseJSON); err != nil {
			logger.Error("Failed to queue response event: %v", err)
		}

		// For the HTTP response, just return 202 Accepted
		w.WriteHeader(http.StatusAccepted)
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

// sendErrorResponse sends a JSON-RPC error response to the client
func (t *SSETransport) sendErrorResponse(w http.ResponseWriter, id interface{}, err *jsonrpc.Error) {
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
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	logger.Debug("Sending error response: %s", string(responseJSON))

	// If this is a parse error or other error that occurs before we have a valid session,
	// send it directly in the HTTP response
	if id == nil || w.Header().Get(headerContentType) == "" {
		w.Header().Set(headerContentType, "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
	} else {
		// For session-related errors, we'll rely on the direct HTTP response
		// since we don't have access to the session here
		w.Header().Set(headerContentType, "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(responseJSON)
	}
}
