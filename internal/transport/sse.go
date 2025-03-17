package transport

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"mcpserver/internal/logger"
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
	methodHandlers map[string]MethodHandler
	basePath       string
	mu             sync.RWMutex
}

// MethodHandler is a function that handles a JSON-RPC method
type MethodHandler func(req *jsonrpc.Request, sess *session.Session) (interface{}, *jsonrpc.Error)

// NewSSETransport creates a new SSE transport
func NewSSETransport(sessionManager *session.Manager, basePath string) *SSETransport {
	return &SSETransport{
		sessionManager: sessionManager,
		methodHandlers: make(map[string]MethodHandler),
		basePath:       basePath,
	}
}

// RegisterMethodHandler registers a method handler
func (t *SSETransport) RegisterMethodHandler(method string, handler MethodHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.methodHandlers[method] = handler
}

// GetMethodHandler gets a method handler by name
func (t *SSETransport) GetMethodHandler(method string) (MethodHandler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	handler, ok := t.methodHandlers[method]
	return handler, ok
}

// HandleSSE handles SSE connection requests
func (t *SSETransport) HandleSSE(w http.ResponseWriter, r *http.Request) {
	// Check if the request method is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

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

		// Write the event
		_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
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
	initialMessage := map[string]string{
		"sessionId":       sess.ID,
		"messageEndpoint": fmt.Sprintf("%s/message?sessionId=%s", t.basePath, sess.ID),
		"status":          "connected",
	}

	initialData, err := json.Marshal(initialMessage)
	if err != nil {
		logger.Error("Failed to marshal initial message: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	err = sess.SendEvent("connection", initialData)
	if err != nil {
		logger.Error("Failed to send initial event: %v", err)
		return
	}

	// Start heartbeat in a separate goroutine
	go t.startHeartbeat(sess)

	// Wait for the client to disconnect
	<-sess.Context().Done()
	logger.Info("Client disconnected: %s", sess.ID)
}

// HandleMessage handles JSON-RPC message requests
func (t *SSETransport) HandleMessage(w http.ResponseWriter, r *http.Request) {
	// Check if the request method is POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the session ID
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		logger.Error("Missing sessionId parameter")
		http.Error(w, "Missing sessionId parameter", http.StatusBadRequest)
		return
	}

	// Get the session
	sess, err := t.sessionManager.GetSession(sessionID)
	if err != nil {
		logger.Error("Session not found: %s", sessionID)
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Log the request
	logger.RequestLog(r.Method, r.URL.String(), sessionID, string(body))

	// Parse the JSON-RPC request
	var req jsonrpc.Request
	err = json.Unmarshal(body, &req)
	if err != nil {
		logger.Error("Failed to parse JSON-RPC request: %v", err)
		resp := jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			Error:   jsonrpc.ParseError(err.Error()),
		}

		// Send error response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Check if the method exists
	handler, ok := t.GetMethodHandler(req.Method)
	if !ok {
		logger.Error("Method not found: %s", req.Method)
		resp := jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Error:   jsonrpc.MethodNotFoundError(req.Method),
		}

		// Send error response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Process the request
	result, err := t.processRequest(&req, sess, handler)

	// Handle notification (no response expected)
	if req.IsNotification() {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Send the response through the SSE channel
	if err != nil {
		// Create an error response
		var jsonRPCErr *jsonrpc.Error

		// Check if the error is already a jsonrpc.Error
		if rpcErr, ok := err.(*jsonrpc.Error); ok {
			jsonRPCErr = rpcErr
		} else {
			// Convert generic error to jsonrpc.Error
			jsonRPCErr = jsonrpc.InternalError(err.Error())
		}

		resp := jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Error:   jsonRPCErr,
		}

		// Convert to JSON
		respData, jsonErr := json.Marshal(resp)
		if jsonErr != nil {
			logger.Error("Failed to marshal error response: %v", jsonErr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Send via SSE
		sseErr := sess.SendEvent("message", respData)
		if sseErr != nil {
			logger.Error("Failed to send error response: %v", sseErr)
			http.Error(w, "Failed to send response", http.StatusInternalServerError)
			return
		}

		// Also send in HTTP response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)

	} else {
		// Create a success response
		resp := jsonrpc.Response{
			JSONRPC: jsonrpc.Version,
			ID:      req.ID,
			Result:  result,
		}

		// Convert to JSON
		respData, jsonErr := json.Marshal(resp)
		if jsonErr != nil {
			logger.Error("Failed to marshal success response: %v", jsonErr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Send via SSE
		sseErr := sess.SendEvent("message", respData)
		if sseErr != nil {
			logger.Error("Failed to send success response: %v", sseErr)
			http.Error(w, "Failed to send response", http.StatusInternalServerError)
			return
		}

		// Also send in HTTP response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respData)
	}
}

// processRequest processes a JSON-RPC request and returns the result or error
func (t *SSETransport) processRequest(req *jsonrpc.Request, sess *session.Session, handler MethodHandler) (interface{}, *jsonrpc.Error) {
	// Log the request
	logger.Info("Processing request: method=%s, id=%v", req.Method, req.ID)

	// Call the method handler
	result, jsonRPCErr := handler(req, sess)

	if jsonRPCErr != nil {
		logger.Error("Method handler error: %v", jsonRPCErr)
		return nil, jsonRPCErr
	}

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

			// Send heartbeat
			heartbeat := map[string]string{"type": "heartbeat", "timestamp": time.Now().String()}
			data, err := json.Marshal(heartbeat)
			if err != nil {
				logger.Error("Failed to marshal heartbeat: %v", err)
				continue
			}

			err = sess.SendEvent("heartbeat", data)
			if err != nil {
				logger.Error("Failed to send heartbeat: %v", err)
				sess.Disconnect()
				return
			}
		case <-sess.Context().Done():
			// Session is closed
			return
		}
	}
}
