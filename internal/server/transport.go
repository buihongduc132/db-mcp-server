package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

// Transport defines the interface for server transport implementations
type Transport interface {
	Serve(ctx context.Context, server *MCPServer) error
}

// StdioTransport implements Transport using standard input/output
type StdioTransport struct {
	reader io.Reader
	writer io.Writer
}

// NewStdioTransport creates a new stdio transport
func NewStdioTransport() *StdioTransport {
	return &StdioTransport{
		reader: os.Stdin,
		writer: os.Stdout,
	}
}

// Serve starts the stdio transport
func (t *StdioTransport) Serve(ctx context.Context, server *MCPServer) error {
	decoder := json.NewDecoder(t.reader)
	encoder := json.NewEncoder(t.writer)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var message json.RawMessage
			if err := decoder.Decode(&message); err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("failed to decode message: %w", err)
			}

			response := server.HandleMessage(ctx, message)
			if err := encoder.Encode(response); err != nil {
				return fmt.Errorf("failed to encode response: %w", err)
			}
		}
	}
}

// SSETransport implements Transport using Server-Sent Events
type SSETransport struct {
	port   int
	server *http.Server
	mu     sync.RWMutex
	conns  map[string]*sseConnection
}

type sseConnection struct {
	flusher http.Flusher
	writer  io.Writer
	done    chan struct{}
}

// NewSSETransport creates a new SSE transport
func NewSSETransport(port int) *SSETransport {
	return &SSETransport{
		port:  port,
		conns: make(map[string]*sseConnection),
	}
}

// generateClientID creates a random client ID when one isn't provided
func generateClientID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// Serve starts the SSE transport
func (t *SSETransport) Serve(ctx context.Context, server *MCPServer) error {
	mux := http.NewServeMux()

	// Main MCP endpoint for Cursor integration
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		// Handle preflight CORS requests
		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusOK)
			return
		}

		// For POST requests, handle RPC calls
		if r.Method == "POST" {
			t.handleRPCCall(w, r, server, ctx)
			return
		}

		// For GET requests, set up SSE stream
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		// Set up SSE headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Get or generate client ID
		clientID := r.URL.Query().Get("clientId")
		if clientID == "" {
			// For Cursor compatibility, instead of returning an error, generate a client ID
			clientID = generateClientID()
			log.Printf("Auto-generated clientId: %s", clientID)
		}

		sessionID := r.URL.Query().Get("sessionId")
		if sessionID == "" {
			sessionID = clientID
		}

		// Create the SSE connection
		conn := &sseConnection{
			flusher: flusher,
			writer:  w,
			done:    make(chan struct{}),
		}

		// Register the connection
		t.mu.Lock()
		t.conns[clientID] = conn
		t.mu.Unlock()

		log.Printf("SSE connection established for client: %s", clientID)

		// Send initial keepalive message
		fmt.Fprintf(w, "event: keepalive\ndata: connected\n\n")
		flusher.Flush()

		// Start goroutine to process notifications for this client
		notificationCtx, cancel := context.WithCancel(ctx)
		go t.processNotifications(notificationCtx, server, clientID, sessionID, conn)

		// Clean up on disconnect
		defer func() {
			cancel() // Cancel the notification context
			t.mu.Lock()
			delete(t.conns, clientID)
			t.mu.Unlock()
			close(conn.done)
			log.Printf("SSE connection closed for client: %s", clientID)
		}()

		// Wait for client disconnect
		<-r.Context().Done()
	})

	// Status endpoint for health checks
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"mode":   "sse",
		})
	})

	// Root endpoint for simple instructions
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "<html><body><h1>MCP Server</h1><p>MCP Server is running. Use the /mcp endpoint for SSE connections.</p></body></html>")
	})

	// Create and start HTTP server
	addr := fmt.Sprintf(":%d", t.port)
	t.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("Starting SSE transport on http://localhost%s", addr)
	log.Printf("MCP endpoint: http://localhost%s/mcp", addr)
	log.Printf("Use this URL in Cursor settings")

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		log.Println("Shutting down SSE transport...")
		t.server.Shutdown(context.Background())
	}()

	// Start server
	if err := t.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

// handleRPCCall processes incoming JSON-RPC calls
func (t *SSETransport) handleRPCCall(w http.ResponseWriter, r *http.Request, server *MCPServer, ctx context.Context) {
	// Set response headers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}

	// Process the message through the server
	response := server.HandleMessage(ctx, body)

	// Marshal the response to JSON
	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	// Send the response
	w.WriteHeader(http.StatusOK)
	w.Write(responseJSON)
}

// processNotifications listens for notifications and sends them to the SSE client
func (t *SSETransport) processNotifications(ctx context.Context, server *MCPServer, clientID, sessionID string, conn *sseConnection) {
	// Send an initial tools list notification
	// Add this client context to the server
	server.clientMu.Lock()
	server.currentClient = NotificationContext{
		ClientID:  clientID,
		SessionID: sessionID,
	}
	server.clientMu.Unlock()

	// Explicitly send tools list notification if server is initialized
	if server.initialized.Load() && server.capabilities.tools != nil {
		server.sendToolListChangedNotification()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case notification := <-server.notifications:
			// Check if notification is for this client
			if notification.Context.ClientID == clientID || notification.Context.ClientID == "" {
				// Serialize notification
				data, err := json.Marshal(notification.Notification)
				if err != nil {
					log.Printf("Error marshaling notification: %v", err)
					continue
				}

				// Send to client
				fmt.Fprintf(conn.writer, "event: message\ndata: %s\n\n", data)
				conn.flusher.Flush()
				log.Printf("Sent notification to client %s: %s", clientID, notification.Notification.Notification.Method)
			}
		case <-conn.done:
			return
		}
	}
}
