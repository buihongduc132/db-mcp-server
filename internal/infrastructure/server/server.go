package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcpserver/internal/config"
	"mcpserver/internal/domain/repositories"
	"mcpserver/internal/infrastructure/transport"
	"mcpserver/internal/interfaces/api"
	"mcpserver/internal/usecase"
	"net/http"
	"os"
	"time"
)

// MCPServer represents the MCP server
type MCPServer struct {
	httpServer     *http.Server
	dbRepo         repositories.DBRepository
	clientRepo     repositories.ClientRepository
	toolRepo       repositories.ToolRepository
	eventHandler   *api.EventHandler
	executeHandler *api.ExecuteHandler
	cursorHandler  *api.CursorMCPHandler
	config         Config
	startTime      time.Time
	transportMode  config.TransportMode
}

// Config holds the server configuration
type Config struct {
	Port          int
	DBType        string
	DBConfig      string
	TransportMode config.TransportMode
}

// NewServer creates a new MCP server
func NewServer(cfg Config, dbRepo repositories.DBRepository) (*MCPServer, error) {
	// Create repositories
	clientRepo := NewInMemoryClientRepository()
	toolRepo := NewDatabaseToolRepository(dbRepo)

	// Create use cases
	clientUseCase := usecase.NewClientUseCase(clientRepo)
	dbUseCase := usecase.NewDBUseCase(dbRepo, clientUseCase)
	cursorMCPUseCase := usecase.NewCursorMCPUseCase(toolRepo)

	// Create handlers
	eventHandler := api.NewEventHandler(clientUseCase)
	executeHandler := api.NewExecuteHandler(dbUseCase, clientUseCase)
	cursorHandler := api.NewCursorMCPHandler(cursorMCPUseCase)

	// Setup HTTP server
	mux := http.NewServeMux()

	// Original MCP server endpoints
	mux.Handle("/events", eventHandler)
	mux.Handle("/execute", executeHandler)

	// Cursor MCP protocol endpoint
	mux.Handle("/cursor-mcp", cursorHandler)

	// Add SSE endpoint for Cursor MCP protocol
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		// Create SSE transport
		transportFactory := transport.NewFactory()
		sseTransport, err := transportFactory.CreateTransport(config.SSETransport, w, r)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create transport: %v", err), http.StatusInternalServerError)
			return
		}

		// Create transport use case
		transportUseCase := usecase.NewTransportUseCase(sseTransport, cursorMCPUseCase)

		// Start transport
		if err := transportUseCase.Start(r.Context()); err != nil {
			http.Error(w, fmt.Sprintf("Failed to start transport: %v", err), http.StatusInternalServerError)
			return
		}

		// Wait for client to disconnect
		<-r.Context().Done()

		// Stop transport
		if err := transportUseCase.Stop(context.Background()); err != nil {
			log.Printf("Error stopping transport: %v", err)
		}
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}

	server := &MCPServer{
		httpServer:     httpServer,
		dbRepo:         dbRepo,
		clientRepo:     clientRepo,
		toolRepo:       toolRepo,
		eventHandler:   eventHandler,
		executeHandler: executeHandler,
		cursorHandler:  cursorHandler,
		config:         cfg,
		startTime:      time.Now(),
		transportMode:  cfg.TransportMode,
	}

	server.AddConnectionDebugHandler()

	return server, nil
}

// Start starts the MCP server
func (s *MCPServer) Start() error {
	// Ensure all logs go to stderr to avoid corrupting stdout JSON
	log.SetOutput(os.Stderr)

	log.Printf("Server starting on %s", s.httpServer.Addr)
	log.Printf("Cursor MCP endpoint available at http://localhost%s/cursor-mcp", s.httpServer.Addr)
	log.Printf("SSE endpoint available at http://localhost%s/sse", s.httpServer.Addr)

	// If using stdio transport, start it in a separate goroutine
	if s.transportMode == config.StdioTransport {
		log.Println("Starting in stdio transport mode...")

		// Create stdio transport
		transportFactory := transport.NewFactory()
		stdioTransport, err := transportFactory.CreateTransport(config.StdioTransport, nil, nil)
		if err != nil {
			// Make sure error goes to stderr
			fmt.Fprintf(os.Stderr, "Failed to create stdio transport: %v\n", err)
			return fmt.Errorf("failed to create stdio transport: %w", err)
		}

		// Create transport use case
		cursorMCPUseCase := usecase.NewCursorMCPUseCase(s.toolRepo)
		transportUseCase := usecase.NewTransportUseCase(stdioTransport, cursorMCPUseCase)

		// Start transport in a separate goroutine
		go func() {
			ctx := context.Background()
			if err := transportUseCase.Start(ctx); err != nil {
				// Make sure all errors go to stderr
				fmt.Fprintf(os.Stderr, "Error starting stdio transport: %v\n", err)
				os.Exit(1)
			}
		}()

		// Also start HTTP server in a separate goroutine to allow for debug endpoints
		go func() {
			log.Printf("Starting HTTP server for debug endpoints on %s", s.httpServer.Addr)
			if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("HTTP server error: %v", err)
			}
		}()

		// Keep the main goroutine alive
		select {}
	}

	// Otherwise, start the HTTP server for SSE mode
	log.Println("Starting in SSE transport mode...")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the server
func (s *MCPServer) Shutdown(ctx context.Context) error {
	// If using stdio transport, we don't need to shut down the HTTP server
	if s.transportMode == config.StdioTransport {
		return s.dbRepo.Close()
	}

	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		return err
	}

	err = s.dbRepo.Close()
	if err != nil {
		return err
	}

	return nil
}

// AddConnectionDebugHandler adds a debug handler to help troubleshoot connection issues
func (s *MCPServer) AddConnectionDebugHandler() {
	s.httpServer.Handler.(*http.ServeMux).HandleFunc("/debug/connection", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*") // Allow cross-origin requests

		// Test database connection
		err := s.dbRepo.Ping()
		dbStatus := "connected"
		if err != nil {
			dbStatus = fmt.Sprintf("error: %v", err)
		}

		// Collect server information
		info := map[string]interface{}{
			"server_status":   "running",
			"port":            s.config.Port,
			"db_type":         s.config.DBType,
			"db_status":       dbStatus,
			"uptime":          time.Since(s.startTime).String(),
			"client_ip":       r.RemoteAddr,
			"transport_mode":  s.transportMode,
			"connection_time": time.Now().Format(time.RFC3339),
		}

		// Send JSON response
		if err := json.NewEncoder(w).Encode(info); err != nil {
			log.Printf("Error encoding debug response: %v", err)
			http.Error(w, "Error generating debug info", http.StatusInternalServerError)
			return
		}
	})

	// Add a simple test endpoint for checking SSE
	s.httpServer.Handler.(*http.ServeMux).HandleFunc("/test/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		html := `
<!DOCTYPE html>
<html>
<head>
    <title>SSE Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        #events { border: 1px solid #ccc; padding: 10px; height: 300px; overflow-y: auto; }
        pre { margin: 0; white-space: pre-wrap; }
    </style>
</head>
<body>
    <h1>SSE Connection Test</h1>
    <div id="events"></div>
    <script>
        const eventsDiv = document.getElementById('events');
        const eventSource = new EventSource('/sse');

        eventSource.onopen = function() {
            addEvent('Connection opened');
        };

        eventSource.onmessage = function(event) {
            addEvent('Event received: ' + event.data);
        };

        eventSource.onerror = function(error) {
            addEvent('Error: Connection failed');
            console.error('EventSource error:', error);
        };

        function addEvent(message) {
            const time = new Date().toLocaleTimeString();
            eventsDiv.innerHTML += '<pre>[' + time + '] ' + message + '</pre>';
            eventsDiv.scrollTop = eventsDiv.scrollHeight;
        }
    </script>
</body>
</html>
`
		w.Write([]byte(html))
	})

	log.Printf("Debug endpoints added at http://localhost:%d/debug/connection and http://localhost:%d/test/sse", s.config.Port, s.config.Port)
}
