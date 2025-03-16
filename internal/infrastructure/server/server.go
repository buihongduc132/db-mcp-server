package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mcpserver/internal/domain/repositories"
	"mcpserver/internal/interfaces/api"
	"mcpserver/internal/usecase"
	"net/http"
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
}

// Config holds the server configuration
type Config struct {
	Port     int
	DBType   string
	DBConfig string
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
	}

	server.AddConnectionDebugHandler()

	return server, nil
}

// Start starts the MCP server
func (s *MCPServer) Start() error {
	log.Printf("Server starting on %s", s.httpServer.Addr)
	log.Printf("Cursor MCP endpoint available at http://localhost%s/cursor-mcp", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *MCPServer) Shutdown(ctx context.Context) error {
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

		// Test database connection
		err := s.dbRepo.Ping()
		dbStatus := "connected"
		if err != nil {
			dbStatus = fmt.Sprintf("error: %v", err)
		}

		// Collect server information
		info := map[string]interface{}{
			"server_status": "running",
			"port":          s.config.Port,
			"db_type":       s.config.DBType,
			"db_status":     dbStatus,
			"uptime":        time.Since(s.startTime).String(),
			"client_ip":     r.RemoteAddr,
			"headers":       r.Header,
		}

		json.NewEncoder(w).Encode(info)
	})

	log.Printf("Connection debug endpoint added at http://localhost:%d/debug/connection", s.config.Port)
}
