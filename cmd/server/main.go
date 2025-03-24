package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/config"
	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/FreePeak/db-mcp-server/internal/mcp"
	"github.com/FreePeak/db-mcp-server/internal/session"
	"github.com/FreePeak/db-mcp-server/internal/transport"
	"github.com/FreePeak/db-mcp-server/pkg/dbtools"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// Server represents the MCP server instance
type Server struct {
	registry       *tools.Registry
	sessionManager *session.Manager
	config         *config.Config
}

func (s *Server) startSSEServer() error {
	// Create SSE transport
	basePath := fmt.Sprintf("http://localhost:%d", s.config.ServerPort)
	sseTransport := transport.NewSSETransport(s.sessionManager, basePath)

	// Create MCP handler with the tool registry
	mcpHandler := mcp.NewHandler(s.registry)

	// Register MCP handler with transport
	for method, handler := range mcpHandler.GetAllMethodHandlers() {
		sseTransport.RegisterMethodHandler(method, handler)
	}

	// Create HTTP server
	mux := http.NewServeMux()
	mux.Handle("/sse", http.HandlerFunc(sseTransport.HandleSSE))
	mux.Handle("/message", http.HandlerFunc(sseTransport.HandleMessage))

	// Create server with graceful shutdown
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.ServerPort),
		Handler: mux,
	}

	// Channel to listen for errors coming from the server
	serverErrors := make(chan error, 1)

	// Start server
	go func() {
		serverErrors <- srv.ListenAndServe()
	}()

	// Listen for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Wait for interrupt signal
	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)
	case <-stop:
		logger.Info("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	return nil
}

func (s *Server) startStdioServer() error {
	// Create MCP handler with the tool registry
	_ = mcp.NewHandler(s.registry)

	// Start server using stdio
	logger.Info("stdio transport not implemented yet")
	return fmt.Errorf("stdio transport not implemented")
}

func main() {
	// Initialize random number generator
	// As of Go 1.20, rand.Seed is no longer necessary

	// Parse command line flags
	transportMode := flag.String("transport", "", "Transport mode (sse or stdio)")
	serverPort := flag.Int("port", 0, "Server port")
	configFile := flag.String("config", "", "Path to database configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger with debug level
	logger.Initialize("debug")

	// Override configuration with command line flags if provided
	if *transportMode != "" {
		cfg.TransportMode = *transportMode
	}
	if *serverPort != 0 {
		cfg.ServerPort = *serverPort
	}
	if *configFile != "" {
		cfg.DBConfigFile = *configFile
	}

	// Initialize session manager
	sessionManager := session.NewManager()
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			sessionManager.CleanupSessions(30 * time.Minute)
		}
	}()

	// Initialize tools registry
	registry := tools.NewRegistry()

	// Initialize database connections
	if err := dbtools.InitDatabase(cfg); err != nil {
		log.Fatalf("Failed to initialize database connections: %v", err)
	}

	// Register database tools
	if err := dbtools.RegisterDatabaseTools(registry); err != nil {
		log.Fatalf("Failed to register database tools: %v", err)
	}

	// Verify database connections
	ctx := context.Background()
	dbIDs := dbtools.ListDatabases()
	if len(dbIDs) == 0 {
		log.Printf("Warning: No database connections configured")
	} else {
		for _, dbID := range dbIDs {
			db, err := dbtools.GetDatabase(dbID)
			if err != nil {
				log.Printf("Warning: Failed to get database %s: %v", dbID, err)
				continue
			}
			if err := db.Ping(ctx); err != nil {
				log.Printf("Warning: Failed to ping database %s: %v", dbID, err)
			} else {
				log.Printf("Successfully connected to database %s", dbID)
			}
		}
	}

	// Create server instance
	server := &Server{
		registry:       registry,
		sessionManager: sessionManager,
		config:         cfg,
	}

	// Handle transport mode
	switch cfg.TransportMode {
	case "sse":
		log.Printf("Starting server in SSE mode on port %d", cfg.ServerPort)
		if err := server.startSSEServer(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	case "stdio":
		log.Printf("Starting server in stdio mode")
		if err := server.startStdioServer(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	default:
		log.Fatalf("Invalid transport mode: %s", cfg.TransportMode)
	}
}

// startSSEServer starts the server using Server-Sent Events transport
//
//nolint:unused // Retained for future use
func startSSEServer(cfg *config.Config, sessionManager *session.Manager, mcpHandler *mcp.Handler) error {
	// Create SSE transport
	basePath := fmt.Sprintf("http://localhost:%d", cfg.ServerPort)
	sseTransport := transport.NewSSETransport(sessionManager, basePath)

	// Register method handlers
	methodHandlers := mcpHandler.GetAllMethodHandlers()
	for method, handler := range methodHandlers {
		sseTransport.RegisterMethodHandler(method, handler)
	}

	// Create HTTP server
	mux := http.NewServeMux()

	// Register SSE endpoint
	mux.HandleFunc("/sse", sseTransport.HandleSSE)

	// Register message endpoint
	mux.HandleFunc("/message", sseTransport.HandleMessage)

	// Create server
	addr := fmt.Sprintf(":%d", cfg.ServerPort)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		logger.Info("Server listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server error: %v", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	// Shutdown server gracefully
	logger.Info("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Server shutdown error: %v", err)
	}

	logger.Info("Server stopped")
	return nil
}

// registerDatabaseTools registers database tools with the MCP tool registry
//
//nolint:unused // Retained for future use
func registerDatabaseTools(toolRegistry *tools.Registry, cfg *config.Config) error {
	// Initialize database connections
	if err := dbtools.InitDatabase(cfg); err != nil {
		logger.Error("Failed to initialize databases: %v", err)
		return fmt.Errorf("database initialization failed: %w", err)
	}

	// Register database tools
	if err := dbtools.RegisterDatabaseTools(toolRegistry); err != nil {
		logger.Error("Failed to register database tools: %v", err)
		return fmt.Errorf("failed to register database tools: %w", err)
	}
	logger.Info("Database tools registered successfully")

	// Verify connections to all databases
	for _, conn := range cfg.MultiDBConfig.Connections {
		db, err := dbtools.GetDatabase(conn.ID)
		if err != nil {
			logger.Error("Failed to get database connection for %s: %v", conn.ID, err)
			return fmt.Errorf("failed to get database connection for %s: %w", conn.ID, err)
		}

		if err := db.Ping(context.Background()); err != nil {
			logger.Error("Failed to connect to database %s: %v", conn.ID, err)
			return fmt.Errorf("failed to connect to database %s: %w", conn.ID, err)
		}
		logger.Info("Successfully connected to database %s (%s:%d)", conn.ID, conn.Host, conn.Port)
	}

	return nil
}
