package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
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

func main() {
	// Initialize random number generator
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Parse command line flags
	transportMode := flag.String("t", "", "Transport mode (sse or stdio)")
	port := flag.Int("port", 0, "Server port")
	flag.Parse()

	// Load configuration
	cfg := config.LoadConfig()

	// Override config with command line flags if provided
	if *transportMode != "" {
		cfg.TransportMode = *transportMode
	}
	if *port != 0 {
		cfg.ServerPort = *port
	}

	// Initialize logger
	logger.Initialize(cfg.LogLevel)
	logger.Info("Starting MCP server with %s transport on port %d", cfg.TransportMode, cfg.ServerPort)

	// Create session manager
	sessionManager := session.NewManager()

	// Start session cleanup goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			sessionManager.CleanupSessions(30 * time.Minute)
		}
	}()

	// Create tool registry
	toolRegistry := tools.NewRegistry()

	// Create MCP handler with the tool registry
	mcpHandler := mcp.NewHandler(toolRegistry)

	// Register database tools
	logger.Info("Registering database tools...")
	registerDatabaseTools(toolRegistry)

	// Verify tools were registered
	registeredTools := mcpHandler.ListAvailableTools()
	if registeredTools == "none" {
		logger.Error("No tools were registered! Tools won't be available to clients.")
	} else {
		logger.Info("Successfully registered tools: %s", registeredTools)
	}

	// Create and configure the server based on transport mode
	switch cfg.TransportMode {
	case "sse":
		startSSEServer(cfg, sessionManager, mcpHandler)
	case "stdio":
		logger.Info("stdio transport not implemented yet")
		os.Exit(1)
	default:
		logger.Error("Unknown transport mode: %s", cfg.TransportMode)
		os.Exit(1)
	}
}

func startSSEServer(cfg *config.Config, sessionManager *session.Manager, mcpHandler *mcp.Handler) {
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
}

func registerDatabaseTools(toolRegistry *tools.Registry) {
	// Initialize database connection
	cfg := config.LoadConfig()

	// Initialize database
	err := dbtools.InitDatabase(cfg)
	if err != nil {
		logger.Error("Failed to initialize database: %v", err)
		logger.Warn("Database tools will not be available")
		return
	}

	// Register database tools
	dbtools.RegisterDatabaseTools(toolRegistry)

	// Log success
	logger.Info("Database tools registered successfully")
}
