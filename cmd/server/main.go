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

	"github.com/mark3labs/mcp-go/server"

	"github.com/FreePeak/db-mcp-server/internal/config"
	"github.com/FreePeak/db-mcp-server/internal/delivery/mcp"
	"github.com/FreePeak/db-mcp-server/internal/repository"
	"github.com/FreePeak/db-mcp-server/internal/usecase"
	"github.com/FreePeak/db-mcp-server/pkg/dbtools"
)

func main() {
	// Parse command-line arguments
	configFile := flag.String("c", "config.json", "Database configuration file")
	transportMode := flag.String("t", "sse", "Transport mode (stdio or sse)")
	serverPort := flag.Int("p", 9092, "Server port for SSE transport")
	serverHost := flag.String("h", "localhost", "Server host for SSE transport")
	flag.Parse()

	// Load configuration after environment variables are set
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Printf("Warning: Failed to load configuration: %v", err)
		// Create a default config if loading fails
		cfg = &config.Config{
			ServerPort:    *serverPort,
			TransportMode: *transportMode,
			ConfigPath:    *configFile,
		}
	}
	// Set environment variables from command line arguments if provided
	if *configFile != "config.json" {
		os.Setenv("CONFIG_PATH", *configFile)
	}
	if *transportMode != "sse" {
		os.Setenv("TRANSPORT_MODE", *transportMode)
	}
	if *serverPort != 9092 {
		os.Setenv("SERVER_PORT", fmt.Sprintf("%d", *serverPort))
	}

	// Initialize database connection from config
	dbConfig := &dbtools.Config{
		ConfigFile: cfg.ConfigPath,
	}

	// Try to initialize database from config
	if err := dbtools.InitDatabase(dbConfig); err != nil {
		log.Printf("Warning: Failed to initialize database: %v", err)
	}

	// Set up signal handling for clean shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Create mcp-go server
	mcpServer := server.NewMCPServer(
		"DB MCP Server", // Server name
		"1.0.0",         // Server version
	)

	// Set up Clean Architecture layers
	dbRepo := repository.NewDatabaseRepository()
	dbUseCase := usecase.NewDatabaseUseCase(dbRepo)
	toolRegistry := mcp.NewToolRegistry(mcpServer, dbUseCase)

	// Debug log: Check database connections before registering tools
	dbIDs := dbUseCase.ListDatabases()
	log.Printf("Available database connections before registering tools: %v", dbIDs)

	// Register tools
	toolRegistry.RegisterAllTools()
	log.Printf("Finished registering tools")

	// If no database connections, register mock tools to ensure at least some tools are available
	if len(dbIDs) == 0 {
		log.Printf("No database connections available. Adding mock tools...")
		toolRegistry.RegisterMockTools()
	}

	// Handle transport mode
	switch cfg.TransportMode {
	case "sse":
		log.Printf("Starting SSE server on port %d", cfg.ServerPort)

		// Configure base URL with explicit protocol
		baseURL := fmt.Sprintf("http://%s:%d", *serverHost, cfg.ServerPort)
		log.Printf("Using base URL: %s", baseURL)

		// Set logging mode based on configuration
		if cfg.DisableLogging {
			log.Printf("Logging in SSE transport is disabled")
			// Redirect standard output to null device if logging is disabled
			// This only works on Unix-like systems
			os.Setenv("MCP_DISABLE_LOGGING", "true")
		}

		// Create SSE server with options
		sseServer := server.NewSSEServer(
			mcpServer,
			server.WithBaseURL(baseURL),
		)

		log.Printf("Created SSE server, starting server...")

		// Start the server
		errCh := make(chan error, 1)
		go func() {
			errCh <- sseServer.Start(fmt.Sprintf(":%d", cfg.ServerPort))
		}()

		// Wait for interrupt or error
		select {
		case err := <-errCh:
			log.Fatalf("Server error: %v", err)
		case <-stop:
			log.Println("Shutting down server...")

			// Create shutdown context
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()

			// Shutdown HTTP server
			server := &http.Server{Addr: fmt.Sprintf(":%d", cfg.ServerPort)}
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Printf("Error during server shutdown: %v", err)
			}

			// Close database connections
			if err := dbtools.CloseDatabase(); err != nil {
				log.Printf("Error closing database connections: %v", err)
			}
		}

	case "stdio":
		log.Printf("Starting STDIO server")

		// Set logging mode based on configuration
		if cfg.DisableLogging {
			log.Printf("Logging in STDIO transport is disabled")
			// Set environment variable to signal to the MCP library to disable logging
			os.Setenv("MCP_DISABLE_LOGGING", "true")
		}

		// No graceful shutdown needed for stdio
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("STDIO server error: %v", err)
		}

	default:
		log.Fatalf("Invalid transport mode: %s", cfg.TransportMode)
	}

	log.Println("Server shutdown complete")
}
