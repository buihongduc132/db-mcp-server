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

	// Initialize database connection from config
	dbConfig := &dbtools.Config{
		ConfigFile: *configFile,
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

	// Register tools
	toolRegistry.RegisterAllTools()

	// Handle transport mode
	switch *transportMode {
	case "sse":
		log.Printf("Starting SSE server on port %d", *serverPort)

		// Create SSE server
		sseServer := server.NewSSEServer(
			mcpServer,
			server.WithBaseURL(fmt.Sprintf("http://%s:%d", *serverHost, *serverPort)),
		)

		// Start the server
		errCh := make(chan error, 1)
		go func() {
			errCh <- sseServer.Start(fmt.Sprintf(":%d", *serverPort))
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
			server := &http.Server{Addr: fmt.Sprintf(":%d", *serverPort)}
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
		// No graceful shutdown needed for stdio
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("STDIO server error: %v", err)
		}

	default:
		log.Fatalf("Invalid transport mode: %s", *transportMode)
	}

	log.Println("Server shutdown complete")
}
