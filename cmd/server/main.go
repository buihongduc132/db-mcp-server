package main

import (
	"context"
	"flag"
	"log"
	"mcpserver/internal/config"
	"mcpserver/internal/infrastructure/database"
	"mcpserver/internal/infrastructure/server"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Parse command line flags (for backward compatibility)
	var (
		port          = flag.Int("port", 0, "Server port (override env SERVER_PORT)")
		dbType        = flag.String("db-type", "", "Database type (override env DB_TYPE)")
		dbConfig      = flag.String("db-config", "", "Database connection string (override env-based config)")
		transportMode = flag.String("transport", "", "Transport mode: stdio or sse (override env TRANSPORT_MODE)")
	)
	flag.Parse()

	// Load configuration from .env file and environment variables
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override config with command line arguments if provided
	if *port > 0 {
		cfg.ServerPort = *port
	}

	if *dbType != "" {
		cfg.DB.Type = *dbType
	}

	if *transportMode != "" {
		switch *transportMode {
		case string(config.StdioTransport):
			cfg.TransportMode = config.StdioTransport
		case string(config.SSETransport):
			cfg.TransportMode = config.SSETransport
		default:
			log.Printf("Warning: Invalid transport mode '%s', using default '%s'", *transportMode, cfg.TransportMode)
		}
	}

	// Create server config
	serverConfig := server.Config{
		Port:          cfg.ServerPort,
		DBType:        cfg.DB.Type,
		DBConfig:      *dbConfig, // This will be empty if not provided via command line
		TransportMode: cfg.TransportMode,
	}

	// If no explicit connection string provided, build it from environment variables
	if serverConfig.DBConfig == "" {
		serverConfig.DBConfig = cfg.DB.GetDSN()
	}

	// Create database repository
	dbFactory := database.NewFactory()
	dbRepo, err := dbFactory.CreateRepository(serverConfig.DBType, serverConfig.DBConfig)
	if err != nil {
		log.Fatalf("Failed to create database repository: %v", err)
	}

	// Create and start server
	srv, err := server.NewServer(serverConfig, dbRepo)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle graceful shutdown
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Create a deadline to wait for
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server gracefully stopped")
}
