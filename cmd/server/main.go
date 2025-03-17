package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mcpserver/internal/config"
	"mcpserver/internal/logger"
	"mcpserver/internal/mcp"
	"mcpserver/internal/session"
	"mcpserver/internal/transport"
	"mcpserver/pkg/tools"
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

	// Register some example tools
	logger.Info("Registering example tools...")
	registerExampleTools(toolRegistry)

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

func registerExampleTools(toolRegistry *tools.Registry) {
	// Example echo tool
	echoTool := &tools.Tool{
		Name:        "echo",
		Description: "Echoes back the input",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"message": map[string]interface{}{
					"type":        "string",
					"description": "Message to echo",
				},
			},
			Required: []string{"message"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			message, ok := params["message"].(string)
			if !ok {
				return nil, fmt.Errorf("message must be a string")
			}
			return map[string]interface{}{
				"message": message,
			}, nil
		},
	}

	// Calculator tool
	calculatorTool := &tools.Tool{
		Name:        "calculator",
		Description: "Performs basic mathematical operations",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"operation": map[string]interface{}{
					"type":        "string",
					"description": "Operation to perform (add, subtract, multiply, divide)",
					"enum":        []string{"add", "subtract", "multiply", "divide"},
				},
				"a": map[string]interface{}{
					"type":        "number",
					"description": "First number",
				},
				"b": map[string]interface{}{
					"type":        "number",
					"description": "Second number",
				},
			},
			Required: []string{"operation", "a", "b"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			operation, ok := params["operation"].(string)
			if !ok {
				return nil, fmt.Errorf("operation must be a string")
			}

			a, ok := params["a"].(float64)
			if !ok {
				// Try to convert from JSON number
				if aNum, ok := params["a"].(json.Number); ok {
					aFloat, err := aNum.Float64()
					if err != nil {
						return nil, fmt.Errorf("a must be a number")
					}
					a = aFloat
				} else {
					return nil, fmt.Errorf("a must be a number")
				}
			}

			b, ok := params["b"].(float64)
			if !ok {
				// Try to convert from JSON number
				if bNum, ok := params["b"].(json.Number); ok {
					bFloat, err := bNum.Float64()
					if err != nil {
						return nil, fmt.Errorf("b must be a number")
					}
					b = bFloat
				} else {
					return nil, fmt.Errorf("b must be a number")
				}
			}

			var result float64
			switch operation {
			case "add":
				result = a + b
			case "subtract":
				result = a - b
			case "multiply":
				result = a * b
			case "divide":
				if b == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				result = a / b
			default:
				return nil, fmt.Errorf("unknown operation: %s", operation)
			}

			return map[string]interface{}{
				"result": result,
			}, nil
		},
	}

	// Timestamp tool
	timestampTool := &tools.Tool{
		Name:        "timestamp",
		Description: "Returns current timestamp in various formats",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"format": map[string]interface{}{
					"type":        "string",
					"description": "Timestamp format (unix, rfc3339, or custom Go time format)",
					"enum":        []string{"unix", "rfc3339", "iso8601", "custom"},
				},
				"customFormat": map[string]interface{}{
					"type":        "string",
					"description": "Custom time format (Go time format string)",
				},
			},
			Required: []string{},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			format, _ := params["format"].(string)
			if format == "" {
				format = "rfc3339"
			}

			now := time.Now()
			var result string

			switch format {
			case "unix":
				result = fmt.Sprintf("%d", now.Unix())
			case "rfc3339":
				result = now.Format(time.RFC3339)
			case "iso8601":
				result = now.Format("2006-01-02T15:04:05-0700")
			case "custom":
				customFormat, ok := params["customFormat"].(string)
				if !ok || customFormat == "" {
					return nil, fmt.Errorf("customFormat is required for custom format")
				}
				result = now.Format(customFormat)
			default:
				// Try to use the format string directly
				result = now.Format(format)
			}

			return map[string]interface{}{
				"timestamp": result,
			}, nil
		},
	}

	// Random tool
	randomTool := &tools.Tool{
		Name:        "random",
		Description: "Generates random numbers",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"min": map[string]interface{}{
					"type":        "integer",
					"description": "Minimum value (inclusive)",
				},
				"max": map[string]interface{}{
					"type":        "integer",
					"description": "Maximum value (inclusive)",
				},
			},
			Required: []string{},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// Default min and max
			min := 1
			max := 100

			// Override with params if provided
			if minParam, ok := params["min"]; ok {
				if minVal, ok := minParam.(float64); ok {
					min = int(minVal)
				} else if minNum, ok := minParam.(json.Number); ok {
					minInt, err := minNum.Int64()
					if err == nil {
						min = int(minInt)
					}
				}
			}

			if maxParam, ok := params["max"]; ok {
				if maxVal, ok := maxParam.(float64); ok {
					max = int(maxVal)
				} else if maxNum, ok := maxParam.(json.Number); ok {
					maxInt, err := maxNum.Int64()
					if err == nil {
						max = int(maxInt)
					}
				}
			}

			// Validate
			if min >= max {
				return nil, fmt.Errorf("min must be less than max")
			}

			// Generate random number
			rand.Seed(time.Now().UnixNano())
			randomNum := rand.Intn(max-min+1) + min

			return map[string]interface{}{
				"random": randomNum,
			}, nil
		},
	}

	// Text tool
	textTool := &tools.Tool{
		Name:        "text",
		Description: "Performs various text operations",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"operation": map[string]interface{}{
					"type":        "string",
					"description": "Operation to perform (upper, lower, reverse, count)",
					"enum":        []string{"upper", "lower", "reverse", "count"},
				},
				"text": map[string]interface{}{
					"type":        "string",
					"description": "The text to process",
				},
			},
			Required: []string{"operation", "text"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			operation, ok := params["operation"].(string)
			if !ok {
				return nil, fmt.Errorf("operation must be a string")
			}

			text, ok := params["text"].(string)
			if !ok {
				return nil, fmt.Errorf("text must be a string")
			}

			var result interface{}
			switch operation {
			case "upper":
				result = strings.ToUpper(text)
			case "lower":
				result = strings.ToLower(text)
			case "reverse":
				// Reverse the string
				runes := []rune(text)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				result = string(runes)
			case "count":
				// Count characters and words
				words := len(strings.Fields(text))
				chars := len(text)
				result = map[string]int{
					"characters": chars,
					"words":      words,
				}
			default:
				return nil, fmt.Errorf("unknown operation: %s", operation)
			}

			return map[string]interface{}{
				"result": result,
			}, nil
		},
	}

	// File info tool - for editor integration
	fileInfoTool := &tools.Tool{
		Name:        "getFileInfo",
		Description: "Gets information about a file in the workspace",
		Category:    "editor",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"path": map[string]interface{}{
					"type":        "string",
					"description": "Path to the file",
				},
			},
			Required: []string{"path"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			path, ok := params["path"].(string)
			if !ok {
				return nil, fmt.Errorf("path must be a string")
			}

			// This is a stub implementation - in a real implementation,
			// you would use the editor context to access file information
			return map[string]interface{}{
				"path":   path,
				"exists": true,
				"size":   1024,
				"type":   "text",
			}, nil
		},
	}

	// Code completion tool - for editor integration
	codeCompletionTool := &tools.Tool{
		Name:        "completeCode",
		Description: "Provides code completion for the current position",
		Category:    "editor",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"code": map[string]interface{}{
					"type":        "string",
					"description": "Code snippet to complete",
				},
				"language": map[string]interface{}{
					"type":        "string",
					"description": "Programming language",
				},
				"line": map[string]interface{}{
					"type":        "integer",
					"description": "Line number (0-based)",
				},
				"character": map[string]interface{}{
					"type":        "integer",
					"description": "Character offset (0-based)",
				},
			},
			Required: []string{"code", "language"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// This is a stub implementation - in a real implementation,
			// you would use a language server or similar to provide completions
			return map[string]interface{}{
				"completions": []string{
					"function() { ... }",
					"class { ... }",
					"const variable = ...",
				},
			}, nil
		},
	}

	// Code analysis tool - for editor integration
	codeAnalysisTool := &tools.Tool{
		Name:        "analyzeCode",
		Description: "Analyzes code for issues and improvements",
		Category:    "editor",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"code": map[string]interface{}{
					"type":        "string",
					"description": "Code to analyze",
				},
				"language": map[string]interface{}{
					"type":        "string",
					"description": "Programming language",
				},
				"analysisType": map[string]interface{}{
					"type":        "string",
					"description": "Type of analysis to perform",
					"enum":        []string{"security", "performance", "style", "all"},
				},
			},
			Required: []string{"code", "language"},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			// This is a stub implementation
			return map[string]interface{}{
				"issues": []map[string]interface{}{
					{
						"line":    10,
						"type":    "warning",
						"message": "Unused variable",
					},
					{
						"line":    15,
						"type":    "error",
						"message": "Null pointer exception possible",
					},
				},
				"suggestions": []map[string]interface{}{
					{
						"line":    20,
						"message": "Consider using a more efficient algorithm",
					},
				},
			}, nil
		},
	}

	// Register tools
	logger.Info("Registering tools:")
	logger.Info("- echo: Simple echo tool")
	toolRegistry.RegisterTool(echoTool)

	logger.Info("- calculator: Mathematical operations tool")
	toolRegistry.RegisterTool(calculatorTool)

	logger.Info("- timestamp: Timestamp formatting tool")
	toolRegistry.RegisterTool(timestampTool)

	logger.Info("- random: Random number generator")
	toolRegistry.RegisterTool(randomTool)

	logger.Info("- text: Text manipulation tool")
	toolRegistry.RegisterTool(textTool)

	logger.Info("- getFileInfo: File information tool (editor integration)")
	toolRegistry.RegisterTool(fileInfoTool)

	logger.Info("- completeCode: Code completion tool (editor integration)")
	toolRegistry.RegisterTool(codeCompletionTool)

	logger.Info("- analyzeCode: Code analysis tool (editor integration)")
	toolRegistry.RegisterTool(codeAnalysisTool)

	logger.Info("Total tools registered: 8")
}
