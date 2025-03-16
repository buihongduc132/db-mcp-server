package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/usecase"
	"net/http"
	"os"
)

// CursorMCPHandler handles Cursor MCP protocol requests over HTTP
type CursorMCPHandler struct {
	mcpUseCase *usecase.CursorMCPUseCase
}

// NewCursorMCPHandler creates a new Cursor MCP handler
func NewCursorMCPHandler(mcpUseCase *usecase.CursorMCPUseCase) *CursorMCPHandler {
	return &CursorMCPHandler{
		mcpUseCase: mcpUseCase,
	}
}

// ServeHTTP handles HTTP requests for Cursor MCP
func (h *CursorMCPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	// Handle preflight requests
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Set JSON content type
	w.Header().Set("Content-Type", "application/json")

	// Handle GET requests (tools listing)
	if r.Method == http.MethodGet {
		h.handleToolsListing(w, r)
		return
	}

	// Handle POST requests (tool execution)
	if r.Method == http.MethodPost {
		h.handleToolExecution(w, r)
		return
	}

	// Method not allowed
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

// handleToolsListing handles GET requests for tools listing
func (h *CursorMCPHandler) handleToolsListing(w http.ResponseWriter, r *http.Request) {
	// Get tools event
	toolsEvent, err := h.mcpUseCase.GetToolsEvent(r.Context())
	if err != nil {
		log.Printf("Error getting tools: %v", err)
		http.Error(w, fmt.Sprintf("Error getting tools: %v", err), http.StatusInternalServerError)
		return
	}

	// Debug: Print the tools event
	jsonBytes, _ := json.MarshalIndent(toolsEvent, "", "  ")
	fmt.Fprintf(os.Stderr, "DEBUG - Sending tools event:\n%s\n", string(jsonBytes))

	// Write response
	if err := json.NewEncoder(w).Encode(toolsEvent); err != nil {
		log.Printf("Error encoding tools event: %v", err)
		http.Error(w, fmt.Sprintf("Error encoding tools event: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleToolExecution handles POST requests for tool execution
func (h *CursorMCPHandler) handleToolExecution(w http.ResponseWriter, r *http.Request) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v", err)
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Debug: Print the request body
	fmt.Fprintf(os.Stderr, "DEBUG - Received tool request:\n%s\n", string(body))

	// Parse tool request
	var toolRequest entities.MCPToolRequest
	if err := json.Unmarshal(body, &toolRequest); err != nil {
		log.Printf("Error parsing tool request: %v", err)
		http.Error(w, fmt.Sprintf("Error parsing tool request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate JSON-RPC 2.0 format
	if toolRequest.JsonRPC != entities.JSONRPCVersion {
		errorMsg := fmt.Sprintf("Invalid JSON-RPC version: expected %s", entities.JSONRPCVersion)
		log.Print(errorMsg)
		http.Error(w, errorMsg, http.StatusBadRequest)
		return
	}

	// Execute the tool
	response, err := h.mcpUseCase.ExecuteTool(r.Context(), &toolRequest)
	if err != nil {
		log.Printf("Error executing tool: %v", err)
		http.Error(w, fmt.Sprintf("Error executing tool: %v", err), http.StatusInternalServerError)
		return
	}

	// Debug: Print the response
	responseBytes, _ := json.MarshalIndent(response, "", "  ")
	fmt.Fprintf(os.Stderr, "DEBUG - Sending tool response:\n%s\n", string(responseBytes))

	// Write response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding tool response: %v", err)
		http.Error(w, fmt.Sprintf("Error encoding tool response: %v", err), http.StatusInternalServerError)
		return
	}
}
