package api

import (
	"encoding/json"
	"fmt"
	"mcpserver/internal/domain/entities"
	"mcpserver/internal/usecase"
	"net/http"
)

// ExecuteHandler handles MCP execute requests
type ExecuteHandler struct {
	dbUseCase     *usecase.DBUseCase
	clientUseCase *usecase.ClientUseCase
}

// NewExecuteHandler creates a new execute handler
func NewExecuteHandler(dbUseCase *usecase.DBUseCase, clientUseCase *usecase.ClientUseCase) *ExecuteHandler {
	return &ExecuteHandler{
		dbUseCase:     dbUseCase,
		clientUseCase: clientUseCase,
	}
}

// ServeHTTP handles the HTTP request for MCP execute operations
func (h *ExecuteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req entities.MCPRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.ClientID == "" {
		http.Error(w, "Missing client_id", http.StatusBadRequest)
		return
	}

	// Get client
	client, err := h.clientUseCase.GetClient(r.Context(), req.ClientID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Client not found: %v", err), http.StatusNotFound)
		return
	}

	// Process the request
	result, err := h.dbUseCase.ProcessRequest(r.Context(), &req)
	if err != nil {
		errMsg := fmt.Sprintf(`{"error": "%s"}`, err.Error())
		client.EventChan <- errMsg
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"data":   result,
	})
}
