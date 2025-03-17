package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// SimpleJSONRPCRequest represents a JSON-RPC request
type SimpleJSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// SimpleJSONRPCResponse represents a JSON-RPC response
type SimpleJSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	} `json:"error,omitempty"`
}

func main() {
	// Parse command line flags
	serverURL := flag.String("server", "http://localhost:9090", "MCP server URL")
	flag.Parse()

	fmt.Printf("Testing MCP server at %s\n", *serverURL)

	// Create a random session ID for testing
	sessionID := fmt.Sprintf("test-session-%d", time.Now().Unix())
	messageEndpoint := fmt.Sprintf("%s/message?sessionId=%s", *serverURL, sessionID)

	// Send initialize request
	fmt.Println("\nSending initialize request...")
	initializeReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "1.0.0",
			"clientInfo": map[string]string{
				"name":    "Simple Test Client",
				"version": "1.0.0",
			},
			"capabilities": map[string]interface{}{
				"toolsSupported": true,
			},
		},
	}
	sendRequest(messageEndpoint, initializeReq)

	// Wait a moment
	time.Sleep(500 * time.Millisecond)

	// Send tools/list request
	fmt.Println("\nSending tools/list request...")
	listReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}
	sendRequest(messageEndpoint, listReq)

	// Test each tool
	testTools(messageEndpoint)
}

func sendRequest(endpoint string, req SimpleJSONRPCRequest) {
	// Convert request to JSON
	reqData, err := json.Marshal(req)
	if err != nil {
		log.Printf("Failed to marshal request: %v", err)
		return
	}

	fmt.Printf("Request: %s\n", string(reqData))

	// Send request
	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		log.Printf("Failed to send request: %v", err)
		return
	}
	defer resp.Body.Close()

	// Read response
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response: %v", err)
		return
	}

	fmt.Printf("Response: %s\n", string(respData))

	// Parse the response
	var response SimpleJSONRPCResponse
	if err := json.Unmarshal(respData, &response); err != nil {
		log.Printf("Failed to parse response: %v", err)
		return
	}

	// Check for errors
	if response.Error != nil {
		fmt.Printf("Error: %s (code: %d)\n", response.Error.Message, response.Error.Code)
		return
	}

	// Print pretty result
	prettyResult, _ := json.MarshalIndent(response.Result, "", "  ")
	fmt.Printf("Result: %s\n", string(prettyResult))
}

func testTools(endpoint string) {
	// Test echo tool
	fmt.Println("\nTesting echo tool...")
	echoReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "tools/execute",
		Params: map[string]interface{}{
			"tool": "echo",
			"input": map[string]interface{}{
				"message": "Hello, MCP Server!",
			},
		},
	}
	sendRequest(endpoint, echoReq)

	// Test calculator tool
	fmt.Println("\nTesting calculator tool...")
	calcReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "tools/execute",
		Params: map[string]interface{}{
			"tool": "calculator",
			"input": map[string]interface{}{
				"operation": "add",
				"a":         5,
				"b":         3,
			},
		},
	}
	sendRequest(endpoint, calcReq)

	// Test timestamp tool
	fmt.Println("\nTesting timestamp tool...")
	timeReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/execute",
		Params: map[string]interface{}{
			"tool": "timestamp",
			"input": map[string]interface{}{
				"format": "rfc3339",
			},
		},
	}
	sendRequest(endpoint, timeReq)

	// Test random tool
	fmt.Println("\nTesting random tool...")
	randReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      6,
		Method:  "tools/execute",
		Params: map[string]interface{}{
			"tool": "random",
			"input": map[string]interface{}{
				"min": 1,
				"max": 100,
			},
		},
	}
	sendRequest(endpoint, randReq)

	// Test text tool
	fmt.Println("\nTesting text tool...")
	textReq := SimpleJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      7,
		Method:  "tools/execute",
		Params: map[string]interface{}{
			"tool": "text",
			"input": map[string]interface{}{
				"operation": "upper",
				"text":      "this text will be converted to uppercase",
			},
		},
	}
	sendRequest(endpoint, textReq)
}
