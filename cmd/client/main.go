package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// Tool represents a tool definition
type Tool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Arguments   []ToolArgument         `json:"arguments,omitempty"`
}

// ToolArgument represents an argument for a tool
type ToolArgument struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Required    bool                   `json:"required"`
	Schema      map[string]interface{} `json:"schema"`
}

// ToolsEvent represents a tools_available event
type ToolsEvent struct {
	JsonRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Tools []Tool `json:"tools"`
	} `json:"params"`
}

// ToolRequest represents a request to execute a tool
type ToolRequest struct {
	JsonRPC string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	Params  map[string]interface{} `json:"params"`
	ID      string                 `json:"id"`
}

// ToolResponse represents a response from tool execution
type ToolResponse struct {
	JsonRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
	ID string `json:"id"`
}

func main() {
	// Use either the provided script or default to mock_cursor.sh
	serverScript := os.Getenv("SCRIPT_PATH")
	if serverScript == "" {
		serverScript = "./mock_cursor.sh"
	}

	// Check if server script exists
	if _, err := os.Stat(serverScript); os.IsNotExist(err) {
		fmt.Printf("Server script not found at %s\n", serverScript)
		return
	}

	fmt.Printf("Using script: %s\n", serverScript)

	// Start server process
	cmd := exec.Command(serverScript)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		fmt.Printf("Error creating stdin pipe: %v\n", err)
		return
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Printf("Error creating stdout pipe: %v\n", err)
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("Error creating stderr pipe: %v\n", err)
		return
	}

	// Start stderr reader in a goroutine
	go func() {
		stderrReader := bufio.NewReader(stderr)
		for {
			line, err := stderrReader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Stderr read error: %v\n", err)
				}
				return
			}
			fmt.Printf("Server stderr: %s", line)
		}
	}()

	if err := cmd.Start(); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
		return
	}

	fmt.Println("Server started, waiting for tools event...")

	// Wait for tools event
	reader := bufio.NewReader(stdout)
	toolsEventJSON, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading tools event: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Parse tools event
	var toolsEvent ToolsEvent
	if err := json.Unmarshal([]byte(toolsEventJSON), &toolsEvent); err != nil {
		fmt.Printf("Error parsing tools event: %v\nRaw JSON: %s\n", err, toolsEventJSON)
		cmd.Process.Kill()
		return
	}

	// Pretty print tools event
	prettyJSON, _ := json.MarshalIndent(toolsEvent, "", "  ")
	fmt.Printf("Received tools event:\n%s\n", string(prettyJSON))

	if len(toolsEvent.Params.Tools) == 0 {
		fmt.Println("Warning: No tools available!")
	}

	// Test execute_query tool
	fmt.Println("\nSending tool execution request for execute_query...")

	// Create request
	var toolRequest ToolRequest
	toolRequest = ToolRequest{
		JsonRPC: "2.0",
		Method:  "execute_tool",
		Params: map[string]interface{}{
			"name": "execute_query",
			"arguments": map[string]interface{}{
				"sql": "SELECT 1",
			},
		},
		ID: "1",
	}

	// Send request
	requestJSON, _ := json.Marshal(toolRequest)
	fmt.Printf("Sending request: %s\n", string(requestJSON))
	if _, err := io.WriteString(stdin, string(requestJSON)+"\n"); err != nil {
		fmt.Printf("Error sending request: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Wait for response
	time.Sleep(100 * time.Millisecond)
	responseJSON, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Parse response
	var toolResponse ToolResponse
	if err := json.Unmarshal([]byte(responseJSON), &toolResponse); err != nil {
		fmt.Printf("Error parsing response: %v\nRaw JSON: %s\n", err, responseJSON)
		cmd.Process.Kill()
		return
	}

	// Pretty print response
	prettyJSON, _ = json.MarshalIndent(toolResponse, "", "  ")
	fmt.Printf("Received response:\n%s\n", string(prettyJSON))

	// Clean up
	cmd.Process.Kill()
	fmt.Println("\nTest completed successfully!")
}
