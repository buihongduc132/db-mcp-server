package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
)

// InitializeRequest represents an initialize request
type InitializeRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  struct {
		ClientID  string `json:"clientId"`
		SessionID string `json:"sessionId"`
	} `json:"params"`
}

// InitializeResponse represents an initialize response
type InitializeResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Result  struct {
		Name         string `json:"name"`
		Version      string `json:"version"`
		Instructions string `json:"instructions,omitempty"`
		Capabilities struct {
			Resources bool `json:"resources"`
			Prompts   bool `json:"prompts"`
			Tools     bool `json:"tools"`
			Logging   bool `json:"logging"`
		} `json:"capabilities"`
	} `json:"result"`
}

// PingRequest represents a ping request
type PingRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
}

// PingResponse represents a ping response
type PingResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      string      `json:"id"`
	Result  interface{} `json:"result"`
}

// Tool represents a tool definition
type Tool struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

// ToolRequest represents a request to execute a tool
type ToolRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Method  string `json:"method"`
	Params  struct {
		Name string                 `json:"name"`
		Args map[string]interface{} `json:"args"`
	} `json:"params"`
}

// ToolResponse represents a response from tool execution
type ToolResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id"`
	Result  struct {
		Result interface{} `json:"result"`
	} `json:"result,omitempty"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func main() {
	// Use either the provided script or default to the server binary
	serverScript := os.Getenv("SERVER_PATH")
	if serverScript == "" {
		serverScript = "./mcp-server"
	}

	// Check if server script exists
	if _, err := os.Stat(serverScript); os.IsNotExist(err) {
		fmt.Printf("Server binary not found at %s\n", serverScript)
		return
	}

	fmt.Printf("Using server: %s\n", serverScript)

	// Start server process
	cmd := exec.Command(serverScript, "--transport", "stdio")
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

	fmt.Println("Server started, sending initialize request...")

	// Create initialize request
	initRequest := InitializeRequest{
		JSONRPC: "2.0",
		ID:      "init-1",
		Method:  "initialize",
		Params: struct {
			ClientID  string `json:"clientId"`
			SessionID string `json:"sessionId"`
		}{
			ClientID:  "test-client",
			SessionID: "test-session",
		},
	}

	// Send initialize request
	initRequestJSON, _ := json.Marshal(initRequest)
	fmt.Printf("Sending initialize request: %s\n", string(initRequestJSON))
	if _, err := io.WriteString(stdin, string(initRequestJSON)+"\n"); err != nil {
		fmt.Printf("Error sending initialize request: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Read response
	reader := bufio.NewReader(stdout)
	initResponseJSON, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading initialize response: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Parse initialize response
	var initResponse InitializeResponse
	if err := json.Unmarshal([]byte(initResponseJSON), &initResponse); err != nil {
		fmt.Printf("Error parsing initialize response: %v\nRaw JSON: %s\n", err, initResponseJSON)
		cmd.Process.Kill()
		return
	}

	// Pretty print initialize response
	prettyJSON, _ := json.MarshalIndent(initResponse, "", "  ")
	fmt.Printf("Received initialize response:\n%s\n", string(prettyJSON))

	// Send ping request
	fmt.Println("\nSending ping request...")
	pingRequest := PingRequest{
		JSONRPC: "2.0",
		ID:      "ping-1",
		Method:  "ping",
	}

	pingRequestJSON, _ := json.Marshal(pingRequest)
	fmt.Printf("Sending ping request: %s\n", string(pingRequestJSON))
	if _, err := io.WriteString(stdin, string(pingRequestJSON)+"\n"); err != nil {
		fmt.Printf("Error sending ping request: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Read ping response
	pingResponseJSON, err := reader.ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading ping response: %v\n", err)
		cmd.Process.Kill()
		return
	}

	// Parse ping response
	var pingResponse PingResponse
	if err := json.Unmarshal([]byte(pingResponseJSON), &pingResponse); err != nil {
		fmt.Printf("Error parsing ping response: %v\nRaw JSON: %s\n", err, pingResponseJSON)
		cmd.Process.Kill()
		return
	}

	// Pretty print ping response
	prettyJSON, _ = json.MarshalIndent(pingResponse, "", "  ")
	fmt.Printf("Received ping response:\n%s\n", string(prettyJSON))

	// Clean up
	cmd.Process.Kill()
	fmt.Println("\nTest completed successfully!")
}
