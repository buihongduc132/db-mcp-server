package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/r3labs/sse/v2"
)

// JSONRPCRequest represents a JSON-RPC request
type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// JSONRPCResponse represents a JSON-RPC response
type JSONRPCResponse struct {
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
	serverURL := flag.String("server", "http://localhost:8080", "MCP server URL")
	flag.Parse()

	// Create SSE client
	client := sse.NewClient(*serverURL + "/sse")

	// Create a channel to receive events
	events := make(chan *sse.Event)
	var sessionID string
	var messageEndpoint string

	// Subscribe to events
	go func() {
		err := client.SubscribeRaw(func(msg *sse.Event) {
			events <- msg
		})
		if err != nil {
			log.Fatalf("Failed to subscribe to events: %v", err)
		}
	}()

	// Handle events
	go func() {
		for event := range events {
			eventType := string(event.Event)
			data := string(event.Data)

			fmt.Printf("Received event: %s\n", eventType)
			fmt.Printf("Event data: %s\n", data)

			// Handle connection event
			if eventType == "connection" {
				var connectionInfo map[string]string
				if err := json.Unmarshal(event.Data, &connectionInfo); err != nil {
					log.Printf("Failed to parse connection info: %v", err)
					continue
				}

				sessionID = connectionInfo["sessionId"]
				messageEndpoint = connectionInfo["messageEndpoint"]

				fmt.Printf("Connected with session ID: %s\n", sessionID)
				fmt.Printf("Message endpoint: %s\n", messageEndpoint)

				// Send initialize request
				go func() {
					time.Sleep(500 * time.Millisecond) // Wait a bit for the connection to be established
					sendInitializeRequest(messageEndpoint)
				}()
			}

			// Handle message event
			if eventType == "message" {
				var response JSONRPCResponse
				if err := json.Unmarshal(event.Data, &response); err != nil {
					log.Printf("Failed to parse message: %v", err)
					continue
				}

				if response.Error != nil {
					fmt.Printf("Error: %s (code: %d)\n", response.Error.Message, response.Error.Code)
				} else {
					fmt.Printf("Result: %+v\n", response.Result)

					// If this is the initialize response, send a tools/list request
					if response.ID == 1 {
						go func() {
							time.Sleep(500 * time.Millisecond)
							sendToolsListRequest(messageEndpoint)
						}()
					}
				}
			}
		}
	}()

	// Wait for interrupt signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("Shutting down client...")
}

func sendInitializeRequest(endpoint string) {
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: map[string]interface{}{
			"protocolVersion": "1.0.0",
			"clientInfo": map[string]string{
				"name":    "Example Client",
				"version": "1.0.0",
			},
			"capabilities": map[string]interface{}{
				"toolsSupported": true,
			},
		},
	}

	sendRequest(endpoint, req)
}

func sendToolsListRequest(endpoint string) {
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	}

	sendRequest(endpoint, req)
}

func sendRequest(endpoint string, req JSONRPCRequest) {
	// Convert request to JSON
	reqData, err := json.Marshal(req)
	if err != nil {
		log.Printf("Failed to marshal request: %v", err)
		return
	}

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

	fmt.Printf("HTTP Response: %s\n", string(respData))
}
