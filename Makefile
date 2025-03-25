.PHONY: build run-stdio run-sse clean test client client-simple test-script build-example

# Build the server
build:
	go build -o server cmd/server/main.go
	GOOS=linux GOARCH=amd64 go build -o server-linux cmd/server/main.go

# Build the example stdio server
build-example:
	cd examples && go build -o mcp-example mcp_stdio_example.go

# Run the server in stdio mode
run-stdio: build
	./server -t stdio

# Run the server in SSE mode
run-sse: clean build
	./server -t sse -p 9090 -h 127.0.0.1

# Build and run the example client
client:
	go build -o mcp-client examples/client/client.go
	./mcp-client

# Build and run the simple client (no SSE dependency)
client-simple:
	go build -o mcp-simple-client examples/client/simple_client.go
	./mcp-simple-client

# Run the test script
test-script:
	./examples/test_script.sh

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -f server server-linux mcp-client mcp-simple-client
	# lsof -i :9090 | grep LISTEN | awk '{print $2}' | xargs kill -9

# Run linter
lint:
	golangci-lint run ./...

# Setup
setup:
	go mod tidy
	go mod download

# Default target
all: build 