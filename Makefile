.PHONY: build run-stdio run-sse clean test client

# Build the server
build:
	go build -o mcp-server cmd/server/main.go

# Run the server in stdio mode
run-stdio: build
	./mcp-server --transport stdio

# Run the server in SSE mode
run-sse: build
	./mcp-server -t sse

# Build and run the example client
client:
	go build -o mcp-client examples/client/client.go
	./mcp-client

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -f mcp-server mcp-client

# Default target
all: build 