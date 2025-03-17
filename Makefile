.PHONY: build run-stdio run-sse clean

# Build the server
build:
	go build -o mcp-server cmd/server/main.go

# Run the server in stdio mode
run-stdio: build
	./mcp-server --transport stdio

# Run the server in SSE mode
run-sse: build
	./mcp-server --transport sse --port 9090

# Clean build artifacts
clean:
	rm -f mcp-server

# Default target
all: build 