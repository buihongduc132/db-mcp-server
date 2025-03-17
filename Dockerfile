FROM golang:1.21-alpine AS builder

# Install necessary build tools
RUN apk add --no-cache make gcc musl-dev

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files to download dependencies
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire project
COPY . .

# Build the application
RUN make build

# Create a smaller production image
FROM alpine:latest

# Add necessary runtime packages
RUN apk add --no-cache ca-certificates tzdata

# Set the working directory
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/mcp-server /app/mcp-server

# Copy example .env file (can be overridden with volume mounts)
COPY .env.example /app/.env

# Expose the server port (default in the .env file is 9090)
EXPOSE 9090

# Command to run the application in SSE mode
ENTRYPOINT ["/app/mcp-server", "-t", "sse"]

# You can override the port by passing it as a command-line argument
# docker run -p 8080:8080 db-mcp-server -port 8080 