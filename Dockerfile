FROM golang:1.22-alpine AS builder

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
COPY config.json /app/config.json

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV SERVER_PORT=9090
ENV TRANSPORT_MODE=sse
ENV DB_CONFIG_FILE=/app/config.json
ENV MCP_TOOL_PREFIX=mcp_cashflow_db_mcp_server_sse

# Expose server port
EXPOSE 9090

# Start the MCP server with proper configuration
CMD ["/app/server", "-c", "/app/config.json", "-p", "9090", "-t", "sse"]

# You can override the port by passing it as a command-line argument
# docker run -p 8080:8080 db-mcp-server -port 8080 