.PHONY: build run run-stdio run-sse test clean

# Variables
BINARY_NAME=server
CMD_DIR=./cmd/server
PORT?=9090
DEBUG?=0

# Build the application
build:
	@echo "Building..."
	@go build -o $(BINARY_NAME) $(CMD_DIR)
	@chmod +x ./$(BINARY_NAME)
	@ls -l ./$(BINARY_NAME)

# Run the application (uses .env configuration)
run: build
	@echo "Running with .env configuration on port $(PORT)..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		./$(BINARY_NAME) -port $(PORT); \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		go run $(CMD_DIR) -port $(PORT); \
	fi

# Run with stdio transport (for local development with Cursor)
run-stdio: build
	@echo "Running with stdio transport..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		./$(BINARY_NAME) -transport stdio; \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		go run $(CMD_DIR) -transport stdio; \
	fi

# Run with SSE transport (for production)
run-sse: build
	@echo "Running with SSE transport on port $(PORT)..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		./$(BINARY_NAME) -transport sse -port $(PORT); \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		go run $(CMD_DIR) -transport sse -port $(PORT); \
	fi

# Run with MySQL (uses .env configuration but enforces MySQL)
run-mysql: build
	@echo "Running with MySQL and .env configuration on port $(PORT)..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		./$(BINARY_NAME) -db-type mysql -port $(PORT); \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		go run $(CMD_DIR) -db-type mysql -port $(PORT); \
	fi

# Run in debug mode with more verbose logging
debug: build
	@echo "Running in debug mode with .env configuration on port $(PORT)..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		DEBUG=1 ./$(BINARY_NAME) -port $(PORT); \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		DEBUG=1 go run $(CMD_DIR) -port $(PORT); \
	fi

# Test the application
test:
	@echo "Testing..."
	@go test -v ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f $(BINARY_NAME)

# Add PostgreSQL support
postgres-deps:
	@echo "Adding PostgreSQL dependencies..."
	@go get github.com/lib/pq

# Run with PostgreSQL (uses .env configuration but enforces PostgreSQL)
run-postgres: build postgres-deps
	@echo "Running with PostgreSQL and .env configuration on port $(PORT)..."
	@if [ -x ./$(BINARY_NAME) ]; then \
		./$(BINARY_NAME) -db-type postgres -port $(PORT); \
	else \
		echo "Permission issue detected, running with 'go run' instead"; \
		go run $(CMD_DIR) -db-type postgres -port $(PORT); \
	fi

# Create .env file from example if it doesn't exist
init-env:
	@if [ ! -f .env ]; then \
		echo "Creating .env file from .env.example..."; \
		cp .env.example .env; \
		echo "Please edit .env file with your configuration"; \
	else \
		echo ".env file already exists"; \
	fi

# Force set executable permissions (use when regular chmod fails)
fix-permissions:
	@echo "Setting executable permissions with sudo (may prompt for password)..."
	@sudo chmod +x ./$(BINARY_NAME)
	@ls -l ./$(BINARY_NAME)

# Kill any running instances of the server on the specified port
kill-server:
	@echo "Killing any running instances on port $(PORT)..."
	@lsof -ti:$(PORT) | xargs kill -9 2>/dev/null || echo "No server running on port $(PORT)"

# Check database connection
check-db:
	@echo "Checking database connection..."
	@if [ "$(DB_TYPE)" = "mysql" ] || [ -z "$(DB_TYPE)" ]; then \
		echo "Testing MySQL connection to $(DB_HOST):$(DB_PORT)..."; \
		mysql -u$(DB_USER) -p$(DB_PASSWORD) -h$(DB_HOST) -P$(DB_PORT) -e "SELECT 1" 2>/dev/null && echo "MySQL connection successful!" || echo "MySQL connection failed!"; \
	elif [ "$(DB_TYPE)" = "postgres" ]; then \
		echo "Testing PostgreSQL connection to $(DB_HOST):$(DB_PORT)..."; \
		PGPASSWORD=$(DB_PASSWORD) psql -U $(DB_USER) -h $(DB_HOST) -p $(DB_PORT) -d $(DB_NAME) -c "SELECT 1" 2>/dev/null && echo "PostgreSQL connection successful!" || echo "PostgreSQL connection failed!"; \
	else \
		echo "Unknown database type: $(DB_TYPE)"; \
	fi 