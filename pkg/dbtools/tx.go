package dbtools

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/FreePeak/db-mcp-server/pkg/db"
	"github.com/FreePeak/db-mcp-server/pkg/tools"
)

// Map to store active transactions
var transactions = make(map[string]*sql.Tx)

// getBoolParam extracts a boolean parameter from the params map
func getBoolParam(params map[string]interface{}, key string) (bool, bool) {
	if val, ok := params[key].(bool); ok {
		return val, true
	}
	return false, false
}

// generateTransactionID generates a unique transaction ID
func generateTransactionID() string {
	return fmt.Sprintf("tx-%d", time.Now().UnixNano())
}

// createTransactionTool creates a tool for managing database transactions
//
//nolint:unused // Retained for future use
func createTransactionTool() *tools.Tool {
	return &tools.Tool{
		Name:        "dbTransaction",
		Description: "Manage database transactions (begin, commit, rollback, execute within transaction)",
		Category:    "database",
		InputSchema: tools.ToolInputSchema{
			Type: "object",
			Properties: map[string]interface{}{
				"action": map[string]interface{}{
					"type":        "string",
					"description": "Action to perform (begin, commit, rollback, execute)",
					"enum":        []string{"begin", "commit", "rollback", "execute"},
				},
				"statement": map[string]interface{}{
					"type":        "string",
					"description": "SQL statement to execute (required for execute action)",
				},
				"params": map[string]interface{}{
					"type":        "array",
					"description": "Parameters for the statement (for prepared statements)",
					"items": map[string]interface{}{
						"type": "string",
					},
				},
				"readOnly": map[string]interface{}{
					"type":        "boolean",
					"description": "Whether the transaction is read-only (for begin action)",
				},
				"timeout": map[string]interface{}{
					"type":        "integer",
					"description": "Timeout in milliseconds (default: 30000)",
				},
				"transactionId": map[string]interface{}{
					"type":        "string",
					"description": "Transaction ID (required for commit, rollback, and execute actions)",
				},
				"database": map[string]interface{}{
					"type":        "string",
					"description": "Database ID to use (optional if only one database is configured)",
				},
			},
			Required: []string{"action", "database"},
		},
		Handler: handleTransaction,
	}
}

// handleTransaction handles the transaction tool execution
func handleTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database manager is initialized
	if dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	// Extract parameters
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Get database ID
	databaseID, ok := getStringParam(params, "database")
	if !ok {
		return nil, fmt.Errorf("database parameter is required")
	}

	// Get database instance
	db, err := dbManager.GetDB(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	// Extract optional parameters
	statement, _ := getStringParam(params, "statement")
	transactionID, _ := getStringParam(params, "transactionId")
	readOnly, _ := getBoolParam(params, "readOnly")
	paramArray, _ := getArrayParam(params, "params")
	timeout, hasTimeout := getIntParam(params, "timeout")
	if !hasTimeout {
		timeout = 30000 // Default timeout: 30 seconds
	}

	// Convert interface array to string array
	var paramStrings []string
	if paramArray != nil {
		paramStrings = make([]string, len(paramArray))
		for i, p := range paramArray {
			if str, ok := p.(string); ok {
				paramStrings[i] = str
			}
		}
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Execute requested action
	switch action {
	case "begin":
		return beginTransaction(timeoutCtx, db, readOnly)
	case "commit":
		if transactionID == "" {
			return nil, fmt.Errorf("transactionId parameter is required for commit action")
		}
		return commitTransaction(timeoutCtx, transactionID)
	case "rollback":
		if transactionID == "" {
			return nil, fmt.Errorf("transactionId parameter is required for rollback action")
		}
		return rollbackTransaction(timeoutCtx, transactionID)
	case "execute":
		if transactionID == "" {
			return nil, fmt.Errorf("transactionId parameter is required for execute action")
		}
		if statement == "" {
			return nil, fmt.Errorf("statement parameter is required for execute action")
		}
		return executeInTransaction(timeoutCtx, transactionID, statement, paramStrings)
	default:
		return nil, fmt.Errorf("invalid action: %s", action)
	}
}

// beginTransaction starts a new transaction
func beginTransaction(ctx context.Context, db db.Database, readOnly bool) (interface{}, error) {
	txOpts := &sql.TxOptions{
		ReadOnly: readOnly,
	}

	tx, err := db.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Generate a unique transaction ID
	txID := generateTransactionID()
	transactions[txID] = tx

	return map[string]interface{}{
		"transactionId": txID,
		"readOnly":      readOnly,
	}, nil
}

// commitTransaction commits a transaction
func commitTransaction(ctx context.Context, txID string) (interface{}, error) {
	tx, ok := transactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	delete(transactions, txID)
	return map[string]interface{}{
		"status":  "success",
		"message": "Transaction committed successfully",
	}, nil
}

// rollbackTransaction rolls back a transaction
func rollbackTransaction(ctx context.Context, txID string) (interface{}, error) {
	tx, ok := transactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	if err := tx.Rollback(); err != nil {
		return nil, fmt.Errorf("failed to rollback transaction: %w", err)
	}

	delete(transactions, txID)
	return map[string]interface{}{
		"status":  "success",
		"message": "Transaction rolled back successfully",
	}, nil
}

// executeInTransaction executes a statement within a transaction
func executeInTransaction(ctx context.Context, txID string, statement string, params []string) (interface{}, error) {
	tx, ok := transactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Convert string parameters to interface{}
	args := make([]interface{}, len(params))
	for i, p := range params {
		args[i] = p
	}

	result, err := tx.Exec(statement, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement in transaction: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return map[string]interface{}{
		"status":       "success",
		"rowsAffected": rowsAffected,
	}, nil
}

// isQueryStatement checks if a SQL statement is a query
//
//nolint:unused // Retained for future use
func isQueryStatement(statement string) bool {
	// Simple heuristic: if the statement starts with SELECT, it's a query
	// This is a simplification; a real implementation would use a proper SQL parser
	return len(statement) >= 6 && statement[0:6] == "SELECT"
}

// createMockTransactionTool creates a mock version of the transaction tool
//
//nolint:unused // Retained for future use
func createMockTransactionTool() *tools.Tool {
	// Create the tool using the same schema as the real transaction tool
	tool := createTransactionTool()

	// Replace the handler with mock implementation
	tool.Handler = handleMockTransaction

	return tool
}

//nolint:unused // Retained for future use
var mockActiveTransactions = make(map[string]bool)

// handleMockTransaction is a mock implementation of the transaction handler
//
//nolint:unused // Retained for future use
func handleMockTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract action parameter
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Validate action
	validActions := map[string]bool{"begin": true, "commit": true, "rollback": true, "execute": true}
	if !validActions[action] {
		return nil, fmt.Errorf("invalid action: %s", action)
	}

	// Handle different actions
	switch action {
	case "begin":
		return handleMockBeginTransaction(params)
	case "commit":
		return handleMockCommitTransaction(params)
	case "rollback":
		return handleMockRollbackTransaction(params)
	case "execute":
		return handleMockExecuteTransaction(params)
	default:
		return nil, fmt.Errorf("unsupported action: %s", action)
	}
}

// handleMockBeginTransaction is a mock implementation for beginning a transaction
//
//nolint:unused // Retained for future use
func handleMockBeginTransaction(params map[string]interface{}) (interface{}, error) {
	// Extract read-only parameter (optional)
	readOnly, _ := params["readOnly"].(bool)

	// Generate a transaction ID
	txID := fmt.Sprintf("mock-tx-%d", time.Now().UnixNano())

	// Store in mock transaction state
	mockActiveTransactions[txID] = true

	// Return transaction info
	return map[string]interface{}{
		"transactionId": txID,
		"readOnly":      readOnly,
		"status":        "active",
	}, nil
}

// handleMockCommitTransaction is a mock implementation for committing a transaction
//
//nolint:unused // Retained for future use
func handleMockCommitTransaction(params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Verify transaction exists
	if !mockActiveTransactions[txID] {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Remove from active transactions
	delete(mockActiveTransactions, txID)

	// Return success
	return map[string]interface{}{
		"transactionId": txID,
		"status":        "committed",
	}, nil
}

// handleMockRollbackTransaction is a mock implementation for rolling back a transaction
//
//nolint:unused // Retained for future use
func handleMockRollbackTransaction(params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Verify transaction exists
	if !mockActiveTransactions[txID] {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Remove from active transactions
	delete(mockActiveTransactions, txID)

	// Return success
	return map[string]interface{}{
		"transactionId": txID,
		"status":        "rolled back",
	}, nil
}

// handleMockExecuteTransaction is a mock implementation for executing a statement in a transaction
//
//nolint:unused // Retained for future use
func handleMockExecuteTransaction(params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Verify transaction exists
	if !mockActiveTransactions[txID] {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Extract statement
	statement, ok := getStringParam(params, "statement")
	if !ok {
		return nil, fmt.Errorf("statement parameter is required")
	}

	// Extract statement parameters if provided
	var statementParams []interface{}
	if paramsArray, ok := getArrayParam(params, "params"); ok {
		statementParams = paramsArray
	}

	// Determine if this is a query or not (SELECT = query, otherwise execute)
	isQuery := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement)), "SELECT")

	var result map[string]interface{}

	if isQuery {
		// Generate mock query results
		mockRows := []map[string]interface{}{
			{"column1": "mock value 1", "column2": 42},
			{"column1": "mock value 2", "column2": 84},
		}

		result = map[string]interface{}{
			"rows":  mockRows,
			"count": len(mockRows),
		}
	} else {
		// Generate mock execute results
		var rowsAffected int64 = 1
		var lastInsertID int64 = -1

		if strings.Contains(strings.ToUpper(statement), "INSERT") {
			lastInsertID = time.Now().Unix() % 1000
		} else if strings.Contains(strings.ToUpper(statement), "UPDATE") {
			rowsAffected = int64(1 + (time.Now().Unix() % 3))
		} else if strings.Contains(strings.ToUpper(statement), "DELETE") {
			rowsAffected = int64(time.Now().Unix() % 3)
		}

		result = map[string]interface{}{
			"rowsAffected": rowsAffected,
			"lastInsertId": lastInsertID,
		}
	}

	// Return results
	return map[string]interface{}{
		"transactionId": txID,
		"statement":     statement,
		"params":        statementParams,
		"result":        result,
	}, nil
}
