package dbtools

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"mcpserver/pkg/tools"
)

// Transaction state storage (in-memory)
var activeTransactions = make(map[string]*sql.Tx)

// createTransactionTool creates a tool for managing database transactions
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
				"transactionId": map[string]interface{}{
					"type":        "string",
					"description": "Transaction ID (returned from begin, required for all other actions)",
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
			},
			Required: []string{"action"},
		},
		Handler: handleTransaction,
	}
}

// handleTransaction handles the transaction tool execution
func handleTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Check if database is initialized
	if dbInstance == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	// Extract action
	action, ok := getStringParam(params, "action")
	if !ok {
		return nil, fmt.Errorf("action parameter is required")
	}

	// Handle different actions
	switch action {
	case "begin":
		return beginTransaction(ctx, params)
	case "commit":
		return commitTransaction(ctx, params)
	case "rollback":
		return rollbackTransaction(ctx, params)
	case "execute":
		return executeInTransaction(ctx, params)
	default:
		return nil, fmt.Errorf("invalid action: %s", action)
	}
}

// beginTransaction starts a new transaction
func beginTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract timeout
	timeout := 30000 // Default timeout: 30 seconds
	if timeoutParam, ok := getIntParam(params, "timeout"); ok {
		timeout = timeoutParam
	}

	// Create context with timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	defer cancel()

	// Extract read-only flag
	readOnly := false
	if readOnlyParam, ok := params["readOnly"].(bool); ok {
		readOnly = readOnlyParam
	}

	// Set transaction options
	txOpts := &sql.TxOptions{
		ReadOnly: readOnly,
	}

	// Begin transaction
	tx, err := dbInstance.BeginTx(timeoutCtx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Generate transaction ID
	txID := fmt.Sprintf("tx-%d", time.Now().UnixNano())

	// Store transaction
	activeTransactions[txID] = tx

	// Return transaction ID
	return map[string]interface{}{
		"transactionId": txID,
		"readOnly":      readOnly,
		"status":        "active",
	}, nil
}

// commitTransaction commits a transaction
func commitTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Get transaction
	tx, ok := activeTransactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Commit transaction
	err := tx.Commit()

	// Remove transaction from storage
	delete(activeTransactions, txID)

	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return success
	return map[string]interface{}{
		"transactionId": txID,
		"status":        "committed",
	}, nil
}

// rollbackTransaction rolls back a transaction
func rollbackTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Get transaction
	tx, ok := activeTransactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Rollback transaction
	err := tx.Rollback()

	// Remove transaction from storage
	delete(activeTransactions, txID)

	if err != nil {
		return nil, fmt.Errorf("failed to rollback transaction: %w", err)
	}

	// Return success
	return map[string]interface{}{
		"transactionId": txID,
		"status":        "rolled back",
	}, nil
}

// executeInTransaction executes a statement within a transaction
func executeInTransaction(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract transaction ID
	txID, ok := getStringParam(params, "transactionId")
	if !ok {
		return nil, fmt.Errorf("transactionId parameter is required")
	}

	// Get transaction
	tx, ok := activeTransactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	// Extract statement
	statement, ok := getStringParam(params, "statement")
	if !ok {
		return nil, fmt.Errorf("statement parameter is required")
	}

	// Extract statement parameters
	var statementParams []interface{}
	if paramsArray, ok := getArrayParam(params, "params"); ok {
		statementParams = make([]interface{}, len(paramsArray))
		for i, param := range paramsArray {
			statementParams[i] = param
		}
	}

	// Check if statement is a query or an execute statement
	isQuery := isQueryStatement(statement)

	var result interface{}

	if isQuery {
		// Execute query within transaction
		rows, err := tx.QueryContext(ctx, statement, statementParams...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute query in transaction: %w", err)
		}
		defer rows.Close()

		// Convert rows to map
		results, err := rowsToMaps(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to process query results in transaction: %w", err)
		}

		result = map[string]interface{}{
			"rows":  results,
			"count": len(results),
		}
	} else {
		// Execute statement within transaction
		execResult, err := tx.ExecContext(ctx, statement, statementParams...)
		if err != nil {
			return nil, fmt.Errorf("failed to execute statement in transaction: %w", err)
		}

		// Get affected rows
		rowsAffected, err := execResult.RowsAffected()
		if err != nil {
			rowsAffected = -1 // Unable to determine
		}

		// Get last insert ID (if applicable)
		lastInsertID, err := execResult.LastInsertId()
		if err != nil {
			lastInsertID = -1 // Unable to determine
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

// isQueryStatement determines if a statement is a query (SELECT) or not
func isQueryStatement(statement string) bool {
	// Simple heuristic: if the statement starts with SELECT, it's a query
	// This is a simplification; a real implementation would use a proper SQL parser
	return len(statement) >= 6 && statement[0:6] == "SELECT"
}
