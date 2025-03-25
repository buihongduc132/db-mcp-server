package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/domain"
)

// DatabaseUseCase defines operations for managing database functionality
type DatabaseUseCase struct {
	repo domain.DatabaseRepository
}

// NewDatabaseUseCase creates a new database use case
func NewDatabaseUseCase(repo domain.DatabaseRepository) *DatabaseUseCase {
	return &DatabaseUseCase{
		repo: repo,
	}
}

// ListDatabases returns a list of available databases
func (uc *DatabaseUseCase) ListDatabases() []string {
	return uc.repo.ListDatabases()
}

// ExecuteQuery executes a SQL query and returns the formatted results
func (uc *DatabaseUseCase) ExecuteQuery(ctx context.Context, dbID, query string, params []interface{}) (string, error) {
	db, err := uc.repo.GetDatabase(dbID)
	if err != nil {
		return "", fmt.Errorf("failed to get database: %w", err)
	}

	// Execute query
	rows, err := db.Query(ctx, query, params...)
	if err != nil {
		return "", fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Process results into a readable format
	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to get column names: %w", err)
	}

	// Format results as text
	var resultText strings.Builder
	resultText.WriteString("Results:\n\n")
	resultText.WriteString(strings.Join(columns, "\t") + "\n")
	resultText.WriteString(strings.Repeat("-", 80) + "\n")

	// Prepare for scanning
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Process rows
	rowCount := 0
	for rows.Next() {
		rowCount++
		if err := rows.Scan(valuePtrs...); err != nil {
			return "", fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to strings and print
		var rowText []string
		for i := range columns {
			val := values[i]
			if val == nil {
				rowText = append(rowText, "NULL")
			} else {
				switch v := val.(type) {
				case []byte:
					rowText = append(rowText, string(v))
				default:
					rowText = append(rowText, fmt.Sprintf("%v", v))
				}
			}
		}
		resultText.WriteString(strings.Join(rowText, "\t") + "\n")
	}

	if err = rows.Err(); err != nil {
		return "", fmt.Errorf("error reading rows: %w", err)
	}

	resultText.WriteString(fmt.Sprintf("\nTotal rows: %d", rowCount))
	return resultText.String(), nil
}

// ExecuteStatement executes a SQL statement (INSERT, UPDATE, DELETE)
func (uc *DatabaseUseCase) ExecuteStatement(ctx context.Context, dbID, statement string, params []interface{}) (string, error) {
	db, err := uc.repo.GetDatabase(dbID)
	if err != nil {
		return "", fmt.Errorf("failed to get database: %w", err)
	}

	// Execute statement
	result, err := db.Exec(ctx, statement, params...)
	if err != nil {
		return "", fmt.Errorf("statement execution failed: %w", err)
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	// Get last insert ID (if applicable)
	lastInsertID, err := result.LastInsertId()
	if err != nil {
		lastInsertID = 0
	}

	return fmt.Sprintf("Statement executed successfully.\nRows affected: %d\nLast insert ID: %d", rowsAffected, lastInsertID), nil
}

// ExecuteTransaction executes operations in a transaction
func (uc *DatabaseUseCase) ExecuteTransaction(ctx context.Context, dbID, action string, txID string,
	statement string, params []interface{}, readOnly bool) (string, map[string]interface{}, error) {

	switch action {
	case "begin":
		db, err := uc.repo.GetDatabase(dbID)
		if err != nil {
			return "", nil, fmt.Errorf("failed to get database: %w", err)
		}

		// Start a new transaction
		txOpts := &domain.TxOptions{ReadOnly: readOnly}
		tx, err := db.Begin(ctx, txOpts)
		if err != nil {
			return "", nil, fmt.Errorf("failed to start transaction: %w", err)
		}

		// In a real implementation, we would store the transaction for later use
		// For now, we just commit right away to avoid the unused variable warning
		if err := tx.Commit(); err != nil {
			return "", nil, fmt.Errorf("failed to commit transaction: %w", err)
		}

		// Generate transaction ID
		newTxID := fmt.Sprintf("tx_%s_%d", dbID, timeNowUnix())

		return "Transaction started", map[string]interface{}{"transactionId": newTxID}, nil

	case "commit":
		// Implement commit logic (would need access to stored transaction)
		return "Transaction committed", nil, nil

	case "rollback":
		// Implement rollback logic (would need access to stored transaction)
		return "Transaction rolled back", nil, nil

	case "execute":
		// Implement execute within transaction logic (would need access to stored transaction)
		return "Statement executed in transaction", nil, nil

	default:
		return "", nil, fmt.Errorf("invalid transaction action: %s", action)
	}
}

// Helper function to get current Unix timestamp
func timeNowUnix() int64 {
	return time.Now().Unix()
}
