package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"mcpserver/internal/domain/repositories"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresRepository implements the DBRepository interface for PostgreSQL
type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository creates a new PostgreSQL repository
func NewPostgresRepository(connStr string) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &PostgresRepository{
		db: db,
	}, nil
}

// ExecuteQuery executes a SQL query and returns the result rows
func (r *PostgresRepository) ExecuteQuery(ctx context.Context, sqlQuery string) ([]map[string]interface{}, error) {
	rows, err := r.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i].(*interface{})
			row[col] = *val
		}

		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return results, nil
}

// InsertData inserts data into a table and returns the inserted ID
func (r *PostgresRepository) InsertData(ctx context.Context, table string, data map[string]interface{}) (int64, error) {
	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}
	paramCount := 1

	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", paramCount))
		values = append(values, val)
		paramCount++
	}

	// PostgreSQL uses RETURNING for getting the inserted ID
	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","))

	var id int64
	err := r.db.QueryRowContext(ctx, sqlStr, values...).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert error: %w", err)
	}

	return id, nil
}

// UpdateData updates data in a table based on a condition and returns number of affected rows
func (r *PostgresRepository) UpdateData(ctx context.Context, table string, data map[string]interface{}, condition string) (int64, error) {
	sets := []string{}
	values := []interface{}{}
	paramCount := 1

	for col, val := range data {
		sets = append(sets, fmt.Sprintf("%s = $%d", col, paramCount))
		values = append(values, val)
		paramCount++
	}

	sqlStr := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(sets, ","), condition)
	result, err := r.db.ExecContext(ctx, sqlStr, values...)
	if err != nil {
		return 0, fmt.Errorf("update error: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return affected, nil
}

// DeleteData deletes data from a table based on a condition and returns number of affected rows
func (r *PostgresRepository) DeleteData(ctx context.Context, table string, condition string) (int64, error) {
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE %s", table, condition)
	result, err := r.db.ExecContext(ctx, sqlStr)
	if err != nil {
		return 0, fmt.Errorf("delete error: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error getting rows affected: %w", err)
	}

	return affected, nil
}

// Close closes the database connection
func (r *PostgresRepository) Close() error {
	return r.db.Close()
}

// Ping checks if the database connection is alive
func (r *PostgresRepository) Ping() error {
	return r.db.Ping()
}

// SubscribeToChanges subscribes to changes in a table
func (r *PostgresRepository) SubscribeToChanges(table string, callback func(change repositories.ChangeEvent)) error {
	// This is a stub implementation for now
	// In a real implementation, this would set up database triggers or listen/notify to track changes
	log.Printf("Subscription to changes for table %s is not fully implemented yet", table)
	return nil
}

// UnsubscribeFromChanges unsubscribes from changes in a table
func (r *PostgresRepository) UnsubscribeFromChanges(table string) error {
	// This is a stub implementation for now
	log.Printf("Unsubscription from changes for table %s is not fully implemented yet", table)
	return nil
}
