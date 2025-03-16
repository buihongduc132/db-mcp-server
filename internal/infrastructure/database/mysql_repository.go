package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"mcpserver/internal/domain/repositories"
	"strings"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
)

// MySQLRepository implements the DBRepository interface for MySQL
type MySQLRepository struct {
	db *sql.DB
}

// NewMySQLRepository creates a new MySQL repository
func NewMySQLRepository(dsn string) (*MySQLRepository, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	return &MySQLRepository{
		db: db,
	}, nil
}

// ExecuteQuery executes a SQL query and returns the result rows
func (r *MySQLRepository) ExecuteQuery(ctx context.Context, sqlQuery string) ([]map[string]interface{}, error) {
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
func (r *MySQLRepository) InsertData(ctx context.Context, table string, data map[string]interface{}) (int64, error) {
	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}

	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ","), strings.Join(placeholders, ","))
	result, err := r.db.ExecContext(ctx, sqlStr, values...)
	if err != nil {
		return 0, fmt.Errorf("insert error: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting last insert ID: %w", err)
	}

	return id, nil
}

// UpdateData updates data in a table based on a condition and returns number of affected rows
func (r *MySQLRepository) UpdateData(ctx context.Context, table string, data map[string]interface{}, condition string) (int64, error) {
	sets := []string{}
	values := []interface{}{}

	for col, val := range data {
		sets = append(sets, fmt.Sprintf("%s = ?", col))
		values = append(values, val)
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
func (r *MySQLRepository) DeleteData(ctx context.Context, table string, condition string) (int64, error) {
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
func (r *MySQLRepository) Close() error {
	return r.db.Close()
}

// Ping checks if the database connection is alive
func (r *MySQLRepository) Ping() error {
	return r.db.Ping()
}

// SubscribeToChanges subscribes to changes in a table
func (r *MySQLRepository) SubscribeToChanges(table string, callback func(change repositories.ChangeEvent)) error {
	// This is a stub implementation for now
	// In a real implementation, this would set up database triggers or use binlog to track changes
	log.Printf("Subscription to changes for table %s is not fully implemented yet", table)
	return nil
}

// UnsubscribeFromChanges unsubscribes from changes in a table
func (r *MySQLRepository) UnsubscribeFromChanges(table string) error {
	// This is a stub implementation for now
	log.Printf("Unsubscription from changes for table %s is not fully implemented yet", table)
	return nil
}
