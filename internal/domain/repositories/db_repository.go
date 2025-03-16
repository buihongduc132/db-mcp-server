package repositories

import "context"

// ChangeEvent represents a change event in a database table
type ChangeEvent struct {
	Table     string
	Action    string // "insert", "update", "delete"
	Data      map[string]interface{}
	Timestamp string
}

// DBRepository defines the interface for database operations
type DBRepository interface {
	// ExecuteQuery executes a raw SQL query and returns the results
	ExecuteQuery(ctx context.Context, query string) ([]map[string]interface{}, error)

	// InsertData inserts data into a table and returns the inserted ID
	InsertData(ctx context.Context, table string, data map[string]interface{}) (int64, error)

	// UpdateData updates data in a table based on a condition and returns number of affected rows
	UpdateData(ctx context.Context, table string, data map[string]interface{}, condition string) (int64, error)

	// DeleteData deletes data from a table based on a condition and returns number of affected rows
	DeleteData(ctx context.Context, table string, condition string) (int64, error)

	// SubscribeToChanges subscribes to changes in a table
	SubscribeToChanges(table string, callback func(change ChangeEvent)) error

	// UnsubscribeFromChanges unsubscribes from changes in a table
	UnsubscribeFromChanges(table string) error

	// Ping checks if the database connection is alive
	Ping() error

	// Close closes the database connection
	Close() error
}
