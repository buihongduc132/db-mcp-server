package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config holds the database configuration
type Config struct {
	DBType     string
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	Parameters string
}

// MySQL represents a MySQL database connection
type MySQL struct {
	DB     *sql.DB
	Config Config
	mu     sync.RWMutex
}

var (
	instance *MySQL
	once     sync.Once
)

// LoadConfigFromEnv loads database configuration from environment variables
func LoadConfigFromEnv() Config {
	port, _ := strconv.Atoi(getEnv("DB_PORT", "3306"))

	return Config{
		DBType:     getEnv("DB_TYPE", "mysql"),
		Host:       getEnv("DB_HOST", "localhost"),
		Port:       port,
		User:       getEnv("DB_USER", "root"),
		Password:   getEnv("DB_PASSWORD", ""),
		Database:   getEnv("DB_NAME", ""),
		Parameters: getEnv("DB_PARAMETERS", "parseTime=true&loc=Local"),
	}
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// NewMySQL creates a new MySQL instance
func NewMySQL(config Config) (*MySQL, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
		config.Parameters)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQL{
		DB:     db,
		Config: config,
	}, nil
}

// GetInstance returns a singleton instance of MySQL
func GetInstance() (*MySQL, error) {
	once.Do(func() {
		config := LoadConfigFromEnv()
		mysql, err := NewMySQL(config)
		if err != nil {
			log.Printf("Error initializing database: %v", err)
			return
		}
		instance = mysql
	})

	if instance == nil {
		return nil, fmt.Errorf("failed to initialize database")
	}

	return instance, nil
}

// Close closes the database connection
func (m *MySQL) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.DB != nil {
		return m.DB.Close()
	}
	return nil
}

// ExecuteQuery executes a SQL query and returns the rows
func (m *MySQL) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.DB.Query(query, args...)
}

// ExecuteNonQuery executes a SQL non-query (INSERT, UPDATE, DELETE) and returns the result
func (m *MySQL) ExecuteNonQuery(query string, args ...interface{}) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.DB.Exec(query, args...)
}

// Prepare prepares a SQL statement
func (m *MySQL) Prepare(query string) (*sql.Stmt, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.DB.Prepare(query)
}

// GetDB returns the underlying database connection
func (m *MySQL) GetDB() *sql.DB {
	return m.DB
}

// ColumnInfo represents information about a database column
type ColumnInfo struct {
	Name         string `json:"name"`
	DataType     string `json:"dataType"`
	IsNullable   string `json:"isNullable"`
	ColumnKey    string `json:"columnKey,omitempty"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Extra        string `json:"extra,omitempty"`
	CharacterSet string `json:"characterSet,omitempty"`
	Comment      string `json:"comment,omitempty"`
}

// GetTables returns a list of tables in the database
func (m *MySQL) GetTables() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Query to get all tables in the current database
	query := "SHOW TABLES"
	rows, err := m.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through tables: %w", err)
	}

	return tables, nil
}

// GetTableSchema returns schema information for a table
func (m *MySQL) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Query to get column information
	query := `
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE, 
			IS_NULLABLE, 
			COLUMN_KEY, 
			COLUMN_DEFAULT, 
			EXTRA, 
			CHARACTER_SET_NAME, 
			COLUMN_COMMENT
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_SCHEMA = ? AND 
			TABLE_NAME = ?
	`
	rows, err := m.DB.Query(query, m.Config.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue, charset sql.NullString

		if err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&col.ColumnKey,
			&defaultValue,
			&col.Extra,
			&charset,
			&col.Comment,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}

		if charset.Valid {
			col.CharacterSet = charset.String
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating through columns: %w", err)
	}

	return columns, nil
}
