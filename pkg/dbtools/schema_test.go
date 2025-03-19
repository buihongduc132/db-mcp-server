package dbtools

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/FreePeak/db-mcp-server/pkg/db"
)

// TestSchemaExplorerTool tests the schema explorer tool creation
func TestSchemaExplorerTool(t *testing.T) {
	// Get the tool
	tool := createSchemaExplorerTool()
	
	// Assertions
	assert.NotNil(t, tool)
	assert.Equal(t, "dbSchema", tool.Name)
	assert.Equal(t, "Auto-discover database structure and relationships", tool.Description)
	assert.Equal(t, "database", tool.Category)
	assert.NotNil(t, tool.Handler)
	
	// Check input schema
	assert.Equal(t, "object", tool.InputSchema.Type)
	assert.Contains(t, tool.InputSchema.Properties, "component")
	assert.Contains(t, tool.InputSchema.Properties, "table")
	assert.Contains(t, tool.InputSchema.Properties, "timeout")
	assert.Contains(t, tool.InputSchema.Required, "component")
}

// TestHandleSchemaExplorerWithInvalidComponent tests the schema explorer handler with an invalid component
func TestHandleSchemaExplorerWithInvalidComponent(t *testing.T) {
	// Setup
	ctx := context.Background()
	params := map[string]interface{}{
		"component": "invalid",
	}
	
	// Execute
	result, err := handleSchemaExplorer(ctx, params)
	
	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "invalid component")
}

// TestHandleSchemaExplorerWithMissingTableParam tests the schema explorer handler with a missing table parameter
func TestHandleSchemaExplorerWithMissingTableParam(t *testing.T) {
	// Setup
	ctx := context.Background()
	params := map[string]interface{}{
		"component": "columns",
	}
	
	// Execute
	result, err := handleSchemaExplorer(ctx, params)
	
	// Assertions
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "table parameter is required")
}

// MockDatabase for testing
type MockDatabase struct {
	mock.Mock
}

func (m *MockDatabase) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDatabase) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDatabase) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDatabase) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	mockArgs := []interface{}{ctx, query}
	mockArgs = append(mockArgs, args...)
	results := m.Called(mockArgs...)
	return results.Get(0).(*sql.Rows), results.Error(1)
}

func (m *MockDatabase) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	mockArgs := []interface{}{ctx, query}
	mockArgs = append(mockArgs, args...)
	results := m.Called(mockArgs...)
	return results.Get(0).(*sql.Row)
}

func (m *MockDatabase) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	mockArgs := []interface{}{ctx, query}
	mockArgs = append(mockArgs, args...)
	results := m.Called(mockArgs...)
	return results.Get(0).(sql.Result), results.Error(1)
}

func (m *MockDatabase) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	args := m.Called(ctx, opts)
	return args.Get(0).(*sql.Tx), args.Error(1)
}

func (m *MockDatabase) DriverName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockDatabase) ConnectionString() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockDatabase) DB() *sql.DB {
	args := m.Called()
	return args.Get(0).(*sql.DB)
}

// TestGetTablesWithMock tests the getTables function using mock data
func TestGetTablesWithMock(t *testing.T) {
	// Save the original values to restore after the test
	origDbInstance := dbInstance
	origDbConfig := dbConfig
	
	// Mock database configuration
	dbConfig = &db.Config{
		Type: "mysql",
		Name: "test_db",
	}
	
	// Setup mock database
	mockDb := new(MockDatabase)
	dbInstance = mockDb
	
	// Create context
	ctx := context.Background()
	
	// Setup expected mock behavior (return nil rows and no error)
	mockDb.On("Query", mock.Anything).Return((*sql.Rows)(nil), nil)
	
	// Call function under test - we expect it to use the mock data since
	// the query returns nil, which will trigger the mock data generation
	result, err := getTables(ctx)
	
	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	
	// Check that we're getting mock data by ensuring it has tables
	resultMap, ok := result.(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, resultMap, "tables")
	
	// Restore original values
	dbInstance = origDbInstance
	dbConfig = origDbConfig
}

// TestGetFullSchema tests the getFullSchema function
func TestGetFullSchema(t *testing.T) {
	// Save the original values to restore after the test
	origDbInstance := dbInstance
	origDbConfig := dbConfig
	
	// Setup context
	ctx := context.Background()
	
	// Call function under test - this should return mock data since no real DB is configured
	result, err := getFullSchema(ctx)
	
	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	
	// Check that the result is structured as expected
	resultMap, ok := result.(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, resultMap, "tables")
	assert.Contains(t, resultMap, "mock")
	assert.Equal(t, true, resultMap["mock"])
	
	// Restore original values
	dbInstance = origDbInstance
	dbConfig = origDbConfig
} 