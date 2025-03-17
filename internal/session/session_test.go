package session

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockResponseWriter is a mock implementation of http.ResponseWriter for testing
type mockResponseWriter struct {
	headers     http.Header
	writtenData []byte
	statusCode  int
}

func newMockResponseWriter() *mockResponseWriter {
	return &mockResponseWriter{
		headers: make(http.Header),
	}
}

func (m *mockResponseWriter) Header() http.Header {
	return m.headers
}

func (m *mockResponseWriter) Write(data []byte) (int, error) {
	m.writtenData = append(m.writtenData, data...)
	return len(data), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}

// mockFlusher is a mock implementation of http.Flusher for testing
type mockFlusher struct {
	*mockResponseWriter
	flushed bool
}

func newMockFlusher() *mockFlusher {
	return &mockFlusher{
		mockResponseWriter: newMockResponseWriter(),
	}
}

func (m *mockFlusher) Flush() {
	m.flushed = true
}

func TestNewManager(t *testing.T) {
	manager := NewManager()
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.sessions)
	assert.Empty(t, manager.sessions)
}

func TestCreateSession(t *testing.T) {
	manager := NewManager()
	session := manager.CreateSession()

	assert.NotNil(t, session)
	assert.NotEmpty(t, session.ID)
	assert.WithinDuration(t, time.Now(), session.CreatedAt, 1*time.Second)
	assert.WithinDuration(t, time.Now(), session.LastAccessedAt, 1*time.Second)
	assert.False(t, session.Connected)
	assert.False(t, session.Initialized)
	assert.NotNil(t, session.Capabilities)
	assert.NotNil(t, session.Data)
	assert.NotNil(t, session.ctx)
	assert.NotNil(t, session.cancel)

	// Verify the session was added to the manager
	retrievedSession, err := manager.GetSession(session.ID)
	assert.NoError(t, err)
	assert.Equal(t, session, retrievedSession)
}

func TestGetSession(t *testing.T) {
	manager := NewManager()
	session := manager.CreateSession()

	// Test retrieving existing session
	retrievedSession, err := manager.GetSession(session.ID)
	assert.NoError(t, err)
	assert.Equal(t, session, retrievedSession)

	// Test retrieving non-existing session
	_, err = manager.GetSession("non-existent-id")
	assert.Error(t, err)
	assert.Equal(t, ErrSessionNotFound, err)
}

func TestRemoveSession(t *testing.T) {
	manager := NewManager()
	session := manager.CreateSession()

	// Verify session exists
	_, err := manager.GetSession(session.ID)
	assert.NoError(t, err)

	// Remove the session
	manager.RemoveSession(session.ID)

	// Verify session is gone
	_, err = manager.GetSession(session.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrSessionNotFound, err)

	// Test removing non-existent session (should not error)
	manager.RemoveSession("non-existent-id")
}

func TestCleanupSessions(t *testing.T) {
	manager := NewManager()

	// Create an old session
	oldSession := manager.CreateSession()
	oldSession.LastAccessedAt = time.Now().Add(-2 * time.Hour)

	// Create a recent session
	recentSession := manager.CreateSession()
	recentSession.LastAccessedAt = time.Now()

	// Run cleanup with 1 hour max age
	manager.CleanupSessions(1 * time.Hour)

	// Verify old session is gone
	_, err := manager.GetSession(oldSession.ID)
	assert.Error(t, err)

	// Verify recent session is still there
	_, err = manager.GetSession(recentSession.ID)
	assert.NoError(t, err)
}

func TestSetAndGetCapabilities(t *testing.T) {
	session := &Session{
		Capabilities: make(map[string]interface{}),
	}

	// Set capabilities
	capabilities := map[string]interface{}{
		"feature1": true,
		"feature2": "enabled",
		"version":  1.2,
	}
	session.SetCapabilities(capabilities)

	// Get capabilities
	assert.Equal(t, capabilities, session.Capabilities)

	// Get individual capability
	feature1, ok := session.GetCapability("feature1")
	assert.True(t, ok)
	assert.Equal(t, true, feature1)

	feature2, ok := session.GetCapability("feature2")
	assert.True(t, ok)
	assert.Equal(t, "enabled", feature2)

	// Get non-existent capability
	_, ok = session.GetCapability("non-existent")
	assert.False(t, ok)
}

func TestSetAndGetData(t *testing.T) {
	session := &Session{
		Data: make(map[string]interface{}),
	}

	// Set data
	session.SetData("key1", "value1")
	session.SetData("key2", 123)

	// Get data
	value1, ok := session.GetData("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", value1)

	value2, ok := session.GetData("key2")
	assert.True(t, ok)
	assert.Equal(t, 123, value2)

	// Get non-existent data
	_, ok = session.GetData("non-existent")
	assert.False(t, ok)
}

func TestInitialized(t *testing.T) {
	session := &Session{}

	// Default should be false
	assert.False(t, session.IsInitialized())

	// Set to true
	session.SetInitialized(true)
	assert.True(t, session.IsInitialized())

	// Set back to false
	session.SetInitialized(false)
	assert.False(t, session.IsInitialized())
}

func TestDisconnect(t *testing.T) {
	// Create a new session instead of manually constructing one
	manager := NewManager()
	session := manager.CreateSession()

	// Ensure session is connected
	session.Connected = true

	// Disconnect the session
	session.Disconnect()

	// Verify session is disconnected
	assert.False(t, session.Connected)
}
