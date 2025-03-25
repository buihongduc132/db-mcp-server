package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/FreePeak/db-mcp-server/internal/logger"
	"github.com/google/uuid"
)

// EventCallback is a function that handles a session event
type EventCallback func(event string, data []byte) error

// Session represents a client session
type Session struct {
	ID            string                 // Unique ID for the session
	Connected     bool                   // Whether the session is connected
	Initialized   bool                   // Whether the session has been initialized
	LastActive    time.Time              // Last time the session was active
	EventCallback EventCallback          // Callback for sending events to the client
	Capabilities  map[string]interface{} // Client capabilities
	mu            sync.RWMutex           // Mutex for thread safety
	closed        bool                   // Whether the session is closed
}

// Manager manages client sessions
type Manager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

// NewManager creates a new session manager
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

// CreateSession creates a new session
func (m *Manager) CreateSession() *Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessionID := uuid.New().String()

	// Create a new session
	sess := &Session{
		ID:           sessionID,
		LastActive:   time.Now(),
		Connected:    false,
		Initialized:  false,
		Capabilities: make(map[string]interface{}),
		closed:       false,
	}

	// Add the session to the manager
	m.sessions[sessionID] = sess

	logger.Debug("Created new session: %s", sessionID)
	return sess
}

// GetSession gets a session by ID
func (m *Manager) GetSession(id string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sess, ok := m.sessions[id]
	return sess, ok
}

// RemoveSession removes a session by ID
func (m *Manager) RemoveSession(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sess, ok := m.sessions[id]; ok {
		// Mark session as closed before removing
		sess.mu.Lock()
		sess.closed = true
		sess.Connected = false
		sess.mu.Unlock()

		// Remove from map
		delete(m.sessions, id)
		logger.Debug("Removed session: %s", id)
	}
}

// CleanupSessions removes sessions that have been inactive for the specified duration
func (m *Manager) CleanupSessions(maxAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var removed int

	for id, sess := range m.sessions {
		sess.mu.RLock()
		lastActive := sess.LastActive
		connected := sess.Connected
		sess.mu.RUnlock()

		// Only remove sessions that are not connected and have been inactive
		if !connected && now.Sub(lastActive) > maxAge {
			// Mark as closed
			sess.mu.Lock()
			sess.closed = true
			sess.mu.Unlock()

			// Remove from manager
			delete(m.sessions, id)
			removed++
		}
	}

	if removed > 0 {
		logger.Info("Cleaned up %d inactive sessions", removed)
	}
}

// UpdateLastActive updates the last active time for a session
func (s *Session) UpdateLastActive() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActive = time.Now()
}

// SetInitialized marks the session as initialized
func (s *Session) SetInitialized(initialized bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Initialized = initialized
	if initialized {
		logger.Debug("Session %s marked as initialized", s.ID)
	}
}

// SetCapabilities sets the client capabilities for the session
func (s *Session) SetCapabilities(capabilities map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Capabilities = capabilities
}

// GetCapabilities gets the client capabilities for the session
func (s *Session) GetCapabilities() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Capabilities
}

// SendEvent sends an event to the client using the event callback
func (s *Session) SendEvent(event string, data []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if session is closed
	if s.closed {
		return fmt.Errorf("session is closed")
	}

	// Check if callback is set
	if s.EventCallback == nil {
		return fmt.Errorf("no event callback registered for session %s", s.ID)
	}

	// Check if connected
	if !s.Connected {
		return fmt.Errorf("session %s is not connected", s.ID)
	}

	// Update last active time
	s.LastActive = time.Now()

	// Call the callback
	err := s.EventCallback(event, data)
	if err != nil {
		logger.Error("Failed to send event to client: %v", err)
		return fmt.Errorf("failed to send event: %w", err)
	}

	return nil
}

// GetActiveSessions returns the number of active sessions
func (m *Manager) GetActiveSessions() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var active int
	for _, sess := range m.sessions {
		sess.mu.RLock()
		if sess.Connected {
			active++
		}
		sess.mu.RUnlock()
	}

	return active
}

// IsInitialized returns whether the session has been initialized
func (s *Session) IsInitialized() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Initialized
}

// IsConnected returns whether the session is connected
func (s *Session) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Connected
}
