package session

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EventCallback is a function that handles SSE events
type EventCallback func(event string, data []byte) error

// Session represents a client session
type Session struct {
	ID             string
	CreatedAt      time.Time
	LastAccessedAt time.Time
	Connected      bool
	Initialized    bool // Flag to track if the client has been initialized
	ResponseWriter http.ResponseWriter
	Flusher        http.Flusher
	EventCallback  EventCallback
	ctx            context.Context
	cancel         context.CancelFunc
	Capabilities   map[string]interface{}
	Data           map[string]interface{} // Arbitrary session data
	mu             sync.Mutex
}

// Manager manages client sessions
type Manager struct {
	sessions map[string]*Session
	mu       sync.RWMutex
}

// ErrSessionNotFound is returned when a session is not found
var ErrSessionNotFound = errors.New("session not found")

// NewManager creates a new session manager
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
	}
}

// CreateSession creates a new session
func (m *Manager) CreateSession() *Session {
	ctx, cancel := context.WithCancel(context.Background())

	session := &Session{
		ID:             uuid.NewString(),
		CreatedAt:      time.Now(),
		LastAccessedAt: time.Now(),
		Connected:      false,
		Capabilities:   make(map[string]interface{}),
		Data:           make(map[string]interface{}),
		ctx:            ctx,
		cancel:         cancel,
	}

	m.mu.Lock()
	m.sessions[session.ID] = session
	m.mu.Unlock()

	return session
}

// GetSession gets a session by ID
func (m *Manager) GetSession(id string) (*Session, error) {
	m.mu.RLock()
	session, ok := m.sessions[id]
	m.mu.RUnlock()

	if !ok {
		return nil, ErrSessionNotFound
	}

	session.mu.Lock()
	session.LastAccessedAt = time.Now()
	session.mu.Unlock()

	return session, nil
}

// RemoveSession removes a session by ID
func (m *Manager) RemoveSession(id string) {
	m.mu.Lock()
	session, ok := m.sessions[id]
	if ok {
		session.cancel() // Cancel the context when removing the session
		delete(m.sessions, id)
	}
	m.mu.Unlock()
}

// CleanupSessions removes inactive sessions
func (m *Manager) CleanupSessions(maxAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, session := range m.sessions {
		session.mu.Lock()
		lastAccess := session.LastAccessedAt
		connected := session.Connected
		session.mu.Unlock()

		// Remove disconnected sessions that are older than maxAge
		if !connected && now.Sub(lastAccess) > maxAge {
			session.cancel() // Cancel the context when removing the session
			delete(m.sessions, id)
		}
	}
}

// Connect connects a session to an SSE stream
func (s *Session) Connect(w http.ResponseWriter, r *http.Request) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return errors.New("streaming not supported")
	}

	// Create a new context that's canceled when the request is done
	ctx, cancel := context.WithCancel(r.Context())

	s.mu.Lock()
	// Cancel the old context if it exists
	if s.cancel != nil {
		s.cancel()
	}

	s.ctx = ctx
	s.cancel = cancel
	s.ResponseWriter = w
	s.Flusher = flusher
	s.Connected = true
	s.LastAccessedAt = time.Now()
	s.mu.Unlock()

	// Start a goroutine to monitor for context cancellation
	go func() {
		<-ctx.Done()
		s.Disconnect()
	}()

	return nil
}

// SendEvent sends an SSE event to the client
func (s *Session) SendEvent(event string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.Connected || s.ResponseWriter == nil || s.Flusher == nil {
		return errors.New("session not connected")
	}

	if s.EventCallback != nil {
		return s.EventCallback(event, data)
	}

	return errors.New("no event callback registered")
}

// SetCapabilities sets the session capabilities
func (s *Session) SetCapabilities(capabilities map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range capabilities {
		s.Capabilities[k] = v
	}
}

// GetCapability gets a session capability
func (s *Session) GetCapability(key string) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.Capabilities[key]
	return val, ok
}

// Context returns the session context
func (s *Session) Context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctx
}

// Disconnect disconnects the session
func (s *Session) Disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Connected = false
	s.ResponseWriter = nil
	s.Flusher = nil
}

// SetInitialized marks the session as initialized
func (s *Session) SetInitialized(initialized bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Initialized = initialized
}

// IsInitialized returns whether the session has been initialized
func (s *Session) IsInitialized() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Initialized
}

// SetData stores arbitrary data in the session
func (s *Session) SetData(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Data == nil {
		s.Data = make(map[string]interface{})
	}

	s.Data[key] = value
}

// GetData retrieves arbitrary data from the session
func (s *Session) GetData(key string) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Data == nil {
		return nil, false
	}

	value, ok := s.Data[key]
	return value, ok
}
