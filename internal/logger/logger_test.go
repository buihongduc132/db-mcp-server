package logger

import (
	"bytes"
	"errors"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

// captureOutput captures log output during a test
func captureOutput(f func()) string {
	var buf bytes.Buffer
	oldLogger := logger
	logger = log.New(&buf, "", 0)
	defer func() { logger = oldLogger }()

	f()
	return buf.String()
}

func TestSetLogLevel(t *testing.T) {
	tests := []struct {
		level    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"unknown", LevelInfo}, // Default
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			setLogLevel(tt.level)
			assert.Equal(t, tt.expected, logLevel)
		})
	}
}

func TestDebug(t *testing.T) {
	// Test when debug is enabled
	logLevel = LevelDebug
	output := captureOutput(func() {
		Debug("Test debug message: %s", "value")
	})
	assert.Contains(t, output, "DEBUG")
	assert.Contains(t, output, "Test debug message: value")

	// Test when debug is disabled
	logLevel = LevelInfo
	output = captureOutput(func() {
		Debug("This should not appear")
	})
	assert.Empty(t, output)
}

func TestInfo(t *testing.T) {
	// Test when info is enabled
	logLevel = LevelInfo
	output := captureOutput(func() {
		Info("Test info message: %s", "value")
	})
	assert.Contains(t, output, "INFO")
	assert.Contains(t, output, "Test info message: value")

	// Test when info is disabled
	logLevel = LevelError
	output = captureOutput(func() {
		Info("This should not appear")
	})
	assert.Empty(t, output)
}

func TestWarn(t *testing.T) {
	// Test when warn is enabled
	logLevel = LevelWarn
	output := captureOutput(func() {
		Warn("Test warn message: %s", "value")
	})
	assert.Contains(t, output, "WARN")
	assert.Contains(t, output, "Test warn message: value")

	// Test when warn is disabled
	logLevel = LevelError
	output = captureOutput(func() {
		Warn("This should not appear")
	})
	assert.Empty(t, output)
}

func TestError(t *testing.T) {
	// Error should always be logged
	logLevel = LevelError
	output := captureOutput(func() {
		Error("Test error message: %s", "value")
	})
	assert.Contains(t, output, "ERROR")
	assert.Contains(t, output, "Test error message: value")
}

func TestErrorWithStack(t *testing.T) {
	err := errors.New("test error")
	output := captureOutput(func() {
		ErrorWithStack(err)
	})
	assert.Contains(t, output, "ERROR")
	assert.Contains(t, output, "test error")
	// Just check that some stack trace data is included
	assert.Contains(t, output, "goroutine")
}

// For the Request/Response logging tests, we'll just test that the functions don't panic
// rather than asserting the specific output format which may change

func TestRequestLog(t *testing.T) {
	assert.NotPanics(t, func() {
		RequestLog("POST", "/api/data", "session123", `{"key":"value"}`)
	})
}

func TestResponseLog(t *testing.T) {
	assert.NotPanics(t, func() {
		ResponseLog(200, "session123", `{"result":"success"}`)
	})
}

func TestSSEEventLog(t *testing.T) {
	assert.NotPanics(t, func() {
		SSEEventLog("message", "session123", `{"data":"content"}`)
	})
}

func TestRequestResponseLog(t *testing.T) {
	assert.NotPanics(t, func() {
		RequestResponseLog("RPC", "session123", `{"method":"getData"}`, `{"result":"data"}`)
	})
}
