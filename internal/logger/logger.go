package logger

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

// Level represents the severity of a log message
type Level int

const (
	// LevelDebug for detailed troubleshooting
	LevelDebug Level = iota
	// LevelInfo for general operational entries
	LevelInfo
	// LevelWarn for non-critical issues
	LevelWarn
	// LevelError for errors that should be addressed
	LevelError
)

var (
	// Default logger
	logger   *log.Logger
	logLevel Level
)

// Initialize sets up the logger with the specified level
func Initialize(level string) {
	logger = log.New(os.Stdout, "", 0)
	setLogLevel(level)
}

// InitializeWithWriter sets up the logger with the specified level and output writer
func InitializeWithWriter(level string, writer *os.File) {
	logger = log.New(writer, "", 0)
	setLogLevel(level)
}

// setLogLevel sets the log level from a string
func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		logLevel = LevelDebug
	case "info":
		logLevel = LevelInfo
	case "warn":
		logLevel = LevelWarn
	case "error":
		logLevel = LevelError
	default:
		logLevel = LevelInfo
	}
}

// log logs a message with the given level
func logMessage(level Level, format string, v ...interface{}) {
	if level < logLevel {
		return
	}

	prefix := ""
	var colorCode string

	switch level {
	case LevelDebug:
		prefix = "DEBUG"
		colorCode = "\033[36m" // Cyan
	case LevelInfo:
		prefix = "INFO"
		colorCode = "\033[32m" // Green
	case LevelWarn:
		prefix = "WARN"
		colorCode = "\033[33m" // Yellow
	case LevelError:
		prefix = "ERROR"
		colorCode = "\033[31m" // Red
	}

	resetColor := "\033[0m" // Reset color
	timestamp := time.Now().Format("2006/01/02 15:04:05.000")
	message := fmt.Sprintf(format, v...)

	// Use color codes only if output is terminal
	fileInfo, err := os.Stdout.Stat()
	if err == nil && (fileInfo.Mode()&os.ModeCharDevice) != 0 {
		logger.Printf("%s %s%s%s: %s", timestamp, colorCode, prefix, resetColor, message)
	} else {
		logger.Printf("%s %s: %s", timestamp, prefix, message)
	}
}

// Debug logs a debug message
func Debug(format string, v ...interface{}) {
	logMessage(LevelDebug, format, v...)
}

// Info logs an info message
func Info(format string, v ...interface{}) {
	logMessage(LevelInfo, format, v...)
}

// Warn logs a warning message
func Warn(format string, v ...interface{}) {
	logMessage(LevelWarn, format, v...)
}

// Error logs an error message
func Error(format string, v ...interface{}) {
	logMessage(LevelError, format, v...)
}

// ErrorWithStack logs an error with a stack trace
func ErrorWithStack(err error) {
	if err == nil {
		return
	}
	logMessage(LevelError, "%v\n%s", err, debug.Stack())
}

// RequestLog logs details of an HTTP request
func RequestLog(method, url, sessionID, body string) {
	Debug("HTTP Request: %s %s", method, url)
	if sessionID != "" {
		Debug("Session ID: %s", sessionID)
	}
	if body != "" {
		Debug("Request Body: %s", body)
	}
}

// ResponseLog logs details of an HTTP response
func ResponseLog(statusCode int, sessionID, body string) {
	Debug("HTTP Response: Status %d", statusCode)
	if sessionID != "" {
		Debug("Session ID: %s", sessionID)
	}
	if body != "" {
		Debug("Response Body: %s", body)
	}
}

// SSEEventLog logs details of an SSE event
func SSEEventLog(eventType, sessionID, data string) {
	Debug("SSE Event: %s", eventType)
	Debug("Session ID: %s", sessionID)
	Debug("Event Data: %s", data)
}

// RequestResponseLog logs a combined request and response log entry
func RequestResponseLog(method, sessionID string, requestData, responseData string) {
	if logLevel > LevelDebug {
		return
	}

	// Format for more readable logs
	formattedRequest := requestData
	formattedResponse := responseData

	// Try to format JSON if it's valid
	if strings.HasPrefix(requestData, "{") || strings.HasPrefix(requestData, "[") {
		var obj interface{}
		if err := json.Unmarshal([]byte(requestData), &obj); err == nil {
			if formatted, err := json.MarshalIndent(obj, "", "  "); err == nil {
				formattedRequest = string(formatted)
			}
		}
	}

	if strings.HasPrefix(responseData, "{") || strings.HasPrefix(responseData, "[") {
		var obj interface{}
		if err := json.Unmarshal([]byte(responseData), &obj); err == nil {
			if formatted, err := json.MarshalIndent(obj, "", "  "); err == nil {
				formattedResponse = string(formatted)
			}
		}
	}

	Debug("==== BEGIN %s [Session: %s] ====", method, sessionID)
	Debug("REQUEST:\n%s", formattedRequest)
	Debug("RESPONSE:\n%s", formattedResponse)
	Debug("==== END %s ====", method)
}
