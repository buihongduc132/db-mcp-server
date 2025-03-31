package logger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	zapLogger *zap.Logger
	logLevel  Level
)

// Initialize sets up the logger with the specified level
func Initialize(level string) {
	setLogLevel(level)

	// Check if we're in stdio mode
	transportMode := os.Getenv("TRANSPORT_MODE")
	if transportMode == "stdio" {
		// In stdio mode, we need to avoid any JSON output to stdout
		// That would interfere with JSON-RPC communications, but we
		// must be careful not to break tool functionality

		// Create a log file in logs directory
		logsDir := "logs"
		if _, err := os.Stat(logsDir); os.IsNotExist(err) {
			os.Mkdir(logsDir, 0755)
		}

		timestamp := time.Now().Format("20060102-150405")
		logFileName := filepath.Join(logsDir, fmt.Sprintf("mcp-logger-%s.log", timestamp))

		// Try to create the log file
		logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			// Successfully created log file, initialize with a non-JSON console encoder
			// to make the log more readable for debugging
			encoderConfig := zap.NewDevelopmentEncoderConfig()
			encoder := zapcore.NewConsoleEncoder(encoderConfig)

			// Create a multi-writer core that writes to both stderr and the log file
			// This maintains both file logging and compatibility with tools that
			// might expect stderr to be available
			stderr := zapcore.Lock(os.Stderr)
			fileSync := zapcore.AddSync(logFile)

			core := zapcore.NewTee(
				// File logger with all messages
				zapcore.NewCore(encoder, fileSync, getZapLevel(logLevel)),
				// Stderr logger with only warnings and errors
				zapcore.NewCore(encoder, stderr, zap.NewAtomicLevelAt(zapcore.WarnLevel)),
			)

			zapLogger = zap.New(core)
			zapLogger.Info("Logger initialized in stdio mode, writing full logs to file",
				zap.String("filename", logFileName))
			return
		} else {
			// Fall back to stderr if we can't create a log file
			fmt.Fprintf(os.Stderr, "Failed to create log file: %v\n", err)
		}
	}

	// Standard logger initialization for non-stdio mode or fallback
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "time"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// In stdio mode, always use stderr to avoid contaminating stdout
	if transportMode == "stdio" {
		config.OutputPaths = []string{"stderr"}
	} else {
		config.OutputPaths = []string{"stdout"}
	}

	config.Level = getZapLevel(logLevel)

	var err error
	zapLogger, err = config.Build()
	if err != nil {
		// If Zap logger cannot be built, fall back to standard logger
		fmt.Printf("Failed to initialize zap logger: %v. Falling back to standard logger.\n", err)
		zapLogger = zap.NewNop()
	}
}

// InitializeWithWriter sets up the logger with the specified level and output writer
func InitializeWithWriter(level string, writer *os.File) {
	setLogLevel(level)

	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "time"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Create custom core with the provided writer
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(writer),
		getZapLevel(logLevel),
	)

	zapLogger = zap.New(core)
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

// getZapLevel converts our level to zap.AtomicLevel
func getZapLevel(level Level) zap.AtomicLevel {
	switch level {
	case LevelDebug:
		return zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case LevelInfo:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case LevelWarn:
		return zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case LevelError:
		return zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		return zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
}

// Debug logs a debug message
func Debug(format string, v ...interface{}) {
	if logLevel > LevelDebug {
		return
	}
	msg := fmt.Sprintf(format, v...)
	zapLogger.Debug(msg)
}

// Info logs an info message
func Info(format string, v ...interface{}) {
	if logLevel > LevelInfo {
		return
	}
	msg := fmt.Sprintf(format, v...)
	zapLogger.Info(msg)
}

// Warn logs a warning message
func Warn(format string, v ...interface{}) {
	if logLevel > LevelWarn {
		return
	}
	msg := fmt.Sprintf(format, v...)
	zapLogger.Warn(msg)
}

// Error logs an error message
func Error(format string, v ...interface{}) {
	if logLevel > LevelError {
		return
	}
	msg := fmt.Sprintf(format, v...)
	zapLogger.Error(msg)
}

// ErrorWithStack logs an error with a stack trace
func ErrorWithStack(err error) {
	if err == nil {
		return
	}
	zapLogger.Error(
		err.Error(),
		zap.String("stack", string(debug.Stack())),
	)
}

// RequestLog logs details of an HTTP request
func RequestLog(method, url, sessionID, body string) {
	if logLevel > LevelDebug {
		return
	}
	zapLogger.Debug("HTTP Request",
		zap.String("method", method),
		zap.String("url", url),
		zap.String("sessionID", sessionID),
		zap.String("body", body),
	)
}

// ResponseLog logs details of an HTTP response
func ResponseLog(statusCode int, sessionID, body string) {
	if logLevel > LevelDebug {
		return
	}
	zapLogger.Debug("HTTP Response",
		zap.Int("statusCode", statusCode),
		zap.String("sessionID", sessionID),
		zap.String("body", body),
	)
}

// SSEEventLog logs details of an SSE event
func SSEEventLog(eventType, sessionID, data string) {
	if logLevel > LevelDebug {
		return
	}
	zapLogger.Debug("SSE Event",
		zap.String("eventType", eventType),
		zap.String("sessionID", sessionID),
		zap.String("data", data),
	)
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

	zapLogger.Debug("Request/Response",
		zap.String("method", method),
		zap.String("sessionID", sessionID),
		zap.String("request", formattedRequest),
		zap.String("response", formattedResponse),
	)
}
