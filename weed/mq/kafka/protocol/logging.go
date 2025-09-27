package protocol

import (
	"log"
	"os"
)

// Logger provides structured logging for Kafka protocol operations
type Logger struct {
	debug   *log.Logger
	info    *log.Logger
	warning *log.Logger
	error   *log.Logger
}

// NewLogger creates a new logger instance
func NewLogger() *Logger {
	return &Logger{
		debug:   log.New(os.Stdout, "[KAFKA-DEBUG] ", log.LstdFlags|log.Lshortfile),
		info:    log.New(os.Stdout, "[KAFKA-INFO] ", log.LstdFlags),
		warning: log.New(os.Stdout, "[KAFKA-WARN] ", log.LstdFlags),
		error:   log.New(os.Stderr, "[KAFKA-ERROR] ", log.LstdFlags|log.Lshortfile),
	}
}

// Debug logs debug messages (only in debug mode)
func (l *Logger) Debug(format string, args ...interface{}) {
	if os.Getenv("KAFKA_DEBUG") != "" {
		l.debug.Printf(format, args...)
	}
}

// Info logs informational messages
func (l *Logger) Info(format string, args ...interface{}) {
	l.info.Printf(format, args...)
}

// Warning logs warning messages
func (l *Logger) Warning(format string, args ...interface{}) {
	l.warning.Printf(format, args...)
}

// Error logs error messages
func (l *Logger) Error(format string, args ...interface{}) {
	l.error.Printf(format, args...)
}

// Global logger instance
var logger = NewLogger()

// Debug logs debug messages using the global logger
func Debug(format string, args ...interface{}) {
	logger.Debug(format, args...)
}

// Info logs informational messages using the global logger
func Info(format string, args ...interface{}) {
	logger.Info(format, args...)
}

// Warning logs warning messages using the global logger
func Warning(format string, args ...interface{}) {
	logger.Warning(format, args...)
}

// Error logs error messages using the global logger
func Error(format string, args ...interface{}) {
	logger.Error(format, args...)
}
