package log

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Level is an alias for zapcore.Level
type Level = zapcore.Level

// LogConfig holds the configuration for logging
type LogConfig struct {
	// LogFile is the path to the log file. If empty, logs will be written to stdout
	LogFile string
	// MaxSize is the maximum size in megabytes of the log file before it gets rotated
	MaxSize int
	// MaxBackups is the maximum number of old log files to retain
	MaxBackups int
	// MaxAge is the maximum number of days to retain old log files
	MaxAge int
	// Compress determines if the rotated log files should be compressed
	Compress bool
}

var (
	// Logger is the global logger instance
	Logger *zap.Logger
	// Sugar is the global sugared logger instance
	Sugar *zap.SugaredLogger
	// atom is the atomic level for dynamic log level changes
	atom zap.AtomicLevel
	// once ensures initialization happens only once
	once sync.Once
	// defaultLevel is the default logging level if not specified
	defaultLevel = zapcore.InfoLevel
)

// VerboseLogger wraps a sugared logger with verbosity level
type VerboseLogger struct {
	level Level
}

// Verbose returns a VerboseLogger for the given verbosity level
func Verbose(level Level) *VerboseLogger {
	return &VerboseLogger{level: level}
}

// Infof logs a formatted message at info level if the verbosity level is enabled
func (v *VerboseLogger) Infof(format string, args ...interface{}) {
	if atom.Enabled(v.level) {
		Sugar.Infof(format, args...)
	}
}

// Info logs a message at info level if the verbosity level is enabled
func (v *VerboseLogger) Info(args ...interface{}) {
	if atom.Enabled(v.level) {
		Sugar.Info(args...)
	}
}

// Infoln logs a message at info level with a newline if the verbosity level is enabled
func (v *VerboseLogger) Infoln(args ...interface{}) {
	if atom.Enabled(v.level) {
		Sugar.Infoln(args...)
	}
}

// Warning logs a message at warn level if the verbosity level is enabled
func (v *VerboseLogger) Warning(args ...interface{}) {
	if atom.Enabled(v.level) {
		Sugar.Warn(args...)
	}
}

// Warningf logs a formatted message at warn level if the verbosity level is enabled
func (v *VerboseLogger) Warningf(format string, args ...interface{}) {
	if atom.Enabled(v.level) {
		Sugar.Warnf(format, args...)
	}
}

// Init initializes the logger with the given level and configuration
func Init(level Level, config *LogConfig) {
	once.Do(func() {
		// Initialize with default level if not specified
		if level == 0 {
			level = defaultLevel
		}

		atom = zap.NewAtomicLevel()
		atom.SetLevel(level)

		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		var writeSyncer zapcore.WriteSyncer
		if config != nil && config.LogFile != "" {
			// Create a lumberjack logger for log rotation
			rotator := &lumberjack.Logger{
				Filename:   config.LogFile,
				MaxSize:    config.MaxSize, // megabytes
				MaxBackups: config.MaxBackups,
				MaxAge:     config.MaxAge, // days
				Compress:   config.Compress,
			}
			writeSyncer = zapcore.AddSync(rotator)
		} else {
			writeSyncer = zapcore.AddSync(os.Stdout)
		}

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			writeSyncer,
			atom,
		)

		Logger = zap.New(core)
		Sugar = Logger.Sugar()
	})
}

// SetLevel changes the logging level dynamically
func SetLevel(level Level) {
	if atom == (zap.AtomicLevel{}) {
		Init(level, nil)
		return
	}
	atom.SetLevel(level)
}

// V returns a VerboseLogger for the given verbosity level
func V(level Level) *VerboseLogger {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	return Verbose(level)
}

// Info logs a message at info level
func Info(args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Info(args...)
}

// Infof logs a formatted message at info level
func Infof(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Infof(format, args...)
}

// Warning logs a message at warn level
func Warning(args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Warn(args...)
}

// Warningf logs a formatted message at warn level
func Warningf(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Warnf(format, args...)
}

// Error logs a message at error level
func Error(args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Error(args...)
}

// Errorf logs a formatted message at error level
func Errorf(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Errorf(format, args...)
}

// Fatal logs a message at fatal level and then calls os.Exit(1)
func Fatal(args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Fatal(args...)
}

// Fatalf logs a formatted message at fatal level and then calls os.Exit(1)
func Fatalf(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Fatalf(format, args...)
}

// Exitf logs a formatted message at fatal level and then calls os.Exit(1)
func Exitf(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Fatalf(format, args...)
	os.Exit(1)
}

// With returns a logger with the given fields
func With(fields ...zap.Field) *zap.Logger {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	return Logger.With(fields...)
}

// WithSugar returns a sugared logger with the given fields
func WithSugar(args ...interface{}) *zap.SugaredLogger {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	return Sugar.With(args...)
}

// Printf logs a formatted message at info level
func Printf(format string, args ...interface{}) {
	if atom == (zap.AtomicLevel{}) {
		Init(defaultLevel, nil)
	}
	Sugar.Infof(format, args...)
}
