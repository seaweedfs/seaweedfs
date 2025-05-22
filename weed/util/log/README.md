# SeaweedFS Logging Package

This package provides a logging interface for SeaweedFS using [zap](https://github.com/uber-go/zap) as the underlying logging library. It provides a similar interface to glog while offering the performance and features of zap.

## Features

- High-performance structured logging
- JSON output format
- Dynamic log level changes
- Support for both structured and unstructured logging
- Compatible with existing glog-style code
- Thread-safe

## Usage

### Basic Setup

```go
import "github.com/seaweedfs/seaweedfs/weed/util/log"
import "go.uber.org/zap/zapcore"

// Initialize the logger with info level
log.Init(zapcore.InfoLevel)
```

### Basic Logging

```go
// Basic logging
log.Info("This is an info message")
log.Infof("This is a formatted info message: %s", "hello")
log.Warning("This is a warning message")
log.Warningf("This is a formatted warning message: %s", "hello")
log.Error("This is an error message")
log.Errorf("This is a formatted error message: %s", "hello")
```

### Verbose Logging

```go
// Using V for verbose logging
if log.V(1) {
    log.Info("This is a verbose message")
}
```

### Structured Logging

```go
// Using structured logging
logger := log.With(
    zap.String("service", "example"),
    zap.Int("version", 1),
)
logger.Info("This is a structured log message")

// Using sugared logger with fields
sugar := log.WithSugar("service", "example", "version", 1)
sugar.Infof("This is a sugared log message with fields: %s", "hello")
```

### Fatal Logging

```go
// Fatal logging (will exit the program)
log.Fatal("This is a fatal message")
log.Fatalf("This is a formatted fatal message: %s", "hello")
```

## Log Levels

The package supports the following log levels:

- Debug (-1)
- Info (0)
- Warning (1)
- Error (2)
- Fatal (3)

## Migration from glog

To migrate from glog to this package:

1. Replace `import "github.com/golang/glog"` with `import "github.com/seaweedfs/seaweedfs/weed/util/log"`
2. Replace glog function calls with their log package equivalents:
   - `glog.Info` -> `log.Info`
   - `glog.Infof` -> `log.Infof`
   - `glog.Warning` -> `log.Warning`
   - `glog.Warningf` -> `log.Warningf`
   - `glog.Error` -> `log.Error`
   - `glog.Errorf` -> `log.Errorf`
   - `glog.Fatal` -> `log.Fatal`
   - `glog.Fatalf` -> `log.Fatalf`
   - `glog.V(level)` -> `log.V(level)`

## Example

See the `example` directory for a complete example of how to use the logging package. 