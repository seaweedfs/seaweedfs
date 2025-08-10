package worker

import (
	"fmt"

	wtasks "github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	wtypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// taskLoggerAdapter adapts a tasks.TaskLogger to the types.Logger interface used by tasks
// so that structured WithFields logs from task implementations are captured into file logs.
type taskLoggerAdapter struct {
	base   wtasks.TaskLogger
	fields map[string]interface{}
}

func newTaskLoggerAdapter(base wtasks.TaskLogger) *taskLoggerAdapter {
	return &taskLoggerAdapter{base: base}
}

// WithFields returns a new adapter instance that includes the provided fields.
func (a *taskLoggerAdapter) WithFields(fields map[string]interface{}) wtypes.Logger {
	// copy fields to avoid mutation by caller
	copied := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		copied[k] = v
	}
	return &taskLoggerAdapter{base: a.base, fields: copied}
}

// Info logs an info message, including any structured fields if present.
func (a *taskLoggerAdapter) Info(msg string, args ...interface{}) {
	if a.base == nil {
		return
	}
	if len(a.fields) > 0 {
		a.base.LogWithFields("INFO", fmt.Sprintf(msg, args...), toStringMap(a.fields))
		return
	}
	a.base.Info(msg, args...)
}

func (a *taskLoggerAdapter) Warning(msg string, args ...interface{}) {
	if a.base == nil {
		return
	}
	if len(a.fields) > 0 {
		a.base.LogWithFields("WARNING", fmt.Sprintf(msg, args...), toStringMap(a.fields))
		return
	}
	a.base.Warning(msg, args...)
}

func (a *taskLoggerAdapter) Error(msg string, args ...interface{}) {
	if a.base == nil {
		return
	}
	if len(a.fields) > 0 {
		a.base.LogWithFields("ERROR", fmt.Sprintf(msg, args...), toStringMap(a.fields))
		return
	}
	a.base.Error(msg, args...)
}

func (a *taskLoggerAdapter) Debug(msg string, args ...interface{}) {
	if a.base == nil {
		return
	}
	if len(a.fields) > 0 {
		a.base.LogWithFields("DEBUG", fmt.Sprintf(msg, args...), toStringMap(a.fields))
		return
	}
	a.base.Debug(msg, args...)
}

// toStringMap converts map[string]interface{} to map[string]interface{} where values are printable.
// The underlying tasks.TaskLogger handles arbitrary JSON values, but our gRPC conversion later
// expects strings; we rely on existing conversion there. Here we keep interface{} to preserve detail.
func toStringMap(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
