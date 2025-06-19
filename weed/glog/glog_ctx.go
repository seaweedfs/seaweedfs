package glog

import (
	"context"
	"fmt"

	reqid "github.com/seaweedfs/seaweedfs/weed/util/request_id"
)

const requestIDField = "request_id"

// formatMetaTag returns a formatted request ID tag from the context,
// like "request_id:abc123". Returns an empty string if no request ID is found.
func formatMetaTag(ctx context.Context) string {
	if requestID := reqid.Get(ctx); requestID != "" {
		return fmt.Sprintf("%s:%s", requestIDField, requestID)
	}
	return ""
}

// InfoCtx logs a message at info level, including a request ID from the context
// if present. Only logs if the verbosity level is enabled.
func (v Verbose) InfoCtx(ctx context.Context, args ...interface{}) {
	if !v {
		return
	}
	if metaTag := formatMetaTag(ctx); metaTag != "" {
		args = append([]interface{}{metaTag}, args...)
	}
	logging.print(infoLog, args...)
}

// InfolnCtx logs a message at info level using println-style spacing,
// including a request ID from the context if present. Only logs if verbosity is enabled.
func (v Verbose) InfolnCtx(ctx context.Context, args ...interface{}) {
	if !v {
		return
	}
	if metaTag := formatMetaTag(ctx); metaTag != "" {
		args = append([]interface{}{metaTag}, args...)
	}
	logging.println(infoLog, args...)
}

// InfofCtx logs a formatted message at info level, including a request ID from
// the context if present. Only logs if the verbosity level is enabled.
func (v Verbose) InfofCtx(ctx context.Context, format string, args ...interface{}) {
	if !v {
		return
	}
	if metaTag := formatMetaTag(ctx); metaTag != "" {
		format = metaTag + " " + format
	}
	logging.printf(infoLog, format, args...)
}

// InfofCtx logs a formatted message at info level, prepending a request ID from
// the context if it exists. This is a context-aware alternative to Infof.
func InfofCtx(ctx context.Context, format string, args ...interface{}) {
	if metaTag := formatMetaTag(ctx); metaTag != "" {
		format = metaTag + " " + format
	}
	logging.printf(infoLog, format, args...)
}

// InfoCtx logs a message at info level, prepending a request ID from the context
// if it exists. This is a context-aware alternative to Info.
func InfoCtx(ctx context.Context, args ...interface{}) {
	if metaTag := formatMetaTag(ctx); metaTag != "" {
		args = append([]interface{}{metaTag}, args...)
	}
	logging.print(infoLog, args...)
}
