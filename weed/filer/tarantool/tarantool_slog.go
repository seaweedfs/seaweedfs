//go:build tarantool

package tarantool

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// glogSlogHandler forwards go-tarantool/v3's structured logs into weed/glog,
// which is the logging facility used everywhere else in SeaweedFS.
type glogSlogHandler struct {
	preformatted string
}

func newGlogLogger() *slog.Logger {
	return slog.New(&glogSlogHandler{})
}

func (h *glogSlogHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *glogSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	msg := r.Message
	var attrs []string
	if h.preformatted != "" {
		attrs = append(attrs, h.preformatted)
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
		return true
	})
	if len(attrs) > 0 {
		msg = msg + " " + strings.Join(attrs, " ")
	}

	switch {
	case r.Level >= slog.LevelError:
		glog.ErrorfCtx(ctx, "tarantool: %s", msg)
	case r.Level >= slog.LevelWarn:
		glog.WarningfCtx(ctx, "tarantool: %s", msg)
	case r.Level >= slog.LevelInfo:
		glog.InfofCtx(ctx, "tarantool: %s", msg)
	default:
		glog.V(1).InfofCtx(ctx, "tarantool: %s", msg)
	}
	return nil
}

func (h *glogSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	var sb strings.Builder
	if h.preformatted != "" {
		sb.WriteString(h.preformatted)
		sb.WriteString(" ")
	}
	for i, a := range attrs {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
	}
	return &glogSlogHandler{preformatted: sb.String()}
}

func (h *glogSlogHandler) WithGroup(name string) slog.Handler {
	return h
}
