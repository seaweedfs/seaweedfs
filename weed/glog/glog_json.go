package glog

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// JSONMode controls whether log output is in JSON format.
// 0 = classic glog text (default), 1 = JSON lines.
// Safe for concurrent access.
var jsonMode int32

// jsonFlagOnce ensures the --log_json flag is applied on first use,
// even when -logtostderr prevents createLogDirs() from running.
var jsonFlagOnce sync.Once

// SetJSONMode enables or disables JSON-formatted log output.
// When enabled, each log line is a single JSON object:
//
//	{"ts":"2006-01-02T15:04:05.000000Z","level":"INFO","file":"server.go","line":42,"msg":"..."}
//
// This is useful for log aggregation systems (ELK, Loki, Datadog, etc).
func SetJSONMode(enabled bool) {
	// Prevent lazy flag initialization from overriding this explicit call.
	jsonFlagOnce.Do(func() {})
	if enabled {
		atomic.StoreInt32(&jsonMode, 1)
	} else {
		atomic.StoreInt32(&jsonMode, 0)
	}
}

// IsJSONMode returns whether JSON mode is currently active.
// On first call, it applies the --log_json flag value if set.
func IsJSONMode() bool {
	jsonFlagOnce.Do(func() {
		if logJSON != nil && *logJSON {
			atomic.StoreInt32(&jsonMode, 1)
		}
	})
	return atomic.LoadInt32(&jsonMode) == 1
}

// formatJSON builds a JSON log line without using encoding/json
// to avoid allocations and keep it as fast as the text path.
// Output: {"ts":"...","level":"...","file":"...","line":N,"msg":"..."}\n
func (l *loggingT) formatJSON(s severity, depth int) (*buffer, string, int) {
	_, file, line, ok := runtime.Caller(3 + depth)
	if !ok {
		file = "???"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
	}

	buf := l.getBuffer()
	now := timeNow()

	buf.WriteString(`{"ts":"`)
	buf.WriteString(now.UTC().Format(time.RFC3339Nano))
	buf.WriteString(`","level":"`)

	switch {
	case s == infoLog:
		buf.WriteString("INFO")
	case s == warningLog:
		buf.WriteString("WARNING")
	case s == errorLog:
		buf.WriteString("ERROR")
	case s >= fatalLog:
		buf.WriteString("FATAL")
	}

	buf.WriteString(`","file":"`)
	buf.WriteString(jsonEscapeString(file))
	buf.WriteString(`","line":`)
	// Write line number without fmt.Sprintf
	buf.WriteString(itoa(line))
	buf.WriteString(`,"msg":"`)

	return buf, file, line
}

// finishJSON closes the JSON object and adds a newline.
func finishJSON(buf *buffer) {
	buf.WriteString("\"}\n")
}

// jsonEscapeString escapes a string for safe inclusion in JSON (RFC 8259).
// Handles: \, ", \n, \r, \t, control characters, and invalid UTF-8 sequences.
func jsonEscapeString(s string) string {
	// Fast path: no special chars and valid UTF-8
	needsEscape := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c < 0x20 || c > 0x7e {
			needsEscape = true
			break
		}
	}
	if !needsEscape {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + 10)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '"':
			b.WriteString(`\"`)
			i++
		case c == '\\':
			b.WriteString(`\\`)
			i++
		case c == '\n':
			b.WriteString(`\n`)
			i++
		case c == '\r':
			b.WriteString(`\r`)
			i++
		case c == '\t':
			b.WriteString(`\t`)
			i++
		case c < 0x20:
			fmt.Fprintf(&b, `\u%04x`, c)
			i++
		case c < utf8.RuneSelf:
			b.WriteByte(c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				b.WriteString(`\ufffd`)
				i++
			} else {
				b.WriteString(s[i : i+size])
				i += size
			}
		}
	}
	return b.String()
}

// itoa converts an integer to a string.
func itoa(i int) string {
	return strconv.Itoa(i)
}
