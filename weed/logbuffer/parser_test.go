package logbuffer

import (
	"strings"
	"testing"
	"time"
)

// ---------- SeaweedFS text format tests ----------
// SeaweedFS glog format: Lmmdd hh:mm:ss.uuuuuu file:line msg (no threadid, no bracket)

func TestParseLogLine_SeaweedFSFormat(t *testing.T) {
	raw := []byte("I0318 12:34:56.123456 master_server.go:123 some message\n")
	entry := ParseLogLine(0, raw)

	if entry.Level != "INFO" {
		t.Errorf("expected level INFO, got %q", entry.Level)
	}
	if entry.File != "master_server.go" {
		t.Errorf("expected file master_server.go, got %q", entry.File)
	}
	if entry.Line != 123 {
		t.Errorf("expected line 123, got %d", entry.Line)
	}
	if entry.Message != "some message" {
		t.Errorf("expected message 'some message', got %q", entry.Message)
	}
	if entry.Timestamp.Month() != 3 || entry.Timestamp.Day() != 18 {
		t.Errorf("expected March 18, got %v", entry.Timestamp)
	}
	if entry.Timestamp.Hour() != 12 || entry.Timestamp.Minute() != 34 {
		t.Errorf("expected 12:34, got %v", entry.Timestamp)
	}
}

func TestParseLogLine_StandardGlogFormat(t *testing.T) {
	// Standard glog with threadid and bracket: Lmmdd hh:mm:ss.uuuuuu threadid file:line] msg
	raw := []byte("I0318 12:34:56.123456 12345 master_server.go:123] some message\n")
	entry := ParseLogLine(0, raw)

	if entry.Level != "INFO" {
		t.Errorf("expected level INFO, got %q", entry.Level)
	}
	if entry.File != "master_server.go" {
		t.Errorf("expected file master_server.go, got %q", entry.File)
	}
	if entry.Line != 123 {
		t.Errorf("expected line 123, got %d", entry.Line)
	}
	if entry.Message != "some message" {
		t.Errorf("expected message 'some message', got %q", entry.Message)
	}
}

func TestParseLogLine_AllSeverities(t *testing.T) {
	tests := []struct {
		level    int32
		char     byte
		wantName string
	}{
		{0, 'I', "INFO"},
		{1, 'W', "WARNING"},
		{2, 'E', "ERROR"},
		{3, 'F', "FATAL"},
	}

	for _, tt := range tests {
		// SeaweedFS format (no bracket)
		raw := []byte(string(tt.char) + "0318 12:34:56.123456 test.go:1 msg")
		entry := ParseLogLine(tt.level, raw)
		if entry.Level != tt.wantName {
			t.Errorf("level %d: expected %q, got %q", tt.level, tt.wantName, entry.Level)
		}
	}
}

func TestParseLogLine_WithRequestID(t *testing.T) {
	// SeaweedFS format
	raw := []byte("I0318 12:34:56.123456 server.go:42 request_id:abc-123 operation completed")
	entry := ParseLogLine(0, raw)

	if entry.RequestID != "abc-123" {
		t.Errorf("expected request_id 'abc-123', got %q", entry.RequestID)
	}
	if entry.Message != "operation completed" {
		t.Errorf("expected message 'operation completed', got %q", entry.Message)
	}
}

func TestParseLogLine_RequestID_OnlyID(t *testing.T) {
	// Message is only the request_id with no trailing text
	raw := []byte("I0318 12:34:56.123456 server.go:42 request_id:abc-123")
	entry := ParseLogLine(0, raw)

	if entry.RequestID != "abc-123" {
		t.Errorf("expected request_id 'abc-123', got %q", entry.RequestID)
	}
	if entry.Message != "" {
		t.Errorf("expected empty message, got %q", entry.Message)
	}
}

func TestParseLogLine_ShortLine(t *testing.T) {
	raw := []byte("short msg")
	entry := ParseLogLine(0, raw)

	if entry.Message != "short msg" {
		t.Errorf("expected 'short msg', got %q", entry.Message)
	}
	if entry.Level != "INFO" {
		t.Errorf("expected level fallback INFO, got %q", entry.Level)
	}
}

func TestParseLogLine_EmptyLine(t *testing.T) {
	entry := ParseLogLine(0, []byte(""))
	if entry.Level != "INFO" {
		t.Errorf("expected INFO for empty, got %q", entry.Level)
	}
	if entry.Message != "" {
		t.Errorf("expected empty message, got %q", entry.Message)
	}
}

func TestParseLogLine_LevelFromInt(t *testing.T) {
	// 'X' is not a known severity char, should keep ERROR from level=2
	raw := []byte("X0318 12:34:56.123456 test.go:1 msg")
	entry := ParseLogLine(2, raw)

	if entry.Level != "ERROR" {
		t.Errorf("expected ERROR from level int, got %q", entry.Level)
	}
}

func TestParseLogLine_ErrorMessage(t *testing.T) {
	raw := []byte("E0318 12:34:56.123456 server.go:42 panic: runtime error")
	entry := ParseLogLine(2, raw)

	if entry.Level != "ERROR" {
		t.Errorf("expected ERROR, got %q", entry.Level)
	}
	if !strings.Contains(entry.Message, "panic") {
		t.Errorf("expected message containing 'panic', got %q", entry.Message)
	}
}

func TestParseLogLine_TimestampParsing(t *testing.T) {
	raw := []byte("I1225 23:59:59.999999 test.go:1 christmas")
	entry := ParseLogLine(0, raw)

	if entry.Timestamp.Month() != time.December {
		t.Errorf("expected December, got %v", entry.Timestamp.Month())
	}
	if entry.Timestamp.Day() != 25 {
		t.Errorf("expected day 25, got %d", entry.Timestamp.Day())
	}
	if entry.Timestamp.Hour() != 23 {
		t.Errorf("expected hour 23, got %d", entry.Timestamp.Hour())
	}
	if entry.Timestamp.Second() != 59 {
		t.Errorf("expected second 59, got %d", entry.Timestamp.Second())
	}
}

// ---------- JSON format tests ----------

func TestParseLogLine_JSON_Basic(t *testing.T) {
	raw := []byte(`{"ts":"2026-03-19T15:30:00.123456Z","level":"INFO","file":"server.go","line":42,"msg":"hello world"}`)
	entry := ParseLogLine(0, raw)

	if entry.Level != "INFO" {
		t.Errorf("expected INFO, got %q", entry.Level)
	}
	if entry.File != "server.go" {
		t.Errorf("expected server.go, got %q", entry.File)
	}
	if entry.Line != 42 {
		t.Errorf("expected line 42, got %d", entry.Line)
	}
	if entry.Message != "hello world" {
		t.Errorf("expected 'hello world', got %q", entry.Message)
	}
	if entry.Timestamp.Year() != 2026 || entry.Timestamp.Month() != 3 {
		t.Errorf("expected 2026-03, got %v", entry.Timestamp)
	}
}

func TestParseLogLine_JSON_AllLevels(t *testing.T) {
	levels := []string{"INFO", "WARNING", "ERROR", "FATAL"}
	for _, lvl := range levels {
		raw := []byte(`{"ts":"2026-03-19T12:00:00Z","level":"` + lvl + `","file":"t.go","line":1,"msg":"x"}`)
		entry := ParseLogLine(0, raw)
		if entry.Level != lvl {
			t.Errorf("expected %s, got %q", lvl, entry.Level)
		}
	}
}

func TestParseLogLine_JSON_WithRequestID(t *testing.T) {
	raw := []byte(`{"ts":"2026-03-19T12:00:00Z","level":"INFO","file":"t.go","line":1,"msg":"request_id:abc-123 operation done"}`)
	entry := ParseLogLine(0, raw)

	if entry.RequestID != "abc-123" {
		t.Errorf("expected request_id 'abc-123', got %q", entry.RequestID)
	}
	if entry.Message != "operation done" {
		t.Errorf("expected 'operation done', got %q", entry.Message)
	}
}

func TestParseLogLine_JSON_Invalid(t *testing.T) {
	raw := []byte(`{invalid json}`)
	entry := ParseLogLine(0, raw)

	if entry.Message == "" {
		t.Error("expected non-empty message for invalid JSON")
	}
}

func TestParseLogLine_JSON_EmptyObject(t *testing.T) {
	raw := []byte(`{}`)
	entry := ParseLogLine(0, raw)

	// Should not panic, fields default to zero values
	if entry.Level != "INFO" {
		t.Logf("level after empty JSON: %q (ok)", entry.Level)
	}
}

func TestParseLogLine_AutoDetect(t *testing.T) {
	// Text format (SeaweedFS)
	textRaw := []byte("E0319 12:00:00.000000 server.go:42 error happened")
	textEntry := ParseLogLine(2, textRaw)
	if textEntry.Level != "ERROR" {
		t.Errorf("text: expected ERROR, got %q", textEntry.Level)
	}
	if textEntry.File != "server.go" {
		t.Errorf("text: expected server.go, got %q", textEntry.File)
	}

	// JSON format
	jsonRaw := []byte(`{"ts":"2026-03-19T12:00:00Z","level":"ERROR","file":"server.go","line":42,"msg":"error happened"}`)
	jsonEntry := ParseLogLine(2, jsonRaw)
	if jsonEntry.Level != "ERROR" {
		t.Errorf("json: expected ERROR, got %q", jsonEntry.Level)
	}
	if jsonEntry.File != "server.go" {
		t.Errorf("json: expected server.go, got %q", jsonEntry.File)
	}

	// Both should produce same logical content
	if textEntry.Message != jsonEntry.Message {
		t.Errorf("message mismatch: text=%q json=%q", textEntry.Message, jsonEntry.Message)
	}
}
