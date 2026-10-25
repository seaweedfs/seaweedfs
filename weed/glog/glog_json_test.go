package glog

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJSONMode_Toggle(t *testing.T) {
	// Default is off
	if IsJSONMode() {
		t.Error("JSON mode should be off by default")
	}

	SetJSONMode(true)
	if !IsJSONMode() {
		t.Error("JSON mode should be on after SetJSONMode(true)")
	}

	SetJSONMode(false)
	if IsJSONMode() {
		t.Error("JSON mode should be off after SetJSONMode(false)")
	}
}

func TestJSONMode_Output(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	defer SetJSONMode(false)

	SetJSONMode(true)

	defer func(previous func() time.Time) { timeNow = previous }(timeNow)
	timeNow = func() time.Time {
		return time.Date(2026, 3, 19, 15, 30, 0, 0, time.UTC)
	}

	Info("hello json")

	output := contents(infoLog)
	if !strings.HasPrefix(output, "{") {
		t.Fatalf("JSON output should start with '{', got: %q", output)
	}

	// Parse as JSON
	var parsed map[string]interface{}
	// Trim trailing newline
	line := strings.TrimSpace(output)
	if err := json.Unmarshal([]byte(line), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %q", err, line)
	}

	// Check required fields
	if _, ok := parsed["ts"]; !ok {
		t.Error("JSON missing 'ts' field")
	}
	if level, ok := parsed["level"]; !ok || level != "INFO" {
		t.Errorf("expected level=INFO, got %v", level)
	}
	if _, ok := parsed["file"]; !ok {
		t.Error("JSON missing 'file' field")
	}
	if _, ok := parsed["line"]; !ok {
		t.Error("JSON missing 'line' field")
	}
	if msg, ok := parsed["msg"]; !ok || msg != "hello json" {
		t.Errorf("expected msg='hello json', got %v", msg)
	}
}

func TestJSONMode_AllLevels(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	defer SetJSONMode(false)
	SetJSONMode(true)

	Info("info msg")
	Warning("warn msg")
	Error("error msg")

	for _, sev := range []severity{infoLog, warningLog, errorLog} {
		output := contents(sev)
		if output == "" {
			continue
		}
		// Each line should be valid JSON
		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			if line == "" {
				continue
			}
			var parsed map[string]interface{}
			if err := json.Unmarshal([]byte(line), &parsed); err != nil {
				t.Errorf("severity %d: invalid JSON: %v\nline: %q", sev, err, line)
			}
		}
	}
}

func TestJSONMode_Infof(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	defer SetJSONMode(false)
	SetJSONMode(true)

	Infof("count=%d name=%s", 42, "test")

	output := strings.TrimSpace(contents(infoLog))
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(output), &parsed); err != nil {
		t.Fatalf("Infof JSON invalid: %v\noutput: %q", err, output)
	}
	msg := parsed["msg"].(string)
	if !strings.Contains(msg, "count=42") || !strings.Contains(msg, "name=test") {
		t.Errorf("Infof message wrong: %q", msg)
	}
}

func TestJSONEscapeString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{`say "hi"`, `say \"hi\"`},
		{"line1\nline2", `line1\nline2`},
		{"tab\there", `tab\there`},
		{`back\slash`, `back\\slash`},
		{"ctrl\x00char", `ctrl\u0000char`},
		{"", ""},
	}

	for _, tt := range tests {
		got := jsonEscapeString(tt.input)
		if got != tt.want {
			t.Errorf("jsonEscapeString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestItoa(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{0, "0"},
		{5, "5"},
		{9, "9"},
		{10, "10"},
		{42, "42"},
		{12345, "12345"},
	}
	for _, tt := range tests {
		got := itoa(tt.input)
		if got != tt.want {
			t.Errorf("itoa(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestJSONMode_TextFallback(t *testing.T) {
	setFlags()
	defer logging.swap(logging.newBuffers())

	// With JSON mode OFF, output should NOT be JSON
	SetJSONMode(false)
	Info("text mode")

	output := contents(infoLog)
	if strings.HasPrefix(output, "{") {
		t.Error("text mode should not produce JSON output")
	}
	if !strings.Contains(output, "text mode") {
		t.Error("text mode output missing message")
	}
}

func BenchmarkJSONMode(b *testing.B) {
	setFlags()
	defer logging.swap(logging.newBuffers())
	defer SetJSONMode(false)
	SetJSONMode(true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info("benchmark json message")
	}
}
