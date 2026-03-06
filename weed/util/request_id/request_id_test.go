package request_id

import (
	"regexp"
	"testing"
)

var requestIDPattern = regexp.MustCompile(`^[0-9A-F]+$`)

func TestNewUsesUppercaseHexFormat(t *testing.T) {
	id := New()
	if !requestIDPattern.MatchString(id) {
		t.Fatalf("expected uppercase hex request id, got %q", id)
	}
	if len(id) < 16 {
		t.Fatalf("expected request id to be at least 16 characters, got %q", id)
	}
}
