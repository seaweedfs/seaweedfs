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
	if len(id) < 24 {
		t.Fatalf("expected request id to be at least 24 characters, got %q (len=%d)", id, len(id))
	}
}

func TestNewIsUnique(t *testing.T) {
	a := New()
	b := New()
	if a == b {
		t.Fatalf("expected unique request ids, got %q twice", a)
	}
}
