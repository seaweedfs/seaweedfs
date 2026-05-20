package s3_constants

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple key",
			input:    "file.txt",
			expected: "file.txt",
		},
		{
			name:     "key with leading slash",
			input:    "/file.txt",
			expected: "file.txt",
		},
		{
			name:     "key with directory",
			input:    "folder/file.txt",
			expected: "folder/file.txt",
		},
		{
			name:     "key with leading slash and directory",
			input:    "/folder/file.txt",
			expected: "folder/file.txt",
		},
		{
			name:     "key with duplicate slashes",
			input:    "folder//subfolder///file.txt",
			expected: "folder/subfolder/file.txt",
		},
		{
			name:     "Windows backslash - simple",
			input:    "folder\\file.txt",
			expected: "folder/file.txt",
		},
		{
			name:     "Windows backslash - nested",
			input:    "folder\\subfolder\\file.txt",
			expected: "folder/subfolder/file.txt",
		},
		{
			name:     "Windows backslash - with leading slash",
			input:    "/folder\\subfolder\\file.txt",
			expected: "folder/subfolder/file.txt",
		},
		{
			name:     "mixed slashes",
			input:    "folder\\subfolder/another\\file.txt",
			expected: "folder/subfolder/another/file.txt",
		},
		{
			name:     "Windows full path style (edge case)",
			input:    "C:\\Users\\test\\file.txt",
			expected: "C:/Users/test/file.txt",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just a slash",
			input:    "/",
			expected: "",
		},
		{
			name:     "just a backslash",
			input:    "\\",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeObjectKey(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeObjectKey(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestRemoveDuplicateSlashes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no duplicates",
			input:    "/folder/file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "double slash",
			input:    "/folder//file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "triple slash",
			input:    "/folder///file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "multiple duplicate locations",
			input:    "//folder//subfolder///file.txt",
			expected: "/folder/subfolder/file.txt",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeDuplicateSlashes(tt.input)
			if result != tt.expected {
				t.Errorf("removeDuplicateSlashes(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestIdentityHolderPropagation reproduces the audit-log middleware chain: an
// outer handler installs the holder, an inner handler authenticates and records
// the identity on a request copy, and the outer handler must still recover the
// requester from its own (earlier) copy of the request.
func TestIdentityHolderPropagation(t *testing.T) {
	outer := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)

	// Before the holder is installed there is no identity to recover.
	if got := GetIdentityNameFromContext(outer); got != "" {
		t.Fatalf("unauthenticated request reports requester %q, want empty", got)
	}

	outer = EnsureIdentityHolder(outer)

	// The inner handler sets the identity on a copy, mirroring how auth wraps
	// the request with r.WithContext before invoking the next handler.
	innerCtx := SetIdentityNameInContext(outer.Context(), "admin")
	inner := outer.WithContext(innerCtx)

	if got := GetIdentityNameFromContext(inner); got != "admin" {
		t.Errorf("inner request requester = %q, want admin", got)
	}
	if got := GetIdentityNameFromContext(outer); got != "admin" {
		t.Errorf("outer request requester = %q, want admin (must propagate via holder)", got)
	}
}

func TestEnsureIdentityHolderIdempotent(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)

	first := EnsureIdentityHolder(req)
	if first == req {
		t.Fatal("EnsureIdentityHolder should return a new request when no holder is present")
	}

	again := EnsureIdentityHolder(first)
	if again != first {
		t.Error("EnsureIdentityHolder should be idempotent when a holder is already present")
	}
}

func TestGetIdentityNameFromContextWithoutHolder(t *testing.T) {
	// Without a holder, the per-request context value is still honored so paths
	// that set identity without installing a holder keep working.
	req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
	req = req.WithContext(SetIdentityNameInContext(req.Context(), "admin"))

	if got := GetIdentityNameFromContext(req); got != "admin" {
		t.Errorf("requester = %q, want admin", got)
	}
}
