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

func TestIsValidObjectKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", true},
		{"plain", "folder/file.txt", true},
		{"leading slash", "/folder/file.txt", true},
		{"trailing slash", "folder/", true},
		{"hidden file ok", ".hidden", true},
		{"dotdot in name ok", "..hidden", true},
		{"double dots inside name", "foo..bar/baz", true},

		{"bare dotdot", "..", false},
		{"bare dot", ".", false},
		{"leading dotdot segment", "../evil-bucket/test.txt", false},
		{"leading dot-slash", "./evil/test.txt", false},
		{"nested dotdot segment", "good/../evil/test.txt", false},
		{"trailing dotdot segment", "good/..", false},
		{"backslash dotdot", "..\\evil\\test.txt", false},
		{"mixed-slash dotdot", "good\\..\\evil/test.txt", false},
		{"dotdot after duplicate slash", "good//../evil", false},
		{"nul byte", "foo\x00bar", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidObjectKey(tt.input); got != tt.want {
				t.Errorf("IsValidObjectKey(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeObjectKeyAllowBackslash(t *testing.T) {
	SetAllowBackslashInObjectKey(true)
	defer SetAllowBackslashInObjectKey(false)

	normalizeTests := []struct {
		name     string
		input    string
		expected string
	}{
		{"backslash kept verbatim", "folder\\file.txt", "folder\\file.txt"},
		{"nested backslash kept", "folder\\sub\\file.txt", "folder\\sub\\file.txt"},
		{"trailing backslash kept", "folder\\", "folder\\"},
		{"lone backslash kept", "\\", "\\"},
		{"double backslash kept", "a\\\\b", "a\\\\b"},
		{"mixed separators", "folder\\sub/another\\file.txt", "folder\\sub/another\\file.txt"},
		{"duplicate slashes still collapse", "folder//sub///file.txt", "folder/sub/file.txt"},
		{"leading slash still trimmed", "/folder\\file.txt", "folder\\file.txt"},
		{"trailing slash preserved", "folder/", "folder/"},
		{"windows path style", "C:\\Users\\test\\file.txt", "C:\\Users\\test\\file.txt"},
	}
	for _, tt := range normalizeTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeObjectKey(tt.input); got != tt.expected {
				t.Errorf("NormalizeObjectKey(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}

	validTests := []struct {
		name  string
		input string
		want  bool
	}{
		{"backslash dotdot is one segment", "..\\evil\\test.txt", true},
		{"backslash dotdot inside", "good\\..\\evil/test.txt", true},
		{"nested dotdot still rejected", "good/../evil/test.txt", false},
		{"leading dotdot still rejected", "../evil/test.txt", false},
		{"plain backslash key", "folder\\file.txt", true},
	}
	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidObjectKey(tt.input); got != tt.want {
				t.Errorf("IsValidObjectKey(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidBucketName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty ok", "", true},
		{"plain", "my-bucket", true},
		{"name containing dots", "my.bucket.name", true},

		{"bare dot", ".", false},
		{"bare dotdot", "..", false},
		{"with slash", "evil/bucket", false},
		{"with backslash", "evil\\bucket", false},
		{"with nul", "evil\x00bucket", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidBucketName(tt.input); got != tt.want {
				t.Errorf("IsValidBucketName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidPathSegment(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"opaque-id_123", true},
		{"..hidden", true},
		{"", false},
		{".", false},
		{"..", false},
		{"dir/value", false},
		{"dir\\value", false},
		{"nul\x00value", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := IsValidPathSegment(tt.input); got != tt.want {
				t.Errorf("IsValidPathSegment(%q) = %v, want %v", tt.input, got, tt.want)
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

func TestStoredCredentialFingerprintIsKeyed(t *testing.T) {
	key := []byte("test-signing-key-32-characters!!")
	fp := StoredCredentialFingerprint(key, "AKIAEXAMPLE", "secret")
	if fp == "" {
		t.Fatal("fingerprint is empty with a key")
	}
	if again := StoredCredentialFingerprint(key, "AKIAEXAMPLE", "secret"); again != fp {
		t.Error("fingerprint is not deterministic")
	}
	if other := StoredCredentialFingerprint([]byte("another-signing-key-padded-out"), "AKIAEXAMPLE", "secret"); other == fp {
		t.Error("fingerprint ignores the server key, leaving the secret guessable")
	}
	if noKey := StoredCredentialFingerprint(nil, "AKIAEXAMPLE", "secret"); noKey != "" {
		t.Error("an empty key must yield an empty fingerprint")
	}
}
