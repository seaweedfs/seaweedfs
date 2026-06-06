package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAppendQueryParameter(t *testing.T) {
	testCases := []struct {
		name     string
		rawURL   string
		key      string
		value    string
		expected string
	}{
		{
			name:     "without existing query",
			rawURL:   "http://example.com/3,abc",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/3,abc?readDeleted=true",
		},
		{
			name:     "with existing query",
			rawURL:   "http://example.com/?proxyChunkId=3,abc",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?proxyChunkId=3,abc&readDeleted=true",
		},
		{
			name:     "with trailing question mark",
			rawURL:   "http://example.com/?",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?readDeleted=true",
		},
		{
			name:     "with trailing ampersand",
			rawURL:   "http://example.com/?proxyChunkId=3,abc&",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?proxyChunkId=3,abc&readDeleted=true",
		},
		{
			name:     "encodes values",
			rawURL:   "http://example.com/data",
			key:      "note",
			value:    "space value",
			expected: "http://example.com/data?note=space+value",
		},
		{
			name:     "preserves fragment",
			rawURL:   "http://example.com/data#frag",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/data?readDeleted=true#frag",
		},
		{
			name:     "blank url",
			rawURL:   "",
			key:      "readDeleted",
			value:    "true",
			expected: "?readDeleted=true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := AppendQueryParameter(tc.rawURL, tc.key, tc.value)
			if actual != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}
func TestReadUrlAsStreamReturnsGzipReaderError(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not gzip"))
	}))
	defer server.Close()

	_, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {})
	if err == nil {
		t.Fatal("ReadUrlAsStream returned nil error for invalid gzip response")
	}
}

func TestDeleteReturnsInvalidRequestErrorBeforeAddingAuth(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Delete panicked before returning the request error: %v", r)
		}
	}()

	if err := Delete("http://[::1", "jwt"); err == nil {
		t.Fatal("expected invalid request error")
	}
}

func TestDeleteTreatsNoContentAsSuccess(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := Delete(server.URL, ""); err != nil {
		t.Fatalf("expected 204 DELETE to succeed, got %v", err)
	}
}

func TestDeleteProxiedReturnsInvalidRequestErrorBeforeAddingAuth(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DeleteProxied panicked before returning the request error: %v", r)
		}
	}()

	if _, _, err := DeleteProxied("http://[::1", "jwt"); err == nil {
		t.Fatal("expected invalid request error")
	}
}
