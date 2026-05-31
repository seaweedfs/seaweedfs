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
	defer GetGlobalHttpClient().Close()

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
