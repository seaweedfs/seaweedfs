package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
			t.Errorf("expected DELETE, got %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
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

// TestRetriedFetchChunkDataRetriesFreshUrlsImmediately covers the case the
// refresh hook exists for: every location the caller knew about is gone, and
// the data is live somewhere the caller has not heard of yet. The read must
// land on the fresh location without first sitting through the backoff ladder.
func TestRetriedFetchChunkDataRetriesFreshUrlsImmediately(t *testing.T) {
	payload := []byte("chunk contents")
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer live.Close()

	// A port nothing listens on: the address is well formed, the dial fails.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	refreshed := 0
	buffer := make([]byte, len(payload))
	start := time.Now()
	n, err := RetriedFetchChunkData(context.Background(), buffer, []string{deadURL + "/3,abc"}, nil, false, true, 0, "3,abc",
		func() []string {
			refreshed++
			return []string{live.URL + "/3,abc"}
		})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("fetch with a refreshed location: %v", err)
	}
	if n != len(payload) || string(buffer[:n]) != string(payload) {
		t.Fatalf("got %q, want %q", buffer[:n], payload)
	}
	if refreshed != 1 {
		t.Fatalf("refresh called %d times, want exactly 1", refreshed)
	}
	// The first backoff is a full second; landing well under it is the point.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("took %v, expected the retry to skip the backoff", elapsed)
	}
}

// TestRetriedFetchChunkDataKeepsBackoffWhenLocationsAreUnchanged makes sure a
// refresh that returns the same list is treated as "the locations were never
// the problem" rather than as a reason to spin.
func TestRetriedFetchChunkDataKeepsBackoffWhenLocationsAreUnchanged(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	urls := []string{deadURL + "/3,abc"}
	refreshed := 0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	buffer := make([]byte, 4)
	_, err := RetriedFetchChunkData(ctx, buffer, urls, nil, false, true, 0, "3,abc", func() []string {
		refreshed++
		return urls
	})
	if err == nil {
		t.Fatal("expected the fetch to fail against a dead location")
	}
	if refreshed != 1 {
		t.Fatalf("refresh called %d times, want exactly 1", refreshed)
	}
}
