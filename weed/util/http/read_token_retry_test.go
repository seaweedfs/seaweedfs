package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
)

// TestRetriedFetchChunkDataSelectsTokenPerAttempt proves that a filer-issued
// volume read token is chosen fresh for each attempt. The first attempt is
// answered with a retryable 500, forcing a backoff and retry; the second
// attempt must carry a different token than the first. A token selected once
// before the loop would be reused and could be expired by the time the retry
// runs, which the volume server would reject as non-retryable.
func TestRetriedFetchChunkDataSelectsTokenPerAttempt(t *testing.T) {
	for _, isFullChunk := range []bool{true, false} {
		name := "streamed"
		if isFullChunk {
			name = "direct"
		}
		t.Run(name, func(t *testing.T) {
			InitGlobalHttpClient()

			var mu sync.Mutex
			var seenAuth []string
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				seenAuth = append(seenAuth, r.Header.Get("Authorization"))
				mu.Unlock()
				if atomic.AddInt32(&calls, 1) == 1 {
					// Retryable (>= 499) so the fetch backs off and tries again.
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, _ = w.Write([]byte("abc"))
			}))
			defer server.Close()

			var seq int32
			security.SetVolumeReadTokenProvider(func(ctx context.Context, fileId string) string {
				return fmt.Sprintf("tok-%d", atomic.AddInt32(&seq, 1))
			})
			defer security.SetVolumeReadTokenProvider(nil)

			buffer := make([]byte, 3)
			n, err := RetriedFetchChunkData(context.Background(), buffer, []string{server.URL + "/3,abc"}, nil, false, isFullChunk, 0, "3,abc", nil)
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			if n != 3 || string(buffer) != "abc" {
				t.Fatalf("read %d bytes %q, want 3 abc", n, string(buffer))
			}

			mu.Lock()
			defer mu.Unlock()
			if len(seenAuth) < 2 {
				t.Fatalf("expected a retry, got %d attempt(s): %v", len(seenAuth), seenAuth)
			}
			if seenAuth[0] == seenAuth[1] {
				t.Fatalf("token was not reselected per attempt: both attempts sent %q", seenAuth[0])
			}
			if seenAuth[0] == "" || seenAuth[1] == "" {
				t.Fatalf("expected a bearer token on each attempt, got %v", seenAuth)
			}
		})
	}
}
