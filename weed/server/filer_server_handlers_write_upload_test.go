package weed_server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
)

// chunkUploadError returns what the filer's chunk upload yields against a
// volume answering status.
func chunkUploadError(t *testing.T, status int) error {
	t.Helper()
	holder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(status)
		io.WriteString(w, `{"error":"volume server upload limit"}`)
	}))
	defer holder.Close()
	uploader, err := operation.NewUploader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = uploader.UploadData(context.Background(), []byte("hello"), &operation.UploadOption{
		UploadUrl:   holder.URL + "/3,01637037d6",
		MaxAttempts: 1,
	})
	if err == nil {
		t.Fatalf("upload against a %d volume succeeded", status)
	}
	return err
}

// The filer's per-chunk loop assigns anew on each attempt. An overloaded
// cluster gets one fresh assignment and then the error, rather than a retry
// every second until RetryWaitTime while the chunk buffer is held. Other
// reassignable failures keep today's retries.
func TestChunkUploadRetryGateReassignsOnceOnOverload(t *testing.T) {
	overload := chunkUploadError(t, http.StatusServiceUnavailable)
	fault := chunkUploadError(t, http.StatusInternalServerError)

	shouldRetry := newChunkUploadRetryGate()
	if !shouldRetry(overload) {
		t.Fatal("first overload should get one fresh assignment")
	}
	if shouldRetry(overload) {
		t.Fatal("second overload should give up")
	}

	shouldRetry = newChunkUploadRetryGate()
	for i := 0; i < 5; i++ {
		if !shouldRetry(fault) {
			t.Fatalf("500 attempt %d: want retry as before", i+1)
		}
	}
	if shouldRetry(fmt.Errorf("mismatching cookie")) {
		t.Fatal("a permanent error must not retry")
	}
}
