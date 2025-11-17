package operation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Test that Upload respects request context timeout and does not hang indefinitely
func TestUploadTimesOutOnStalledConnectionViaContext(t *testing.T) {
	// Create a test server that stalls and never writes a response
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do not read the body and do not write a response; just stall
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
			// Proactively close the underlying connection to avoid server Close delay
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
				}
			}
		}
	}))
	// Make the server's own timeouts aggressive so Close does not block long
	ts.Config.ReadTimeout = 200 * time.Millisecond
	ts.Config.WriteTimeout = 200 * time.Millisecond
	ts.Config.IdleTimeout = 200 * time.Millisecond
	ts.Start()
	defer ts.Close()

	u, err := NewUploader()
	if err != nil {
		t.Fatalf("failed to create uploader: %v", err)
	}

	// Short timeout to make the test fast
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	data := bytes.Repeat([]byte("a"), 1024)
	// Call the lower-level upload to avoid internal retries and keep test fast
	_, err = u.upload_content(ctx, func(w io.Writer) error {
		_, writeErr := w.Write(data)
		return writeErr
	}, len(data), &UploadOption{
		UploadUrl: ts.URL + "/upload",
		Filename:  "test.bin",
		MimeType:  "application/octet-stream",
	})

	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}


