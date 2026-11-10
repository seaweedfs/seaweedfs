package weed_server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func filerPutRequest(ctx context.Context) *http.Request {
	return httptest.NewRequest(http.MethodPut, "/dir/file", strings.NewReader("data")).WithContext(ctx)
}

func TestFilerWaitForUploadSlot(t *testing.T) {
	newFiler := func(timeout time.Duration) *FilerServer {
		fs := &FilerServer{
			option:                &FilerOption{ConcurrentFileUploadLimit: 1, InflightUploadDataTimeout: timeout},
			inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
		}
		if !fs.waitForUploadSlot(httptest.NewRecorder(), filerPutRequest(context.Background()), 4) {
			t.Fatal("first upload was not admitted")
		}
		return fs
	}

	t.Run("times out with 429", func(t *testing.T) {
		fs := newFiler(100 * time.Millisecond)
		rec := httptest.NewRecorder()
		start := time.Now()
		if fs.waitForUploadSlot(rec, filerPutRequest(context.Background()), 4) {
			t.Fatal("upload admitted without a free slot")
		}
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("got %d, want 429", rec.Code)
		}
		if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
			t.Fatalf("rejected after %v, before the wait timeout", elapsed)
		}
		if fs.inFlightUploads != 1 || fs.inFlightDataSize != 4 {
			t.Fatalf("in-flight uploads %d bytes %d, want 1 and 4", fs.inFlightUploads, fs.inFlightDataSize)
		}
	})

	t.Run("stalled body still gets the 429", func(t *testing.T) {
		fs := newFiler(100 * time.Millisecond)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if fs.waitForUploadSlot(w, r, r.ContentLength) {
				t.Error("upload admitted without a free slot")
			}
		}))
		defer srv.Close()
		conn, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(conn, "PUT /dir/file HTTP/1.1\r\nHost: filer\r\nContent-Length: 10\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("no reply while the body is unsent: %v", err)
		}
		if resp.StatusCode != http.StatusTooManyRequests || !resp.Close {
			t.Fatalf("got %d close=%v, want 429 with Connection: close", resp.StatusCode, resp.Close)
		}
	})

	t.Run("cancelled request stops waiting", func(t *testing.T) {
		fs := newFiler(0)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		done := make(chan int)
		go func() {
			rec := httptest.NewRecorder()
			if fs.waitForUploadSlot(rec, filerPutRequest(ctx), 4) {
				t.Error("cancelled upload admitted")
			}
			done <- rec.Code
		}()
		select {
		case code := <-done:
			if code != util.HttpStatusCancelled {
				t.Fatalf("got %d, want %d", code, util.HttpStatusCancelled)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled upload kept waiting for a slot")
		}
	})
}
