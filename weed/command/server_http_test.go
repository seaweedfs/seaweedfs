package command

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestNewHttpServerKeepsQuietRequestContexts checks that a server built by
// newHttpServer (S3, filer, IAM, master) keeps the context of a request whose
// handler works for longer than the listener's idle timeout without reading
// or writing, and the context of the next request on that connection.
func TestNewHttpServerKeepsQuietRequestContexts(t *testing.T) {
	const idle = 200 * time.Millisecond
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/quiet" {
			select {
			case <-time.After(3 * idle):
			case <-r.Context().Done():
			}
		}
		if err := r.Context().Err(); err != nil {
			fmt.Fprint(w, err)
			return
		}
		fmt.Fprint(w, "ok")
	})
	ln, err := util.NewListener("127.0.0.1:0", idle)
	if err != nil {
		t.Fatal(err)
	}
	s := newHttpServer(handler, nil)
	var conns atomic.Int32
	s.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	go s.Serve(ln)
	t.Cleanup(func() { s.Close() })

	c := &http.Client{
		Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1},
		Timeout:   10 * time.Second,
	}
	for _, path := range []string{"/quiet", "/next"} {
		resp, err := c.Get("http://" + ln.Addr().String() + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if string(b) != "ok" {
			t.Fatalf("%s: request context %s", path, b)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("requests used %d connections, want 1", n)
	}
}
