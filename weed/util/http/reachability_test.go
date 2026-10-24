package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func forgetUnreachable(t *testing.T) {
	t.Helper()
	forget := func() {
		unreachable.Range(func(host, _ any) bool {
			unreachable.Delete(host)
			return true
		})
	}
	forget()
	t.Cleanup(forget)
}

func assertOrder(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestReachableFirstTriesUnansweringHostLast(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://a:8080/3,x", "http://b:8080/3,x", "http://c:8080/3,x"}

	assertOrder(t, ReachableFirst(urls), urls...)
	recordUnreachable("a:8080")
	assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])
	recordReachable("a:8080")
	assertOrder(t, ReachableFirst(urls), urls...)
}

func TestReachableFirstProbesOnceAfterRetryInterval(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://b:8080/3,x", "http://a:8080/3,x"}
	unreachable.Store("a:8080", time.Now().Add(-unreachableRetryInterval))

	// the first read to come by probes a, the next keeps it last until that settles
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	assertOrder(t, ReachableFirst(urls), urls...)
	recordReachable("a:8080")
	assertOrder(t, ReachableFirst(urls), urls...)
}

func TestReachableFirstProbesOneExpiredHostPerRead(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://a:8080/3,x", "http://b:8080/3,x", "http://c:8080/3,x"}
	expired := time.Now().Add(-unreachableRetryInterval)
	unreachable.Store("a:8080", expired)
	unreachable.Store("c:8080", expired)

	assertOrder(t, ReachableFirst(urls), urls[0], urls[1], urls[2])
	assertOrder(t, ReachableFirst(urls), urls[2], urls[1], urls[0])
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0], urls[2])
}

// hangupServer accepts the connection and drops it without answering, the way
// a replica behind a broken network does, and counts how often that happened.
func hangupServer(t *testing.T) (*httptest.Server, *int32) {
	t.Helper()
	var hangups int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hangups, 1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	t.Cleanup(srv.Close)
	return srv, &hangups
}

func TestRetriedFetchChunkDataTriesUnansweringServerLast(t *testing.T) {
	forgetUnreachable(t)
	payload := []byte("chunk contents")
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer live.Close()
	hangup, hangups := hangupServer(t)

	urls := []string{hangup.URL + "/3,abc", live.URL + "/3,abc"}
	for i := 0; i < 3; i++ {
		buffer := make([]byte, len(payload))
		n, err := RetriedFetchChunkData(context.Background(), buffer, urls, nil, false, true, 0, "3,abc", nil)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(buffer[:n]) != string(payload) {
			t.Fatalf("read %d got %q, want %q", i, buffer[:n], payload)
		}
	}
	if got := atomic.LoadInt32(hangups); got != 1 {
		t.Fatalf("the server that hung up was tried %d times, want once", got)
	}
}
