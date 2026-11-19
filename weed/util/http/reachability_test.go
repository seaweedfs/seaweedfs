package http

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
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
	recordUnreachable("a:8080", io.EOF)
	assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])
	recordReachable("a:8080")
	assertOrder(t, ReachableFirst(urls), urls...)
}

func expireUnreachable(t *testing.T, host string) {
	t.Helper()
	v, found := unreachable.Load(host)
	if !found {
		t.Fatalf("%s is not recorded as unreachable", host)
	}
	failure := v.(unanswered)
	failure.at = failure.at.Add(-unreachableRetryInterval)
	unreachable.Store(host, failure)
}

func waitUnreachable(t *testing.T, host string, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * reachabilityProbeTimeout)
	for {
		v, found := unreachable.Load(host)
		if found == want && (!found || time.Since(v.(unanswered).at) < unreachableRetryInterval) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s recorded unreachable: %v, want %v", host, found, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReachableFirstProbesInBackgroundAfterRetryInterval(t *testing.T) {
	forgetUnreachable(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host := listener.Addr().String()
	urls := []string{"http://" + host + "/3,x", "http://b:8080/3,x"}
	recordUnreachable(host, io.EOF)
	expireUnreachable(t, host)

	// the read that finds the interval passed still tries the host last
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	waitUnreachable(t, host, false)
	assertOrder(t, ReachableFirst(urls), urls...)
}

func TestReachableFirstKeepsHostLastWhenProbeFails(t *testing.T) {
	forgetUnreachable(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := listener.Addr().String()
	listener.Close()
	urls := []string{"http://" + host + "/3,x", "http://b:8080/3,x"}
	recordUnreachable(host, io.EOF)
	expireUnreachable(t, host)

	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	waitUnreachable(t, host, true)
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
}

func dialError(err error) error {
	return &url.Error{Op: "Get", URL: "http://10.0.0.9:9501/3,x", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
}

func TestReachableFirstTriesEveryServerOfAnUnansweringMachineLast(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://10.0.0.9:9502/3,x", "http://10.0.0.8:9501/3,x"}

	recordUnreachable("10.0.0.9:9501", dialError(os.ErrDeadlineExceeded))
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	recordReachable("10.0.0.9:9503")
	assertOrder(t, ReachableFirst(urls), urls...)
}

func TestReachableFirstKeepsOtherPortsFirstWhenOneRefuses(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://10.0.0.9:9501/3,x", "http://10.0.0.9:9502/3,x", "http://10.0.0.8:9501/3,x"}
	refused := dialError(os.NewSyscallError("connect", syscall.ECONNREFUSED))

	recordUnreachable("10.0.0.9:9501", refused)
	assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])

	// a refusal shows the machine answers again
	recordUnreachable("10.0.0.9:9502", dialError(os.ErrDeadlineExceeded))
	assertOrder(t, ReachableFirst(urls), urls[2], urls[0], urls[1])
	recordUnreachable("10.0.0.9:9501", refused)
	assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])
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
