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
	"testing"
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
	t.Cleanup(func() {
		probes.Wait()
		forget()
	})
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

// waitProbe waits for the background probe started by ReachableFirst.
func waitProbe(host string) (stillUnreachable bool) {
	probes.Wait()
	_, stillUnreachable = unreachable.Load(host)
	return stillUnreachable
}

func TestReachableFirstProbesInBackgroundAfterRetryInterval(t *testing.T) {
	forgetUnreachable(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	host := srv.Listener.Addr().String()
	urls := []string{"http://" + host + "/3,x", "http://b:8080/3,x"}
	recordUnreachable(host, io.EOF)
	expireUnreachable(t, host)

	// the read that finds the interval passed still tries the host last
	assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	if waitProbe(host) {
		t.Fatalf("an answering server stayed unreachable")
	}
	assertOrder(t, ReachableFirst(urls), urls...)
}

func TestReachableFirstKeepsHostLastWhenProbeFails(t *testing.T) {
	forgetUnreachable(t)
	refusing, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusing.Close()
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	go func() {
		for {
			conn, err := silent.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	for _, host := range []string{refusing.Addr().String(), silent.Addr().String()} {
		urls := []string{"http://" + host + "/3,x", "http://b:8080/3,x"}
		recordUnreachable(host, io.EOF)
		expireUnreachable(t, host)

		assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
		if !waitProbe(host) {
			t.Fatalf("%s was cleared by a probe it did not answer", host)
		}
		assertOrder(t, ReachableFirst(urls), urls[1], urls[0])
	}
}

func dialError(err error) error {
	return &url.Error{Op: "Get", URL: "http://10.0.0.9:9501/3,x", Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}}
}

func TestReachableFirstTriesEveryServerOfAnUnansweringMachineLast(t *testing.T) {
	for _, tc := range []struct{ host, sibling, other string }{
		{"10.0.0.9:9501", "10.0.0.9:9502", "10.0.0.8:9501"},
		{"10.0.0.9", "10.0.0.9:9502", "10.0.0.8:9501"},
		{"[2001:db8::9]:9501", "[2001:db8::9]", "[2001:db8::8]:9501"},
		{"[2001:db8::9]", "[2001:db8::9]:9502", "[2001:db8::8]:9501"},
	} {
		t.Run(tc.host, func(t *testing.T) {
			forgetUnreachable(t)
			urls := []string{"http://" + tc.host + "/3,x", "http://" + tc.sibling + "/3,x", "http://" + tc.other + "/3,x"}

			recordUnreachable(tc.host, dialError(os.ErrDeadlineExceeded))
			assertOrder(t, ReachableFirst(urls), urls[2], urls[0], urls[1])

			// a sibling port answering clears the machine, not the host that timed out
			recordReachable(tc.sibling)
			assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])
			recordReachable(tc.host)
			assertOrder(t, ReachableFirst(urls), urls...)
		})
	}
}

func refusedDial(t *testing.T) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
		t.Skip("closed port accepted a connection")
	}
	if !connRefused(err) {
		t.Fatalf("dial to a closed port: %v is not a refusal", err)
	}
	return &url.Error{Op: "Get", URL: "http://" + addr + "/3,x", Err: err}
}

func TestReachableFirstKeepsOtherPortsFirstWhenOneRefuses(t *testing.T) {
	forgetUnreachable(t)
	urls := []string{"http://10.0.0.9:9501/3,x", "http://10.0.0.9:9502/3,x", "http://10.0.0.8:9501/3,x"}
	refused := refusedDial(t)

	recordUnreachable("10.0.0.9:9501", refused)
	assertOrder(t, ReachableFirst(urls), urls[1], urls[2], urls[0])

	recordUnreachable("10.0.0.9:9502", dialError(os.ErrDeadlineExceeded))
	assertOrder(t, ReachableFirst(urls), urls[2], urls[0], urls[1])

	// a refusal shows the machine answers again
	forgetUnreachable(t)
	recordUnreachable("10.0.0.9:9503", dialError(os.ErrDeadlineExceeded))
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
