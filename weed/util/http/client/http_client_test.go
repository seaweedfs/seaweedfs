package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewHttpClientSetsResponseTimeouts(t *testing.T) {
	c, err := NewHttpClient(Client)
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	if got := c.Transport.ResponseHeaderTimeout; got != responseHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", got, responseHeaderTimeout)
	}
	if got := c.Transport.IdleConnTimeout; got != idleConnTimeout {
		t.Errorf("IdleConnTimeout = %v, want %v", got, idleConnTimeout)
	}
	// AddDialContext must keep the response timeouts intact.
	c2, err := NewHttpClient(Client, AddDialContext)
	if err != nil {
		t.Fatalf("NewHttpClient(AddDialContext): %v", err)
	}
	if got := c2.Transport.ResponseHeaderTimeout; got != responseHeaderTimeout {
		t.Errorf("AddDialContext dropped ResponseHeaderTimeout: got %v", got)
	}
	// WithResponseHeaderTimeout overrides the default.
	c3, err := NewHttpClient(Client, WithResponseHeaderTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewHttpClient(WithResponseHeaderTimeout): %v", err)
	}
	if got := c3.Transport.ResponseHeaderTimeout; got != time.Second {
		t.Errorf("WithResponseHeaderTimeout not applied: got %v", got)
	}
	// The TLS constructor must carry the same defaults.
	cTLS, err := NewHttpClientWithTLS("", "", "", true)
	if err != nil {
		t.Fatalf("NewHttpClientWithTLS: %v", err)
	}
	if got := cTLS.Transport.ResponseHeaderTimeout; got != responseHeaderTimeout {
		t.Errorf("NewHttpClientWithTLS ResponseHeaderTimeout = %v, want %v", got, responseHeaderTimeout)
	}
	if got := cTLS.Transport.IdleConnTimeout; got != idleConnTimeout {
		t.Errorf("NewHttpClientWithTLS IdleConnTimeout = %v, want %v", got, idleConnTimeout)
	}
}

// A peer that is TCP-reachable but never answers -- a volume server still
// loading its volumes after a restart, or a stale keep-alive to a container
// that came back on a new IP -- must not block a read or a replicated write
// forever. ResponseHeaderTimeout turns that into a prompt timeout error so the
// caller can fail over to another replica or retry.
func TestResponseHeaderTimeoutUnblocksUnresponsivePeer(t *testing.T) {
	server, release := newUnresponsiveServer(t, false)
	defer server.Close()
	defer close(release)

	c, err := NewHttpClient(Client, WithResponseHeaderTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatalf("NewHttpClient: %v", err)
	}
	assertTimesOut(t, c, server.URL)
}

// Same guarantee over the TLS client path (NewHttpClientWithTLS).
func TestResponseHeaderTimeoutUnblocksUnresponsivePeerTLS(t *testing.T) {
	server, release := newUnresponsiveServer(t, true)
	defer server.Close()
	defer close(release)

	c, err := NewHttpClientWithTLS("", "", "", true, WithResponseHeaderTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatalf("NewHttpClientWithTLS: %v", err)
	}
	assertTimesOut(t, c, server.URL)
}

// newUnresponsiveServer returns a server whose handler blocks until the returned
// channel is closed, so it accepts the connection but never sends response
// headers. Callers must close the channel before server.Close() -- defers run
// LIFO, so register the Close defer first.
func newUnresponsiveServer(t *testing.T, tls bool) (*httptest.Server, chan struct{}) {
	t.Helper()
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	})
	if tls {
		return httptest.NewTLSServer(handler), release
	}
	return httptest.NewServer(handler), release
}

func assertTimesOut(t *testing.T, c *HTTPClient, url string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		resp, doErr := c.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- doErr
	}()

	select {
	case doErr := <-done:
		if doErr == nil {
			t.Fatal("expected a timeout error from an unresponsive peer, got nil")
		}
		var netErr net.Error
		if !errors.As(doErr, &netErr) || !netErr.Timeout() {
			t.Fatalf("expected a timeout error, got %v", doErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Do() blocked well past the response header timeout")
	}
}
