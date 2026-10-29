package weed_server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// The guarded client of a remote carries the shared transport's cap and deadlines.
func TestNewGuardedHTTPClientHasDeadlines(t *testing.T) {
	tr, ok := newGuardedHTTPClient("https://storage.example").Transport.(*http.Transport)
	if !ok {
		t.Fatal("not an *http.Transport")
	}
	if tr.ResponseHeaderTimeout != guardedResponseHeaderTimeout || tr.TLSHandshakeTimeout != guardedTLSHandshakeTimeout {
		t.Fatalf("deadlines: header %v, tls %v", tr.ResponseHeaderTimeout, tr.TLSHandshakeTimeout)
	}
	if tr.MaxConnsPerHost != guardedMaxConnsPerHost() || tr.MaxIdleConnsPerHost != 32 {
		t.Fatalf("caps: per host %d, idle per host %d", tr.MaxConnsPerHost, tr.MaxIdleConnsPerHost)
	}
}

// A remote that accepts and never answers: each request fails at the header deadline instead of hanging, and once the
// hung requests have freed the capped slots a later request to the same remote gets through.
func TestSharedTransportHungRequestsFreeTheirSlots(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hang" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	defer close(release)

	tr := &http.Transport{DialContext: (&net.Dialer{}).DialContext}
	tuneSharedTransport(tr)
	tr.ResponseHeaderTimeout = 300 * time.Millisecond // the production value is a minute
	tr.MaxConnsPerHost = 2
	client := &http.Client{Transport: tr}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := time.Now()
	for i := 0; i < 2; i++ { // fill both slots
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(srv.URL + "/hang")
			if err == nil {
				resp.Body.Close()
			}
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/ok", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("a request after the hung ones: %v (the slots were never freed)", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("a hung request succeeded; expected the header deadline")
		}
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v; the hung requests should fail at the 300 ms header deadline", d)
	}
}

// A remote that sends headers and then stalls mid-body: the connection's idle deadline cuts the stalled read, freeing
// its slot so a later request to the same remote gets through.
func TestSharedTransportStalledBodyFreesItsSlot(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stall" {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	defer close(release)

	old := guardedIOIdleTimeout
	guardedIOIdleTimeout = 300 * time.Millisecond // the production value is minutes
	defer func() { guardedIOIdleTimeout = old }()

	tr := &http.Transport{DialContext: (&net.Dialer{}).DialContext}
	tuneSharedTransport(tr)
	tr.MaxConnsPerHost = 2
	client := &http.Client{Transport: tr}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := time.Now()
	for i := 0; i < 2; i++ { // fill both slots
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(srv.URL + "/stall")
			if err == nil {
				_, err = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/ok", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("a request after the stalled ones: %v (the slots were never freed)", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("a stalled body read succeeded; expected the idle deadline")
		}
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v; the stalled reads should fail at the 300 ms idle deadline", d)
	}
}
