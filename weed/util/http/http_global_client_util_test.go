package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestAppendQueryParameter(t *testing.T) {
	testCases := []struct {
		name     string
		rawURL   string
		key      string
		value    string
		expected string
	}{
		{
			name:     "without existing query",
			rawURL:   "http://example.com/3,abc",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/3,abc?readDeleted=true",
		},
		{
			name:     "with existing query",
			rawURL:   "http://example.com/?proxyChunkId=3,abc",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?proxyChunkId=3,abc&readDeleted=true",
		},
		{
			name:     "with trailing question mark",
			rawURL:   "http://example.com/?",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?readDeleted=true",
		},
		{
			name:     "with trailing ampersand",
			rawURL:   "http://example.com/?proxyChunkId=3,abc&",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/?proxyChunkId=3,abc&readDeleted=true",
		},
		{
			name:     "encodes values",
			rawURL:   "http://example.com/data",
			key:      "note",
			value:    "space value",
			expected: "http://example.com/data?note=space+value",
		},
		{
			name:     "preserves fragment",
			rawURL:   "http://example.com/data#frag",
			key:      "readDeleted",
			value:    "true",
			expected: "http://example.com/data?readDeleted=true#frag",
		},
		{
			name:     "blank url",
			rawURL:   "",
			key:      "readDeleted",
			value:    "true",
			expected: "?readDeleted=true",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := AppendQueryParameter(tc.rawURL, tc.key, tc.value)
			if actual != tc.expected {
				t.Fatalf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}
func TestReadUrlAsStreamReturnsGzipReaderError(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not gzip"))
	}))
	defer server.Close()

	_, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {})
	if err == nil {
		t.Fatal("ReadUrlAsStream returned nil error for invalid gzip response")
	}
}

// A truncated chunk keeps its needle, so a full-chunk read must stop at the
// chunk's recorded size instead of returning every stored byte.
func TestReadUrlAsStreamStopsAtChunkSize(t *testing.T) {
	InitGlobalHttpClient()

	stored := bytes.Repeat([]byte("t"), 300)
	cipherKey := util.GenCipherKey()
	encrypted, err := util.Encrypt(stored, cipherKey)
	if err != nil {
		t.Fatal(err)
	}
	var gzipped bytes.Buffer
	zw := gzip.NewWriter(&gzipped)
	zw.Write(stored)
	zw.Close()

	for _, tc := range []struct {
		name      string
		body      []byte
		encoding  string
		cipherKey []byte
	}{
		{name: "plain", body: stored},
		{name: "gzip", body: gzipped.Bytes(), encoding: "gzip"},
		{name: "cipher", body: encrypted, cipherKey: cipherKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()

			var got []byte
			if _, err := ReadUrlAsStream(context.Background(), server.URL, "", tc.cipherKey, false, true, 0, 100, func(data []byte) {
				got = append(got, data...)
			}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, stored[:100]) {
				t.Fatalf("full-chunk read returned %d bytes, want 100", len(got))
			}

			got = nil
			if _, err := ReadUrlAsStream(context.Background(), server.URL, "", tc.cipherKey, false, true, 0, 0, func(data []byte) {
				got = append(got, data...)
			}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, stored) {
				t.Fatalf("whole-needle read returned %d bytes, want %d", len(got), len(stored))
			}
		})
	}
}

func TestReadUrlAsStreamReportsAbortPastChunkSize(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "300")
		_, _ = w.Write(bytes.Repeat([]byte("t"), 200))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()

	var got int
	retryable, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 100, func(data []byte) {
		got += len(data)
	})
	if err == nil || !retryable {
		t.Fatalf("aborted response read as retryable=%v err=%v, want a retryable error", retryable, err)
	}
	if got != 100 {
		t.Fatalf("delivered %d bytes, want 100", got)
	}
}

func TestReadErrorsCarryTheServerMessage(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"invalid CRC for needle 2a (got 0a1b2c3d, want 4e5f6071), data on disk corrupted: needle data corrupted"}`))
	}))
	defer server.Close()

	want := server.URL + ": 500 Internal Server Error: invalid CRC for needle 2a (got 0a1b2c3d, want 4e5f6071), data on disk corrupted: needle data corrupted"

	if _, err := ReadUrl(context.Background(), server.URL, nil, false, true, 0, 0, make([]byte, 16)); err == nil || err.Error() != want {
		t.Errorf("ReadUrl: got %v, want %q", err, want)
	}
	if _, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {}); err == nil || err.Error() != want {
		t.Errorf("ReadUrlAsStream: got %v, want %q", err, want)
	}
	if _, _, err := ReadUrlAsReaderCloser(server.URL, "", ""); err == nil || err.Error() != want {
		t.Errorf("ReadUrlAsReaderCloser: got %v, want %q", err, want)
	}
	if _, _, err := readUrlDirectToBuffer(context.Background(), server.URL, "", make([]byte, 16), nil); err == nil || err.Error() != want {
		t.Errorf("readUrlDirectToBuffer: got %v, want %q", err, want)
	}
	if _, _, err := GetAuthenticated(context.Background(), server.URL, ""); err == nil || err.Error() != want {
		t.Errorf("GetAuthenticated: got %v, want %q", err, want)
	}
}

func TestReadErrorsDecodeGzipBody(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write([]byte(`{"error":"invalid CRC for needle 2a"}`))
		_ = gz.Close()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(buf.Bytes())
	}))
	defer server.Close()

	want := server.URL + ": 500 Internal Server Error: invalid CRC for needle 2a"
	if _, err := ReadUrl(context.Background(), server.URL, nil, false, true, 0, 0, make([]byte, 16)); err == nil || err.Error() != want {
		t.Errorf("ReadUrl: got %v, want %q", err, want)
	}
	if _, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {}); err == nil || err.Error() != want {
		t.Errorf("ReadUrlAsStream: got %v, want %q", err, want)
	}
	if _, _, err := GetAuthenticated(context.Background(), server.URL, ""); err == nil || err.Error() != want {
		t.Errorf("GetAuthenticated: got %v, want %q", err, want)
	}
}

func TestReadErrorWithoutBodyIsTheStatus(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	want := server.URL + ": 500 Internal Server Error"
	if _, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {}); err == nil || err.Error() != want {
		t.Errorf("got %v, want %q", err, want)
	}
}

func TestDeleteErrorWithoutBodyIsTheStatus(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	if err := Delete(server.URL, ""); err == nil || err.Error() != "500 Internal Server Error" {
		t.Errorf("got %v, want %q", err, "500 Internal Server Error")
	}
}

func TestDeleteReturnsInvalidRequestErrorBeforeAddingAuth(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Delete panicked before returning the request error: %v", r)
		}
	}()

	if err := Delete("http://[::1", "jwt"); err == nil {
		t.Fatal("expected invalid request error")
	}
}

func TestDeleteTreatsNoContentAsSuccess(t *testing.T) {
	InitGlobalHttpClient()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := Delete(server.URL, ""); err != nil {
		t.Fatalf("expected 204 DELETE to succeed, got %v", err)
	}
}

func TestDeleteProxiedReturnsInvalidRequestErrorBeforeAddingAuth(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("DeleteProxied panicked before returning the request error: %v", r)
		}
	}()

	if _, _, err := DeleteProxied("http://[::1", "jwt"); err == nil {
		t.Fatal("expected invalid request error")
	}
}

// TestRetriedFetchChunkDataRetriesFreshUrlsImmediately covers the case the
// refresh hook exists for: every location the caller knew about is gone, and
// the data is live somewhere the caller has not heard of yet. The read must
// land on the fresh location without first sitting through the backoff ladder.
func TestRetriedFetchChunkDataRetriesFreshUrlsImmediately(t *testing.T) {
	payload := []byte("chunk contents")
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer live.Close()

	// A port nothing listens on: the address is well formed, the dial fails.
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	refreshed := 0
	buffer := make([]byte, len(payload))
	start := time.Now()
	n, err := RetriedFetchChunkData(context.Background(), buffer, []string{deadURL + "/3,abc"}, nil, false, true, 0, "3,abc",
		func() []string {
			refreshed++
			return []string{live.URL + "/3,abc"}
		})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("fetch with a refreshed location: %v", err)
	}
	if n != len(payload) || string(buffer[:n]) != string(payload) {
		t.Fatalf("got %q, want %q", buffer[:n], payload)
	}
	if refreshed != 1 {
		t.Fatalf("refresh called %d times, want exactly 1", refreshed)
	}
	// The first backoff is a full second; landing well under it is the point.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("took %v, expected the retry to skip the backoff", elapsed)
	}
}

// TestRetriedFetchChunkDataKeepsBackoffWhenLocationsAreUnchanged makes sure a
// refresh that returns the same list is treated as "the locations were never
// the problem" rather than as a reason to spin.
func TestRetriedFetchChunkDataKeepsBackoffWhenLocationsAreUnchanged(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	urls := []string{deadURL + "/3,abc"}
	refreshed := 0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	buffer := make([]byte, 4)
	_, err := RetriedFetchChunkData(ctx, buffer, urls, nil, false, true, 0, "3,abc", func() []string {
		refreshed++
		return urls
	})
	if err == nil {
		t.Fatal("expected the fetch to fail against a dead location")
	}
	if refreshed != 1 {
		t.Fatalf("refresh called %d times, want exactly 1", refreshed)
	}
}

// TestRetriedFetchChunkDataRefreshesLocationsAfterPartialFailure covers a read
// that fails on a cached dead replica and succeeds on the next one: the read
// itself is fine, but the list it came from is stale and must be refreshed so
// later reads do not start with the dead replica again. Both the direct and
// the streaming read paths take this branch.
func TestRetriedFetchChunkDataRefreshesLocationsAfterPartialFailure(t *testing.T) {
	forgetUnreachable(t)
	payload := []byte("chunk contents")
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer live.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	for _, isFullChunk := range []bool{true, false} {
		forgetUnreachable(t)
		refreshed := 0
		refresh := func() []string {
			refreshed++
			return []string{live.URL + "/3,abc"}
		}
		buffer := make([]byte, len(payload))
		n, err := RetriedFetchChunkData(context.Background(), buffer, []string{deadURL + "/3,abc", live.URL + "/3,abc"}, nil, false, isFullChunk, 0, "3,abc", refresh)
		if err != nil || string(buffer[:n]) != string(payload) {
			t.Fatalf("fullChunk=%v: got %q, %v; want %q", isFullChunk, buffer[:n], err, payload)
		}
		if refreshed != 1 {
			t.Fatalf("fullChunk=%v: refresh called %d times after a partial failure, want exactly 1", isFullChunk, refreshed)
		}

		// a read answered by the first location leaves the list alone
		refreshed = 0
		n, err = RetriedFetchChunkData(context.Background(), buffer, []string{live.URL + "/3,abc", deadURL + "/3,abc"}, nil, false, isFullChunk, 0, "3,abc", refresh)
		if err != nil || string(buffer[:n]) != string(payload) {
			t.Fatalf("fullChunk=%v: got %q, %v; want %q", isFullChunk, buffer[:n], err, payload)
		}
		if refreshed != 0 {
			t.Fatalf("fullChunk=%v: refresh called %d times without a failure, want none", isFullChunk, refreshed)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// cancelOnReadBody cancels the request's context inside the read that returns
// the last piece of the body, after ReadUrlAsStream's check of ctx has passed.
type cancelOnReadBody struct {
	data   []byte
	cancel context.CancelFunc
}

func (b *cancelOnReadBody) Read(p []byte) (int, error) {
	b.cancel()
	n := copy(p, b.data)
	b.data = b.data[n:]
	if len(b.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (b *cancelOnReadBody) Close() error { return nil }

// TestRetriedFetchChunkDataCancelledWhileTheLastPieceArrives covers a read
// whose caller gives up as the last piece of the body arrives, as a FUSE read
// does when a signal interrupts it. It must return the whole data or an error:
// fewer bytes and no error reach the kernel as a short read, which it takes as
// the end of the file. An encrypted chunk reaches the callback in one piece, so
// there a lost piece is the whole chunk.
func TestRetriedFetchChunkDataCancelledWhileTheLastPieceArrives(t *testing.T) {
	payload := []byte("chunk contents")
	cipherKey := util.GenCipherKey()
	encrypted, err := util.Encrypt(payload, cipherKey)
	if err != nil {
		t.Fatal(err)
	}
	client := GetGlobalHttpClient().Client
	saved := client.Transport
	t.Cleanup(func() { client.Transport = saved })

	for _, tc := range []struct {
		name        string
		body        []byte
		cipherKey   []byte
		isFullChunk bool
	}{
		{"range", payload, nil, false},
		{"encrypted", encrypted, cipherKey, true},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
				Body: &cancelOnReadBody{data: tc.body, cancel: cancel}, Request: r}, nil
		})

		buffer := make([]byte, len(payload))
		n, err := RetriedFetchChunkData(ctx, buffer, []string{"http://volume:8080/3,abc"}, tc.cipherKey, false, tc.isFullChunk, 0, "3,abc", nil)
		cancel()
		if err == nil && !bytes.Equal(buffer[:n], payload) {
			t.Errorf("%s: got %q and no error, want %q or an error", tc.name, buffer[:n], payload)
		}
	}
}

func TestSameUrlsIgnoresOrder(t *testing.T) {
	a := []string{"http://a:8080/3,x", "http://b:8080/3,x"}
	if !SameUrls(a, []string{a[1], a[0]}) {
		t.Fatal("a reshuffle of the same locations should not count as fresh")
	}
	if SameUrls(a, []string{a[0], "http://c:8080/3,x"}) {
		t.Fatal("a different location should count as fresh")
	}
	if SameUrls(a, a[:1]) {
		t.Fatal("a shorter list should count as fresh")
	}
}

// stalledBodyServer answers with the status and the start of a body, and then
// sends nothing more until the test ends.
func stalledBodyServer(t *testing.T, status int, bodyStart string) *httptest.Server {
	return stalledServer(t, status, "", bodyStart)
}

// stalledGzipServer announces a gzip body and then stalls before the gzip
// header. A request that leaves Accept-Encoding to the transport gets a body
// the transport decodes, and up to Go 1.26 closing that body cannot interrupt
// a read waiting for the gzip header.
func stalledGzipServer(t *testing.T, status int) *httptest.Server {
	return stalledServer(t, status, "gzip", "")
}

func stalledServer(t *testing.T, status int, contentEncoding, bodyStart string) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentEncoding != "" {
			w.Header().Set("Content-Encoding", contentEncoding)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(bodyStart))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	return server
}

func setUnwantedBodyTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	old := unwantedBodyTimeout
	unwantedBodyTimeout = timeout
	t.Cleanup(func() { unwantedBodyTimeout = old })
}

// returnsWithin fails the test if fn is still running after limit, instead of
// hanging the test run.
func returnsWithin(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("still waiting on the response body after %v", limit)
	}
}

// A server that sends its headers and then stalls the body must not hold
// CloseResponse: the caller is done with the response and may have another
// replica to try.
func TestCloseResponseGivesUpOnAStalledBody(t *testing.T) {
	InitGlobalHttpClient()
	setUnwantedBodyTimeout(t, 50*time.Millisecond)
	server := stalledBodyServer(t, http.StatusInternalServerError, "disk ")

	resp, err := GetGlobalHttpClient().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	returnsWithin(t, 5*time.Second, func() { CloseResponse(resp) })
	returnsWithin(t, 5*time.Second, func() {
		if _, err := resp.Body.Read(make([]byte, 1)); err == nil || err == io.EOF {
			t.Errorf("read after CloseResponse: got %v, want the body closed", err)
		}
	})
}

// The same holds when the transport is decoding gzip and the server stalls
// before the gzip header, where closing the body does not end the read.
func TestUnwantedBodyReadsDoNotWaitForAStalledGzipHeader(t *testing.T) {
	InitGlobalHttpClient()
	setUnwantedBodyTimeout(t, 50*time.Millisecond)
	server := stalledGzipServer(t, http.StatusInternalServerError)

	get := func() *http.Response {
		resp, err := GetGlobalHttpClient().Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.Uncompressed {
			t.Fatal("the transport is not decoding the body")
		}
		return resp
	}
	resp := get()
	returnsWithin(t, 5*time.Second, func() { CloseResponse(resp) })

	resp = get()
	want := server.URL + ": 500 Internal Server Error"
	returnsWithin(t, 5*time.Second, func() {
		if err := StatusError(server.URL, resp); err.Error() != want {
			t.Errorf("StatusError: got %v, want %q", err, want)
		}
	})
	returnsWithin(t, 5*time.Second, func() { CloseResponse(resp) })
}

// A body that arrives is still read to its end, which lets the next request
// reuse the connection.
func TestCloseResponseKeepsTheConnectionOfADrainedBody(t *testing.T) {
	InitGlobalHttpClient()
	setUnwantedBodyTimeout(t, 5*time.Second)
	leftover := bytes.Repeat([]byte("leftover"), 64*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(leftover)
	}))
	defer server.Close()

	var reused []bool
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = append(reused, info.Reused) }}
	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := GetGlobalHttpClient().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		CloseResponse(resp)
	}
	if len(reused) != 2 || reused[0] || !reused[1] {
		t.Errorf("connection reused per request: got %v, want [false true]", reused)
	}
}

// A read that fails on the status must return it even when the server never
// finishes the error body. 404 goes straight to CloseResponse; 500 waits for
// the server's message first and then goes without it.
func TestReadErrorsDoNotWaitForAStalledBody(t *testing.T) {
	InitGlobalHttpClient()
	setUnwantedBodyTimeout(t, 50*time.Millisecond)

	for _, tc := range []struct {
		name             string
		status           int
		wantStream       string
		wantReaderCloser string
	}{
		{"not found", http.StatusNotFound, ": 404 Not Found: not found", ": 404 Not Found"},
		{"server error", http.StatusInternalServerError, ": 500 Internal Server Error", ": 500 Internal Server Error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := stalledBodyServer(t, tc.status, "disk ")
			returnsWithin(t, 5*time.Second, func() {
				want := server.URL + tc.wantStream
				if _, err := ReadUrlAsStream(context.Background(), server.URL, "", nil, false, true, 0, 0, func(data []byte) {}); err == nil || err.Error() != want {
					t.Errorf("ReadUrlAsStream: got %v, want %q", err, want)
				}
			})
			returnsWithin(t, 5*time.Second, func() {
				want := server.URL + tc.wantReaderCloser
				if _, _, err := ReadUrlAsReaderCloser(server.URL, "", ""); err == nil || err.Error() != want {
					t.Errorf("ReadUrlAsReaderCloser: got %v, want %q", err, want)
				}
			})
		})
	}
}
