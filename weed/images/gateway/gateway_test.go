package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const transform = "image/resize,w_640/quality,Q_85/format,webp"

// fixture models S3 access, revisions, and an external encoder for HTTP-level tests.
type fixture struct {
	origin, processor        *httptest.Server
	gateway                  *Gateway
	mu                       sync.Mutex
	status                   int
	etag, version            string
	sourceBytes              int64
	processStatus            int
	processType              string
	data                     []byte
	original                 []byte
	sources                  []string
	heads                    atomic.Int32
	encodes                  atomic.Int32
	started, release         chan struct{}
	headStarted, headRelease chan struct{}
	headPaused               atomic.Bool
}

// newFixture creates HTTP backends that fail tests if credentials are forwarded.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{status: 200, etag: "\"source-v1\"", sourceBytes: 100, processStatus: 200,
		processType: "image/webp", data: []byte("RIFF transformed webp bytes"), original: []byte("original-image")}
	f.origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Pause only the initial source check; the post-encoding recheck remains fast.
		if r.Method == http.MethodHead && f.headRelease != nil && f.headPaused.CompareAndSwap(false, true) {
			close(f.headStarted)
			select {
			case <-f.headRelease:
			case <-r.Context().Done():
				return
			}
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.URL.Query().Get("x-oss-process") != "" {
			t.Error("source request contained credentials or processing parameters")
		}
		if r.Method == http.MethodHead {
			f.heads.Add(1)
			w.Header().Set("ETag", f.etag)
			w.Header().Set("Content-Length", fmt.Sprint(f.sourceBytes))
			w.Header().Set("Last-Modified", "Sun, 04 Oct 2026 10:00:00 GMT")
			if f.version != "" {
				w.Header().Set("x-amz-version-id", f.version)
			}
			w.WriteHeader(f.status)
			return
		}
		if f.status != 200 {
			w.WriteHeader(f.status)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("ETag", f.etag)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.original))
	}))
	f.processor = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.encodes.Add(1)
		encoded := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], ".webp")
		source, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			t.Errorf("encoder received an invalid source URL: %v", err)
		}
		f.mu.Lock()
		f.sources = append(f.sources, string(source))
		status, contentType, data := f.processStatus, f.processType, append([]byte(nil), f.data...)
		f.mu.Unlock()
		if f.started != nil {
			select {
			case f.started <- struct{}{}:
			default:
			}
		}
		if f.release != nil {
			select {
			case <-f.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(data)
	}))
	var err error
	f.gateway, err = New(Config{Source: f.origin.URL + "/bucket", Imgproxy: f.processor.URL,
		Concurrency: 16, MaxDimension: 4096, CacheBytes: 4096, MaxSourceBytes: 1024, MaxResultBytes: 1024, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.origin.Close(); f.processor.Close(); f.gateway.client.CloseIdleConnections() })
	return f
}

// request sends conditional or range requests to the gateway handler.
func (f *fixture) request(method, path string, headers http.Header) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://untrusted-host"+path, nil)
	if headers != nil {
		r.Header = headers
	}
	w := httptest.NewRecorder()
	f.gateway.ServeHTTP(w, r)
	return w
}

// imagePath escapes the processing query while preserving its slash-separated semantics.
func imagePath() string { return "/image.png?x-oss-process=" + url.QueryEscape(transform) }

// TestCacheRechecksAccess checks cache hits, revocation of public access, and source deletion.
func TestCacheRechecksAccess(t *testing.T) {
	f := newFixture(t)
	first := f.request("GET", imagePath(), nil)
	if first.Code != 200 || first.Header().Get("ETag") == "\"source-v1\"" || first.Header().Get("Content-Type") != "image/webp" {
		t.Fatalf("unexpected first processed response: %d %v", first.Code, first.Header())
	}
	second := f.request("GET", imagePath(), nil)
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) || f.encodes.Load() != 1 || f.heads.Load() != 3 {
		t.Fatal("cache hits must recheck access without re-encoding")
	}
	f.mu.Lock()
	f.status = 403
	f.mu.Unlock()
	denied := f.request("GET", imagePath(), http.Header{"If-None-Match": {first.Header().Get("ETag")}})
	if denied.Code != 403 || denied.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("conditional request bypassed revoked access")
	}
	f.mu.Lock()
	f.status = 404
	f.mu.Unlock()
	if f.request("HEAD", imagePath(), nil).Code != 404 {
		t.Fatal("deleted source still served a cached result")
	}
}

// TestRepresentationHeaders checks HEAD, 304, and Range semantics for processed bytes.
func TestRepresentationHeaders(t *testing.T) {
	f := newFixture(t)
	full := f.request("GET", imagePath(), nil)
	etag := full.Header().Get("ETag")
	head := f.request("HEAD", imagePath(), nil)
	if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(len(f.data)) {
		t.Fatal("HEAD used the source length or returned a body")
	}
	conditional := f.request("GET", imagePath(), http.Header{"If-None-Match": {etag}})
	if conditional.Code != 304 || conditional.Body.Len() != 0 {
		t.Fatal("output ETag did not produce 304")
	}
	if f.request("GET", imagePath(), http.Header{"If-None-Match": {"\"source-v1\""}}).Code != 200 {
		t.Fatal("source ETag was incorrectly used")
	}
	ranged := f.request("GET", imagePath(), http.Header{"Range": {"bytes=0-3"}})
	if ranged.Code != 206 || ranged.Body.String() != "RIFF" || ranged.Header().Get("Content-Range") != fmt.Sprintf("bytes 0-3/%d", len(f.data)) {
		t.Fatal("Range did not apply to processed bytes")
	}
	invalid := f.request("GET", imagePath(), http.Header{"Range": {"bytes=999-"}})
	if invalid.Code != 416 || invalid.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unsatisfiable output range was not rejected")
	}
	failed := f.request("GET", imagePath(), http.Header{"If-Match": {"\"wrong\""}})
	if failed.Code != 412 || failed.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("failed precondition response was cacheable")
	}
}

// TestChangedSource checks invalidation after overwrites and explicit version reads.
func TestChangedSource(t *testing.T) {
	f := newFixture(t)
	f.request("GET", imagePath(), nil)
	f.mu.Lock()
	f.etag = "\"source-v2\""
	f.data = []byte("new result")
	f.mu.Unlock()
	if f.request("GET", imagePath(), nil).Body.String() != "new result" || f.encodes.Load() != 2 {
		t.Fatal("source overwrite returned stale cache")
	}
	if f.request("GET", imagePath()+"&versionId=v1", nil).Code != 200 {
		t.Fatal("explicit version read failed")
	}
	f.mu.Lock()
	last := f.sources[len(f.sources)-1]
	f.mu.Unlock()
	if last != f.origin.URL+"/bucket/image.png?versionId=v1" {
		t.Fatalf("version was not forwarded to the source: %s", last)
	}
}

// TestSourceChangesDuringEncoding checks that changing sources do not populate stale cache entries.
func TestSourceChangesDuringEncoding(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.request("GET", imagePath(), nil) }()
	<-f.started
	f.mu.Lock()
	f.etag = "\"replaced\""
	f.mu.Unlock()
	close(f.release)
	if (<-done).Code != 409 {
		t.Fatal("source replacement still cached a processed result")
	}
	if f.request("GET", imagePath(), nil).Code != 200 || f.encodes.Load() != 2 {
		t.Fatal("retry did not re-encode")
	}
}

// TestRejectUnsafeRequests checks that credentials, arbitrary URLs, duplicate queries, and writes do not reach backends.
func TestRejectUnsafeRequests(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		method, path string
		header       http.Header
		status       int
	}{
		{"PUT", imagePath(), nil, 405},
		{"GET", imagePath(), http.Header{"Authorization": {"AWS4-HMAC-SHA256 secret"}}, 403},
		{"GET", imagePath(), http.Header{"X-Amz-Security-Token": {"secret"}}, 403},
		{"GET", imagePath() + "&X-Amz-Signature=secret", nil, 400},
		{"GET", imagePath() + "&source=http://private-target", nil, 400},
		{"GET", imagePath() + "&x-oss-process=image", nil, 400},
		{"GET", "/image.png?x-oss-process=%", nil, 400},
		{"GET", "/image.png?x-oss-process=", nil, 400},
	}
	for _, test := range tests {
		if response := f.request(test.method, test.path, test.header); response.Code != test.status {
			t.Errorf("request %s returned %d; expected %d", test.path, response.Code, test.status)
		}
	}
	if f.encodes.Load() != 0 || f.heads.Load() != 0 {
		t.Fatal("invalid request reached a backend")
	}
}

// TestLimitsAndBackendFailures checks resource limits, media types, missing sources, and encoder errors.
func TestLimitsAndBackendFailures(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fixture)
		status int
	}{
		{"oversized source", func(f *fixture) { f.sourceBytes = 1025 }, 413},
		{"oversized result", func(f *fixture) { f.data = make([]byte, 1025) }, 413},
		{"empty result", func(f *fixture) { f.data = nil }, 413},
		{"incorrect media type", func(f *fixture) { f.processType = "text/html" }, 502},
		{"encoder failure", func(f *fixture) { f.processStatus = 500 }, 502},
		{"source denied", func(f *fixture) { f.status = 403 }, 403},
		{"source missing", func(f *fixture) { f.status = 404 }, 404},
		{"source failure", func(f *fixture) { f.status = 500 }, 502},
		{"redirect", func(f *fixture) { f.status = 302 }, 502},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			test.change(f)
			for i := 0; i < 2; i++ {
				w := f.request("GET", imagePath(), nil)
				if w.Code != test.status || w.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("unexpected error response: %d %v", w.Code, w.Header())
				}
			}
			if f.gateway.cache.order.Len() != 0 {
				t.Fatal("failed result entered the cache")
			}
		})
	}
}

// TestConcurrentMisses checks that concurrent misses for the same image encode only once.
func TestConcurrentMisses(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			if w := f.request("GET", imagePath(), nil); w.Code != 200 {
				t.Errorf("concurrent request failed: %d", w.Code)
			}
		}()
	}
	<-f.started
	close(f.release)
	wg.Wait()
	if f.encodes.Load() != 1 {
		t.Fatalf("encoder calls for concurrent misses: %d", f.encodes.Load())
	}
}

// TestConcurrencyAndCancellation checks immediate overload rejection and prompt cancellation of waiting requests.
func TestConcurrencyAndCancellation(t *testing.T) {
	f := newFixture(t)
	f.gateway.requests = make(chan struct{}, 1)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", imagePath(), nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { f.gateway.ServeHTTP(httptest.NewRecorder(), r); close(done) }()
	<-f.started
	if f.request("GET", imagePath(), nil).Code != 429 {
		t.Fatal("overload did not return 429")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled request continued waiting for encoding")
	}
	close(f.release)
}

// TestOriginalRead checks source ETag, HEAD, and Range without processing.
func TestOriginalRead(t *testing.T) {
	f := newFixture(t)
	get := f.request("GET", "/image.png", nil)
	if get.Code != 200 || get.Body.String() != "original-image" || get.Header().Get("Content-Type") != "image/png" {
		t.Fatal("unexpected source read")
	}
	ranged := f.request("GET", "/image.png", http.Header{"Range": {"bytes=0-2"}})
	if ranged.Code != 206 || ranged.Body.String() != "ori" {
		t.Fatal("unexpected source range read")
	}
	if f.request("GET", "/image.png", http.Header{"If-None-Match": {"\"source-v1\""}}).Code != 304 {
		t.Fatal("unexpected source conditional read")
	}
	if f.request("HEAD", "/image.png", nil).Body.Len() != 0 || f.encodes.Load() != 0 {
		t.Fatal("source request unexpectedly encoded an image")
	}
}

// TestBrowserCookiesAreNotForwarded checks anonymous reads when browsers send site cookies.
func TestBrowserCookiesAreNotForwarded(t *testing.T) {
	f := newFixture(t)
	if f.request("GET", imagePath(), http.Header{"Cookie": {"session=secret"}}).Code != 200 {
		t.Fatal("site cookie interfered with a public image read")
	}
	f.mu.Lock()
	f.status = 403
	f.mu.Unlock()
	if f.request("GET", imagePath(), http.Header{"Cookie": {"session=secret"}}).Code != 403 {
		t.Fatal("cookie elevated source read permissions")
	}
}

// TestNoETagDisablesCache checks that sources without validators are processed on each request.
func TestNoETagDisablesCache(t *testing.T) {
	f := newFixture(t)
	f.etag = ""
	f.request("GET", imagePath(), nil)
	f.request("GET", imagePath(), nil)
	if f.encodes.Load() != 2 {
		t.Fatal("result was reused without a source ETag")
	}
}

// TestFixedSourceAndSigning checks path escaping, fixed hosts, and imgproxy HMAC signatures.
func TestFixedSourceAndSigning(t *testing.T) {
	f := newFixture(t)
	f.gateway.key, f.gateway.salt = []byte("secret"), []byte("salt")
	r := httptest.NewRequest("GET", "http://attacker/a%20b/%23.png?versionId=v%2B1", nil)
	source := f.gateway.sourceURL(r, r.URL.Query())
	if source.String() != f.origin.URL+"/bucket/a%20b/%23.png?versionId=v%2B1" {
		t.Fatal("fixed source or object escaping changed")
	}
	u := f.gateway.processorURL(source, options{width: 640, quality: 85, format: "webp"})
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte("salt/" + parts[1]))
	if parts[0] != base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatal("imgproxy signature did not match the protocol")
	}
}

// TestRealHTTPHead checks output length and body suppression for real HTTP HEAD responses.
func TestRealHTTPHead(t *testing.T) {
	f := newFixture(t)
	s := httptest.NewServer(f.gateway)
	defer s.Close()
	resp, err := http.Head(s.URL + imagePath())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || resp.ContentLength != int64(len(f.data)) || len(body) != 0 {
		t.Fatal("real HEAD response violated HTTP semantics")
	}
}

// waitUntil bounds synchronization waits so a regression fails rather than hanging.
func waitUntil(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for test synchronization")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCancelledLeaderDoesNotFailWaiter checks that shared work outlives its first caller.
func TestCancelledLeaderDoesNotFailWaiter(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leader := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		f.gateway.ServeHTTP(w, httptest.NewRequest("GET", imagePath(), nil).WithContext(ctx))
		leader <- w
	}()
	<-f.started
	waiter := make(chan *httptest.ResponseRecorder, 1)
	go func() { waiter <- f.request("GET", imagePath(), nil) }()
	waitUntil(t, func() bool { return f.heads.Load() >= 2 })
	cancel()
	if (<-leader).Code != 504 {
		t.Fatal("cancelled leader did not leave promptly")
	}
	close(f.release)
	response := <-waiter
	if response.Code != 200 || response.Body.String() != string(f.data) || f.encodes.Load() != 1 {
		t.Fatalf("leader cancellation failed shared work: status=%d encodes=%d", response.Code, f.encodes.Load())
	}
}

// TestCancelledWorkRemainsBounded checks that cancellation cannot free an active encoding slot.
func TestCancelledWorkRemainsBounded(t *testing.T) {
	f := newFixture(t)
	f.gateway.requests, f.gateway.work = make(chan struct{}, 1), make(chan struct{}, 1)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		f.gateway.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", imagePath(), nil).WithContext(ctx))
		close(done)
	}()
	<-f.started
	cancel()
	<-done
	other := strings.Replace(imagePath(), "image.png", "other.png", 1)
	if response := f.request("GET", other, nil); response.Code != 429 || f.encodes.Load() != 1 {
		t.Fatal("cancelled request bypassed active encoding concurrency limit")
	}
	close(f.release)
	waitUntil(t, func() bool { return len(f.gateway.work) == 0 })
	if f.request("GET", other, nil).Code != 200 {
		t.Fatal("completed work did not release its concurrency slot")
	}
}

// TestAbandonedWorkTimesOut checks that a job with no remaining callers releases its slot.
func TestAbandonedWorkTimesOut(t *testing.T) {
	f := newFixture(t)
	f.gateway.config.Timeout = 50 * time.Millisecond
	f.gateway.work = make(chan struct{}, 1)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.gateway.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", imagePath(), nil).WithContext(ctx))
		close(done)
	}()
	<-f.started
	cancel()
	<-done
	waitUntil(t, func() bool { return len(f.gateway.work) == 0 })
	close(f.release)
	if f.request("GET", imagePath(), nil).Code != 200 || f.encodes.Load() != 2 {
		t.Fatal("abandoned encoding did not time out and permit a new job")
	}
}

// TestSameSecondOverwriteDoesNotReturn304 checks that source dates cannot validate output bytes.
func TestSameSecondOverwriteDoesNotReturn304(t *testing.T) {
	f := newFixture(t)
	first := f.request("GET", imagePath(), nil)
	f.mu.Lock()
	f.etag, f.data = "\"source-v2\"", []byte("replacement bytes")
	f.mu.Unlock()
	headers := http.Header{"If-Modified-Since": {"Sun, 04 Oct 2026 10:00:00 GMT"}}
	response := f.request("GET", imagePath(), headers)
	if response.Code != 200 || response.Body.String() != "replacement bytes" || response.Header().Get("Last-Modified") != "" {
		t.Fatal("same-second overwrite incorrectly used the source modification date")
	}
	if response.Header().Get("ETag") == first.Header().Get("ETag") {
		t.Fatal("changed output reused the old validator")
	}
	if f.request("GET", imagePath(), http.Header{"If-None-Match": {response.Header().Get("ETag")}}).Code != 304 {
		t.Fatal("output ETag stopped validating unchanged output")
	}
}

// TestOriginalRangeLimitsAndErrors checks complete-object limits and unsatisfied range metadata.
func TestOriginalRangeLimitsAndErrors(t *testing.T) {
	f := newFixture(t)
	f.original = make([]byte, 2048)
	if response := f.request("GET", "/image.png", http.Header{"Range": {"bytes=0-0"}}); response.Code != 413 {
		t.Fatalf("small range bypassed the complete source size limit: %d", response.Code)
	}
	f.original = []byte("original-image")
	response := f.request("GET", "/image.png", http.Header{"Range": {"bytes=999-"}})
	if response.Code != 416 || response.Header().Get("Content-Range") != "bytes */14" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unsatisfied source range lost its complete length: %d %v", response.Code, response.Header())
	}
}

// TestOriginalRangeRequiresCompleteLength rejects unknown or invalid source range totals.
func TestOriginalRangeRequiresCompleteLength(t *testing.T) {
	for _, contentRange := range []string{"", "bytes 0-0/*", "bytes 0-0/no-number", "items 0-0/100", "bytes 0-0/0"} {
		t.Run(contentRange, func(t *testing.T) {
			f := newFixture(t)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Range", contentRange)
				w.WriteHeader(206)
				_, _ = w.Write([]byte("x"))
			}))
			defer s.Close()
			f.gateway.source, _ = url.Parse(s.URL)
			if f.request("GET", "/image.png", http.Header{"Range": {"bytes=0-0"}}).Code != 502 {
				t.Fatal("source range without a valid complete length was accepted")
			}
		})
	}
}

// TestRejectNormalizedTraversal protects a configured bucket prefix, including escaped paths.
func TestRejectNormalizedTraversal(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/../other/image.png", "/./image.png", "/%2e%2e/other.png", "/%252e%252e/other.png", "/%25252e%25252e/other.png", "/x%2f..%2fother.png", "/x%252f..%252fother.png", "/x%5c..%5cother.png", "/x%255c..%255cother.png"} {
		for _, query := range []string{"", "?x-oss-process=" + url.QueryEscape(transform)} {
			if response := f.request("GET", path+query, nil); response.Code != 400 {
				t.Errorf("unsafe path accepted: %s status=%d", path, response.Code)
			}
		}
	}
	if f.heads.Load() != 0 || f.encodes.Load() != 0 {
		t.Fatal("unsafe path reached a backend")
	}
	if f.request("GET", "/a%20b/%23%25.png?x-oss-process="+url.QueryEscape(transform), nil).Code != 200 {
		t.Fatal("ordinary escaped object name was rejected")
	}
}

// deadlineRecorder exposes the HTTP server deadline hook without relying on timing-sensitive sockets.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
	calls    int
}

// SetWriteDeadline records when the gateway allocates its client write budget.
func (w *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	w.deadline, w.calls = deadline, w.calls+1
	return nil
}

// TestWriteBudgetStartsAfterProcessing checks that slow backends do not consume the write budget.
func TestWriteBudgetStartsAfterProcessing(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	w := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() { f.gateway.ServeHTTP(w, httptest.NewRequest("GET", imagePath(), nil)); close(done) }()
	<-f.started
	if w.calls != 0 {
		t.Fatal("write budget started while backend processing was blocked")
	}
	released := time.Now()
	close(f.release)
	<-done
	if w.calls != 1 || w.deadline.Before(released.Add(f.gateway.config.Timeout)) || w.Code != 200 {
		t.Fatal("response did not receive a separate full write timeout")
	}
}

// TestSlowMetadataDoesNotConsumeEncodingWait checks independent source and work budgets.
func TestSlowMetadataDoesNotConsumeEncodingWait(t *testing.T) {
	f := newFixture(t)
	f.gateway.config.Timeout = 300 * time.Millisecond
	f.headStarted, f.headRelease = make(chan struct{}), make(chan struct{})
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.request("GET", imagePath(), nil) }()
	<-f.headStarted
	time.Sleep(200 * time.Millisecond)
	close(f.headRelease)
	<-f.started
	time.Sleep(200 * time.Millisecond)
	close(f.release)
	response := <-done
	if response.Code != 200 || response.Body.String() != string(f.data) {
		t.Fatalf("initial metadata consumed the encoding wait budget: %d", response.Code)
	}
}

// TestOriginalContentEncoding checks original encoded bytes, HEAD, ranges, and real client decoding.
func TestOriginalContentEncoding(t *testing.T) {
	f := newFixture(t)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write(f.original)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", fmt.Sprint(compressed.Len()))
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(compressed.Bytes()))
	}))
	defer source.Close()
	f.gateway.source, _ = url.Parse(source.URL)
	get := f.request("GET", "/encoded.png", nil)
	if get.Code != 200 || get.Header().Get("Content-Encoding") != "gzip" || !bytes.Equal(get.Body.Bytes(), compressed.Bytes()) {
		t.Fatal("original response lost its content encoding or changed stored bytes")
	}
	head := f.request("HEAD", "/encoded.png", nil)
	if head.Header().Get("Content-Encoding") != "gzip" || head.Header().Get("Content-Length") != fmt.Sprint(compressed.Len()) || head.Body.Len() != 0 {
		t.Fatal("encoded original HEAD lost representation metadata")
	}
	ranged := f.request("GET", "/encoded.png", http.Header{"Range": {"bytes=0-2"}})
	if ranged.Code != 206 || ranged.Header().Get("Content-Encoding") != "gzip" || !bytes.Equal(ranged.Body.Bytes(), compressed.Bytes()[:3]) {
		t.Fatal("encoded original range did not preserve stored representation")
	}
	server := httptest.NewServer(f.gateway)
	defer server.Close()
	response, err := http.Get(server.URL + "/encoded.png")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	decoded, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(decoded, f.original) {
		t.Fatal("real client could not decode the original representation")
	}
}

// TestRepeatedSlashesPreserveBucketPrefix checks actual forwarded URLs rather than assuming traversal.
func TestRepeatedSlashesPreserveBucketPrefix(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"//other-bucket/image.png", "/nested//image.png", "/%2fother-bucket/image.png"} {
		if response := f.request("GET", path+"?x-oss-process="+url.QueryEscape(transform), nil); response.Code != 200 {
			t.Fatalf("valid repeated-slash key was not served: %s %d", path, response.Code)
		}
		f.mu.Lock()
		encodedSource := f.sources[len(f.sources)-1]
		f.mu.Unlock()
		source, err := url.Parse(encodedSource)
		if err != nil || source.Host != strings.TrimPrefix(f.origin.URL, "http://") || !strings.HasPrefix(source.Path, "/bucket/") {
			t.Fatalf("repeated slashes escaped the fixed source: %s", encodedSource)
		}
	}
}
