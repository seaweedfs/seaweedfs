package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type scriptedHTTPResponse struct {
	status int
	body   string
	header http.Header
}

type scriptedHTTPClient struct {
	mu        sync.Mutex
	responses map[string][]scriptedHTTPResponse
	calls     []string
}

func (c *scriptedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	url := req.URL.String()
	c.calls = append(c.calls, url)

	plans := c.responses[url]
	if len(plans) == 0 {
		return nil, fmt.Errorf("unexpected request to %s", url)
	}
	plan := plans[0]
	c.responses[url] = plans[1:]

	return &http.Response{
		StatusCode: plan.status,
		Header:     plan.header.Clone(),
		Body:       io.NopCloser(strings.NewReader(plan.body)),
	}, nil
}

func TestShouldReassignUpload(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "no response (transport)", err: &uploadStatusError{StatusCode: 0, err: fmt.Errorf("dial tcp: no such host")}, want: true},
		{name: "500 replica write failed", err: &uploadStatusError{StatusCode: 500, err: fmt.Errorf("failed to write to replicas")}, want: true},
		{name: "503 service unavailable", err: &uploadStatusError{StatusCode: 503, err: fmt.Errorf("busy")}, want: true},
		{name: "proven conflict", err: &uploadStatusError{StatusCode: http.StatusConflict, refusedBeforeAppend: true, err: errors.New("read only")}, want: true},
		{name: "unmarked conflict", err: &uploadStatusError{StatusCode: http.StatusConflict, err: errors.New("read only")}, want: false},
		{name: "400 bad request", err: &uploadStatusError{StatusCode: 400, err: fmt.Errorf("bad needle")}, want: false},
		{name: "401 unauthorized", err: &uploadStatusError{StatusCode: 401, err: fmt.Errorf("wrong jwt")}, want: false},
		{name: "assign rpc transport", err: fmt.Errorf("assign volume: transport is closing"), want: true},
		{name: "other permanent", err: fmt.Errorf("mismatching cookie"), want: false},
		// A read-only path is the filer's verdict for any volume, unlike a read-only volume above.
		{
			name: "read-only path, path mentions transport",
			err: fmt.Errorf("assign volume failure path:\"/buckets/b/transport/x\": %w", filer_pb.AssignVolumeResponseError(&filer_pb.AssignVolumeResponse{
				Error:     "assign volume: read only: /buckets/b/ (e.g. bucket over quota)",
				ErrorCode: filer_pb.FilerError_READ_ONLY,
			})),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldReassignUpload(tc.err); got != tc.want {
				t.Fatalf("ShouldReassignUpload(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestUploadWithRetryDataReassignsOnProvenConflict(t *testing.T) {
	for _, retryForever := range []bool{false, true} {
		for _, outcome := range []string{"refused-before-append", "", "unknown"} {
			t.Run(fmt.Sprintf("retryForever=%t/outcome=%s", retryForever, outcome), func(t *testing.T) {
				httpClient := &scriptedHTTPClient{responses: map[string][]scriptedHTTPResponse{
					"http://volume-a/1,first":  {{status: http.StatusConflict, body: `{}`, header: http.Header{"X-Seaweed-Write-Outcome": {outcome}}}},
					"http://volume-b/2,second": {{status: http.StatusCreated, body: `{"name":"test.bin","size":3}`}},
				}}
				uploader := newUploader(httpClient)
				assignments := 0
				fid, _, err := uploader.uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
					assignments++
					if assignments == 1 {
						return "1,first", "volume-a", "", false, nil
					}
					return "2,second", "volume-b", "", false, nil
				}, &UploadOption{Filename: "test.bin", RetryForever: retryForever}, []byte("abc"))
				if outcome == "refused-before-append" {
					if err != nil || fid != "2,second" || assignments != 2 || len(httpClient.calls) != 2 {
						t.Fatalf("reassignment: fid=%s, assignments=%d, error=%v", fid, assignments, err)
					}
				} else if err == nil || fid != "1,first" || assignments != 1 || len(httpClient.calls) != 1 {
					t.Fatalf("unproven conflict retried or acknowledged: assignments=%d, error=%v", assignments, err)
				}
			})
		}
	}
}

func TestUploadWithRetryDataRetryForeverPreservesAssignmentRetries(t *testing.T) {
	httpClient := &scriptedHTTPClient{responses: map[string][]scriptedHTTPResponse{
		"http://volume/1,first": {{status: http.StatusCreated, body: `{"name":"test.bin","size":3}`}},
	}}
	assignments := 0
	fileID, _, err := newUploader(httpClient).uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
		assignments++
		if assignments < 3 {
			return "", "", "", false, errors.New("no writable volumes")
		}
		return "1,first", "volume", "", false, nil
	}, &UploadOption{Filename: "test.bin", RetryForever: true}, []byte("abc"))
	if err != nil || fileID != "1,first" || assignments != 3 || len(httpClient.calls) != 1 {
		t.Fatalf("assignment retries: fid=%s, assignments=%d, uploads=%d, error=%v", fileID, assignments, len(httpClient.calls), err)
	}
}

func TestUploadWithRetryDataReassignsOnVolumeSizeExceeded(t *testing.T) {
	httpClient := &scriptedHTTPClient{
		responses: map[string][]scriptedHTTPResponse{
			"http://volume-a/1,first": {
				{status: http.StatusInternalServerError, body: `{"error":"failed to write to local disk: append to volume 1 size 0 actualSize 0: Volume Size 33555976 Exceeded 33554432"}`},
				{status: http.StatusInternalServerError, body: `{"error":"failed to write to local disk: append to volume 1 size 0 actualSize 0: Volume Size 33555976 Exceeded 33554432"}`},
				{status: http.StatusInternalServerError, body: `{"error":"failed to write to local disk: append to volume 1 size 0 actualSize 0: Volume Size 33555976 Exceeded 33554432"}`},
			},
			"http://volume-b/2,second": {
				{status: http.StatusCreated, body: `{"name":"test.bin","size":3}`},
			},
		},
	}
	uploader := newUploader(httpClient)

	assignCalls := 0
	fileID, uploadResult, err := uploader.uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
		assignCalls++
		if assignCalls == 1 {
			return "1,first", "volume-a", "", false, nil
		}
		return "2,second", "volume-b", "", false, nil
	}, &UploadOption{Filename: "test.bin"}, []byte("abc"))

	if err != nil {
		t.Fatalf("expected success after reassignment, got %v", err)
	}
	if fileID != "2,second" {
		t.Fatalf("expected second file id, got %s", fileID)
	}
	if assignCalls != 2 {
		t.Fatalf("expected 2 assign attempts, got %d", assignCalls)
	}
	if uploadResult == nil || uploadResult.Name != "test.bin" {
		t.Fatalf("expected successful upload result, got %#v", uploadResult)
	}
	if len(httpClient.calls) != 4 {
		t.Fatalf("expected 4 upload attempts (3 same-url retries + 1 reassigned upload), got %d", len(httpClient.calls))
	}
	if httpClient.calls[0] != "http://volume-a/1,first" || httpClient.calls[3] != "http://volume-b/2,second" {
		t.Fatalf("unexpected upload call sequence: %#v", httpClient.calls)
	}
}

// A volume whose replica peer is down reports "failed to write to replicas":
// the primary write lands but replication fails. The client must re-assign to a
// volume on live nodes and retry rather than losing the write.
func TestUploadWithRetryDataReassignsOnReplicaWriteFailure(t *testing.T) {
	replicaErr := `{"error":"failed to write to replicas for volume 1: [volume-down:8080]: dial tcp: lookup volume-down: no such host"}`
	httpClient := &scriptedHTTPClient{
		responses: map[string][]scriptedHTTPResponse{
			"http://volume-a/1,first": {
				{status: http.StatusInternalServerError, body: replicaErr},
				{status: http.StatusInternalServerError, body: replicaErr},
				{status: http.StatusInternalServerError, body: replicaErr},
			},
			"http://volume-b/2,second": {
				{status: http.StatusCreated, body: `{"name":"test.bin","size":3}`},
			},
		},
	}
	uploader := newUploader(httpClient)

	assignCalls := 0
	fileID, uploadResult, err := uploader.uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
		assignCalls++
		if assignCalls == 1 {
			return "1,first", "volume-a", "", false, nil
		}
		return "2,second", "volume-b", "", false, nil
	}, &UploadOption{Filename: "test.bin"}, []byte("abc"))

	if err != nil {
		t.Fatalf("expected success after reassignment, got %v", err)
	}
	if fileID != "2,second" {
		t.Fatalf("expected reassigned file id, got %s", fileID)
	}
	if assignCalls != 2 {
		t.Fatalf("expected 2 assign attempts, got %d", assignCalls)
	}
	if uploadResult == nil || uploadResult.Name != "test.bin" {
		t.Fatalf("expected successful upload result, got %#v", uploadResult)
	}
}

// bodyCapturingHTTPClient drains req.Body on every Do, optionally failing the
// first attempt with a transport error so we can verify upload_content rewinds
// the body before retrying.
type bodyCapturingHTTPClient struct {
	mu          sync.Mutex
	bodies      [][]byte
	failFirst   string
	successJSON string
}

func (c *bodyCapturingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var captured []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		captured = b
	}
	c.bodies = append(c.bodies, captured)

	if len(c.bodies) == 1 && c.failFirst != "" {
		return nil, errors.New(c.failFirst)
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(c.successJSON)),
	}, nil
}

// stallingHTTPClient parks the first stalls calls without draining the request
// body (a conn that never drains) until the request ctx is cancelled, and
// delays the rest by slowFor after draining.
type stallingHTTPClient struct {
	mu          sync.Mutex
	stalls      int
	slowFor     time.Duration
	requestCtxs []context.Context
}

func (c *stallingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.requestCtxs = append(c.requestCtxs, req.Context())
	stall := c.stalls > 0
	if stall {
		c.stalls--
	}
	c.mu.Unlock()

	if stall {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	if c.slowFor > 0 {
		time.Sleep(c.slowFor)
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"size":1}`)),
	}, nil
}

// markHostFast arms the stall watchdog the way a recent fast upload would:
// the recorded rate is high enough that the adaptive budget lands on
// uploadStallMinWait for test-sized bodies.
func markHostFast(u *Uploader, host string) {
	u.fastUploadHosts.Store(host, fastHostMark{at: time.Now().UnixNano(), bps: 1 << 30})
}

// A host that just served a fast upload arms the stall watchdog: a request
// parked well past the floor is riding a wedged conn, so it is cancelled and
// the retry runs on a fresh conn without the watchdog.
func TestUploadStallWatchdogRetriesOnFreshConn(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 50 * time.Millisecond

	client := &stallingHTTPClient{stalls: 1}
	uploader := newUploader(client)
	markHostFast(uploader, "test-host:8080")

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://test-host:8080/1,deadbeef",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("upload should have retried on a fresh conn: %v", err)
	}
	if result.RetryCount != 1 || len(client.requestCtxs) != 2 {
		t.Fatalf("got retries=%d calls=%d, want one retry on a fresh conn", result.RetryCount, len(client.requestCtxs))
	}
	if err := client.requestCtxs[0].Err(); err == nil {
		t.Fatalf("first attempt context was not cancelled")
	}
}

// readStallHTTPClient answers with a response body that never delivers until
// the request ctx is cancelled — the conn that took the request then wedged on
// the read side.
type readStallHTTPClient struct {
	mu    sync.Mutex
	stall bool
}

func (c *readStallHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	c.mu.Lock()
	stall := c.stall
	c.stall = false
	c.mu.Unlock()
	var body io.ReadCloser = io.NopCloser(strings.NewReader(`{"size":1}`))
	if stall {
		body = &ctxBlockedBody{ctx: req.Context()}
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       body,
	}, nil
}

type ctxBlockedBody struct{ ctx context.Context }

func (b *ctxBlockedBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *ctxBlockedBody) Close() error { return nil }

// The watchdog stays armed through the response read: a conn that accepts the
// body then wedges on the reply is as wedged as one that never drains.
func TestUploadStallWatchdogCoversResponseRead(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 50 * time.Millisecond

	client := &readStallHTTPClient{stall: true}
	uploader := newUploader(client)
	markHostFast(uploader, "test-host:8080")

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://test-host:8080/1,deadbeef",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("upload should have retried past the stalled read: %v", err)
	}
	if result.RetryCount != 1 {
		t.Fatalf("got retries=%d, want one retry on a fresh conn", result.RetryCount)
	}
}

// rearmHTTPClient parks the first attempt on a conn that never drains and
// re-marks the host fast as it fails — a concurrent healthy upload can restore
// the mark while this upload is between attempts. The retry drains slowly, so
// a re-armed watchdog would cancel it too.
type rearmHTTPClient struct {
	uploader *Uploader
	calls    int32
	slowFor  time.Duration
}

func (c *rearmHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if atomic.AddInt32(&c.calls, 1) == 1 {
		<-req.Context().Done()
		markHostFast(c.uploader, req.URL.Host)
		return nil, req.Context().Err()
	}
	// Park past the watchdog floor like a real wedged conn: a re-armed
	// watchdog must cancel this attempt instead of the sleep masking it.
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-time.After(c.slowFor):
	}
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"size":1}`)),
	}, nil
}

// A re-proven host must not re-arm the watchdog for the retry: the conn it
// cancelled says nothing about the next conn, and re-arming could burn every
// remaining attempt.
func TestUploadStallWatchdogStaysDisarmedOnRetry(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 40 * time.Millisecond

	uploader := newUploader(nil)
	uploader.httpClient = &rearmHTTPClient{uploader: uploader, slowFor: 60 * time.Millisecond}
	markHostFast(uploader, "test-host:8080")

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://test-host:8080/1,deadbeef",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("retry should run unbounded once the watchdog has fired: %v", err)
	}
	if result.RetryCount != 1 {
		t.Fatalf("got retries=%d, want one retry", result.RetryCount)
	}
}

// The disarm is per upload: a caller reusing its UploadOption still gets an
// armed watchdog on the next call — a second stalled conn must cancel too.
func TestUploadStallWatchdogRearmsForNextUpload(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 40 * time.Millisecond

	client := &stallingHTTPClient{}
	uploader := newUploader(client)
	markHostFast(uploader, "test-host:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// IsInputCompressed keeps the body large enough to re-prove the host:
	// compressed near-zeros would send too few bytes to re-mark it.
	option := &UploadOption{UploadUrl: "http://test-host:8080/1,deadbeef", MaxAttempts: 2, IsInputCompressed: true}
	for i := 0; i < 2; i++ {
		client.mu.Lock()
		client.stalls = 1
		client.mu.Unlock()
		result, err := uploader.UploadData(ctx, make([]byte, 300*1024), option)
		if err != nil {
			t.Fatalf("upload %d should have retried on a fresh conn: %v", i, err)
		}
		if result.RetryCount != 1 {
			t.Fatalf("upload %d got retries=%d, want its own stall caught", i, result.RetryCount)
		}
	}
	if option.stallWatchdogDisarmed {
		t.Fatalf("a stall must not disarm the watchdog on the caller's option")
	}
}

// A real transport must drop the conn a cancelled request was riding: the
// retry has to dial again rather than land back on the parked conn. The first
// conn here wedges mid-reply — headers flush, then the body never arrives.
func TestUploadStallWatchdogDialsFreshConn(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 50 * time.Millisecond

	var requests, dials int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if atomic.AddInt32(&requests, 1) == 1 {
			w.WriteHeader(http.StatusCreated)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"size":1}`))
	}))
	defer srv.Close()

	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&dials, 1)
			return new(net.Dialer).DialContext(ctx, network, addr)
		},
	}
	defer tr.CloseIdleConnections()
	uploader := newUploader(&http.Client{Transport: tr})

	host := strings.TrimPrefix(srv.URL, "http://")
	markHostFast(uploader, host)

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   srv.URL + "/1,deadbeef",
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("upload should have retried on a fresh conn: %v", err)
	}
	if result.RetryCount != 1 || atomic.LoadInt32(&dials) != 2 {
		t.Fatalf("got retries=%d dials=%d, want one retry on a fresh conn", result.RetryCount, dials)
	}
}

// Once the body drains the wait is the server's commit: a write that takes
// longer than the stall floor to acknowledge must not be cancelled — the conn
// is healthy, the server is just busy.
func TestUploadStallWatchdogAllowsSlowCommit(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 30 * time.Millisecond

	client := &stallingHTTPClient{slowFor: 80 * time.Millisecond}
	uploader := newUploader(client)
	markHostFast(uploader, "test-host:8080")

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://test-host:8080/1,deadbeef",
		MaxAttempts: 1,
	})
	if err != nil || result.RetryCount != 0 {
		t.Fatalf("a slow commit must not be cancelled: result=%+v err=%v", result, err)
	}
}

// A fast rejection proves nothing about how the conn drains, so it must not
// mark the host fast and arm the watchdog for the next upload.
func TestUploadStallWatchdogIgnoresFailedWrites(t *testing.T) {
	uploader := newUploader(&rejectHTTPClient{})
	_, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://test-host:8080/1,deadbeef",
		MaxAttempts: 1,
	})
	if err == nil {
		t.Fatalf("rejected upload should fail")
	}
	if _, ok := uploader.fastUploadHosts.Load("test-host:8080"); ok {
		t.Fatalf("a failed upload must not mark the host fast")
	}
}

type rejectHTTPClient struct{}

func (c *rejectHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"error":"volume is read only"}`)),
	}, nil
}

// Without a recent fast upload the watchdog stays off: a chronically slow link
// is the link, not a conn, and must not get its uploads cancelled.
func TestUploadStallWatchdogKeepsUnprovenHost(t *testing.T) {
	defer func(orig time.Duration) { uploadStallMinWait = orig }(uploadStallMinWait)
	uploadStallMinWait = 20 * time.Millisecond

	client := &stallingHTTPClient{slowFor: 60 * time.Millisecond}
	uploader := newUploader(client)

	result, err := uploader.UploadData(context.Background(), make([]byte, 300*1024), &UploadOption{
		UploadUrl:   "http://slow-host:8080/1,deadbeef",
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("unproven host upload should not be cancelled: %v", err)
	}
	if result.RetryCount != 0 || len(client.requestCtxs) != 1 {
		t.Fatalf("got retries=%d calls=%d, want a single unbounded attempt", result.RetryCount, len(client.requestCtxs))
	}
}

// Any sizeable completed upload marks its host at the rate it proved — slow
// or fast — because the stall budget scales off that rate. Only a too-small
// body fails to measure.
func TestUploadObservedBps(t *testing.T) {
	if bps := uploadObservedBps(4<<20, 4*time.Second); bps != 1<<20 {
		t.Fatalf("4MiB in 4s observed %d B/s, want 1MiB/s", bps)
	}
	if bps := uploadObservedBps(4<<20, 3*time.Hour); bps <= 0 {
		t.Fatalf("a slow upload still proves the rate it moved at")
	}
	if bps := uploadObservedBps(64*1024, time.Millisecond); bps != 0 {
		t.Fatalf("a small upload says nothing about host speed")
	}
	if bps := uploadObservedBps(4<<20, 0); bps != 0 {
		t.Fatalf("a zero-duration sample is not a measurement")
	}
}

// The stall budget scales with the rate the host last proved, so the same
// body gets little slack on a fast link and plenty on a slow one — no fixed
// floor fits both.
func TestUploadStallTimeoutScalesWithMeasuredRate(t *testing.T) {
	// 8MiB/s observed → a 64MiB body gets 8x the time it needs at that rate.
	if d := uploadStallTimeout(64<<20, 8<<20); d != 64*time.Second {
		t.Fatalf("stall budget = %v, want 64s", d)
	}
	// The same body on a 64MiB/s host gets 8s — under the floor, so clamped.
	if d := uploadStallTimeout(64<<20, 64<<20); d != uploadStallMinWait {
		t.Fatalf("stall budget = %v, want the min wait %v", d, uploadStallMinWait)
	}
	// A tiny body on a fast host cannot race its own conn.
	if d := uploadStallTimeout(1, 1<<30); d != uploadStallMinWait {
		t.Fatalf("stall budget = %v, want the min wait %v", d, uploadStallMinWait)
	}
}

type fsyncAssignClient struct {
	filer_pb.SeaweedFilerClient
	response *filer_pb.AssignVolumeResponse
}

func (c *fsyncAssignClient) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest, ...grpc.CallOption) (*filer_pb.AssignVolumeResponse, error) {
	return c.response, nil
}

func (c *fsyncAssignClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}
func (c *fsyncAssignClient) AdjustedUrl(loc *filer_pb.Location) string { return loc.GetUrl() }
func (c *fsyncAssignClient) GetDataCenter() string                     { return "" }

func TestUploadWithRetryAppendsFsyncWhenAssigned(t *testing.T) {
	for _, tc := range []struct {
		name  string
		proxy bool
	}{
		{name: "direct"},
		{name: "filer proxy", proxy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const fileID = "1,0123456789"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if gotFsync := r.URL.Query().Get("fsync"); gotFsync != "true" {
					t.Errorf("upload fsync = %q, want true when assignment requires fsync", gotFsync)
				}
				if tc.proxy {
					if got := r.URL.Query().Get("proxyChunkId"); got != fileID {
						t.Errorf("proxyChunkId = %q, want %q", got, fileID)
					}
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"size":3}`)
			}))
			defer server.Close()

			host := strings.TrimPrefix(server.URL, "http://")
			client := &fsyncAssignClient{response: &filer_pb.AssignVolumeResponse{
				FileId:   fileID,
				Location: &filer_pb.Location{Url: host},
				Fsync:    true,
			}}
			option := &UploadOption{}
			if tc.proxy {
				option.GenUploadUrl = GenUploadUrlProxy(host)
			}
			_, _, err, _ := newUploader(server.Client()).UploadWithRetry(client,
				&filer_pb.AssignVolumeRequest{Path: "/bucket/object"}, option, strings.NewReader("abc"))
			if err != nil {
				t.Fatalf("upload failed: %v", err)
			}
		})
	}
}

// hangingAssignSeaweedClient is a SeaweedFilerClient whose AssignVolume blocks
// until its context is done, so a test can prove UploadWithRetry bounds the RPC.
type hangingAssignSeaweedClient struct {
	filer_pb.SeaweedFilerClient
	sawDeadline chan bool
}

func (c *hangingAssignSeaweedClient) AssignVolume(ctx context.Context, in *filer_pb.AssignVolumeRequest, opts ...grpc.CallOption) (*filer_pb.AssignVolumeResponse, error) {
	_, ok := ctx.Deadline()
	// Non-blocking so a retry that calls this more than once can't wedge here.
	select {
	case c.sawDeadline <- ok:
	default:
	}
	<-ctx.Done() // simulate an overwhelmed filer that never answers
	return nil, ctx.Err()
}

type hangingAssignFilerClient struct {
	inner *hangingAssignSeaweedClient
}

func (c *hangingAssignFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c.inner)
}
func (c *hangingAssignFilerClient) AdjustedUrl(loc *filer_pb.Location) string { return loc.GetUrl() }
func (c *hangingAssignFilerClient) GetDataCenter() string                     { return "" }

// TestUploadWithRetryBoundsAssignVolume covers the case where an AssignVolume
// against an overwhelmed filer carried no deadline, so the upload (and the FUSE
// flush driving it) blocked forever. UploadWithRetry must give the RPC a
// deadline and return once it expires instead of hanging.
func TestUploadWithRetryBoundsAssignVolume(t *testing.T) {
	original := assignVolumeTimeout
	assignVolumeTimeout = 200 * time.Millisecond
	t.Cleanup(func() { assignVolumeTimeout = original })

	client := &hangingAssignFilerClient{inner: &hangingAssignSeaweedClient{sawDeadline: make(chan bool, 1)}}
	uploader := newUploader(&scriptedHTTPClient{responses: map[string][]scriptedHTTPResponse{}})

	done := make(chan error, 1)
	go func() {
		_, _, err, _ := uploader.UploadWithRetry(client,
			&filer_pb.AssignVolumeRequest{Count: 1},
			&UploadOption{Filename: "test.bin"},
			bytes.NewReader([]byte("abc")),
		)
		done <- err
	}()

	select {
	case sawDeadline := <-client.inner.sawDeadline:
		if !sawDeadline {
			t.Fatal("AssignVolume received a context with no deadline")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AssignVolume was never called")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected UploadWithRetry to fail once the assign deadline expired")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("UploadWithRetry hung well past the assign timeout")
	}
}

// TestUploadRewindsBodyOnConnectionReset reproduces issue #9139 follow-up:
// when the inner Do retry fires on a "connection reset" / "closed network"
// error, the *bytes.Reader body has already been consumed, so without an
// explicit rewind the second attempt sends 0 bytes and Go's transport surfaces
// "ContentLength=N with Body length 0".
func TestUploadRewindsBodyOnConnectionReset(t *testing.T) {
	for _, transient := range []string{
		"connection reset by peer",
		"use of closed network connection",
	} {
		t.Run(transient, func(t *testing.T) {
			client := &bodyCapturingHTTPClient{
				failFirst:   transient,
				successJSON: `{"name":"test.bin","size":42}`,
			}
			uploader := newUploader(client)

			payload := bytes.Repeat([]byte("payload-"), 256) // 2048 bytes
			_, err := uploader.UploadData(context.Background(), payload, &UploadOption{
				UploadUrl: "http://volume/1,abc",
				Filename:  "test.bin",
			})
			if err != nil {
				t.Fatalf("upload should succeed after inner retry, got %v", err)
			}
			if len(client.bodies) != 2 {
				t.Fatalf("expected 2 Do attempts, got %d", len(client.bodies))
			}
			if len(client.bodies[0]) == 0 {
				t.Fatalf("first attempt sent an empty body; test setup wrong")
			}
			if !bytes.Equal(client.bodies[0], client.bodies[1]) {
				t.Fatalf("retry body length=%d differs from first attempt length=%d (body was not rewound)",
					len(client.bodies[1]), len(client.bodies[0]))
			}
		})
	}
}

func TestReplicationUploadPreservesUncompressedNeedle(t *testing.T) {
	testCases := []struct {
		name    string
		payload []byte
	}{
		{
			name:    "unknown binary sampling",
			payload: bytes.Repeat([]byte{0}, 32*1024),
		},
		{
			name:    "receiver MIME detection",
			payload: bytes.Repeat([]byte("plain text content\n"), 2048),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var replicatedNeedle *needle.Needle
			var parseErr error

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				replicatedNeedle, _, _, parseErr = needle.CreateNeedleFromRequest(r, false, 1024*1024, &bytes.Buffer{})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"name":"payload.custom","size":32768}`)
			}))
			defer server.Close()

			uploader := newUploader(server.Client())
			_, err := uploader.UploadData(context.Background(), tc.payload, &UploadOption{
				UploadUrl:     server.URL + "/3,01637037d6?type=replicate",
				Filename:      "payload.custom",
				IsReplication: true,
				MaxAttempts:   1,
			})
			if err != nil {
				t.Fatalf("replication upload failed: %v", err)
			}
			if parseErr != nil {
				t.Fatalf("parse replicated upload: %v", parseErr)
			}
			if replicatedNeedle == nil {
				t.Fatal("replica did not receive a needle")
			}
			if replicatedNeedle.IsCompressed() {
				t.Fatal("replication changed an uncompressed needle to compressed")
			}
			if !bytes.Equal(replicatedNeedle.Data, tc.payload) {
				t.Fatalf("replicated data differs: got %d bytes, want %d", len(replicatedNeedle.Data), len(tc.payload))
			}
		})
	}
}

// deadReplicaClient simulates an unreachable replica: every request blocks for
// dialDelay (standing in for the TCP dial timeout) and then fails, unless the
// request context is cancelled first.
type deadReplicaClient struct {
	mu        sync.Mutex
	calls     int
	dialDelay time.Duration
}

func (c *deadReplicaClient) attempts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *deadReplicaClient) Do(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	select {
	case <-time.After(c.dialDelay):
		return nil, fmt.Errorf("dial tcp %s: i/o timeout", req.URL.Host)
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
}

// TestUploadToDeadReplicaRetriesThreeTimes reproduces the dead-replica upload
// stall: a synchronous replica write to an unreachable host pays the dial
// timeout three times over, so one dead replica stalls the caller for ~3x the
// dial timeout. In production dialDelay is the 10s dialer timeout, so the
// caller blocks ~30s before the failure surfaces.
func TestUploadToDeadReplicaRetriesThreeTimes(t *testing.T) {
	client := &deadReplicaClient{dialDelay: 100 * time.Millisecond}
	uploader := newUploader(client)

	start := time.Now()
	_, err := uploader.UploadData(context.Background(), []byte("hello"), &UploadOption{
		UploadUrl: "http://dead-replica:8080/3,01",
		Filename:  "test.bin",
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error uploading to a dead replica")
	}
	if got := client.attempts(); got != 3 {
		t.Fatalf("dial attempts = %d, want 3 (each attempt pays the full dial timeout)", got)
	}
	t.Logf("dead replica stalled the caller for %v across %d attempts", elapsed, client.attempts())
}

// TestUploadToDeadReplicaSingleAttempt verifies the fix: a synchronous replica
// write makes a single attempt (MaxAttempts=1), so a dead replica fails after
// one dial timeout instead of three. The outer client write still retries.
func TestUploadToDeadReplicaSingleAttempt(t *testing.T) {
	client := &deadReplicaClient{dialDelay: 100 * time.Millisecond}
	uploader := newUploader(client)

	_, err := uploader.UploadData(context.Background(), []byte("hello"), &UploadOption{
		UploadUrl:   "http://dead-replica:8080/3,01",
		Filename:    "test.bin",
		MaxAttempts: 1,
	})
	if err == nil {
		t.Fatal("expected an error uploading to a dead replica")
	}
	if got := client.attempts(); got != 1 {
		t.Fatalf("dial attempts = %d, want 1", got)
	}
}

// TestUploadRetryStopsOnContextCancel verifies the retry loop honors context
// cancellation so DistributedOperation can abandon a slow replica once the
// write outcome is already decided by another replica.
func TestUploadRetryStopsOnContextCancel(t *testing.T) {
	client := &deadReplicaClient{dialDelay: time.Hour} // would block ~forever per attempt
	uploader := newUploader(client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already decided elsewhere

	start := time.Now()
	_, err := uploader.UploadData(ctx, []byte("hello"), &UploadOption{
		UploadUrl: "http://dead-replica:8080/3,01",
		Filename:  "test.bin",
	})
	if err == nil {
		t.Fatal("expected a context error")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("upload did not abort on cancellation: took %v", elapsed)
	}
	if got := client.attempts(); got > 1 {
		t.Fatalf("dial attempts = %d, want at most 1 after cancellation", got)
	}
}

// A ciphered upload hands the volume server ciphertext, so it cannot echo a
// Content-MD5 back; without the caller's plaintext digest the chunk lands with
// no ETag and every ETag computed from those chunks comes out empty
// (issue #10968).
func TestCipherUploadKeepsPlaintextETag(t *testing.T) {
	payload := bytes.Repeat([]byte("encrypt me\n"), 4096)
	digest := md5.Sum(payload)
	wantMd5 := base64.StdEncoding.EncodeToString(digest[:])

	var storedNeedle *needle.Needle
	var sentContentMd5 string
	var parseErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sentContentMd5 = r.Header.Get("Content-MD5")
		storedNeedle, _, _, parseErr = needle.CreateNeedleFromRequest(r, false, 1024*1024, &bytes.Buffer{})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"name":"payload","size":45056}`)
	}))
	defer server.Close()

	uploader := newUploader(server.Client())
	result, err := uploader.UploadData(context.Background(), payload, &UploadOption{
		UploadUrl:   server.URL + "/3,01637037d6",
		Cipher:      true,
		Md5:         wantMd5,
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("cipher upload failed: %v", err)
	}
	if parseErr != nil {
		t.Fatalf("parse ciphered upload: %v", parseErr)
	}
	if sentContentMd5 != "" {
		t.Fatalf("Content-MD5 %q sent to the volume server, which only sees ciphertext", sentContentMd5)
	}
	if result.ContentMd5 != wantMd5 {
		t.Fatalf("chunk ETag = %q, want the plaintext digest %q", result.ContentMd5, wantMd5)
	}
	if bytes.Equal(storedNeedle.Data, payload) {
		t.Fatal("volume server stored plaintext for a ciphered upload")
	}
	decrypted, err := util.Decrypt(storedNeedle.Data, util.CipherKey(result.CipherKey))
	if err != nil {
		t.Fatalf("decrypt stored needle: %v", err)
	}
	if result.Gzip > 0 {
		if decrypted, err = util.DecompressData(decrypted); err != nil {
			t.Fatalf("decompress stored needle: %v", err)
		}
	}
	if !bytes.Equal(decrypted, payload) {
		t.Fatalf("decrypted %d bytes, want %d", len(decrypted), len(payload))
	}
}

// A read-only rule is the filer's verdict, not an outage: callers that retry
// transient failures must still be able to tell it apart.
func TestUploadWithRetryKeepsReadOnlyVerdict(t *testing.T) {
	client := &fsyncAssignClient{response: &filer_pb.AssignVolumeResponse{
		Error:     "assign volume: read only: /unavailable",
		ErrorCode: filer_pb.FilerError_READ_ONLY,
	}}
	_, _, err, _ := newUploader(http.DefaultClient).UploadWithRetry(client,
		&filer_pb.AssignVolumeRequest{Path: "/unavailable/object"}, &UploadOption{}, strings.NewReader("abc"))
	if !errors.Is(err, filer_pb.ErrReadOnly) {
		t.Fatalf("UploadWithRetry error = %v, want it to wrap filer_pb.ErrReadOnly", err)
	}
}

// RetryForever waits out failures that clear. A read-only verdict does not
// clear, so the upload must return it instead of looping.
func TestUploadWithRetryForeverStopsOnReadOnlyVerdict(t *testing.T) {
	client := &fsyncAssignClient{response: &filer_pb.AssignVolumeResponse{
		Error:     "assign volume: read only: /unavailable",
		ErrorCode: filer_pb.FilerError_READ_ONLY,
	}}
	done := make(chan error, 1)
	go func() {
		_, _, err, _ := newUploader(http.DefaultClient).UploadWithRetry(client,
			&filer_pb.AssignVolumeRequest{Path: "/unavailable/object"}, &UploadOption{RetryForever: true}, strings.NewReader("abc"))
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, filer_pb.ErrReadOnly) {
			t.Fatalf("UploadWithRetry error = %v, want it to wrap filer_pb.ErrReadOnly", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RetryForever kept retrying a read-only verdict")
	}
}

func TestIsUploadOverloaded(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "429", err: &uploadStatusError{StatusCode: http.StatusTooManyRequests, err: errors.New("slow down")}, want: true},
		{name: "503", err: &uploadStatusError{StatusCode: http.StatusServiceUnavailable, err: errors.New("upload limit")}, want: true},
		{name: "wrapped 503", err: fmt.Errorf("upload chunk: %w", &uploadStatusError{StatusCode: http.StatusServiceUnavailable, err: errors.New("upload limit")}), want: true},
		{name: "500", err: &uploadStatusError{StatusCode: http.StatusInternalServerError, err: errors.New("disk full")}, want: false},
		{name: "no response", err: &uploadStatusError{StatusCode: 0, err: errors.New("dial tcp: refused")}, want: false},
		{name: "plain error", err: errors.New("503 service unavailable"), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUploadOverloaded(tc.err); got != tc.want {
				t.Fatalf("IsUploadOverloaded(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Only a request the stall watchdog cancelled before any response arrived is
// a stall; a dial failure has the same status 0 and is not.
func TestUploadStalledMatchesErrUploadStalled(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "stalled before a response", err: &uploadStatusError{StatusCode: 0, stalled: true, err: errors.New("context canceled")}, want: true},
		{name: "wrapped stall", err: fmt.Errorf("upload chunk: %w", &uploadStatusError{StatusCode: 0, stalled: true, err: errors.New("context canceled")}), want: true},
		{name: "dial failure", err: &uploadStatusError{StatusCode: 0, err: errors.New("dial tcp: refused")}, want: false},
		{name: "stalled reading a response", err: &uploadStatusError{StatusCode: http.StatusCreated, stalled: true, err: errors.New("context canceled")}, want: false},
		{name: "plain error", err: errors.New("stalled"), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(tc.err, ErrUploadStalled); got != tc.want {
				t.Fatalf("errors.Is(%v, ErrUploadStalled) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// An overloaded holder is not hammered again from the same caller while it
// holds the chunk buffer: the overload goes straight back to the one place
// that decides on a reassign. Other failures keep their same-URL retries.
func TestRetriedUploadDataDoesNotRetryOverload(t *testing.T) {
	testCases := []struct {
		name      string
		status    int
		wantPosts int32
	}{
		{name: "503 overload", status: http.StatusServiceUnavailable, wantPosts: 1},
		{name: "429 overload", status: http.StatusTooManyRequests, wantPosts: 1},
		{name: "500 keeps its retries", status: http.StatusInternalServerError, wantPosts: 3},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var posts int32
			holder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&posts, 1)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, `{"error":"volume server upload limit"}`)
			}))
			defer holder.Close()

			_, err := newUploader(http.DefaultClient).UploadData(context.Background(), []byte("hello"), &UploadOption{
				UploadUrl: holder.URL + "/3,01637037d6",
				Filename:  "test.bin",
			})
			if err == nil {
				t.Fatal("expected the upload to fail")
			}
			if got := atomic.LoadInt32(&posts); got != tc.wantPosts {
				t.Fatalf("holder saw %d POSTs, want %d", got, tc.wantPosts)
			}
		})
	}
}

// tricklingHTTPClient drains the request body chunk bytes per interval, the
// way a degraded link does, and answers 201 once it has it all. It honours the
// request ctx between reads, as a transport aborts a cancelled write. With
// resetFirstAfter set, the first call instead drains at once, holds that long
// and fails with a connection reset, which triggers the inner rewind retry.
type tricklingHTTPClient struct {
	chunk           int
	interval        time.Duration
	resetFirstAfter time.Duration
	calls           int32
}

func (c *tricklingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	defer req.Body.Close()
	if atomic.AddInt32(&c.calls, 1) == 1 && c.resetFirstAfter > 0 {
		_, _ = io.Copy(io.Discard, req.Body)
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(c.resetFirstAfter):
		}
		return nil, errors.New("write tcp: connection reset by peer")
	}
	buf := make([]byte, c.chunk)
	for {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(c.interval):
		}
		if _, err := io.ReadFull(req.Body, buf); err != nil {
			break
		}
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"size":1}`)),
	}, nil
}

// setUploadBodyMinWait shrinks the body deadline floor for one test. Tests
// that call it must not run in parallel.
func setUploadBodyMinWait(t *testing.T, d time.Duration) {
	orig := uploadBodyMinWait
	uploadBodyMinWait = d
	t.Cleanup(func() { uploadBodyMinWait = orig })
}

// incompressibleBytes returns n random bytes, so the upload is not gzipped
// down to a body too small to trickle.
func incompressibleBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestUploadBodyDeadlineScalesWithSize(t *testing.T) {
	if d := uploadBodyDeadline(0); d != uploadBodyMinWait {
		t.Fatalf("uploadBodyDeadline(0) = %v, want %v", d, uploadBodyMinWait)
	}
	if d := uploadBodyDeadline(10 << 20); d != uploadBodyMinWait+10*time.Second {
		t.Fatalf("uploadBodyDeadline(10MiB) = %v, want %v", d, uploadBodyMinWait+10*time.Second)
	}
}

// A host that never proved a rate gets no stall watchdog, but a body crawling
// far below the minimum rate is still cut, as a stall the caller can reassign.
func TestUploadBodyDeadlineCutsTrickleToUnprovenHost(t *testing.T) {
	setUploadBodyMinWait(t, 100*time.Millisecond)

	client := &tricklingHTTPClient{chunk: 1 << 10, interval: 50 * time.Millisecond}
	uploader := newUploader(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := uploader.UploadData(ctx, incompressibleBytes(t, 64<<10), &UploadOption{
		UploadUrl:    "http://slow-host:8080/1,deadbeef",
		MaxAttempts:  1,
		BodyDeadline: true,
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("a trickling body must be cut at the body deadline, got success after %v", elapsed)
	}
	if elapsed >= time.Second {
		t.Fatalf("body deadline cut the upload after %v, want under 1s", elapsed)
	}
	if !ShouldReassignUpload(err) {
		t.Fatalf("a body deadline cut must be reassignable: %v", err)
	}
	if !errors.Is(err, ErrUploadStalled) {
		t.Fatalf("a body deadline cut must read as a stall: %v", err)
	}
}

// slowCommitHTTPClient drains the request body at once, then takes commitFor
// to answer 201, giving up as a transport would if the request ctx is
// cancelled while it commits.
type slowCommitHTTPClient struct {
	commitFor time.Duration
}

func (c *slowCommitHTTPClient) Do(req *http.Request) (*http.Response, error) {
	_, _ = io.Copy(io.Discard, req.Body)
	_ = req.Body.Close()
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-time.After(c.commitFor):
	}
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"size":1}`)),
	}, nil
}

// Once the body is sent the wait is the server's commit, bounded by the
// transport's response header timeout, not by the body deadline.
func TestUploadBodyDeadlineAllowsSlowCommit(t *testing.T) {
	setUploadBodyMinWait(t, 100*time.Millisecond)

	client := &slowCommitHTTPClient{commitFor: 300 * time.Millisecond}
	uploader := newUploader(client)

	result, err := uploader.UploadData(context.Background(), incompressibleBytes(t, 64<<10), &UploadOption{
		UploadUrl:    "http://slow-host:8080/1,deadbeef",
		MaxAttempts:  1,
		BodyDeadline: true,
	})
	if err != nil || result.RetryCount != 0 {
		t.Fatalf("a slow commit after the body is sent must not be cut: result=%+v err=%v", result, err)
	}
}

// A host marked at a slow rate gets a stall budget eight times that slow, so
// the body deadline is what bounds it — and it leaves the mark alone, since
// only the watchdog blames a pooled conn.
func TestUploadBodyDeadlineCutsSlowProvenHost(t *testing.T) {
	setUploadBodyMinWait(t, 100*time.Millisecond)

	const host = "slow-host:8080"
	const size = 300 << 10
	slowMark := fastHostMark{at: time.Now().UnixNano(), bps: 100 << 10}
	if budget := uploadStallTimeout(size, slowMark.bps); budget < 10*time.Second {
		t.Fatalf("test setup: watchdog budget %v must dwarf the body deadline", budget)
	}
	client := &tricklingHTTPClient{chunk: 1 << 10, interval: 50 * time.Millisecond}
	uploader := newUploader(client)
	uploader.fastUploadHosts.Store(host, slowMark)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := uploader.UploadData(ctx, incompressibleBytes(t, size), &UploadOption{
		UploadUrl:    "http://" + host + "/1,deadbeef",
		MaxAttempts:  1,
		BodyDeadline: true,
	})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrUploadStalled) {
		t.Fatalf("a slow host's trickle must be cut as a stall at the body deadline, got %v after %v", err, elapsed)
	}
	if elapsed >= time.Second {
		t.Fatalf("body deadline cut the upload after %v, want under 1s", elapsed)
	}
	if mark, ok := uploader.fastUploadHosts.Load(host); !ok || mark.(fastHostMark) != slowMark {
		t.Fatalf("the body deadline must not touch the host's rate mark, got %+v (present=%v)", mark, ok)
	}
}

// The inner connection-reset retry sends a fresh body, which gets its own
// deadline: the first attempt's deadline expired while its body was already
// sent, so without a re-arm the retry would trickle unbounded.
func TestUploadBodyDeadlineRearmsOnConnectionReset(t *testing.T) {
	setUploadBodyMinWait(t, 50*time.Millisecond)

	client := &tricklingHTTPClient{chunk: 1 << 10, interval: 20 * time.Millisecond, resetFirstAfter: 200 * time.Millisecond}
	uploader := newUploader(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := uploader.UploadData(ctx, incompressibleBytes(t, 64<<10), &UploadOption{
		UploadUrl:    "http://slow-host:8080/1,deadbeef",
		MaxAttempts:  1,
		BodyDeadline: true,
	})
	elapsed := time.Since(start)
	if got := atomic.LoadInt32(&client.calls); got != 2 {
		t.Fatalf("Do calls = %d, want the reset plus its inner retry", got)
	}
	if !errors.Is(err, ErrUploadStalled) {
		t.Fatalf("the inner retry's trickle must be cut at its own deadline, got %v after %v", err, elapsed)
	}
	if elapsed >= time.Second {
		t.Fatalf("inner retry was cut after %v, want under 1s", elapsed)
	}
}

// The body deadline is opt-in: an upload that did not ask for it (mount,
// filer.sync, filer.copy, UploadWithRetry) keeps today's watchdog-only
// behaviour, so a body trickling far below the deadline's rate to an unproven
// host — where no watchdog arms — runs to completion.
func TestUploadWithoutBodyDeadlineIsNotCut(t *testing.T) {
	setUploadBodyMinWait(t, 20*time.Millisecond)

	client := &tricklingHTTPClient{chunk: 1 << 10, interval: 10 * time.Millisecond}
	uploader := newUploader(client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	result, err := uploader.UploadData(ctx, incompressibleBytes(t, 16<<10), &UploadOption{
		UploadUrl:   "http://slow-host:8080/1,deadbeef",
		MaxAttempts: 1,
	})
	elapsed := time.Since(start)
	if err != nil || result.RetryCount != 0 {
		t.Fatalf("an opted-out trickle must not be cut: result=%+v err=%v after %v", result, err, elapsed)
	}
	if deadline := uploadBodyDeadline(16 << 10); elapsed <= deadline {
		t.Fatalf("test setup: the trickle took %v, must outlast the %v deadline it would get opted in", elapsed, deadline)
	}
}

// The gateway fan-out writes each holder through retriedUploadData with its
// default attempts. A body deadline cut is back-pressure, not a wedged conn:
// it goes straight back to the chunk loop, which decides on one reassign, so
// a trickling holder is written once per assignment. A watchdog cut keeps its
// same-holder retry on a fresh conn, and the body deadline then bounds that
// retry — still ending as a reassignable stall (S3 SlowDown).
func TestUploadBodyDeadlineStallsEveryFanOutAttempt(t *testing.T) {
	setUploadBodyMinWait(t, 50*time.Millisecond)
	origStall := uploadStallMinWait
	uploadStallMinWait = 20 * time.Millisecond
	t.Cleanup(func() { uploadStallMinWait = origStall })

	testCases := []struct {
		name      string
		proven    bool // the host carries a fast mark, so the watchdog arms first
		wantCalls int32
	}{
		{name: "unproven holder: one call, cut at the deadline", proven: false, wantCalls: 1},
		{name: "proven holder: watchdog cut retried once, then cut at the deadline", proven: true, wantCalls: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			const host = "holder:8080"
			client := &tricklingHTTPClient{chunk: 1 << 10, interval: 20 * time.Millisecond}
			uploader := newUploader(client)
			if tc.proven {
				markHostFast(uploader, host)
			}
			option := &UploadOption{UploadUrl: fmt.Sprintf("http://%s/3,01637037d6?type=replicate&%s=%s", host, ReplicateSourceParam, ReplicateSourceClient), BodyDeadline: true}
			prepared, err := prepareUploadData(incompressibleBytes(t, 64<<10), option)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			start := time.Now()
			_, err = uploader.retriedUploadData(ctx, prepared, option)
			elapsed := time.Since(start)
			if got := atomic.LoadInt32(&client.calls); got != tc.wantCalls {
				t.Fatalf("Do calls = %d, want %d", got, tc.wantCalls)
			}
			if !errors.Is(err, ErrUploadStalled) || !ShouldReassignUpload(err) || !IsUploadBackpressure(err) {
				t.Fatalf("a holder trickling to the deadline must end as a reassignable back-pressure stall, got %v after %v", err, elapsed)
			}
			if _, ok := uploader.fastUploadHosts.Load(host); ok {
				t.Fatalf("no fast mark may survive: the watchdog cut clears it and the deadline never sets one")
			}
			if elapsed >= 2*time.Second {
				t.Fatalf("cut attempts took %v, want well under 2s", elapsed)
			}
		})
	}
}

// Only a body deadline cut and an overload are back-pressure; a watchdog cut
// (one wedged conn) and other failures keep their retry policy.
func TestIsUploadBackpressure(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "503 overload", err: &uploadStatusError{StatusCode: http.StatusServiceUnavailable, err: errors.New("limit")}, want: true},
		{name: "429 overload", err: &uploadStatusError{StatusCode: http.StatusTooManyRequests, err: errors.New("limit")}, want: true},
		{name: "body deadline cut", err: &uploadStatusError{StatusCode: 0, stalled: true, deadlineCut: true, err: errors.New("context canceled")}, want: true},
		{name: "wrapped body deadline cut", err: fmt.Errorf("upload chunk: %w", &uploadStatusError{StatusCode: 0, stalled: true, deadlineCut: true, err: errors.New("context canceled")}), want: true},
		{name: "watchdog cut", err: &uploadStatusError{StatusCode: 0, stalled: true, err: errors.New("context canceled")}, want: false},
		{name: "500 fault", err: &uploadStatusError{StatusCode: http.StatusInternalServerError, err: errors.New("disk")}, want: false},
		{name: "plain error", err: errors.New("deadline"), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsUploadBackpressure(tc.err); got != tc.want {
				t.Fatalf("IsUploadBackpressure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
	// Both kinds of cut still read as a stall and stay reassignable.
	for _, err := range []error{
		&uploadStatusError{StatusCode: 0, stalled: true, deadlineCut: true, err: errors.New("context canceled")},
		&uploadStatusError{StatusCode: 0, stalled: true, err: errors.New("context canceled")},
	} {
		if !errors.Is(err, ErrUploadStalled) || !ShouldReassignUpload(err) {
			t.Fatalf("%+v must match ErrUploadStalled and be reassignable", err)
		}
	}
}
