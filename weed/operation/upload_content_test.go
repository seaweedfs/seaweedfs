package operation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

type scriptedHTTPResponse struct {
	status int
	body   string
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
		Header:     make(http.Header),
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
		{name: "400 bad request", err: &uploadStatusError{StatusCode: 400, err: fmt.Errorf("bad needle")}, want: false},
		{name: "401 unauthorized", err: &uploadStatusError{StatusCode: 401, err: fmt.Errorf("wrong jwt")}, want: false},
		{name: "assign rpc transport", err: fmt.Errorf("assign volume: transport is closing"), want: true},
		{name: "other permanent", err: fmt.Errorf("mismatching cookie"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldReassignUpload(tc.err); got != tc.want {
				t.Fatalf("shouldReassignUpload(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
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
	fileID, uploadResult, err := uploader.uploadWithRetryData(func() (string, string, security.EncodedJwt, error) {
		assignCalls++
		if assignCalls == 1 {
			return "1,first", "volume-a", "", nil
		}
		return "2,second", "volume-b", "", nil
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
	fileID, uploadResult, err := uploader.uploadWithRetryData(func() (string, string, security.EncodedJwt, error) {
		assignCalls++
		if assignCalls == 1 {
			return "1,first", "volume-a", "", nil
		}
		return "2,second", "volume-b", "", nil
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
