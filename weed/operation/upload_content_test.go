package operation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
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

func testIsUploadRetryableAssignError(err error) bool {
	if err == nil {
		return false
	}
	for _, retryable := range uploadRetryableAssignErrList {
		if strings.Contains(err.Error(), retryable) {
			return true
		}
	}
	return false
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

func TestIsUploadRetryableAssignError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "transport", err: fmt.Errorf("transport is closing"), want: true},
		{name: "read only", err: fmt.Errorf("volume 1 is read only"), want: true},
		{name: "volume full", err: fmt.Errorf("failed to write to local disk: append to volume 1 size 0 actualSize 0: Volume Size 33555976 Exceeded 33554432"), want: true},
		{name: "other permanent", err: fmt.Errorf("mismatching cookie"), want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := testIsUploadRetryableAssignError(tc.err); got != tc.want {
				t.Fatalf("testIsUploadRetryableAssignError(%v) = %v, want %v", tc.err, got, tc.want)
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
	}, &UploadOption{Filename: "test.bin"}, func(host, fileId string) string {
		return "http://" + host + "/" + fileId
	}, []byte("abc"))

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
