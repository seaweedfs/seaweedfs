package operation

import (
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
			if got := isUploadRetryableAssignError(tc.err); got != tc.want {
				t.Fatalf("isUploadRetryableAssignError(%v) = %v, want %v", tc.err, got, tc.want)
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
