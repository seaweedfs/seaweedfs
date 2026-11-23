package mount

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func gatewayReader(t *testing.T, bucketPath string, endpoints ...string) *SSES3Reader {
	t.Helper()
	reader, err := NewSSES3Reader(context.Background(), bucketPath, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader.endpoints = func(context.Context) ([]string, error) { return endpoints, nil }
	return reader
}

func TestSSES3GatewayReadAt(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "local-test-reader")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	for _, tc := range []struct {
		name        string
		status      int
		rangeHeader string
		etag        string
		wantOK      bool
	}{
		{"signed range", http.StatusPartialContent, "bytes 3-5/10", `"fixture"`, true},
		{"wrong offset", http.StatusPartialContent, "bytes 0-2/10", `"fixture"`, false},
		{"object replaced", http.StatusPartialContent, "bytes 3-5/10", `"replacement"`, false},
		{"read denied", http.StatusForbidden, "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/bucket/dir/a+b" || req.URL.Query().Get("versionId") != "v1" ||
					req.Header.Get("Range") != "bytes=3-5" || req.Header.Get("If-Match") != `"fixture"` ||
					!strings.HasPrefix(req.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
					t.Errorf("unexpected signed range request for %q", req.URL.Path)
				}
				if tc.status == http.StatusForbidden {
					w.WriteHeader(tc.status)
					return
				}
				w.Header().Set("ETag", tc.etag)
				w.Header().Set("Content-Range", tc.rangeHeader)
				w.Header().Set("Content-Length", "3")
				w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
				w.Header().Set("X-Amz-Version-Id", "v1")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("345"))
			}))
			defer server.Close()
			reader := gatewayReader(t, "/custom/bucket", server.URL)
			buf := make([]byte, 3)
			n, err := reader.readAt(context.Background(), "/custom/bucket/dir/a+b", `"fixture"`, "v1", 10, buf, 3)
			if tc.wantOK {
				if err != nil || n != 3 || string(buf) != "345" {
					t.Fatalf("read = %d, %v, bytes %q", n, err, buf)
				}
			} else if err == nil || n != 0 {
				t.Fatalf("invalid response returned %d bytes, error %v", n, err)
			}
			if tc.status == http.StatusForbidden && !errors.Is(err, errSSES3Permission) {
				t.Fatalf("read error = %v, want denied", err)
			}
		})
	}
}

func TestSSES3GatewayReadAtVersionFile(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "local-test-reader")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/bucket/dir/report.csv" || req.URL.Query().Get("versionId") != "v1" {
			t.Errorf("version file read sent %s?%s", req.URL.Path, req.URL.RawQuery)
		}
		w.Header().Set("ETag", `"fixture"`)
		w.Header().Set("Content-Range", "bytes 0-2/3")
		w.Header().Set("Content-Length", "3")
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		w.Header().Set("X-Amz-Version-Id", "v1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abc"))
	}))
	defer server.Close()
	reader := gatewayReader(t, "/buckets/bucket", server.URL)
	buf := make([]byte, 3)
	n, err := reader.readAt(context.Background(), "/buckets/bucket/dir/report.csv.versions/v_v1", `"fixture"`, "v1", 3, buf, 0)
	if err != nil || n != 3 || string(buf) != "abc" {
		t.Fatalf("read = %d, %v, bytes %q", n, err, buf)
	}
}

func TestSSES3GatewayReadAtNullVersion(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "local-test-reader")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	for _, tc := range []struct {
		name         string
		currentETag  string
		wantRequests []string
	}{
		{"current", `"fixture"`, []string{""}},
		{"superseded", `"newer"`, []string{"", "null"}},
		{"superseded by identical bytes", `"fixture"`, []string{"", "null"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				versionId := req.URL.Query().Get("versionId")
				got = append(got, versionId)
				if versionId == "" && req.Header.Get("If-Match") != tc.currentETag {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
				w.Header().Set("ETag", `"fixture"`)
				w.Header().Set("Content-Range", "bytes 0-2/3")
				w.Header().Set("Content-Length", "3")
				w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
				if versionId == "" && tc.name == "superseded by identical bytes" {
					w.Header().Set("X-Amz-Version-Id", "v2")
				} else {
					w.Header().Set("X-Amz-Version-Id", "null")
				}
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("abc"))
			}))
			defer server.Close()
			reader := gatewayReader(t, "/buckets/bucket", server.URL)
			buf := make([]byte, 3)
			n, err := reader.readAt(context.Background(), "/buckets/bucket/report.csv", `"fixture"`, "null", 3, buf, 0)
			if err != nil || n != 3 || string(buf) != "abc" {
				t.Fatalf("read = %d, %v, bytes %q", n, err, buf)
			}
			if strings.Join(got, ",") != strings.Join(tc.wantRequests, ",") {
				t.Fatalf("versionId requests = %q, want %q", got, tc.wantRequests)
			}
		})
	}
}

func TestSSES3GatewayChoice(t *testing.T) {
	reader := gatewayReader(t, "/buckets/bucket", "ftp://a:21", "http://b:8333", "https://user@c:8443", "https://d:8443")
	client, _, err := reader.gatewayClient(context.Background(), "")
	if err != nil || aws.ToString(client.Options().BaseEndpoint) != "https://d:8443" {
		t.Fatalf("gateway = %v, %v; want the valid HTTPS one", err, client)
	}

	reader = gatewayReader(t, "/buckets/bucket", "ftp://a:21")
	if _, _, err := reader.gatewayClient(context.Background(), ""); !errors.Is(err, errNoS3Gateway) {
		t.Fatalf("no usable gateway: err = %v", err)
	}

	reader = gatewayReader(t, "/buckets/bucket", "http://b:8333", "https://d:8443")
	if client, _, err := reader.gatewayClient(context.Background(), "https://d:8443"); !errors.Is(err, errNoS3Gateway) {
		t.Fatalf("retry after the only HTTPS gateway failed = %v, %v; want no plain-HTTP fallback", err, client)
	}

	for range 20 {
		reader = gatewayReader(t, "/buckets/bucket", "http://b:8333", "HTTPS://d:8443")
		if client, _, err := reader.gatewayClient(context.Background(), ""); err != nil || aws.ToString(client.Options().BaseEndpoint) != "HTTPS://d:8443" {
			t.Fatalf("mixed-case HTTPS scheme: gateway = %v, %v; want the HTTPS one", err, client)
		}
	}
	reader = gatewayReader(t, "/buckets/bucket", "http://b:8333", "HTTPS://d:8443")
	if client, _, err := reader.gatewayClient(context.Background(), "HTTPS://d:8443"); !errors.Is(err, errNoS3Gateway) {
		t.Fatalf("retry after the mixed-case HTTPS gateway failed = %v, %v; want no plain-HTTP fallback", err, client)
	}
}

func TestSSES3GatewayReadAtPicksAnotherGateway(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "local-test-reader")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	stopped := httptest.NewServer(http.NotFoundHandler())
	stopped.Close()
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("ETag", `"fixture"`)
		w.Header().Set("Content-Range", "bytes 0-2/3")
		w.Header().Set("Content-Length", "3")
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abc"))
	}))
	defer live.Close()

	buf := make([]byte, 3)
	for range 20 {
		reader := gatewayReader(t, "/buckets/bucket", stopped.URL, live.URL)
		reader.cfg.Retryer = func() aws.Retryer { return aws.NopRetryer{} }
		if n, err := reader.readAt(context.Background(), "/buckets/bucket/a", `"fixture"`, "", 3, buf, 0); err != nil || string(buf[:n]) != "abc" {
			t.Fatalf("read = %d, %v, bytes %q", n, err, buf)
		}
	}

	reader := gatewayReader(t, "/buckets/bucket", stopped.URL)
	reader.cfg.Retryer = func() aws.Retryer { return aws.NopRetryer{} }
	var sendErr *smithyhttp.RequestSendError
	if _, err := reader.readAt(context.Background(), "/buckets/bucket/a", `"fixture"`, "", 3, buf, 0); !errors.As(err, &sendErr) {
		t.Fatalf("only gateway unreachable: err = %v, want the send error", err)
	}
}
