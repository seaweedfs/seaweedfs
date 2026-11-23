package mount

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
			reader, err := NewSSES3Reader(context.Background(), server.URL, "/custom/bucket")
			if err != nil {
				t.Fatal(err)
			}
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
	reader, err := NewSSES3Reader(context.Background(), server.URL, "/buckets/bucket")
	if err != nil {
		t.Fatal(err)
	}
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
				w.Header().Set("X-Amz-Version-Id", "null")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("abc"))
			}))
			defer server.Close()
			reader, err := NewSSES3Reader(context.Background(), server.URL, "/buckets/bucket")
			if err != nil {
				t.Fatal(err)
			}
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
