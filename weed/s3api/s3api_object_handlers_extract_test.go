package s3api

import (
	"archive/zip"
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func makeZipArchive(t *testing.T, members map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range members {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func newExtractTestServer(t *testing.T) (*S3ApiServer, *ambiguousPutFiler) {
	t.Helper()
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}, apply: true}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
	t.Cleanup(s3a.iam.Shutdown)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	return s3a, filerImpl
}

func TestIsExtractZip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(r *http.Request)
		want  bool
	}{
		{"none", func(r *http.Request) {}, false},
		{"query", func(r *http.Request) {
			q := r.URL.Query()
			q.Set("extract", "zip")
			r.URL.RawQuery = q.Encode()
		}, true},
		{"seaweed header", func(r *http.Request) {
			r.Header.Set(s3_constants.SeaweedFSExtract, "zip")
		}, true},
		{"snowball header", func(r *http.Request) {
			r.Header.Set(s3_constants.AmzMetaSnowballAutoExtract, "true")
		}, true},
		{"snowball false", func(r *http.Request) {
			r.Header.Set(s3_constants.AmzMetaSnowballAutoExtract, "false")
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
			tc.setup(r)
			if got := isExtractZip(r); got != tc.want {
				t.Fatalf("isExtractZip = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExtractZipPutObject(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)

	body := makeZipArchive(t, map[string]string{
		"a.txt":     "hello",
		"sub/b.txt": "world!",
	})
	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip", bytes.NewReader(body))
	r.Header.Set(s3_constants.AmzMetaSnowballAutoExtract, "true")
	rec := httptest.NewRecorder()
	s3a.extractZipObject(rec, r, "b", "dir/archive.zip", r.Body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(s3_constants.SeaweedFSExtractedCount); got != "2" {
		t.Fatalf("extracted count = %q, want 2", got)
	}
	var result extractZipResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected member errors: %v", result.Errors)
	}
	for _, want := range []string{"/buckets/b/dir/a.txt", "/buckets/b/dir/sub/b.txt"} {
		entry, ok := filerImpl.entries[want]
		if !ok {
			t.Fatalf("member %s was not created; entries: %v", want, filerImpl.entries)
		}
		if entry.IsDirectory {
			t.Fatalf("member %s stored as directory", want)
		}
	}
	if _, ok := filerImpl.entries["/buckets/b/dir/archive.zip"]; ok {
		t.Fatal("archive object itself should not be stored")
	}
}

func TestExtractZipPutObjectArchiveChecksum(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)

	body := makeZipArchive(t, map[string]string{"a.txt": "hello"})
	md5sum := md5.Sum(body)

	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip", bytes.NewReader(body))
	r.Header.Set(s3_constants.SeaweedFSExtract, "zip")
	r.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5sum[:]))
	r.Header.Set(s3_constants.AmzChecksumSHA256, base64.StdEncoding.EncodeToString([]byte("not-the-sha256")))
	rec := httptest.NewRecorder()
	s3a.extractZipObject(rec, r, "b", "dir/archive.zip", r.Body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(filerImpl.entries) != 0 {
		t.Fatalf("entries created despite checksum mismatch: %v", filerImpl.entries)
	}
}

func TestExtractZipPutObjectGoodArchiveMd5(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)

	body := makeZipArchive(t, map[string]string{"a.txt": "hello"})
	md5sum := md5.Sum(body)

	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip", bytes.NewReader(body))
	r.Header.Set(s3_constants.SeaweedFSExtract, "zip")
	r.Header.Set("Content-Md5", base64.StdEncoding.EncodeToString(md5sum[:]))
	rec := httptest.NewRecorder()
	s3a.extractZipObject(rec, r, "b", "dir/archive.zip", r.Body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	if _, ok := filerImpl.entries["/buckets/b/dir/a.txt"]; !ok {
		t.Fatalf("member not created; entries: %v", filerImpl.entries)
	}
}

func TestExtractZipPutObjectNotAZip(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)

	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip", bytes.NewReader([]byte("not a zip")))
	r.Header.Set(s3_constants.SeaweedFSExtract, "zip")
	rec := httptest.NewRecorder()
	s3a.extractZipObject(rec, r, "b", "dir/archive.zip", r.Body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(filerImpl.entries) != 0 {
		t.Fatalf("entries created for invalid zip: %v", filerImpl.entries)
	}
}

func TestExtractZipPutObjectRejectsExplicitSSE(t *testing.T) {
	s3a, filerImpl := newExtractTestServer(t)

	body := makeZipArchive(t, map[string]string{"a.txt": "hello"})
	r := httptest.NewRequest(http.MethodPut, "/b/dir/archive.zip", bytes.NewReader(body))
	r.Header.Set(s3_constants.SeaweedFSExtract, "zip")
	r.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
	rec := httptest.NewRecorder()
	s3a.extractZipObject(rec, r, "b", "dir/archive.zip", r.Body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if len(filerImpl.entries) != 0 {
		t.Fatalf("entries created despite explicit SSE headers: %v", filerImpl.entries)
	}
}
