package s3api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// TestNewPutChunkedUploadOption_ContentLocation pins that putToFiler's upload
// options carry the object path, including for UploadPart (object == "").
func TestNewPutChunkedUploadOption_ContentLocation(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	partReq := mux.SetURLVars(httptest.NewRequest("PUT", "/b/dir/key?partNumber=1&uploadId=u", nil),
		map[string]string{"bucket": "b", "object": "dir/a b"})

	opt := s3a.newPutChunkedUploadOption(partReq, "b", "", "", nil)
	want := map[string]string{"Seaweed-Content-Location": "/buckets/b/dir/a%20b"}
	if !reflect.DeepEqual(opt.PairMap, want) {
		t.Errorf("PairMap = %v, want %v", opt.PairMap, want)
	}
}

func TestObjectContentLocation(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	partReq := mux.SetURLVars(httptest.NewRequest("PUT", "/b/dir/key?partNumber=1&uploadId=u", nil),
		map[string]string{"bucket": "b", "object": "dir/key"})

	if got := s3a.objectContentLocation(partReq, "b", "obj"); got != "/buckets/b/obj" {
		t.Errorf("explicit object: got %q, want %q", got, "/buckets/b/obj")
	}
	if got := s3a.objectContentLocation(partReq, "b", ""); got != "/buckets/b/dir/key" {
		t.Errorf("UploadPart (object from mux vars): got %q, want %q", got, "/buckets/b/dir/key")
	}
	if got := s3a.objectContentLocation(httptest.NewRequest("POST", "/b", nil), "b", ""); got != "" {
		t.Errorf("no object anywhere: got %q, want empty", got)
	}
}

func TestEscapeFilerPath(t *testing.T) {
	cases := map[string]string{
		"/buckets/b/a/b.txt":    "/buckets/b/a/b.txt",
		"/buckets/b/a b/ключ\n": "/buckets/b/a%20b/%D0%BA%D0%BB%D1%8E%D1%87%0A",
		"/buckets/b/100%+?#":    "/buckets/b/100%25+%3F%23",
	}
	for in, want := range cases {
		got := escapeFilerPath(in)
		if got != want {
			t.Errorf("escapeFilerPath(%q) = %q, want %q", in, got, want)
		}
		back, err := url.PathUnescape(got)
		if err != nil || back != in {
			t.Errorf("url.PathUnescape(%q) = %q, %v; want %q", got, back, err, in)
		}
	}
}

func TestContentLocationPairMap(t *testing.T) {
	if got := contentLocationPairMap(""); got != nil {
		t.Errorf("contentLocationPairMap(\"\") = %v, want nil", got)
	}
	want := map[string]string{"Seaweed-Content-Location": "/buckets/b/k"}
	if got := contentLocationPairMap("/buckets/b/k"); !reflect.DeepEqual(got, want) {
		t.Errorf("contentLocationPairMap(\"/buckets/b/k\") = %v, want %v", got, want)
	}
}

// contentLocationCopyFiler assigns every copy to dst and serves the source
// volume from src.
type contentLocationCopyFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	src, dst string
}

func (f *contentLocationCopyFiler) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	return &filer_pb.AssignVolumeResponse{FileId: "3,01637037d6", Location: &filer_pb.Location{Url: f.dst}}, nil
}

func (f *contentLocationCopyFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	locations := map[string]*filer_pb.Locations{}
	for _, vid := range req.VolumeIds {
		locations[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.src}}}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

// TestCopySingleChunk_ContentLocation checks that a buffered chunk copy records
// the destination path, except for a chunk encrypted for the volume server,
// whose copied ciphertext must not get a plaintext path beside it.
func TestCopySingleChunk_ContentLocation(t *testing.T) {
	util_http.InitGlobalHttpClient()
	key := util.GenCipherKey()
	ciphertext, err := util.Encrypt([]byte("hello"), key)
	if err != nil {
		t.Fatal(err)
	}
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(ciphertext))
	}))
	defer src.Close()

	var mu sync.Mutex
	var got []string
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, r.Header.Get("Seaweed-Content-Location"))
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"size":5}`)
	}))
	defer dst.Close()

	s3a := newPutTestServer(t, startFakeFiler(t, &contentLocationCopyFiler{
		src: strings.TrimPrefix(src.URL, "http://"),
		dst: strings.TrimPrefix(dst.URL, "http://"),
	}))
	const dstPath = "/buckets/b/a b"
	cipherChunk := &filer_pb.FileChunk{FileId: "7,01637037d6", Size: 5, CipherKey: key}
	sseChunk := &filer_pb.FileChunk{FileId: "7,01637037d6", Size: 5, SseType: filer_pb.SSEType_SSE_S3}

	cases := []struct {
		name string
		copy func() error
		want string
	}{
		{"cipher chunk", func() error {
			_, err := s3a.copySingleChunk(cipherChunk, dstPath)
			return err
		}, ""},
		{"cipher chunk range", func() error {
			_, err := s3a.copySingleChunkForRange(cipherChunk, &filer_pb.FileChunk{Size: 5}, 0, 4, dstPath)
			return err
		}, ""},
		{"sse chunk", func() error {
			_, err := s3a.copySingleChunk(sseChunk, dstPath)
			return err
		}, "/buckets/b/a%20b"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mu.Lock()
			got = nil
			mu.Unlock()
			if err := c.copy(); err != nil {
				t.Fatalf("copy: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 || got[0] != c.want {
				t.Errorf("Seaweed-Content-Location = %q, want [%q]", got, c.want)
			}
		})
	}
}
