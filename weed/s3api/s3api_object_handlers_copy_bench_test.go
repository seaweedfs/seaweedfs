package s3api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// benchEnv stands up a fake source volume (returns N bytes on GET) and a
// fake destination volume (parses an incoming multipart POST and discards
// the body, asserting the data arrived intact via SHA-256). It returns the
// URLs and an AssignVolumeResponse pointing at the destination.
type benchEnv struct {
	srcSrv  *httptest.Server
	dstSrv  *httptest.Server
	payload []byte
	dstHash [32]byte
	assign  *filer_pb.AssignVolumeResponse
}

func newBenchEnv(b *testing.B, payloadSize int) *benchEnv {
	b.Helper()
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i*31 + 7) // arbitrary repeatable pattern
	}
	wantHash := sha256.Sum256(payload)

	srcSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Honor Range header if present (UploadPartCopy passes one).
		start, end := int64(0), int64(len(payload)-1)
		if rng := r.Header.Get("Range"); rng != "" {
			var s, e int64
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &s, &e); err == nil {
				start, end = s, e
			}
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload[start : end+1])
	}))

	dstSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mr, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h := sha256.New()
		if _, err := io.Copy(h, part); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var got [32]byte
		copy(got[:], h.Sum(nil))
		if got != wantHash {
			http.Error(w, "hash mismatch", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"size":`+strconv.Itoa(payloadSize)+`}`)
	}))

	dstURL, _ := neturl(dstSrv.URL)
	assign := &filer_pb.AssignVolumeResponse{
		FileId: "1,benchmark",
		Location: &filer_pb.Location{
			Url:       dstURL,
			PublicUrl: dstURL,
		},
		Auth: "",
	}

	return &benchEnv{
		srcSrv:  srcSrv,
		dstSrv:  dstSrv,
		payload: payload,
		dstHash: wantHash,
		assign:  assign,
	}
}

func (e *benchEnv) close() {
	e.srcSrv.Close()
	e.dstSrv.Close()
}

// neturl extracts host:port from "http://host:port".
func neturl(rawURL string) (string, error) {
	const prefix = "http://"
	if len(rawURL) < len(prefix) || rawURL[:len(prefix)] != prefix {
		return "", fmt.Errorf("unexpected URL: %q", rawURL)
	}
	host := rawURL[len(prefix):]
	if _, _, err := net.SplitHostPort(host); err != nil {
		return "", err
	}
	return host, nil
}

// BenchmarkCopyChunk_Buffered measures the cost of the existing buffered
// chunk-copy path (downloadChunkData + uploadChunkData). The path
// allocates one chunk-sized []byte and one multipart-encoded buffer per
// call.
func BenchmarkCopyChunk_Buffered(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20, 64 << 20} {
		b.Run(humanByteName(size), func(b *testing.B) {
			util_http.InitGlobalHttpClient()
			env := newBenchEnv(b, size)
			defer env.close()

			s3a := &S3ApiServer{}
			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				data, err := s3a.downloadChunkData(env.srcSrv.URL, env.assign.FileId, 0, int64(size), nil)
				if err != nil {
					b.Fatalf("download: %v", err)
				}
				if err := s3a.uploadChunkData(data, env.assign, false); err != nil {
					b.Fatalf("upload: %v", err)
				}
			}
		})
	}
}

// BenchmarkCopyChunk_Streamed measures the cost of the io.Pipe streaming
// chunk-copy path. The path holds only the pipe's hand-off buffer (~32
// KiB) plus http transport buffers per call, regardless of chunk size.
func BenchmarkCopyChunk_Streamed(b *testing.B) {
	for _, size := range []int{1 << 20, 8 << 20, 64 << 20} {
		b.Run(humanByteName(size), func(b *testing.B) {
			util_http.InitGlobalHttpClient()
			env := newBenchEnv(b, size)
			defer env.close()

			s3a := &S3ApiServer{}
			b.ResetTimer()
			b.SetBytes(int64(size))
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := s3a.streamCopyChunkRange(context.Background(),
					env.srcSrv.URL, env.assign.FileId, 0, int64(size),
					env.assign, false); err != nil {
					b.Fatalf("stream: %v", err)
				}
			}
		})
	}
}

func humanByteName(n int) string {
	switch {
	case n >= 1<<30:
		return strconv.Itoa(n>>30) + "GiB"
	case n >= 1<<20:
		return strconv.Itoa(n>>20) + "MiB"
	case n >= 1<<10:
		return strconv.Itoa(n>>10) + "KiB"
	default:
		return strconv.Itoa(n) + "B"
	}
}

// Compile-time sanity: ensure the multipart library version we depend on
// is the one whose framing we mirror in streamCopyChunkRange.
var _ = multipart.NewWriter
