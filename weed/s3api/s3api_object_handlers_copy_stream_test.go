package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestStreamCopyChunkRangeFsync(t *testing.T) {
	util_http.InitGlobalHttpClient()
	payload := bytes.Repeat([]byte("copy data"), 512)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer source.Close()

	for _, tc := range []struct {
		name      string
		fsync     bool
		wantQuery string
		status    int
		wantErr   bool
	}{
		{"without sync", false, "", http.StatusCreated, false},
		{"durable copy", true, "true", http.StatusCreated, false},
		{"durable write fails", true, "true", http.StatusInternalServerError, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("fsync"); got != tc.wantQuery {
					t.Errorf("upload fsync=%q, want %q", got, tc.wantQuery)
				}
				reader, err := r.MultipartReader()
				if err != nil {
					t.Error(err)
					http.Error(w, "invalid upload", http.StatusBadRequest)
					return
				}
				part, err := reader.NextPart()
				if err != nil {
					t.Error(err)
					http.Error(w, "missing data", http.StatusBadRequest)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil || !bytes.Equal(data, payload) {
					t.Errorf("copied payload differs: %v", err)
				}
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprintf(w, `{"size":%d}`, len(payload))
			}))
			defer destination.Close()
			assign := &filer_pb.AssignVolumeResponse{
				FileId:   "1,test",
				Location: &filer_pb.Location{Url: strings.TrimPrefix(destination.URL, "http://")},
				Fsync:    tc.fsync,
			}
			err := (&S3ApiServer{}).streamCopyChunkRange(context.Background(), []string{source.URL},
				assign.FileId, 0, int64(len(payload)), true, assign, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("stream copy error=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// A replica that cannot serve the chunk (it is missing the needle, errors out,
// or is down) must not fail the copy while another replica holds the data.
func TestStreamCopyChunkRangeFallsBackToNextReplica(t *testing.T) {
	util_http.InitGlobalHttpClient()
	payload := bytes.Repeat([]byte("copy data"), 512)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer healthy.Close()
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "disk error", http.StatusInternalServerError)
	}))
	defer failing.Close()
	down := httptest.NewServer(http.NotFoundHandler())
	downURL := down.URL
	down.Close()
	short := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)/2))
		_, _ = w.Write(payload[:len(payload)/2])
	}))
	defer short.Close()

	for _, tc := range []struct {
		name      string
		sources   []string
		wantErr   string
		wantWrite bool
	}{
		{"first replica misses the needle", []string{missing.URL, healthy.URL}, "", true},
		{"first replica errors", []string{failing.URL, healthy.URL}, "", true},
		{"first replica is down", []string{downURL, healthy.URL}, "", true},
		{"healthy replica is last of three", []string{missing.URL, downURL, healthy.URL}, "", true},
		{"first replica serves a shorter needle", []string{short.URL, healthy.URL}, "", true},
		{"only replica serves a shorter needle", []string{short.URL}, fmt.Sprintf("of %d bytes", len(payload)), false},
		{"no replica has the needle", []string{missing.URL, missing.URL}, "404", false},
		{"no source location", nil, "no source location", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var uploads int
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				uploads++
				reader, err := r.MultipartReader()
				if err != nil {
					t.Error(err)
					http.Error(w, "invalid upload", http.StatusBadRequest)
					return
				}
				part, err := reader.NextPart()
				if err != nil {
					t.Error(err)
					http.Error(w, "missing data", http.StatusBadRequest)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil || !bytes.Equal(data, payload) {
					t.Errorf("copied payload differs: %v", err)
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = fmt.Fprintf(w, `{"size":%d}`, len(payload))
			}))
			defer destination.Close()
			assign := &filer_pb.AssignVolumeResponse{
				FileId:   "1,test",
				Location: &filer_pb.Location{Url: strings.TrimPrefix(destination.URL, "http://")},
			}
			err := (&S3ApiServer{}).streamCopyChunkRange(context.Background(), tc.sources,
				assign.FileId, 0, int64(len(payload)), true, assign, nil)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("stream copy: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("stream copy error=%v, want it to mention %q", err, tc.wantErr)
			}
			if wrote := uploads > 0; wrote != tc.wantWrite {
				t.Fatalf("destination uploads=%d, wantWrite=%v", uploads, tc.wantWrite)
			}
		})
	}
}

// The buffered copy path (encrypted or rewritten chunks) reads the source the
// same way and must fall back to the next replica too, without mixing bytes
// from a replica that failed part-way.
func TestDownloadChunkDataFallsBackToNextReplica(t *testing.T) {
	util_http.InitGlobalHttpClient()
	payload := bytes.Repeat([]byte("chunk"), 1024)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(payload))
	}))
	defer healthy.Close()
	missing := httptest.NewServer(http.NotFoundHandler())
	defer missing.Close()
	// claims the full length, sends half, then drops the connection
	truncated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[:len(payload)/2])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
		}
	}))
	defer truncated.Close()
	// holds a shorter needle, so the range comes back clamped with a clean end
	short := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(payload[:len(payload)/2]))
	}))
	defer short.Close()

	s3a := &S3ApiServer{}
	for _, tc := range []struct {
		name    string
		sources []string
		wantErr bool
	}{
		{"first replica misses the needle", []string{missing.URL, healthy.URL}, false},
		{"first replica breaks off mid-chunk", []string{truncated.URL, healthy.URL}, false},
		{"first replica serves a shorter needle", []string{short.URL, healthy.URL}, false},
		{"only replica serves a shorter needle", []string{short.URL}, true},
		{"no replica has the needle", []string{missing.URL, missing.URL}, true},
		{"no source location", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := s3a.downloadChunkData(tc.sources, "1,test", 0, int64(len(payload)), nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("download error=%v, wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && !bytes.Equal(data, payload) {
				t.Fatalf("downloaded %d bytes that differ from the %d-byte chunk", len(data), len(payload))
			}
		})
	}
}

// An encrypted chunk is copied as ciphertext, so a replica whose needle is
// cut short or diverged must be skipped rather than stored as the copy.
func TestDownloadChunkDataSkipsUndecryptableReplica(t *testing.T) {
	util_http.InitGlobalHttpClient()
	plaintext := bytes.Repeat([]byte("secret chunk"), 512)
	key := util.GenCipherKey()
	ciphertext, err := util.Encrypt(plaintext, key)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := util.Encrypt(plaintext, util.GenCipherKey())
	if err != nil {
		t.Fatal(err)
	}
	serve := func(data []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
		}))
	}
	healthy := serve(ciphertext)
	defer healthy.Close()
	short := serve(ciphertext[:len(ciphertext)-7])
	defer short.Close()
	diverged := serve(foreign)
	defer diverged.Close()

	s3a := &S3ApiServer{}
	for _, tc := range []struct {
		name    string
		sources []string
		wantErr string
	}{
		{"first replica serves a shorter needle", []string{short.URL, healthy.URL}, ""},
		{"first replica serves a diverged needle", []string{diverged.URL, healthy.URL}, ""},
		{"only replica serves a shorter needle", []string{short.URL}, "do not decrypt"},
		{"no replica decrypts", []string{diverged.URL, short.URL}, "do not decrypt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := s3a.downloadChunkData(tc.sources, "1,test", 0, int64(len(plaintext)), key)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("download error=%v, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("download: %v", err)
			}
			if !bytes.Equal(data, ciphertext) {
				t.Fatalf("downloaded %d bytes that differ from the %d-byte ciphertext", len(data), len(ciphertext))
			}
		})
	}
}

func TestVolumeLocationUrls(t *testing.T) {
	got := volumeLocationUrls([]*filer_pb.Location{{Url: "a:8080"}, {Url: ""}, nil, {Url: "b:8080"}}, "3,01637037d6")
	want := []string{"http://a:8080/3,01637037d6", "http://b:8080/3,01637037d6"}
	if len(got) != len(want) {
		t.Fatalf("urls=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("urls=%v, want %v", got, want)
		}
	}
}
