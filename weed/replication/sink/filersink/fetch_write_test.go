package filersink

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestMain(m *testing.M) {
	util_http.InitGlobalHttpClient()
	os.Exit(m.Run())
}

func TestTargetPathToSourcePath(t *testing.T) {
	tests := []struct {
		name       string
		targetRoot string
		sourceRoot string
		targetPath string
		wantPath   util.FullPath
		wantOK     bool
	}{
		{
			name:       "basic mapping",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/target/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "trailing slash roots",
			targetRoot: "/target/",
			sourceRoot: "/source/",
			targetPath: "/target/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "root target mapping",
			targetRoot: "/",
			sourceRoot: "/source",
			targetPath: "/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "target root itself",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/target",
			wantPath:   "/source",
			wantOK:     true,
		},
		{
			name:       "outside target root",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/other/path/file.txt",
			wantPath:   "",
			wantOK:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &FilerSink{
				dir: tc.targetRoot,
				filerSource: &source.FilerSource{
					Dir: tc.sourceRoot,
				},
			}

			gotPath, ok := fs.targetPathToSourcePath(tc.targetPath)
			if ok != tc.wantOK {
				t.Fatalf("ok mismatch: got %v, want %v", ok, tc.wantOK)
			}
			if gotPath != tc.wantPath {
				t.Fatalf("path mismatch: got %q, want %q", gotPath, tc.wantPath)
			}
		})
	}
}

// FilerSink must reject chunks whose received byte count disagrees with the
// source filer metadata, instead of silently writing 0-byte needles with the
// source size in the destination metadata.
func TestValidateReplicatedChunkSize(t *testing.T) {
	const fid = "74,047d16a94aa581"

	tests := []struct {
		name         string
		expectedSize uint64
		readSize     int
		wantErr      bool
	}{
		{
			name:         "healthy",
			expectedSize: 5171,
			readSize:     5171,
			wantErr:      false,
		},
		{
			name:         "legitimately empty file",
			expectedSize: 0,
			readSize:     0,
			wantErr:      false,
		},
		{
			name:         "zero-byte read for non-empty source",
			expectedSize: 5171,
			readSize:     0,
			wantErr:      true,
		},
		{
			name:         "short read",
			expectedSize: 5171,
			readSize:     100,
			wantErr:      true,
		},
		{
			name:         "over-read (server returned more than metadata)",
			expectedSize: 5171,
			readSize:     8192,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := &filer_pb.FileChunk{FileId: fid, Size: tc.expectedSize}

			gotErr := validateReplicatedReadSize(chunk, tc.readSize)

			if tc.wantErr {
				if gotErr == nil {
					t.Fatalf("expected error, got nil (read=%d expected=%d)",
						tc.readSize, tc.expectedSize)
				}
				if !errors.Is(gotErr, errChunkSizeMismatch) {
					t.Fatalf("expected errChunkSizeMismatch, got %v", gotErr)
				}
				if !strings.Contains(gotErr.Error(), fid) {
					t.Fatalf("error %q does not mention chunk id %q", gotErr, fid)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("unexpected read-size error: %v", gotErr)
			}
		})
	}
}

// End-to-end regression :
// a source volume that responds 200 OK with Content-Length: 0
// for a chunk that filer metadata claims is 5171 bytes must be rejected
// by fetchAndWrite with a (non-retriable) size mismatch error,
// instead of being silently propagated to the destination as a 0-byte needle.
func TestFetchAndWriteRejectsZeroByteSource(t *testing.T) {
	const fid = "74,047d16a94aa581"
	const expectedSize uint64 = 5171

	// Shorten retry backoff so a fail-fast test that briefly enters the retry
	// loop doesn't pay the production 1s+ wait. Scoped to this test so any
	// future test in the package keeps the production constant.
	prevRetryWaitTime := util.RetryWaitTime
	util.RetryWaitTime = 100 * time.Millisecond
	t.Cleanup(func() { util.RetryWaitTime = prevRetryWaitTime })

	var hits atomic.Int32
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		// Intentionally write no body — mimic the buggy volume response.
	}))
	defer sourceServer.Close()

	serverAddr := strings.TrimPrefix(sourceServer.URL, "http://")

	filerSrc := &source.FilerSource{}
	if err := filerSrc.DoInitialize(serverAddr, serverAddr, "/", true); err != nil {
		t.Fatalf("filerSource.DoInitialize: %v", err)
	}

	fs := &FilerSink{
		filerSource: filerSrc,
		address:     serverAddr,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	sourceChunk := &filer_pb.FileChunk{
		FileId: fid,
		Size:   expectedSize,
	}

	done := make(chan struct {
		fileId string
		err    error
	}, 1)
	go func() {
		gotFileId, gotErr := fs.fetchAndWrite(sourceChunk, "/dst/index.bin", 0)
		done <- struct {
			fileId string
			err    error
		}{gotFileId, gotErr}
	}()

	select {
	case result := <-done:
		if result.err == nil {
			t.Fatalf("expected size mismatch error, got nil (fileId=%q)", result.fileId)
		}
		if !errors.Is(result.err, errChunkSizeMismatch) {
			t.Fatalf("expected errChunkSizeMismatch, got %v", result.err)
		}
		if !strings.Contains(result.err.Error(), "5171") {
			t.Fatalf("error %q does not mention expected size 5171", result.err)
		}
		if !strings.Contains(result.err.Error(), fid) {
			t.Fatalf("error %q does not mention chunk id %q", result.err, fid)
		}
		if h := hits.Load(); h != 1 {
			t.Fatalf("expected exactly 1 source hit (fail-fast), got %d", h)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("fetchAndWrite did not return within 5s (retry loop not aborted on size mismatch); hits=%d", hits.Load())
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "synthetic timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

// A transient network failure (interrupted read, idle-deadline timeout while
// the destination reads the upload body, reset/broken pipe) must route through
// the escalating backoff so an overloaded destination can recover instead of
// being hammered. The volume server returns its idle timeout as a JSON error
// string, so the text path matters as much as the net.Error interface.
func TestIsRetryableNetworkError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"eof", io.EOF, true},
		{"unexpected eof", io.ErrUnexpectedEOF, true},
		{"volume idle timeout json", fmt.Errorf("upload result: read tcp 10.0.0.1:8082->10.0.0.1:54848: i/o timeout"), true},
		{"volume idle timeout capitalized", fmt.Errorf("Upload result: read tcp 10.0.0.1:8082->10.0.0.1:54848: I/O timeout"), true},
		{"connection reset", fmt.Errorf("upload data: write tcp ...: connection reset by peer"), true},
		{"connection reset capitalized", fmt.Errorf("Connection reset by peer"), true},
		{"broken pipe", fmt.Errorf("broken pipe"), true},
		{"broken pipe capitalized", fmt.Errorf("Broken pipe"), true},
		{"net.Error timeout", fmt.Errorf("dial: %w", timeoutErr{}), true},
		{"size mismatch is permanent", errChunkSizeMismatch, false},
		{"unrelated error", errors.New("not found"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableNetworkError(tc.err); got != tc.want {
				t.Fatalf("isRetryableNetworkError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Lock in that the errChunkSizeMismatch sentinel survives the wrap in
// replicateOneChunk + pass-through in util.Retry, so filer_sink.go's
// errors.Is check actually fires.
func TestReplicateChunksPreservesSizeMismatchSentinel(t *testing.T) {
	const fid = "74,047d16a94aa581"
	const expectedSize uint64 = 5171

	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
	}))
	defer sourceServer.Close()

	serverAddr := strings.TrimPrefix(sourceServer.URL, "http://")

	filerSrc := &source.FilerSource{}
	if err := filerSrc.DoInitialize(serverAddr, serverAddr, "/", true); err != nil {
		t.Fatalf("filerSource.DoInitialize: %v", err)
	}

	fs := &FilerSink{
		filerSource: filerSrc,
		address:     serverAddr,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	sourceChunks := []*filer_pb.FileChunk{{FileId: fid, Size: expectedSize}}

	_, err := fs.replicateChunks(nil, sourceChunks, "/dst/index.bin", 0)
	if err == nil {
		t.Fatal("expected error from replicateChunks, got nil")
	}
	if !errors.Is(err, errChunkSizeMismatch) {
		t.Fatalf("error chain broken: errors.Is(err, errChunkSizeMismatch) = false; got %v", err)
	}
}
