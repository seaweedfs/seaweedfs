package filersink

import (
	"errors"
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
	// Shorten retry backoff so a fail-fast test that briefly enters the retry
	// loop doesn't pay the production 1s+ wait.
	util.RetryWaitTime = 100 * time.Millisecond
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

// FilerSink must reject chunks whose read/upload byte count disagrees with the
// source filer metadata, instead of silently writing 0-byte needles with the
// source size in the destination metadata.
func TestValidateReplicatedChunkSize(t *testing.T) {
	const fid = "74,047d16a94aa581"

	tests := []struct {
		name             string
		expectedSize     uint64
		readSize         int
		uploadResultSize uint32
		wantErr          bool
	}{
		{
			name:             "healthy",
			expectedSize:     5171,
			readSize:         5171,
			uploadResultSize: 5171,
			wantErr:          false,
		},
		{
			name:             "legitimately empty file",
			expectedSize:     0,
			readSize:         0,
			uploadResultSize: 0,
			wantErr:          false,
		},
		{
			name:             "zero-byte read for non-empty source",
			expectedSize:     5171,
			readSize:         0,
			uploadResultSize: 0,
			wantErr:          true,
		},
		{
			name:             "short read",
			expectedSize:     5171,
			readSize:         100,
			uploadResultSize: 100,
			wantErr:          true,
		},
		{
			name:             "upload truncated",
			expectedSize:     5171,
			readSize:         5171,
			uploadResultSize: 0,
			wantErr:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chunk := &filer_pb.FileChunk{FileId: fid, Size: tc.expectedSize}

			readErr := validateReplicatedReadSize(chunk, tc.readSize)
			uploadErr := validateReplicatedUploadSize(chunk, tc.uploadResultSize)

			gotErr := readErr
			if gotErr == nil {
				gotErr = uploadErr
			}

			if tc.wantErr {
				if gotErr == nil {
					t.Fatalf("expected error, got nil (read=%d upload=%d expected=%d)",
						tc.readSize, tc.uploadResultSize, tc.expectedSize)
				}
				if !errors.Is(gotErr, errChunkSizeMismatch) {
					t.Fatalf("expected errChunkSizeMismatch, got %v", gotErr)
				}
				if !strings.Contains(gotErr.Error(), fid) {
					t.Fatalf("error %q does not mention chunk id %q", gotErr, fid)
				}
				return
			}
			if readErr != nil {
				t.Fatalf("unexpected read-size error: %v", readErr)
			}
			if uploadErr != nil {
				t.Fatalf("unexpected upload-size error: %v", uploadErr)
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
