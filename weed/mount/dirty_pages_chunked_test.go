package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Once a chunk upload fails its data is gone, so the handle must reject
// further writes instead of buffering the rest of the file — against a full
// cluster that turned cp into an hours-long crawl that only errored at close.
func TestChunkedDirtyPagesFailWritesAfterUploadError(t *testing.T) {
	fh := &FileHandle{wfs: &WFS{option: &Option{}}}
	pages := newMemoryChunkPages(fh, 1024)
	defer pages.Destroy()
	pages.hasWrites = true

	uploadErr := errors.New("assign volume failure: no writable volumes")
	pages.setLastError(uploadErr)

	if err := pages.AddPage(0, []byte("x"), true, 1); !errors.Is(err, uploadErr) {
		t.Fatalf("AddPage after upload failure = %v, want sticky %v", err, uploadErr)
	}
	if err := pages.FlushData(); !errors.Is(err, uploadErr) {
		t.Fatalf("FlushData after upload failure = %v, want wrapped %v", err, uploadErr)
	}
	// First failure wins; later errors must not mask the root cause.
	pages.setLastError(errors.New("later error"))
	if err := pages.LastError(); !errors.Is(err, uploadErr) {
		t.Fatalf("LastError = %v, want first error %v", err, uploadErr)
	}
}

type flakySave struct {
	failures []error
	calls    int
	bodies   []string
	onCall   func()
}

func (s *flakySave) saveDataAsChunk(util.FullPath) filer.SaveDataAsChunkFunctionType {
	return func(reader io.Reader, name string, offset int64, tsNs int64, _ uint64) (*filer_pb.FileChunk, error) {
		body, _ := io.ReadAll(reader)
		s.bodies = append(s.bodies, string(body))
		s.calls++
		if s.onCall != nil {
			s.onCall()
		}
		if s.calls <= len(s.failures) {
			return nil, s.failures[s.calls-1]
		}
		return &filer_pb.FileChunk{FileId: "1,01", Offset: offset, Size: uint64(len(body)), ModifiedTsNs: tsNs}, nil
	}
}

func newUploadTestPages(t *testing.T, writeback bool, save *flakySave) *ChunkedDirtyPages {
	t.Helper()
	wfs := newInvalidateTestWFS(t)
	wfs.option.WritebackCache = writeback
	fh := newFileHandle(wfs, 1, 2, &filer_pb.Entry{Name: "f", Attributes: &filer_pb.FuseAttributes{}})
	fh.RememberPath("/dir/f")
	pages := newMemoryChunkPages(fh, 1024)
	t.Cleanup(pages.Destroy)
	pages.saveDataAsChunk = save.saveDataAsChunk
	return pages
}

var errVolumeRefused = fmt.Errorf("upload data: %w", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})

// filerProxyOutage is what a filer proxy sends when it cannot reach the
// volume server: a 500 with an empty body.
func filerProxyOutage(t *testing.T) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	_, err := operation.NewUploaderWithHttpClient(server.Client()).UploadData(context.Background(), []byte("hello"),
		&operation.UploadOption{UploadUrl: server.URL, MaxAttempts: 1})
	if err == nil {
		t.Fatal("upload to the failing proxy succeeded")
	}
	return fmt.Errorf("upload data: %w", err)
}

// close() has already returned for a writeback handle, so a volume outage must
// not drop the buffered chunk: the same bytes go out again once it ends.
func TestChunkedDirtyPagesWritebackRetriesTransientUploadError(t *testing.T) {
	for name, outage := range map[string]error{
		"volume refused":  errVolumeRefused,
		"filer proxy 500": filerProxyOutage(t),
		// With no volume server left the assign keeps timing out, and the
		// filer client gives up after its own retries.
		"assign timed out": fmt.Errorf("upload data: filerGrpcAddress assign volume: %w",
			status.Error(codes.Unavailable, "filer 127.0.0.1:18888: call timed out")),
	} {
		t.Run(name, func(t *testing.T) {
			save := &flakySave{failures: []error{outage}}
			pages := newUploadTestPages(t, true, save)

			pages.saveChunkedFileIntervalToStorage(util.NewBytesReader([]byte("hello")), 0, 5, 1, func() {})

			if err := pages.LastError(); err != nil {
				t.Fatalf("LastError = %v, want the retried upload to succeed", err)
			}
			if save.calls != 2 || save.bodies[1] != "hello" {
				t.Fatalf("uploads = %d with bodies %q, want a second upload of the same bytes", save.calls, save.bodies)
			}
			if chunks := pages.fh.GetEntry().GetChunks(); len(chunks) != 1 || chunks[0].Size != 5 {
				t.Fatalf("entry chunks = %v, want the retried chunk", chunks)
			}
		})
	}
}

func TestChunkedDirtyPagesUploadErrorWithoutRetry(t *testing.T) {
	permanent := errors.New("upload data: assign volume failure: no writable volumes")
	readOnly := fmt.Errorf("upload data: assign volume failure path:\"/unavailable/f\": %w",
		filer_pb.AssignVolumeResponseError(&filer_pb.AssignVolumeResponse{
			Error:     "assign volume: read only: /unavailable",
			ErrorCode: filer_pb.FilerError_READ_ONLY,
		}))
	tests := []struct {
		name      string
		writeback bool
		err       error
		discard   func(pages *ChunkedDirtyPages)
		wantErr   error
	}{
		{name: "sync mode reports the outage to close", err: errVolumeRefused},
		{name: "writeback permanent error", writeback: true, err: permanent},
		{name: "writeback read-only path", writeback: true, err: readOnly},
		{name: "writeback pages discarded mid-upload", writeback: true, err: errVolumeRefused,
			discard: func(pages *ChunkedDirtyPages) { pages.Destroy() }},
		{name: "writeback pages discarded during the retry wait", writeback: true, err: errVolumeRefused,
			discard: func(pages *ChunkedDirtyPages) { time.AfterFunc(100*time.Millisecond, pages.Destroy) },
			wantErr: errDirtyPagesDiscarded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			save := &flakySave{failures: []error{tt.err}}
			pages := newUploadTestPages(t, tt.writeback, save)
			if tt.discard != nil {
				save.onCall = func() { tt.discard(pages) }
			}
			wantErr := tt.err
			if tt.wantErr != nil {
				wantErr = tt.wantErr
			}

			pages.saveChunkedFileIntervalToStorage(util.NewBytesReader([]byte("hello")), 0, 5, 1, func() {})

			if save.calls != 1 {
				t.Fatalf("uploads = %d, want 1", save.calls)
			}
			if err := pages.LastError(); !errors.Is(err, wantErr) {
				t.Fatalf("LastError = %v, want %v", err, wantErr)
			}
		})
	}
}

// A chunk whose pages were discarded while it uploaded must not reappear in
// the handle's entry, e.g. after a truncate replaced the content.
func TestChunkedDirtyPagesDiscardedUploadNotAttached(t *testing.T) {
	save := &flakySave{}
	pages := newUploadTestPages(t, true, save)
	save.onCall = pages.Destroy

	pages.saveChunkedFileIntervalToStorage(util.NewBytesReader([]byte("hello")), 0, 5, 1, func() {})

	if chunks := pages.fh.GetEntry().GetChunks(); len(chunks) != 0 {
		t.Fatalf("entry chunks = %v, want none from discarded pages", chunks)
	}
}
