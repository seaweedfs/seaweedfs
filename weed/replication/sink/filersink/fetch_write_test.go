package filersink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
		name        string
		targetRoot  string
		sourceRoot  string
		targetPath  string
		incremental bool
		wantPath    util.FullPath
		wantOK      bool
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
			// incremental keys carry a date prefix that can't be reversed; unmappable
			name:        "incremental sink is unmappable",
			targetRoot:  "/target",
			sourceRoot:  "/source",
			targetPath:  "/target/2026-06-09/path/file.txt",
			incremental: true,
			wantPath:    "",
			wantOK:      false,
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
				dir:           tc.targetRoot,
				isIncremental: tc.incremental,
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
		gotFileId, gotErr := fs.fetchAndWrite(sourceChunk, "/dst/index.bin", 0, "")
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

	_, err := fs.replicateChunks(nil, sourceChunks, "/dst/index.bin", 0, "")
	if err == nil {
		t.Fatal("expected error from replicateChunks, got nil")
	}
	if !errors.Is(err, errChunkSizeMismatch) {
		t.Fatalf("error chain broken: errors.Is(err, errChunkSizeMismatch) = false; got %v", err)
	}
}

// sourceSupersedes decides whether to skip a stale replayed event. The replayed
// mtime is fixed; the table varies what the source lookup returned.
func TestSourceSupersedes(t *testing.T) {
	const eventNs int64 = 5_000_000_500 // the version being replayed (sec 5, ns 500)

	withMtime := func(sec int64, ns int32) *filer_pb.Entry {
		return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: sec, MtimeNs: ns}}
	}

	tests := []struct {
		name      string
		entry     *filer_pb.Entry
		lookupErr error
		want      bool
	}{
		// deleted on source: ErrNotFound in several shapes, all read as gone -> skip
		{"not-found sentinel", nil, filer_pb.ErrNotFound, true},
		{"not-found wrapped", nil, fmt.Errorf("lookup /x: %w", filer_pb.ErrNotFound), true},
		{"not-found as string (gRPC)", nil, errors.New("rpc error: " + filer_pb.ErrNotFound.Error()), true},
		{"nil entry, nil error", nil, nil, true},
		// transient lookup failure must NOT skip a possibly-live file
		{"network error", nil, errors.New("dial tcp: i/o timeout"), false},
		// live entry: compare full-ns mtime against the replayed version
		{"source strictly newer", withMtime(5, 600), nil, true},
		{"source same version", withMtime(5, 500), nil, false},
		{"source older (out-of-order replay)", withMtime(5, 400), nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sourceSupersedes("/source/x/config", tc.entry, tc.lookupErr, eventNs)
			if got != tc.want {
				t.Fatalf("sourceSupersedes = %v, want %v", got, tc.want)
			}
		})
	}
}

// An epoch/unset replayed mtime (0) must not block "gone" detection: a deleted
// source still reports superseded so the event is skipped instead of wedging on
// permanent retries. A live source stays not-superseded — no valid mtime to compare.
func TestSourceSupersedesEpochMtime(t *testing.T) {
	live := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 5, MtimeNs: 600}}
	if !sourceSupersedes("/source/x", nil, filer_pb.ErrNotFound, 0) {
		t.Fatal("epoch-mtime deleted source must be reported gone")
	}
	if sourceSupersedes("/source/x", live, nil, 0) {
		t.Fatal("epoch-mtime live source must not be reported superseded")
	}
}

// An incremental sink's dated target keys cannot be mapped back to a source
// path, so supersession is unverifiable: the gate must stop after the bounded
// attempts and propagate instead of spinning forever.
func TestManifestResolveRetryGateUnverifiableSupersessionBounded(t *testing.T) {
	fs := &FilerSink{isIncremental: true, dir: "/backup"}
	gate := fs.manifestResolveRetryGate("/backup/2026-07-10/buckets/x/f.pt", 123, "3,01abc", fs.newMissingSourceChunkGate("3,01abc"))
	resolveErr := errors.New("LookupFileId volume id 3: not found")
	for i := 1; i < maxUnverifiableResolveAttempts; i++ {
		if !gate(resolveErr) {
			t.Fatalf("attempt %d: gate must keep retrying before the bound", i)
		}
	}
	if gate(resolveErr) {
		t.Error("gate must propagate once the bound is reached with supersession unverifiable")
	}
}

// Non-transient resolve errors (corrupt manifest data, bad file ids) must
// propagate immediately — before any attempt counting or supersession
// mapping — so the configured metadata error policy applies, instead of
// retrying until the source is superseded.
func TestManifestResolveRetryGateNonTransientPropagates(t *testing.T) {
	fs := &FilerSink{dir: "/backup"}
	gate := fs.manifestResolveRetryGate("/backup/buckets/x/f.pt", 123, "3,01abc", fs.newMissingSourceChunkGate("3,01abc"))
	permanentErrs := []error{
		errors.New("fail to unmarshal manifest 3,01abc: proto: cannot parse invalid wire-format data"),
		errors.New("invalid fileId abc"),
	}
	for _, err := range permanentErrs {
		if gate(err) {
			t.Errorf("non-transient error must propagate immediately: %v", err)
		}
	}
	if !gate(errors.New("LookupFileId volume id 3: not found")) {
		t.Error("transient lookup race must keep retrying on the first attempt")
	}
	if isTransientResolveError(nil) {
		t.Error("isTransientResolveError(nil) must be false")
	}
}

// sourceFilerServer answers as a source filer that still holds the entry,
// unchanged, while its master locates the chunk's volume only for the volume ids
// in resolvable, which it serves from volumeUrl. With none listed it is a cluster
// that has vacuumed the volume away — or lost every replica of it.
type sourceFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mtime         int64
	volumeUrl     string
	resolvable    []string
	entry         *filer_pb.Entry
	lookupDelay   time.Duration
	lookupFn      func(dir, name string) (*filer_pb.Entry, error)
	volumeLookups *atomic.Int32
}

func (s *sourceFilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	if s.volumeLookups != nil {
		s.volumeLookups.Add(int32(len(req.VolumeIds)))
	}
	locationsMap := make(map[string]*filer_pb.Locations)
	for _, vid := range req.VolumeIds {
		if slices.Contains(s.resolvable, vid) {
			locationsMap[vid] = &filer_pb.Locations{
				Locations: []*filer_pb.Location{{Url: s.volumeUrl}},
			}
		}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locationsMap}, nil
}

func (s *sourceFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if s.lookupDelay > 0 {
		select {
		case <-time.After(s.lookupDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.lookupFn != nil {
		entry, err := s.lookupFn(req.Directory, req.Name)
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, err
	}
	if s.entry != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: s.entry}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
		Name:       req.Name,
		Attributes: &filer_pb.FuseAttributes{Mtime: s.mtime},
	}}, nil
}

// liveVolume serves a chunk read the way a healthy volume server would, so the
// sink's probe finds the source still producing data.
func liveVolume(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk bytes"))
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func startSourceFiler(t *testing.T, mtime int64, volumeUrl string, resolvable ...string) *source.FilerSource {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, &sourceFilerServer{mtime: mtime, volumeUrl: volumeUrl, resolvable: resolvable})
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	address := listener.Addr().String()
	filerSrc := &source.FilerSource{}
	if err := filerSrc.DoInitialize(address, address, "/src", false); err != nil {
		t.Fatalf("filerSource.DoInitialize: %v", err)
	}
	filerSrc.SetGrpcDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	return filerSrc
}

func startSourceFilerWithEntry(t *testing.T, entry *filer_pb.Entry, volumeUrl string, resolvable ...string) *source.FilerSource {
	t.Helper()
	return startSourceFilerServer(t, &sourceFilerServer{volumeUrl: volumeUrl, resolvable: resolvable, entry: entry})
}

func startSourceFilerServer(t *testing.T, impl *sourceFilerServer) *source.FilerSource {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, impl)
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	address := listener.Addr().String()
	filerSrc := &source.FilerSource{}
	if err := filerSrc.DoInitialize(address, address, "/src", false); err != nil {
		t.Fatalf("filerSource.DoInitialize: %v", err)
	}
	filerSrc.SetGrpcDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	return filerSrc
}

// recordingFilerServer is a destination filer that counts CreateEntry and
// UpdateEntry calls and answers entry lookups from a fixed entry.
type recordingFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	existing   *filer_pb.Entry
	creates    atomic.Int32
	updates    atomic.Int32
	renames    atomic.Int32
	lastCreate atomic.Pointer[filer_pb.Entry]
	lastUpdate atomic.Pointer[filer_pb.Entry]
}

func (s *recordingFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if s.existing == nil {
		return &filer_pb.LookupDirectoryEntryResponse{}, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: s.existing}, nil
}

func (s *recordingFilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.creates.Add(1)
	s.lastCreate.Store(req.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *recordingFilerServer) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.updates.Add(1)
	s.lastUpdate.Store(req.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (s *recordingFilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	s.renames.Add(1)
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func startRecordingFiler(t *testing.T, existing *filer_pb.Entry) (*recordingFilerServer, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	impl := &recordingFilerServer{existing: existing}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, impl)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return impl, listener.Addr().String()
}

// A chunk the source cluster cannot produce must stop being retried once the
// grace period is up. Before, the retry loop ran forever: the sync job never
// completed, so it held its slot and pinned the offset watermark at the event
// ahead of it, and filer.sync never checkpointed again.
func TestFetchAndWriteStopsOnMissingSourceChunk(t *testing.T) {
	prevRetryWaitTime, prevGrace := util.RetryWaitTime, missingSourceChunkGrace
	util.RetryWaitTime = 100 * time.Millisecond
	missingSourceChunkGrace = 500 * time.Millisecond
	t.Cleanup(func() {
		util.RetryWaitTime, missingSourceChunkGrace = prevRetryWaitTime, prevGrace
	})

	live := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	filerSrc := startSourceFilerWithEntry(t, live, "")

	fs := &FilerSink{
		filerSource: filerSrc,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	done := make(chan error, 1)
	go func() {
		_, fetchErr := fs.fetchAndWrite(&filer_pb.FileChunk{FileId: "5617,01abc", Size: 10}, "/dst/x.bin", 5*int64(time.Second), "")
		done <- fetchErr
	}()

	select {
	case fetchErr := <-done:
		if !errors.Is(fetchErr, errSourceChunkMissing) {
			t.Fatalf("expected errSourceChunkMissing, got %v", fetchErr)
		}
		if !errors.Is(fetchErr, source.ErrVolumeNotFound) {
			t.Fatalf("expected the underlying lookup failure to survive, got %v", fetchErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fetchAndWrite is still retrying a chunk the source cannot produce")
	}
}

// The gate waits out the shapes a restarting volume server produces, and only
// gives up once the source has been saying "gone" for the whole grace period.
// The wait belongs to the volume: a vacuumed one takes every file it held with
// it, and waiting it out per file would stall the sync for as long as it holds
// files.
func TestMissingSourceChunkGate(t *testing.T) {
	volumeGone := fmt.Errorf("read part 5617,01abc: %w", source.ErrVolumeNotFound)
	needleGone := fmt.Errorf("read part 5617,01abc: 404 Not Found: %w", util_http.ErrNotFound)

	fs := &FilerSink{}
	gate := fs.newMissingSourceChunkGate("5617,01abc")
	for _, err := range []error{volumeGone, needleGone} {
		if gate.isPermanent(err) {
			t.Fatalf("must keep retrying inside the grace period: %v", err)
		}
	}
	if got := gate.wrap(volumeGone); errors.Is(got, errSourceChunkMissing) {
		t.Fatalf("must not mark permanent while still retrying: %v", got)
	}

	// an unrelated failure is not the source answering "gone": it must not start a wait
	other := fs.newMissingSourceChunkGate("5618,01abc")
	other.isPermanent(errors.New("connection reset by peer"))
	if _, found := fs.missingVolumes.Load("5618"); found {
		t.Fatal("a non-missing error must not start the volume's wait")
	}

	// a second file in the same volume inherits that wait instead of restarting it
	fs.missingVolumes.Store("5617", time.Now().Add(-2*missingSourceChunkGrace))
	second := fs.newMissingSourceChunkGate("5617,02def")
	if !second.isPermanent(volumeGone) {
		t.Fatal("a later file must inherit the wait its volume already served")
	}
	wrapped := second.wrap(volumeGone)
	if !errors.Is(wrapped, errSourceChunkMissing) || !errors.Is(wrapped, source.ErrVolumeNotFound) {
		t.Fatalf("wrapped error lost a sentinel: %v", wrapped)
	}
	if second.wrap(nil) != nil {
		t.Fatal("a successful retry must not be turned into an error")
	}

	// the volume answering again clears the wait it had served
	fs.sourceServed("5617,09fff")
	if _, found := fs.missingVolumes.Load("5617"); found {
		t.Fatal("a served chunk must clear its volume's wait")
	}

	// a needle not-found answer keeps its own clock: the volume staying alive
	// for other needles must not restart the wait for the one that is gone
	fs.missingNeedles.Store("5619,01abc", time.Now().Add(-2*missingSourceChunkGrace))
	needleGate := fs.newMissingSourceChunkGate("5619,01abc")
	fs.sourceServed("5619,02def")
	if !needleGate.isPermanent(needleGone) {
		t.Fatal("a needle 404 must keep aging while its volume serves other needles")
	}

	// one needle's elapsed wait must not cut short a different needle's
	fresh := fs.newMissingSourceChunkGate("5619,03ghi")
	if fresh.isPermanent(needleGone) {
		t.Fatal("a different needle on the same volume must get its own wait")
	}

	// the needle itself serving again is the only thing that restarts its wait
	fs.sourceServed("5619,01abc")
	if needleGate.isPermanent(needleGone) {
		t.Fatal("a needle that serves again must restart its wait")
	}
}

// Once the source has lost a chunk for good, holding the sync offset for its
// entry only stops every later event from ever being checkpointed: the bytes are
// not coming back. Skip it loudly instead — but only while the source is
// demonstrably still serving other chunks, since a cluster that cannot locate
// anything is having an outage and skipping would drop live files wholesale.
func TestOnReplicateChunkErrorMissingSourceChunk(t *testing.T) {
	const probe = "5616,01abc"
	liveEntry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 5}}
	missing := fmt.Errorf("copy 5617,02def: %w: %w", errSourceChunkMissing, source.ErrVolumeNotFound)

	t.Run("source still serving", func(t *testing.T) {
		fs := &FilerSink{filerSource: startSourceFiler(t, 5, liveVolume(t), "5616"), dir: "/dst"}
		served := probe
		fs.lastServedFileId.Store(&served)

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, missing); err != nil {
			t.Fatalf("expected the entry to be skipped, got %v", err)
		}
	})

	t.Run("needle 404 needs no probe", func(t *testing.T) {
		fs := &FilerSink{filerSource: startSourceFiler(t, 5, ""), dir: "/dst"}
		notFound := fmt.Errorf("copy 5617,02def: %w: %w", errSourceChunkMissing, util_http.ErrNotFound)

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, notFound); err != nil {
			t.Fatalf("a 404 answered by the source is itself proof of liveness, got %v", err)
		}
	})

	t.Run("proxied 404 still needs the probe", func(t *testing.T) {
		proxied := &source.FilerSource{}
		if err := proxied.DoInitialize("127.0.0.1:1", "127.0.0.1:1", "/src", true); err != nil {
			t.Fatalf("DoInitialize: %v", err)
		}
		fs := &FilerSink{filerSource: proxied, dir: "/dst"}
		notFound := fmt.Errorf("copy 5617,02def: %w: %w", errSourceChunkMissing, util_http.ErrNotFound)

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, notFound); !errors.Is(err, errSourceChunkMissing) {
			t.Fatalf("a filer-proxied 404 also covers missing locations, so it must not skip without the probe, got %v", err)
		}
	})

	t.Run("source locating nothing", func(t *testing.T) {
		fs := &FilerSink{filerSource: startSourceFiler(t, 5, ""), dir: "/dst"}
		served := probe
		fs.lastServedFileId.Store(&served)

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, missing); !errors.Is(err, errSourceChunkMissing) {
			t.Fatalf("expected the error to be held for a retry, got %v", err)
		}
	})

	t.Run("probe locates but cannot be read", func(t *testing.T) {
		gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Not Found", http.StatusNotFound)
		}))
		defer gone.Close()
		fs := &FilerSink{
			filerSource: startSourceFiler(t, 5, strings.TrimPrefix(gone.URL, "http://"), "5616"),
			dir:         "/dst",
		}
		served := probe
		fs.lastServedFileId.Store(&served)

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, missing); !errors.Is(err, errSourceChunkMissing) {
			t.Fatalf("a volume the master lists but no server answers must not license a skip, got %v", err)
		}
	})

	t.Run("nothing served yet", func(t *testing.T) {
		fs := &FilerSink{filerSource: startSourceFiler(t, 5, liveVolume(t), "5616"), dir: "/dst"}

		if err := fs.onReplicateChunkError("/dst/x.bin", liveEntry, missing); !errors.Is(err, errSourceChunkMissing) {
			t.Fatalf("expected the error to be held with no probe to check, got %v", err)
		}
	})
}

// An incremental sink's dated target keys cannot be mapped back to a source path,
// so nothing here can tell a vacuumed chunk from a superseded one. Waiting out the
// grace period would stall every such entry for half an hour; propagate instead
// and let filer.backup decide with the event's real source key.
func TestFetchAndWriteMissingSourceChunkUnverifiableSupersession(t *testing.T) {
	prevRetryWaitTime := util.RetryWaitTime
	util.RetryWaitTime = 100 * time.Millisecond
	t.Cleanup(func() { util.RetryWaitTime = prevRetryWaitTime })

	fs := &FilerSink{
		filerSource:   startSourceFiler(t, 5, ""),
		dir:           "/backup",
		isIncremental: true,
		executor:      util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	done := make(chan error, 1)
	go func() {
		_, fetchErr := fs.fetchAndWrite(&filer_pb.FileChunk{FileId: "5617,01abc", Size: 10},
			"/backup/2026-07-10/x.bin", 5*int64(time.Second), "")
		done <- fetchErr
	}()

	select {
	case fetchErr := <-done:
		if !errors.Is(fetchErr, source.ErrVolumeNotFound) {
			t.Fatalf("expected the lookup failure to propagate, got %v", fetchErr)
		}
		if errors.Is(fetchErr, errSourceChunkMissing) {
			t.Fatalf("must not write the chunk off without checking supersession: %v", fetchErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fetchAndWrite waited out the grace period with supersession unverifiable")
	}
}

func TestSourceDroppedChunk(t *testing.T) {
	live := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,02def", Size: 10}},
	}
	fs := &FilerSink{filerSource: startSourceFilerWithEntry(t, live, ""), dir: "/dst"}

	if !fs.sourceDroppedChunk("/dst/x.bin", "5617,01abc", 5*int64(time.Second)) {
		t.Fatal("live entry no longer referencing the chunk must count as dropped")
	}
	if fs.sourceDroppedChunk("/dst/x.bin", "5617,02def", 5*int64(time.Second)) {
		t.Fatal("a chunk the live entry still lists must not count as dropped")
	}
	live.Attributes.Mtime = 4
	if fs.sourceDroppedChunk("/dst/x.bin", "5617,01abc", 5*int64(time.Second)) {
		t.Fatal("a live entry older than the replayed version cannot prove the chunk dropped")
	}
}

func TestSourceMovedOn(t *testing.T) {
	live := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,02def", Size: 10}},
	}
	replayed := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}

	fs := &FilerSink{filerSource: startSourceFilerWithEntry(t, live, ""), dir: "/dst"}
	if !fs.sourceMovedOn("/dst/x.bin", replayed) {
		t.Fatal("same-mtime entry with different chunks is moved on")
	}
	replayed.Chunks = live.Chunks
	if fs.sourceMovedOn("/dst/x.bin", replayed) {
		t.Fatal("identical chunks at equal mtime is the same version")
	}
	live.Attributes.Mtime = 6
	if !fs.sourceMovedOn("/dst/x.bin", replayed) {
		t.Fatal("newer mtime is still moved on")
	}
	live.Attributes.Mtime = 4
	replayed.Chunks = []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}}
	if fs.sourceMovedOn("/dst/x.bin", replayed) {
		t.Fatal("an older live entry cannot supersede the replayed version")
	}
}

// A source that accepts the lookup but never answers must not hang the
// supersession probe: the bounded context gives up and replication continues.
func TestSourceMovedOnStalledLookupContinues(t *testing.T) {
	prev := sourceLookupTimeout
	sourceLookupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { sourceLookupTimeout = prev })

	replayed := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	fs := &FilerSink{
		filerSource: startSourceFilerServer(t, &sourceFilerServer{lookupDelay: 30 * time.Second}),
		dir:         "/dst",
	}
	start := time.Now()
	if fs.sourceMovedOn("/dst/x.bin", replayed) {
		t.Fatal("an unanswered lookup cannot prove supersession")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("the supersession probe must give up at sourceLookupTimeout")
	}
}

// A replayed version the source rewrote in place must not wait out the
// missing-chunk grace period: the live entry no longer lists the chunk, so the
// loop stops on the first 404.
func TestFetchAndWriteStopsWhenSourceDroppedChunk(t *testing.T) {
	prevRetryWaitTime := util.RetryWaitTime
	util.RetryWaitTime = 100 * time.Millisecond
	t.Cleanup(func() { util.RetryWaitTime = prevRetryWaitTime })

	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer gone.Close()

	live := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,02def", Size: 10}},
	}
	filerSrc := startSourceFilerWithEntry(t, live, strings.TrimPrefix(gone.URL, "http://"), "5617")

	fs := &FilerSink{
		filerSource: filerSrc,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	done := make(chan error, 1)
	go func() {
		_, fetchErr := fs.fetchAndWrite(&filer_pb.FileChunk{FileId: "5617,01abc", Size: 10},
			"/dst/x.bin", 5*int64(time.Second), "")
		done <- fetchErr
	}()

	select {
	case fetchErr := <-done:
		if !errors.Is(fetchErr, util_http.ErrNotFound) {
			t.Fatalf("expected the 404 to propagate, got %v", fetchErr)
		}
		if errors.Is(fetchErr, errSourceChunkMissing) {
			t.Fatalf("an obsolete version must not wait for the give-up gate: %v", fetchErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fetchAndWrite kept retrying a chunk the live entry no longer lists")
	}
}

// A missing needle that lives inside a manifest is judged by whether the live
// entry still lists the manifest, not the child: the manifest id is the
// identity of its contents, so a live entry that kept it still needs the child.
// The child must keep retrying under the gate — its error must come back marked
// permanent by the clock, not returned raw by the dropped-chunk check.
func TestFetchAndWriteManifestChildKeepsRetrying(t *testing.T) {
	prevRetryWaitTime, prevGrace := util.RetryWaitTime, missingSourceChunkGrace
	util.RetryWaitTime = 100 * time.Millisecond
	missingSourceChunkGrace = 500 * time.Millisecond
	t.Cleanup(func() {
		util.RetryWaitTime, missingSourceChunkGrace = prevRetryWaitTime, prevGrace
	})

	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer gone.Close()

	live := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,09fed", Size: 10}},
	}
	filerSrc := startSourceFilerWithEntry(t, live, strings.TrimPrefix(gone.URL, "http://"), "5617")

	fs := &FilerSink{
		filerSource: filerSrc,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	done := make(chan error, 1)
	go func() {
		_, fetchErr := fs.fetchAndWrite(&filer_pb.FileChunk{FileId: "5617,01abc", Size: 10},
			"/dst/x.bin", 5*int64(time.Second), "5617,09fed")
		done <- fetchErr
	}()

	select {
	case fetchErr := <-done:
		if !errors.Is(fetchErr, errSourceChunkMissing) {
			t.Fatalf("a child of a still-listed manifest must expire via the gate, got %v", fetchErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a still-listed manifest's missing child must not retry past the gate")
	}
}

// A replayed version whose source entry is gone must not cost a single chunk
// read or destination write: on a busy directory every skipped fetch is one
// that would have 404'd or, worse, uploaded bytes a later event replaces.
func TestCreateEntrySkipsSupersededVersion(t *testing.T) {
	var volumeLookups atomic.Int32
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{
		lookupFn: func(dir, name string) (*filer_pb.Entry, error) {
			return nil, filer_pb.ErrNotFound
		},
		volumeLookups: &volumeLookups,
	})
	dst, dstAddr := startRecordingFiler(t, nil)

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	replayed := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	if err := fs.CreateEntry("/dst/x.bin", replayed, nil); err != nil {
		t.Fatalf("superseded replay should be skipped, not fail: %v", err)
	}
	if dst.creates.Load() != 0 {
		t.Fatal("a superseded version must not be committed to the destination")
	}
	if volumeLookups.Load() != 0 {
		t.Fatal("a superseded version must not fetch any chunks")
	}
}

// The same skip must not fire while the live source still matches the replayed
// version: replication proceeds and the chunk fetch is attempted.
func TestCreateEntryReplicatesCurrentVersion(t *testing.T) {
	var volumeLookups atomic.Int32
	replayed := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{entry: replayed, volumeLookups: &volumeLookups})
	dst, dstAddr := startRecordingFiler(t, nil)

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	// The source has no volume to serve the chunk, so replication fails — the
	// point is that it was attempted, not skipped as stale. A short grace lets
	// the give-up path return instead of holding the test for the full wait.
	prevGrace := missingSourceChunkGrace
	missingSourceChunkGrace = 200 * time.Millisecond
	t.Cleanup(func() { missingSourceChunkGrace = prevGrace })

	if err := fs.CreateEntry("/dst/x.bin", replayed, nil); err == nil {
		t.Fatal("replication of a chunk the source cannot serve must fail")
	}
	if volumeLookups.Load() == 0 {
		t.Fatal("a version the source still serves must be replicated, not skipped")
	}
	if dst.creates.Load() != 0 {
		t.Fatal("no destination write is expected while chunks cannot be fetched")
	}
}

// UpdateEntry takes the same pre-flight skip: a replayed version whose source
// entry is gone must not re-replicate or update the destination.
func TestUpdateEntrySkipsSupersededVersion(t *testing.T) {
	var volumeLookups atomic.Int32
	existing := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 4},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,00aaa", Size: 10}},
	}
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{
		lookupFn: func(dir, name string) (*filer_pb.Entry, error) {
			return nil, filer_pb.ErrNotFound
		},
		volumeLookups: &volumeLookups,
	})
	dst, dstAddr := startRecordingFiler(t, existing)

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	replayed := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	found, err := fs.UpdateEntry("/dst/x.bin", existing, "", replayed, true, nil)
	if err != nil {
		t.Fatalf("superseded replay should be skipped, not fail: %v", err)
	}
	if !found {
		t.Fatal("the existing entry was looked up; foundExistingEntry must report it")
	}
	if dst.updates.Load() != 0 {
		t.Fatal("a superseded version must not be committed to the destination")
	}
	if volumeLookups.Load() != 0 {
		t.Fatal("a superseded version must not fetch any chunks")
	}
}

// A superseded replay is replaced by the live version the source reports: the
// superseding event may never be delivered to this sink, so the live content
// must be committed now — still without fetching the replayed chunks.
func TestCreateEntrySyncsLiveVersion(t *testing.T) {
	var volumeLookups atomic.Int32
	live := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 6},
		Content:    []byte("live"),
	}
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{entry: live, volumeLookups: &volumeLookups})
	dst, dstAddr := startRecordingFiler(t, nil)

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	replayed := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	if err := fs.CreateEntry("/dst/x.bin", replayed, nil); err != nil {
		t.Fatalf("syncing the live version should succeed: %v", err)
	}
	if dst.creates.Load() != 1 {
		t.Fatal("the live version must be committed to the destination")
	}
	created := dst.lastCreate.Load()
	if created == nil || string(created.GetContent()) != "live" || created.GetAttributes().GetMtime() != 6 {
		t.Fatalf("the destination must get the live version, got %+v", created)
	}
	if volumeLookups.Load() != 0 {
		t.Fatal("the superseded version's chunks must not be fetched")
	}
}

// UpdateEntry applies the same substitution: a superseded replay commits the
// live source version through the normal update path.
func TestUpdateEntrySyncsLiveVersion(t *testing.T) {
	existing := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 4},
		Chunks:     []*filer_pb.FileChunk{{FileId: "4,00aaa", SourceFileId: "5617,00aaa", Size: 10}},
	}
	oldEntry := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 4},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,00aaa", Size: 10}},
	}
	live := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 6},
		Content:    []byte("live"),
	}
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{entry: live})
	dst, dstAddr := startRecordingFiler(t, existing)

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	replayed := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 5},
		Chunks:     []*filer_pb.FileChunk{{FileId: "5617,01abc", Size: 10}},
	}
	found, err := fs.UpdateEntry("/dst/x.bin", oldEntry, "", replayed, true, nil)
	if err != nil {
		t.Fatalf("syncing the live version should succeed: %v", err)
	}
	if !found {
		t.Fatal("the existing entry was looked up; foundExistingEntry must report it")
	}
	if dst.updates.Load() != 1 {
		t.Fatal("the live version must be committed to the destination")
	}
	updated := dst.lastUpdate.Load()
	if updated == nil || string(updated.GetContent()) != "live" || updated.GetAttributes().GetMtime() != 6 {
		t.Fatalf("the destination must get the live version, got %+v", updated)
	}
}

// A rename carries whatever version the destination happened to hold. When the
// preceding content update was skipped — the source had already moved to the
// new name — the moved entry keeps the old bytes. MoveEntry must reconcile the
// moved entry with the event's snapshot, like an in-place update.
func TestMoveEntryReconcilesMovedContent(t *testing.T) {
	existing := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 4},
		Chunks:     []*filer_pb.FileChunk{{FileId: "4,0v1d", SourceFileId: "5617,0v1", Size: 10}},
	}
	dst, dstAddr := startRecordingFiler(t, existing)

	live := &filer_pb.Entry{
		Name:       "y.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 6},
		Content:    []byte("v2"),
	}
	filerSrc := startSourceFilerServer(t, &sourceFilerServer{
		lookupFn: func(dir, name string) (*filer_pb.Entry, error) {
			if name == "y.bin" {
				return live, nil
			}
			return nil, filer_pb.ErrNotFound
		},
	})

	fs := &FilerSink{
		filerSource:    filerSrc,
		dir:            "/dst",
		grpcAddress:    dstAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		executor:       util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	oldEntry := &filer_pb.Entry{
		Name:       "x.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 6},
		Content:    []byte("v2"),
	}
	newEntry := &filer_pb.Entry{
		Name:       "y.bin",
		Attributes: &filer_pb.FuseAttributes{Mtime: 6},
		Content:    []byte("v2"),
	}
	if err := fs.MoveEntry("/dst/x.bin", "/dst/y.bin", oldEntry, newEntry, nil); err != nil {
		t.Fatalf("move with reconcile should succeed: %v", err)
	}
	if dst.renames.Load() != 1 {
		t.Fatal("the entry must be relocated with the native rename")
	}
	if dst.updates.Load() != 1 {
		t.Fatal("the moved entry must be reconciled with the event's snapshot")
	}
	updated := dst.lastUpdate.Load()
	if updated == nil || string(updated.GetContent()) != "v2" || updated.GetAttributes().GetMtime() != 6 {
		t.Fatalf("the moved entry must carry the rename's content, got %+v", updated)
	}
}
