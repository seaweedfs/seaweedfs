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
	mtime      int64
	volumeUrl  string
	resolvable []string
}

func (s *sourceFilerServer) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
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

	filerSrc := startSourceFiler(t, 5, "")

	fs := &FilerSink{
		filerSource: filerSrc,
		dir:         "/dst",
		executor:    util.NewLimitedConcurrentExecutor(1),
	}
	fs.SetUploader(operation.NewUploaderWithHttpClient(http.DefaultClient))

	done := make(chan error, 1)
	go func() {
		_, fetchErr := fs.fetchAndWrite(&filer_pb.FileChunk{FileId: "5617,01abc", Size: 10}, "/dst/x.bin", 5*int64(time.Second))
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
			"/backup/2026-07-10/x.bin", 5*int64(time.Second))
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
