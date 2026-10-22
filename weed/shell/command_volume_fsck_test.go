package shell

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestVolumeFsckCanPurgeDirectory(t *testing.T) {
	testCases := []struct {
		scopedFilerPath string
		dir             util.FullPath
		expected        bool
	}{
		{"/", "/orphan/dir/deep", true},
		{"/", "/orphan", true},
		{"/", "/", false},
		{"/", "/buckets", false},
		{"/", "/buckets/bucket1", false},
		{"/", "/buckets/bucket1/dir", true},
		{"/buckets/bucket1", "/buckets/bucket1", false},
		{"/buckets/bucket1", "/buckets/bucket1/dir", true},
		{"/buckets/bucket1", "/buckets/bucket11/dir", false},
		{"/buckets/bucket1", "/orphan/dir", false},
	}
	for _, tc := range testCases {
		c := &commandVolumeFsck{bucketsPath: "/buckets", scopedFilerPath: tc.scopedFilerPath}
		if actual := c.canPurgeDirectory(tc.dir); actual != tc.expected {
			t.Errorf("scope %s: canPurgeDirectory(%s) = %v, expected %v", tc.scopedFilerPath, tc.dir, actual, tc.expected)
		}
	}
}

func TestVolumeFsckHttpDeleteRecordsParentDirectory(t *testing.T) {
	filer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/locked/") {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer filer.Close()

	verbose := false
	c := &commandVolumeFsck{
		verbose:    &verbose,
		writer:     io.Discard,
		purgedDirs: make(map[util.FullPath]struct{}),
		env:        &CommandEnv{option: &ShellOptions{FilerAddress: pb.ServerAddress(strings.TrimPrefix(filer.URL, "http://"))}},
	}

	c.httpDelete("/orphan/dir/deep/file.txt")
	c.httpDelete("/locked/dir/file.txt")

	if _, found := c.purgedDirs["/orphan/dir/deep"]; !found {
		t.Errorf("expected /orphan/dir/deep to be recorded, got %v", c.purgedDirs)
	}
	if _, found := c.purgedDirs["/locked/dir"]; found {
		t.Errorf("expected a rejected delete not to be recorded, got %v", c.purgedDirs)
	}
}

// stubFilerServer keeps a flat set of full paths and enforces the same delete
// rules as the filer: a directory with children is kept, and so is one modified
// after the caller looked it up. A path in writtenAfterLookup is modified by a
// client right after fsck reads it.
type stubFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries            map[string]*filer_pb.Entry
	writtenAfterLookup map[string]bool
	deleted            []string
}

func (s *stubFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	fullPath := string(util.NewFullPath(req.Directory, req.Name))
	entry := s.entries[fullPath]
	if entry != nil && s.writtenAfterLookup[fullPath] {
		seen := &filer_pb.Entry{IsDirectory: entry.IsDirectory, Attributes: &filer_pb.FuseAttributes{Mtime: entry.Attributes.GetMtime()}}
		entry.Attributes.Mtime++
		return &filer_pb.LookupDirectoryEntryResponse{Entry: seen}, nil
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (s *stubFilerServer) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	fullPath := string(util.NewFullPath(req.Directory, req.Name))
	entry, found := s.entries[fullPath]
	if !found {
		return &filer_pb.DeleteEntryResponse{Error: filer_pb.ErrNotFound.Error()}, nil
	}
	if req.IfNotModifiedAfter > 0 && entry.Attributes.GetMtime() > req.IfNotModifiedAfter {
		return &filer_pb.DeleteEntryResponse{}, nil
	}
	for path := range s.entries {
		if strings.HasPrefix(path, fullPath+"/") {
			return &filer_pb.DeleteEntryResponse{Error: filer.MsgFailDelNonEmptyFolder + ": " + fullPath}, nil
		}
	}
	delete(s.entries, fullPath)
	s.deleted = append(s.deleted, fullPath)
	return &filer_pb.DeleteEntryResponse{}, nil
}

// startStubFiler serves stub on a random localhost port and returns the shell
// environment whose filer client reaches it.
func startStubFiler(t *testing.T, stub *stubFilerServer) *CommandEnv {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, stub)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return &CommandEnv{option: &ShellOptions{
		FilerAddress:   pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port)),
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}
}

func TestVolumeFsckPurgeEmptyDirectories(t *testing.T) {
	settled := &filer_pb.FuseAttributes{Mtime: time.Now().Add(-time.Hour).Unix()}
	directory := &filer_pb.Entry{IsDirectory: true, Attributes: settled}
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		"/orphan":                 directory,
		"/orphan/dir":             directory,
		"/orphan/dir/deep":        directory,
		"/orphan/dir/wide":        directory,
		"/keep":                   directory,
		"/keep/file.txt":          {},
		"/buckets":                directory,
		"/buckets/bucket1":        directory,
		"/buckets/bucket1/folder": {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream", Mtime: settled.Mtime}},
		// no mtime to condition a delete on, so it stays
		"/nomtime":     {IsDirectory: true},
		"/nomtime/dir": {IsDirectory: true},
	}}

	verbose := false
	c := &commandVolumeFsck{
		verbose:         &verbose,
		writer:          io.Discard,
		bucketsPath:     "/buckets",
		scopedFilerPath: "/",
		purgedDirs: map[util.FullPath]struct{}{
			"/orphan/dir/deep":        {},
			"/orphan/dir/wide":        {},
			"/keep":                   {},
			"/buckets/bucket1/folder": {},
			"/nomtime/dir":            {},
		},
		env: startStubFiler(t, stub),
	}

	c.purgeEmptyDirectories()

	// the two emptied leaves, then the parent they shared, then its own parent
	expected := []string{"/orphan/dir/deep", "/orphan/dir/wide", "/orphan/dir", "/orphan"}
	sort.Strings(expected)
	sort.Strings(stub.deleted)
	if strings.Join(stub.deleted, ",") != strings.Join(expected, ",") {
		t.Errorf("deleted %v, expected %v", stub.deleted, expected)
	}
}

func TestVolumeFsckPurgeEmptyDirectoriesKeepsFreshDirectory(t *testing.T) {
	stub := &stubFilerServer{entries: map[string]*filer_pb.Entry{
		"/fresh":     {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}},
		"/fresh/dir": {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()}},
	}}

	verbose := false
	c := &commandVolumeFsck{
		verbose:         &verbose,
		writer:          io.Discard,
		bucketsPath:     "/buckets",
		scopedFilerPath: "/",
		purgedDirs:      map[util.FullPath]struct{}{"/fresh/dir": {}},
		env:             startStubFiler(t, stub),
	}

	c.purgeEmptyDirectories()

	if len(stub.deleted) > 0 {
		t.Errorf("deleted %v, expected a directory modified within the quiet period to be kept", stub.deleted)
	}
}

func TestVolumeFsckPurgeEmptyDirectoriesKeepsChangedDirectory(t *testing.T) {
	stub := &stubFilerServer{
		entries: map[string]*filer_pb.Entry{
			"/race":     {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 100}},
			"/race/dir": {IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 100}},
		},
		writtenAfterLookup: map[string]bool{"/race/dir": true},
	}

	verbose := false
	c := &commandVolumeFsck{
		verbose:         &verbose,
		writer:          io.Discard,
		bucketsPath:     "/buckets",
		scopedFilerPath: "/",
		purgedDirs:      map[util.FullPath]struct{}{"/race/dir": {}},
		env:             startStubFiler(t, stub),
	}

	c.purgeEmptyDirectories()

	if len(stub.deleted) > 0 {
		t.Errorf("deleted %v, expected a directory written to after the lookup to be kept", stub.deleted)
	}
}

// The purge pool may run at most `workers` deletes at once, and at least one
// even when the flag is set to zero.
func TestVolumeFsckPurgeWorkerLimit(t *testing.T) {
	gate := make(chan struct{})
	var inFlight, maxSeen, processed atomic.Int32
	queue, finish := startPurgeWorkers(3, func(util.FullPath) {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		<-gate
		inFlight.Add(-1)
		processed.Add(1)
	})

	for i := 0; i < 10; i++ {
		queue(purgeItem{path: util.FullPath(fmt.Sprintf("/p/%d", i))})
	}
	deadline := time.Now().Add(5 * time.Second)
	for inFlight.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := inFlight.Load(); got != 3 {
		t.Fatalf("in-flight deletes %d, want the 3-worker limit", got)
	}
	time.Sleep(10 * time.Millisecond)
	if got := maxSeen.Load(); got > 3 {
		t.Fatalf("saw %d concurrent deletes, want at most 3", got)
	}
	close(gate)
	finish()
	if got := processed.Load(); got != 10 {
		t.Fatalf("processed %d deletes, want all 10 drained", got)
	}
}

func TestVolumeFsckPurgeWorkersAtLeastOne(t *testing.T) {
	var processed atomic.Int32
	queue, finish := startPurgeWorkers(0, func(util.FullPath) { processed.Add(1) })
	for i := 0; i < 5; i++ {
		queue(purgeItem{path: util.FullPath(fmt.Sprintf("/p/%d", i))})
	}
	finish()
	if got := processed.Load(); got != 5 {
		t.Fatalf("processed %d deletes, want 5", got)
	}
}

// Deletes of paths sharing a hard link id must never overlap: the filer
// decrements a shared counter non-atomically, so concurrent deletes of the
// same id would both read N and write back N-1.
func TestVolumeFsckPurgeHardLinksSerialized(t *testing.T) {
	var inFlight, maxSeen, processed atomic.Int32
	gate := make(chan struct{})
	queue, finish := startPurgeWorkers(8, func(util.FullPath) {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		<-gate
		inFlight.Add(-1)
		processed.Add(1)
	})
	for i := 0; i < 8; i++ {
		queue(purgeItem{path: util.FullPath(fmt.Sprintf("/hl/%d", i)), hardLinkId: []byte{9, 9, 9}})
	}
	time.Sleep(50 * time.Millisecond)
	if got := maxSeen.Load(); got > 1 {
		t.Fatalf("saw %d concurrent deletes on one hard link id, want 1", got)
	}
	close(gate)
	finish()
	if got := processed.Load(); got != 8 {
		t.Fatalf("processed %d deletes, want 8", got)
	}
}

// Orphans found by the per-volume check go through the same purge queue as
// the missing-volume path, and the fid file keeps each entry's hard link id
// so same-id deletes still land on one worker.
func TestVolumeFsckCheckOneVolumeQueuesOrphansWithHardLinkId(t *testing.T) {
	tempDir := t.TempDir()
	var verbose bool
	var buf bytes.Buffer
	c := &commandVolumeFsck{tempFolder: tempDir, verbose: &verbose, writer: &buf}

	volumeId := uint32(7)
	db := needle_map.NewMemDb()
	if err := db.Set(types.NeedleId(11), types.ToOffset(16), types.Size(1)); err != nil {
		t.Fatalf("seed volume idx: %v", err)
	}
	if err := db.SaveToIdx(getVolumeFileIdFile(tempDir, "dn1", volumeId)); err != nil {
		t.Fatalf("write volume idx: %v", err)
	}
	db.Close()

	f, err := os.Create(getFilerFileIdFile(tempDir, volumeId))
	if err != nil {
		t.Fatalf("create fid file: %v", err)
	}
	writeFid := func(fileKey uint64, cookie uint32, p string, hardLinkId []byte) {
		buffer := make([]byte, readbufferSize)
		util.Uint64toBytes(buffer, fileKey)
		util.Uint32toBytes(buffer[8:], cookie)
		util.Uint32toBytes(buffer[12:], uint32(len(p)))
		if _, err := f.Write(buffer); err != nil {
			t.Fatalf("write fid header: %v", err)
		}
		if _, err := f.Write([]byte(p)); err != nil {
			t.Fatalf("write fid path: %v", err)
		}
		util.Uint32toBytes(buffer[:4], uint32(len(hardLinkId)))
		if _, err := f.Write(buffer[:4]); err != nil {
			t.Fatalf("write fid hard link id size: %v", err)
		}
		if _, err := f.Write(hardLinkId); err != nil {
			t.Fatalf("write fid hard link id: %v", err)
		}
	}
	writeFid(11, 1, "/keep/a", nil)
	writeFid(22, 2, "/orphan/b", []byte{1, 2, 3})
	writeFid(33, 3, "/orphan/c", nil)
	if err := f.Close(); err != nil {
		t.Fatalf("close fid file: %v", err)
	}

	var queued []purgeItem
	if err := c.oneVolumeFileIdsCheckOneVolume("dn1", volumeId, func(it purgeItem) {
		queued = append(queued, it)
	}); err != nil {
		t.Fatalf("check volume: %v", err)
	}
	if len(queued) != 2 {
		t.Fatalf("queued %d purges, want 2 (the two orphans): %v", len(queued), queued)
	}
	if queued[0].path != "/orphan/b" || queued[1].path != "/orphan/c" {
		t.Fatalf("queued paths %v, want /orphan/b then /orphan/c", queued)
	}
	if !bytes.Equal(queued[0].hardLinkId, []byte{1, 2, 3}) {
		t.Fatalf("queued hardLinkId %v, want [1 2 3]", queued[0].hardLinkId)
	}
	if queued[1].hardLinkId != nil {
		t.Fatalf("queued hardLinkId %v, want nil", queued[1].hardLinkId)
	}
}

// A .fid file cut short inside the hard link id field is corrupt input, not a
// record without a link id: the reader must fail instead of emitting the entry
// with a nil id.
func TestReadFilerFileIdFileRejectsTruncatedHardLinkId(t *testing.T) {
	tempDir := t.TempDir()
	var verbose bool
	c := &commandVolumeFsck{tempFolder: tempDir, verbose: &verbose, writer: io.Discard}

	f, err := os.Create(getFilerFileIdFile(tempDir, 7))
	if err != nil {
		t.Fatalf("create fid file: %v", err)
	}
	buffer := make([]byte, readbufferSize)
	util.Uint64toBytes(buffer, 11)
	util.Uint32toBytes(buffer[8:], 1)
	util.Uint32toBytes(buffer[12:], uint32(len("/orphan/b")))
	if _, err := f.Write(buffer); err != nil {
		t.Fatalf("write fid header: %v", err)
	}
	if _, err := f.Write([]byte("/orphan/b")); err != nil {
		t.Fatalf("write fid path: %v", err)
	}
	if _, err := f.Write([]byte{0, 0}); err != nil {
		t.Fatalf("write partial hard link id size: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close fid file: %v", err)
	}

	err = c.readFilerFileIdFile(7, func(types.NeedleId, util.FullPath, []byte) {
		t.Fatal("callback fired for a truncated record")
	})
	if err == nil {
		t.Fatal("expected an error for a truncated hard link id size")
	}
}
