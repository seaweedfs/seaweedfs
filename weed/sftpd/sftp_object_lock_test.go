package sftpd

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type lockTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
	deletes []*filer_pb.DeleteEntryRequest
	renames []*filer_pb.AtomicRenameEntryRequest
	updates int
	volume  string

	lastUpdate *filer_pb.UpdateEntryRequest
}

func (f *lockTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, "no entry is found")
}

func (f *lockTestFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *lockTestFiler) deleteRequests() []*filer_pb.DeleteEntryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*filer_pb.DeleteEntryRequest(nil), f.deletes...)
}

const lockTestFilerConf = `{"locations":[{"locationPrefix":"/buckets/worm/","worm":true},{"locationPrefix":"/buckets/worm-1h/","worm":true,"wormRetentionTimeSeconds":3600}]}`

func newLockTestServer(t *testing.T, entries map[string]*filer_pb.Entry) (*SftpServer, *lockTestFiler) {
	t.Helper()
	if _, ok := entries["/etc/seaweedfs/filer.conf"]; !ok {
		entries["/etc/seaweedfs/filer.conf"] = &filer_pb.Entry{Name: "filer.conf", Content: []byte(lockTestFilerConf), Attributes: &filer_pb.FuseAttributes{}}
	}
	for _, entry := range entries {
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{FileMode: 0644, Uid: 1000}
		}
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fake := &lockTestFiler{entries: entries}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, fake)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return &SftpServer{
		filerAddr:      pb.ServerAddress("127.0.0.1:0." + port),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		user: &user.User{
			Username:    "alice",
			Uid:         1000,
			Gid:         1000,
			HomeDir:     "/buckets",
			Permissions: map[string][]string{"/buckets": {PermAll}},
		},
	}, fake
}

func lockedEntry(name string, extended map[string]string) *filer_pb.Entry {
	entry := &filer_pb.Entry{Name: name, Extended: map[string][]byte{}}
	for k, v := range extended {
		entry.Extended[k] = []byte(v)
	}
	return entry
}

func retainUntil(d time.Duration) string {
	return strconv.FormatInt(time.Now().Add(d).Unix(), 10)
}

func TestRemoveRefusesProtectedEntries(t *testing.T) {
	wormAt := time.Now().Add(-2 * time.Hour).UnixNano()
	tests := []struct {
		name    string
		path    string
		entry   *filer_pb.Entry
		refused bool
	}{
		{"legal hold", "/buckets/b/f.versions/v_1", lockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), true},
		{"compliance retention", "/buckets/b/f.versions/v_2", lockedEntry("v_2", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeCompliance, s3_constants.ExtRetentionUntilDateKey: retainUntil(time.Hour)}), true},
		{"governance retention", "/buckets/b/f.versions/v_3", lockedEntry("v_3", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeGovernance, s3_constants.ExtRetentionUntilDateKey: retainUntil(time.Hour)}), true},
		{"expired retention", "/buckets/b/f.versions/v_4", lockedEntry("v_4", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeCompliance, s3_constants.ExtRetentionUntilDateKey: retainUntil(-time.Hour)}), false},
		{"legal hold off", "/buckets/b/f.versions/v_5", lockedEntry("v_5", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOff}), false},
		{"delete marker", "/buckets/b/f.versions/v_6", lockedEntry("v_6", map[string]string{s3_constants.ExtDeleteMarkerKey: "true", s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), false},
		{"worm", "/buckets/worm/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, true},
		{"worm retention elapsed", "/buckets/worm-1h/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, false},
		{"worm not enforced yet", "/buckets/worm/g", &filer_pb.Entry{Name: "g"}, false},
		{"no worm rule", "/buckets/b/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, fake := newLockTestServer(t, map[string]*filer_pb.Entry{tt.path: tt.entry})
			err := fs.removeEntry(tt.path)
			deletes := fake.deleteRequests()
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatalf("rm: got %v, want os.ErrPermission", err)
				}
				if len(deletes) != 0 {
					t.Fatalf("rm of a protected entry reached the filer")
				}
				return
			}
			if err != nil {
				t.Fatalf("rm: %v", err)
			}
			if len(deletes) != 1 {
				t.Fatalf("got %d DeleteEntry calls, want 1", len(deletes))
			}
			for _, key := range []string{s3_constants.ExtLegalHoldKey, s3_constants.ExtObjectLockModeKey, s3_constants.ExtRetentionUntilDateKey} {
				got, ok := deletes[0].ExpectedExtended[key]
				if !ok || string(got) != string(tt.entry.Extended[key]) {
					t.Fatalf("ExpectedExtended[%s] = %q (present %v), want %q", key, got, ok, tt.entry.Extended[key])
				}
			}
		})
	}
}

func TestRmdirRefusesLockedDirectoryMarker(t *testing.T) {
	marker := lockedEntry("dir", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn})
	marker.IsDirectory = true
	fs, fake := newLockTestServer(t, map[string]*filer_pb.Entry{"/buckets/b/dir": marker})
	if err := fs.removeDir("/buckets/b/dir"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("rmdir: got %v, want os.ErrPermission", err)
	}
	if len(fake.deleteRequests()) != 0 {
		t.Fatalf("rmdir of a locked directory marker reached the filer")
	}
}

func (f *lockTestFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renames = append(f.renames, req)
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (f *lockTestFiler) renameRequests() []*filer_pb.AtomicRenameEntryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*filer_pb.AtomicRenameEntryRequest(nil), f.renames...)
}

func TestRenameRefusesProtectedSourceAndTarget(t *testing.T) {
	held := func(name string) *filer_pb.Entry {
		return lockedEntry(name, map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn})
	}
	tests := []struct {
		name    string
		entries map[string]*filer_pb.Entry
		from    string
		to      string
		refused bool
	}{
		{"locked source", map[string]*filer_pb.Entry{"/buckets/b/f.versions/v_1": held("v_1")}, "/buckets/b/f.versions/v_1", "/buckets/b/moved", true},
		{"locked target", map[string]*filer_pb.Entry{"/buckets/b/x": {Name: "x"}, "/buckets/b/f.versions/v_1": held("v_1")}, "/buckets/b/x", "/buckets/b/f.versions/v_1", true},
		{"worm source", map[string]*filer_pb.Entry{"/buckets/worm/f": {Name: "f", WormEnforcedAtTsNs: time.Now().UnixNano()}}, "/buckets/worm/f", "/buckets/b/f", true},
		{"unprotected", map[string]*filer_pb.Entry{"/buckets/b/x": {Name: "x"}, "/buckets/b/y": {Name: "y"}}, "/buckets/b/x", "/buckets/b/y", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.entries["/buckets/b"] = &filer_pb.Entry{Name: "b", IsDirectory: true}
			tt.entries["/buckets/b/f.versions"] = &filer_pb.Entry{Name: "f.versions", IsDirectory: true}
			fs, fake := newLockTestServer(t, tt.entries)
			err := fs.renameEntry(tt.from, tt.to)
			renames := fake.renameRequests()
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatalf("rename: got %v, want os.ErrPermission", err)
				}
				if len(renames) != 0 {
					t.Fatalf("rename of a protected entry reached the filer")
				}
				return
			}
			if err != nil {
				t.Fatalf("rename: %v", err)
			}
			if len(renames) != 1 {
				t.Fatalf("got %d AtomicRenameEntry calls, want 1", len(renames))
			}
		})
	}
}

func (f *lockTestFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	f.lastUpdate = req
	return &filer_pb.UpdateEntryResponse{}, nil
}

func TestWriteAndSetstatRefuseProtectedEntries(t *testing.T) {
	entries := map[string]*filer_pb.Entry{
		"/buckets/b":          {Name: "b", IsDirectory: true},
		"/buckets/b/held":     lockedEntry("held", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}),
		"/buckets/worm":       {Name: "worm", IsDirectory: true},
		"/buckets/worm/f":     {Name: "f", WormEnforcedAtTsNs: time.Now().UnixNano()},
		"/buckets/b/unlocked": {Name: "unlocked"},
	}
	fs, fake := newLockTestServer(t, entries)
	for _, p := range []string{"/b/held", "/worm/f"} {
		req := sftp.NewRequest("Put", p)
		if w, err := fs.newFileWriter(req); !errors.Is(err, os.ErrPermission) {
			if closer, ok := w.(io.Closer); ok {
				closer.Close()
			}
			t.Fatalf("overwrite of %s: got %v, want os.ErrPermission", p, err)
		}
		if err := fs.setFileStatWithRequest("/buckets"+p, sftp.NewRequest("Setstat", p)); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("setstat of %s: got %v, want os.ErrPermission", p, err)
		}
	}
	if fake.updates != 0 {
		t.Fatalf("setstat of a protected entry reached the filer")
	}
	if err := fs.setFileStatWithRequest("/buckets/b/unlocked", sftp.NewRequest("Setstat", "/b/unlocked")); err != nil {
		t.Fatalf("setstat of an unprotected entry: %v", err)
	}
	if _, ok := fake.lastUpdate.ExpectedExtended[s3_constants.ExtLegalHoldKey]; !ok {
		t.Fatalf("setstat is not pinned to the Object Lock state it checked")
	}
}

func TestRenameOntoItselfKeepsProtectedEntry(t *testing.T) {
	p := "/buckets/b/f.versions/v_1"
	fs, _ := newLockTestServer(t, map[string]*filer_pb.Entry{
		"/buckets/b/f.versions": {Name: "f.versions", IsDirectory: true},
		p:                       lockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}),
	})
	if err := fs.renameEntry(p, p); err != nil {
		t.Fatalf("rename onto itself: %v", err)
	}
}

func (f *lockTestFiler) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: make(map[string]*filer_pb.Locations)}
	for _, vid := range req.VolumeIds {
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: f.volume}}}
	}
	return resp, nil
}

func TestRemoveReadsChunkedFilerConf(t *testing.T) {
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(lockTestFilerConf))
	}))
	defer volume.Close()
	conf := &filer_pb.Entry{
		Name:       "filer.conf",
		Attributes: &filer_pb.FuseAttributes{FileSize: uint64(len(lockTestFilerConf))},
		Chunks:     []*filer_pb.FileChunk{fileChunk("3,01", 0, int64(len(lockTestFilerConf)))},
	}
	fs, fake := newLockTestServer(t, map[string]*filer_pb.Entry{
		"/etc/seaweedfs/filer.conf": conf,
		"/buckets/worm/f":           {Name: "f", WormEnforcedAtTsNs: time.Now().UnixNano()},
	})
	fake.volume = volume.Listener.Addr().String()
	if err := fs.removeEntry("/buckets/worm/f"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("rm under a WORM rule from a chunked filer.conf: got %v, want os.ErrPermission", err)
	}
}

func TestUploadRechecksTargetAtClose(t *testing.T) {
	fs, fake := newLockTestServer(t, map[string]*filer_pb.Entry{
		"/buckets/b":     {Name: "b", IsDirectory: true},
		"/buckets/b/dir": {Name: "dir", IsDirectory: true},
		"/buckets/b/f":   {Name: "f"},
	})
	if w, err := fs.newFileWriter(sftp.NewRequest("Put", "/b/dir")); err == nil {
		w.(io.Closer).Close()
		t.Fatalf("upload onto a directory was accepted")
	}

	w, err := fs.newFileWriter(sftp.NewRequest("Put", "/b/f"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	fake.mu.Lock()
	fake.entries["/buckets/b/f"] = lockedEntry("f", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn})
	fake.entries["/buckets/b/f"].Attributes = &filer_pb.FuseAttributes{FileMode: 0644, Uid: 1000}
	fake.mu.Unlock()
	if err := w.(io.Closer).Close(); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("close after a hold was set: got %v, want os.ErrPermission", err)
	}
}

func (f *lockTestFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.mu.Lock()
	var children []*filer_pb.Entry
	for p, entry := range f.entries {
		if dir, name := path.Split(p); strings.TrimSuffix(dir, "/") == req.Directory && name > req.StartFromFileName {
			children = append(children, entry)
		}
	}
	f.mu.Unlock()
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	for _, entry := range children {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func TestRenameRefusesDirectoryHoldingProtectedEntries(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		child   *filer_pb.Entry
		refused bool
	}{
		{"locked version below", "/buckets/b/d", lockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), true},
		{"under a WORM rule", "/buckets/worm/d", &filer_pb.Entry{Name: "v_1"}, true},
		{"unprotected", "/buckets/b/d", lockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOff}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := path.Dir(tt.dir)
			fs, fake := newLockTestServer(t, map[string]*filer_pb.Entry{
				parent:                     {Name: path.Base(parent), IsDirectory: true},
				tt.dir:                     {Name: "d", IsDirectory: true},
				tt.dir + "/f.versions":     {Name: "f.versions", IsDirectory: true},
				tt.dir + "/f.versions/v_1": tt.child,
				"/buckets/b":               {Name: "b", IsDirectory: true},
			})
			err := fs.renameEntry(tt.dir, "/buckets/b/moved")
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatalf("rename: got %v, want os.ErrPermission", err)
				}
				if len(fake.renameRequests()) != 0 {
					t.Fatalf("rename of a protected subtree reached the filer")
				}
				return
			}
			if err != nil {
				t.Fatalf("rename: %v", err)
			}
		})
	}
}
