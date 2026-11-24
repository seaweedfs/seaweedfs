package weed_server

import (
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_objectlock"
)

type webDavLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry
	deletes []*filer_pb.DeleteEntryRequest
	renames []*filer_pb.AtomicRenameEntryRequest
	creates []*filer_pb.CreateEntryRequest
	updates []*filer_pb.UpdateEntryRequest
}

func (f *webDavLockFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.entries[strings.TrimSuffix(req.Directory, "/")+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, status.Error(codes.NotFound, "no entry is found")
}

func (f *webDavLockFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
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

func (f *webDavLockFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *webDavLockFiler) AtomicRenameEntry(_ context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renames = append(f.renames, req)
	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (f *webDavLockFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, req)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *webDavLockFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *webDavLockFiler) counts() (deletes, renames, creates int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletes), len(f.renames), len(f.creates)
}

const webDavLockFilerConf = `{"locations":[{"locationPrefix":"/buckets/worm/","worm":true},{"locationPrefix":"/buckets/worm-1h/","worm":true,"wormRetentionTimeSeconds":3600}]}`

func newWebDavLockFS(t *testing.T, entries map[string]*filer_pb.Entry) (*WebDavFileSystem, *webDavLockFiler) {
	t.Helper()
	if _, ok := entries["/etc/seaweedfs/filer.conf"]; !ok {
		entries["/etc/seaweedfs/filer.conf"] = &filer_pb.Entry{Name: "filer.conf", Content: []byte(webDavLockFilerConf)}
	}
	for _, dir := range []string{"/buckets", "/buckets/b", "/buckets/worm", "/buckets/worm-1h"} {
		if _, ok := entries[dir]; !ok {
			entries[dir] = &filer_pb.Entry{Name: path.Base(dir), IsDirectory: true}
		}
	}
	for _, entry := range entries {
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{FileMode: 0644}
			if entry.IsDirectory {
				entry.Attributes.FileMode = uint32(os.ModeDir | 0755)
			}
		}
	}
	fake := &webDavLockFiler{entries: entries}
	return &WebDavFileSystem{option: &WebDavOption{
		Filer:          startFakeWebDavFiler(t, fake),
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}}, fake
}

func webDavLockedEntry(name string, extended map[string]string) *filer_pb.Entry {
	entry := &filer_pb.Entry{Name: name, Extended: map[string][]byte{}}
	for k, v := range extended {
		entry.Extended[k] = []byte(v)
	}
	return entry
}

func webDavRetainUntil(d time.Duration) string {
	return strconv.FormatInt(time.Now().Add(d).Unix(), 10)
}

type webDavProtectionCase struct {
	name    string
	path    string
	entry   *filer_pb.Entry
	refused bool
}

func webDavProtectionCases() []webDavProtectionCase {
	wormAt := time.Now().Add(-2 * time.Hour).UnixNano()
	return []webDavProtectionCase{
		{"legal hold", "/buckets/b/f.versions/v_1", webDavLockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), true},
		{"compliance retention", "/buckets/b/f.versions/v_2", webDavLockedEntry("v_2", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeCompliance, s3_constants.ExtRetentionUntilDateKey: webDavRetainUntil(time.Hour)}), true},
		{"governance retention", "/buckets/b/f", webDavLockedEntry("f", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeGovernance, s3_constants.ExtRetentionUntilDateKey: webDavRetainUntil(time.Hour)}), true},
		{"expired retention", "/buckets/b/f.versions/v_4", webDavLockedEntry("v_4", map[string]string{s3_constants.ExtObjectLockModeKey: s3_constants.RetentionModeCompliance, s3_constants.ExtRetentionUntilDateKey: webDavRetainUntil(-time.Hour)}), false},
		{"legal hold off", "/buckets/b/f.versions/v_5", webDavLockedEntry("v_5", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOff}), false},
		{"delete marker", "/buckets/b/f.versions/v_6", webDavLockedEntry("v_6", map[string]string{s3_constants.ExtDeleteMarkerKey: "true", s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), false},
		{"worm", "/buckets/worm/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, true},
		{"worm retention elapsed", "/buckets/worm-1h/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, false},
		{"worm not enforced yet", "/buckets/worm/g", &filer_pb.Entry{Name: "g"}, false},
		{"no worm rule", "/buckets/b/f", &filer_pb.Entry{Name: "f", WormEnforcedAtTsNs: wormAt}, false},
	}
}

func TestWebDavRemoveAllRefusesProtectedEntries(t *testing.T) {
	for _, tt := range webDavProtectionCases() {
		t.Run(tt.name, func(t *testing.T) {
			fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{tt.path: tt.entry})
			err := fs.RemoveAll(context.Background(), tt.path)
			deletes, _, _ := fake.counts()
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatalf("delete: got %v, want os.ErrPermission", err)
				}
				if deletes != 0 {
					t.Fatalf("delete reached the filer")
				}
				return
			}
			if err != nil {
				t.Fatalf("delete: %v", err)
			}
			if deletes != 1 {
				t.Fatalf("got %d filer deletes, want 1", deletes)
			}
			if got := fake.deletes[0].ExpectedExtended; len(got) != 3 || !bytesEqualMap(got, s3_objectlock.ObjectLockState(tt.entry)) {
				t.Fatalf("delete preconditions = %v, want the checked Object Lock state", got)
			}
		})
	}
}

func TestWebDavRemoveAllRefusesDirectoryHoldingProtectedEntries(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		child   *filer_pb.Entry
		refused bool
	}{
		{"locked version below", "/buckets/b/f.versions", webDavLockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}), true},
		{"under a WORM rule", "/buckets/worm/d", &filer_pb.Entry{Name: "v_1"}, true},
		{"unprotected", "/buckets/b/d", webDavLockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOff}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{
				tt.dir:                       {Name: path.Base(tt.dir), IsDirectory: true},
				tt.dir + "/" + tt.child.Name: tt.child,
			})
			err := fs.RemoveAll(context.Background(), tt.dir+"/")
			deletes, _, _ := fake.counts()
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) || deletes != 0 {
					t.Fatalf("delete: got %v with %d filer deletes, want os.ErrPermission and none", err, deletes)
				}
				return
			}
			if err != nil || deletes != 1 || !fake.deletes[0].IsRecursive {
				t.Fatalf("delete: got %v with %d filer deletes, want one recursive delete", err, deletes)
			}
		})
	}
}

func TestWebDavRenameRefusesProtectedEntries(t *testing.T) {
	for _, tt := range webDavProtectionCases() {
		t.Run("source "+tt.name, func(t *testing.T) {
			fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{tt.path: tt.entry})
			if tt.path != "/buckets/b/f" {
				fake.entries[path.Dir(tt.path)] = &filer_pb.Entry{Name: path.Base(path.Dir(tt.path)), IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}}
			}
			assertWebDavRename(t, fs, fake, tt.path, "/buckets/b/moved", tt.refused)
		})
		t.Run("target "+tt.name, func(t *testing.T) {
			fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{
				tt.path:          tt.entry,
				"/buckets/b/src": {Name: "src"},
			})
			if tt.path != "/buckets/b/f" {
				fake.entries[path.Dir(tt.path)] = &filer_pb.Entry{Name: path.Base(path.Dir(tt.path)), IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}}
			}
			assertWebDavRename(t, fs, fake, "/buckets/b/src", tt.path, tt.refused)
		})
	}
}

func TestWebDavRenameRefusesDirectoryHoldingProtectedEntries(t *testing.T) {
	fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{
		"/buckets/b/f.versions":     {Name: "f.versions", IsDirectory: true},
		"/buckets/b/f.versions/v_1": webDavLockedEntry("v_1", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn}),
		"/buckets/worm/d":           {Name: "d", IsDirectory: true},
		"/buckets/b/d":              {Name: "d", IsDirectory: true},
	})
	assertWebDavRename(t, fs, fake, "/buckets/b/f.versions", "/buckets/b/moved", true)
	assertWebDavRename(t, fs, fake, "/buckets/worm/d", "/buckets/b/moved", true)
	assertWebDavRename(t, fs, fake, "/buckets/b/d", "/buckets/b/moved", false)
}

func assertWebDavRename(t *testing.T, fs *WebDavFileSystem, fake *webDavLockFiler, from, to string, refused bool) {
	t.Helper()
	_, before, _ := fake.counts()
	err := fs.Rename(context.Background(), from, to)
	_, after, _ := fake.counts()
	if refused {
		if !errors.Is(err, os.ErrPermission) || after != before {
			t.Fatalf("rename %s to %s: got %v with %d filer renames, want os.ErrPermission and none", from, to, err, after-before)
		}
		return
	}
	if err != nil || after != before+1 {
		t.Fatalf("rename %s to %s: got %v with %d filer renames, want one", from, to, err, after-before)
	}
}

func TestWebDavOpenFileRefusesToReplaceProtectedEntries(t *testing.T) {
	for _, tt := range webDavProtectionCases() {
		t.Run(tt.name, func(t *testing.T) {
			fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{tt.path: tt.entry})
			if tt.path != "/buckets/b/f" {
				fake.entries[path.Dir(tt.path)] = &filer_pb.Entry{Name: path.Base(path.Dir(tt.path)), IsDirectory: true, Attributes: &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755)}}
			}
			f, err := fs.OpenFile(context.Background(), tt.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			_, _, creates := fake.counts()
			if tt.refused {
				if !errors.Is(err, os.ErrPermission) || creates != 0 {
					t.Fatalf("put: got %v with %d filer creates, want os.ErrPermission and none", err, creates)
				}
				return
			}
			if err != nil || creates != 1 {
				t.Fatalf("put: got %v with %d filer creates, want one", err, creates)
			}
			f.Close()
		})
	}
}

func TestWebDavUploadRechecksTargetAtClose(t *testing.T) {
	for _, locked := range []bool{false, true} {
		fs, fake := newWebDavLockFS(t, map[string]*filer_pb.Entry{"/buckets/b/f": {Name: "f"}})
		fs.option.MaxMB = 1
		f, err := fs.OpenFile(context.Background(), "/buckets/b/f", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := f.Write(nil); err != nil {
			t.Fatalf("write: %v", err)
		}
		if locked {
			fake.mu.Lock()
			fake.entries["/buckets/b/f"] = webDavLockedEntry("f", map[string]string{s3_constants.ExtLegalHoldKey: s3_constants.LegalHoldOn})
			fake.entries["/buckets/b/f"].Attributes = &filer_pb.FuseAttributes{FileMode: 0644}
			fake.mu.Unlock()
		}
		err = f.Close()
		fake.mu.Lock()
		updates := fake.updates
		fake.mu.Unlock()
		if locked {
			if !errors.Is(err, os.ErrPermission) || len(updates) != 0 {
				t.Fatalf("close after a hold was set: got %v with %d filer updates, want os.ErrPermission and none", err, len(updates))
			}
			continue
		}
		if err != nil || len(updates) != 1 || updates[0].ExpectedExtended != nil {
			t.Fatalf("close: got %v with %d filer updates, want one without a precondition", err, len(updates))
		}
	}
}

func bytesEqualMap(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if string(b[k]) != string(v) {
			return false
		}
	}
	return true
}
