package s3api

import (
	"context"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// anonymousReadHealFiler drives the versioned self-heal path for "folder/": it
// serves a real version file from .versions, runs a hook when the heal first
// rescans (simulating a writer that commits while the heal is scanning), and
// persists pointer repairs through CreateEntry so tests can inspect the
// resulting .versions metadata.
type anonymousReadHealFiler struct {
	*anonymousReadFiler
	onScan func(*anonymousReadHealFiler)
}

// ListEntries serves the version files under .versions for the heal's rescan.
func (f *anonymousReadHealFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	f.mu.Lock()
	if f.onScan != nil {
		onScan := f.onScan
		f.onScan = nil
		f.mu.Unlock()
		onScan(f)
	} else {
		f.mu.Unlock()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, entry := range f.entries {
		if path.Join(req.Directory, entry.Name) == "/buckets/b/folder/.versions/v_v1" {
			if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateEntry records the heal's pointer persist the way a filer upsert would.
func (f *anonymousReadHealFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := proto.Clone(req.Entry).(*filer_pb.Entry)
	f.entries[path.Join(req.Directory, entry.Name)] = entry
	return &filer_pb.CreateEntryResponse{}, nil
}

// TestAnonymousObjectACLHealPointerConcurrentWriter pins the CAS contract of
// the self-heal persist: a pointer that a concurrent writer promoted while the
// heal was rescanning must survive, instead of being rolled back to the
// scanned version, which would make older content or ACLs current again.
func TestAnonymousObjectACLHealPointerConcurrentWriter(t *testing.T) {
	for _, tc := range []struct {
		name          string
		concurrentPut bool
		wantPointer   string
	}{
		{name: "idle key persists the rescanned pointer", wantPointer: "v1"},
		{name: "concurrent writer promotion survives the heal", concurrentPut: true, wantPointer: "v2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &anonymousReadHealFiler{anonymousReadFiler: &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}}
			// The object is "folder/"; the regular path is a physical parent, so a
			// pointerless read falls through to the persistent self-heal.
			f.entries["/buckets/b/folder"] = &filer_pb.Entry{Name: "folder", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000}}
			f.entries["/buckets/b/folder/child"] = anonymousReadEntry([]byte(`[]`))
			f.entries["/buckets/b/folder/.versions"] = &filer_pb.Entry{Name: ".versions", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000}, Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte("")}}
			v1 := anonymousReadEntry([]byte(`[]`))
			v1.Name = "v_v1"
			v1.Extended[s3_constants.ExtVersionIdKey] = []byte("v1")
			f.entries["/buckets/b/folder/.versions/v_v1"] = v1
			if tc.concurrentPut {
				f.onScan = func(f *anonymousReadHealFiler) {
					// A concurrent PUT commits a newer version while the heal scans.
					f.mu.Lock()
					defer f.mu.Unlock()
					f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionIdKey] = []byte("v2")
					f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte("v_v2")
				}
			}
			s3a := newPutTestServer(t, startFakeFiler(t, f))
			s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Ownership: s3_constants.OwnershipObjectWriter, Versioning: "Enabled"})
			s3a.policyEngine = NewBucketPolicyEngine()
			s3a.iam.policyEngine = s3a.policyEngine
			require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject*","Resource":"arn:aws:s3:::b/folder/"}]}`))

			rr := serveAnonymousRead(s3a, http.MethodGet, "folder/", "", nil)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			// This read returns the rescanned entry either way.
			require.Equal(t, "hello world", rr.Body.String())

			// The persisted pointer must reflect the concurrent writer, not the scan.
			pointer := f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionIdKey]
			require.Equal(t, tc.wantPointer, string(pointer))
		})
	}
}
