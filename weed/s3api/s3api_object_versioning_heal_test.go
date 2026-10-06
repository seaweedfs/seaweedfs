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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// anonymousReadHealFiler drives the versioned self-heal path for "folder/": it
// serves a real version file from .versions, runs hooks at the heal's rescan
// and at the persist (simulating a writer committing at either point), and
// evaluates the heal's IF_ENTRY_EQUAL precondition like a filer would, so
// tests can inspect the resulting .versions metadata.
type anonymousReadHealFiler struct {
	*anonymousReadFiler
	onScan  func(*anonymousReadHealFiler)
	onWrite func(*anonymousReadHealFiler)
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

// UpdateEntry evaluates the heal's IF_ENTRY_EQUAL precondition the way a
// filer does under the path lock: the stored entry must still equal the live
// image the heal re-read, otherwise the stale repair is rejected.
func (f *anonymousReadHealFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.mu.Lock()
	if f.onWrite != nil {
		onWrite := f.onWrite
		f.onWrite = nil
		f.mu.Unlock()
		// A writer commits between the heal's re-fetch and the persist.
		onWrite(f)
	} else {
		f.mu.Unlock()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	fullPath := path.Join(req.Directory, req.Entry.Name)
	current := f.entries[fullPath]
	for _, clause := range req.Condition.Clauses {
		if clause.Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL {
			return nil, status.Error(codes.Unimplemented, "unsupported clause")
		}
		expected := proto.Clone(clause.ExpectedEntry).(*filer_pb.Entry)
		actual := proto.Clone(current).(*filer_pb.Entry)
		filer_pb.BeforeEntrySerialization(expected.Chunks)
		filer_pb.BeforeEntrySerialization(actual.Chunks)
		if !proto.Equal(actual, expected) {
			return nil, status.Error(codes.FailedPrecondition, "entry changed")
		}
	}
	f.entries[fullPath] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

// TestAnonymousObjectACLHealPointerConcurrentWriter pins the CAS contract of
// the self-heal persist: a pointer that a concurrent writer promoted while the
// heal was rescanning, or between the heal's re-fetch and its persist, must
// survive, instead of being rolled back to the scanned version, which would
// make older content or ACLs current again.
func TestAnonymousObjectACLHealPointerConcurrentWriter(t *testing.T) {
	for _, tc := range []struct {
		name               string
		writerDuringScan   bool
		writerDuringWrite  bool
		wantPointer        string
		wantPointerVersion string
	}{
		{name: "idle key persists the rescanned pointer", wantPointer: "v1", wantPointerVersion: "v_v1"},
		{name: "concurrent writer during scan keeps its promotion", writerDuringScan: true, wantPointer: "v2", wantPointerVersion: "v_v2"},
		{name: "concurrent writer during persist keeps its promotion", writerDuringWrite: true, wantPointer: "v3", wantPointerVersion: "v_v3"},
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
			promotePointer := func(versionId, fileName string) func(*anonymousReadHealFiler) {
				return func(f *anonymousReadHealFiler) {
					// A concurrent PUT commits a newer version.
					f.mu.Lock()
					defer f.mu.Unlock()
					f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionIdKey] = []byte(versionId)
					f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte(fileName)
				}
			}
			if tc.writerDuringScan {
				f.onScan = promotePointer("v2", "v_v2")
			}
			if tc.writerDuringWrite {
				f.onWrite = promotePointer("v3", "v_v3")
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
			pointerFile := f.entries["/buckets/b/folder/.versions"].Extended[s3_constants.ExtLatestVersionFileNameKey]
			require.Equal(t, tc.wantPointerVersion, string(pointerFile))
		})
	}
}
