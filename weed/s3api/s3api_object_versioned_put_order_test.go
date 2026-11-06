package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type noopObjectWriteLock struct{}

func (noopObjectWriteLock) StopShortLivedLock() error { return nil }

func newVersionedPutFiler(t *testing.T) (*ambiguousPutFiler, *S3ApiServer) {
	t.Helper()
	firstId := generateVersionId(true)
	firstName := "v_" + firstId
	filerImpl := &ambiguousPutFiler{
		volume: startFakeVolumeServer(t),
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {
				Name:        "b",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended:    map[string][]byte{s3_constants.ExtVersioningKey: []byte(s3_constants.VersioningEnabled)},
			},
			"/buckets/b/k.versions": {
				Name:        "k.versions",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionIdKey:       []byte(firstId),
					s3_constants.ExtLatestVersionFileNameKey: []byte(firstName),
				},
			},
			"/buckets/b/k.versions/" + firstName: {
				Name:       firstName,
				Attributes: &filer_pb.FuseAttributes{FileSize: 1},
				Extended: map[string][]byte{
					s3_constants.ExtETagKey:      []byte("abc123"),
					s3_constants.ExtVersionIdKey: []byte(firstId),
				},
			},
		},
		apply: true,
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	return filerImpl, s3a
}

func latestVersionId(f *ambiguousPutFiler) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.entries["/buckets/b/k.versions"].Extended[s3_constants.ExtLatestVersionIdKey])
}

// A conditional PUT that waits on the object write lock while a DELETE commits
// must still become the latest version once its condition passes.
func TestVersionedConditionalPutAfterLockWaitBecomesLatest(t *testing.T) {
	filerImpl, s3a := newVersionedPutFiler(t)
	var markerId string
	s3a.newObjectWriteLock = func(bucket, object string) (objectWriteLock, error) {
		if markerId == "" {
			var err error
			if markerId, err = s3a.createDeleteMarker(bucket, object); err != nil {
				t.Fatalf("create delete marker: %v", err)
			}
		}
		return noopObjectWriteLock{}, nil
	}

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	r.Header.Set(s3_constants.IfNoneMatch, "*")
	versionId, _, code, _ := s3a.putVersionedObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("If-None-Match PUT after the key was deleted = %v, want success", code)
	}
	if markerId == "" || latestVersionId(filerImpl) == markerId {
		t.Fatalf("latest stayed on delete marker %q, want the accepted PUT", markerId)
	}
	if got := latestVersionId(filerImpl); got != versionId {
		t.Fatalf("latest version = %q, want the returned version %q", got, versionId)
	}
	if compareVersionIds(markerId, versionId) <= 0 {
		t.Fatalf("version %q does not order after delete marker %q", versionId, markerId)
	}
	filerImpl.mu.Lock()
	stored := filerImpl.entries["/buckets/b/k.versions/"+s3a.getVersionFileName(versionId)]
	filerImpl.mu.Unlock()
	if stored == nil || string(stored.Extended[s3_constants.ExtVersionIdKey]) != versionId {
		t.Fatalf("version file for %q missing or carries another id", versionId)
	}
}

// A routed PUT whose upload overlaps a DELETE commits after it, so the delete
// marker must not stay latest over the accepted write.
func TestVersionedRoutedPutAfterConcurrentDeleteBecomesLatest(t *testing.T) {
	filerImpl, s3a := newVersionedPutFiler(t)
	filerAddr := s3a.option.Filers[0]
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, filerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{filerAddr}, 1)

	markerId := ""
	filerImpl.beforeAssign = func() {
		if markerId != "" {
			return
		}
		markerId = generateVersionId(true)
		markerName := "v_" + markerId
		filerImpl.entries["/buckets/b/k.versions/"+markerName] = &filer_pb.Entry{
			Name:       markerName,
			Attributes: &filer_pb.FuseAttributes{},
			Extended: map[string][]byte{
				s3_constants.ExtVersionIdKey:    []byte(markerId),
				s3_constants.ExtDeleteMarkerKey: []byte("true"),
			},
		}
		pointer := filerImpl.entries["/buckets/b/k.versions"]
		pointer.Extended[s3_constants.ExtLatestVersionIdKey] = []byte(markerId)
		pointer.Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte(markerName)
	}

	r := httptest.NewRequest(http.MethodPut, "/b/k", strings.NewReader("new"))
	versionId, _, code, _ := s3a.putVersionedObject(r, "b", "k", r.Body, "")
	if code != s3err.ErrNone {
		t.Fatalf("PUT = %v", code)
	}
	if filerImpl.txnCount != 1 {
		t.Fatalf("PUT should route one transaction, got %d", filerImpl.txnCount)
	}
	if got := latestVersionId(filerImpl); got != versionId {
		t.Fatalf("latest version = %q, want the returned version %q (delete marker %q)", got, versionId, markerId)
	}
}
