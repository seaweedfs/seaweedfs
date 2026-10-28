package s3api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gateway's configured filer is a replica, while PUT commits on the key's
// owner. No metadata is replicated in this test, making the visibility gap
// deterministic even though both HTTP handlers run on the same gateway.
func TestCopyAfterRoutedPut(t *testing.T) {
	util_http.InitGlobalHttpClient()
	for _, versioning := range []string{"", s3_constants.VersioningSuspended} {
		for _, multipart := range []bool{false, true} {
			t.Run(fmt.Sprintf("versioning=%s/multipart=%t", versioning, multipart), func(t *testing.T) {
				volume := startFakeVolumeServer(t)
				owner := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
				replica := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
				ownerAddr := startFakeFiler(t, owner)
				replicaAddr := startFakeFiler(t, replica)
				s3a := newPutTestServer(t, replicaAddr)
				s3a.iam = &IdentityAccessManagement{}
				s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
				s3a.bucketConfigCache.Set("source", &BucketConfig{Name: "source"})
				s3a.bucketConfigCache.Set("destination", &BucketConfig{Name: "destination"})
				s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
				s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

				put := httptest.NewRequest(http.MethodPut, "/source/tmp/object", nil)
				put = mux.SetURLVars(put, map[string]string{"bucket": "source", "object": "tmp/object"})
				put.Header.Set("X-Amz-Meta-Revision", "fresh")
				putResult := httptest.NewRecorder()
				s3a.PutObjectHandler(putResult, put)
				require.Equal(t, http.StatusOK, putResult.Code, putResult.Body.String())
				owner.mu.Lock()
				stored := owner.entries["/buckets/source/tmp/object"]
				owner.mu.Unlock()
				require.NotNil(t, stored, "PUT must commit on the owner")
				replica.mu.Lock()
				replicated := replica.entries["/buckets/source/tmp/object"]
				replica.mu.Unlock()
				require.Nil(t, replicated, "the replica must still be behind")
				require.Equal(t, replicaAddr, s3a.filerClient.GetCurrentFiler())

				// A regular object remains the null version when versioning is
				// suspended. Both copy paths must find it on the write owner.
				s3a.bucketConfigCache.Set("source", &BucketConfig{Name: "source", Versioning: versioning})
				copyURL := "/destination/final/object"
				var uploadID string
				if multipart {
					uploadID = s3a.generateUploadID("final/object")
					replica.mu.Lock()
					replica.entries["/buckets/destination/.uploads/"+uploadID] = &filer_pb.Entry{
						Name: uploadID, IsDirectory: true,
						Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("final/object")},
					}
					replica.mu.Unlock()
					copyURL += "?partNumber=1&uploadId=" + uploadID
				}
				copyRequest := httptest.NewRequest(http.MethodPut, copyURL, nil)
				copyRequest = mux.SetURLVars(copyRequest, map[string]string{"bucket": "destination", "object": "final/object"})
				copyRequest.Header.Set("X-Amz-Copy-Source", "/source/tmp/object")
				copyResult := httptest.NewRecorder()
				if multipart {
					s3a.CopyObjectPartHandler(copyResult, copyRequest)
				} else {
					s3a.CopyObjectHandler(copyResult, copyRequest)
				}
				require.Equal(t, http.StatusOK, copyResult.Code, copyResult.Body.String())
				require.Equal(t, replicaAddr, s3a.filerClient.GetCurrentFiler(), "a source read must not change the gateway's default filer")

				replica.mu.Lock()
				if multipart {
					var part *filer_pb.Entry
					for key, entry := range replica.entries {
						if strings.HasPrefix(key, "/buckets/destination/.uploads/"+uploadID+"/") && !entry.IsDirectory {
							part = entry
						}
					}
					require.NotNil(t, part, "copy must persist the part")
					require.Equal(t, stored.Attributes.Md5, part.Attributes.Md5)
					replica.mu.Unlock()
				} else {
					// The destination commits on its write owner, the same filer a
					// later copy resolves it from before replication.
					require.Nil(t, replica.entries["/buckets/destination/final/object"], "the replica must still be behind")
					replica.mu.Unlock()
					owner.mu.Lock()
					copied := owner.entries["/buckets/destination/final/object"]
					owner.mu.Unlock()
					require.NotNil(t, copied, "copy must persist the destination on the owner")
					require.Equal(t, "fresh", string(copied.Extended["X-Amz-Meta-Revision"]))
					require.Equal(t, stored.Attributes.Md5, copied.Attributes.Md5)

					// The destination is immediately usable as a copy source even
					// though the replica never saw it.
					second := httptest.NewRequest(http.MethodPut, "/destination/final/copy2", nil)
					second = mux.SetURLVars(second, map[string]string{"bucket": "destination", "object": "final/copy2"})
					second.Header.Set("X-Amz-Copy-Source", "/destination/final/object")
					secondResult := httptest.NewRecorder()
					s3a.CopyObjectHandler(secondResult, second)
					require.Equal(t, http.StatusOK, secondResult.Code, secondResult.Body.String())
				}
			})
		}
	}
}

// A copy into a versioned bucket commits the version file and the .versions
// pointer on the destination's write owner. Reads by versionId, latest-version
// reads, and copy-by-versionId must resolve there before replication.
func TestVersionedCopyDestinationReadsOnOwner(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	owner := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	replica := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	ownerAddr := startFakeFiler(t, owner)
	replicaAddr := startFakeFiler(t, replica)
	s3a := newPutTestServer(t, replicaAddr)
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("source", &BucketConfig{Name: "source"})
	s3a.bucketConfigCache.Set("destination", &BucketConfig{Name: "destination", Versioning: s3_constants.VersioningEnabled})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	put := httptest.NewRequest(http.MethodPut, "/source/tmp/object", nil)
	put = mux.SetURLVars(put, map[string]string{"bucket": "source", "object": "tmp/object"})
	putResult := httptest.NewRecorder()
	s3a.PutObjectHandler(putResult, put)
	require.Equal(t, http.StatusOK, putResult.Code, putResult.Body.String())

	copyRequest := httptest.NewRequest(http.MethodPut, "/destination/final/object", nil)
	copyRequest = mux.SetURLVars(copyRequest, map[string]string{"bucket": "destination", "object": "final/object"})
	copyRequest.Header.Set("X-Amz-Copy-Source", "/source/tmp/object")
	copyResult := httptest.NewRecorder()
	s3a.CopyObjectHandler(copyResult, copyRequest)
	require.Equal(t, http.StatusOK, copyResult.Code, copyResult.Body.String())
	versionId := copyResult.Header().Get("x-amz-version-id")
	require.NotEmpty(t, versionId)

	versionKey := "/buckets/destination/final/object.versions/v_" + versionId
	owner.mu.Lock()
	require.NotNil(t, owner.entries[versionKey], "version file must commit on the owner")
	require.NotNil(t, owner.entries["/buckets/destination/final/object.versions"], "the owner must hold the .versions pointer")
	owner.mu.Unlock()
	replica.mu.Lock()
	require.Nil(t, replica.entries[versionKey], "the replica must still be behind")
	require.Nil(t, replica.entries["/buckets/destination/final/object.versions"], "the replica must not hold the pointer")
	replica.mu.Unlock()

	entry, err := s3a.getSpecificObjectVersion("destination", "final/object", versionId)
	require.NoError(t, err)
	require.NotNil(t, entry)

	latest, err := s3a.getLatestObjectVersion("destination", "final/object")
	require.NoError(t, err)
	require.Equal(t, versionId, string(latest.Extended[s3_constants.ExtVersionIdKey]))

	src, err := s3a.resolveCopySourceEntry("destination", "final/object", versionId, s3_constants.VersioningEnabled)
	require.NoError(t, err)
	require.NotNil(t, src)

	second := httptest.NewRequest(http.MethodPut, "/destination/final/copy2", nil)
	second = mux.SetURLVars(second, map[string]string{"bucket": "destination", "object": "final/copy2"})
	second.Header.Set("X-Amz-Copy-Source", "/destination/final/object?versionId="+versionId)
	secondResult := httptest.NewRecorder()
	s3a.CopyObjectHandler(secondResult, second)
	require.Equal(t, http.StatusOK, secondResult.Code, secondResult.Body.String())
}

func TestCopySourcePrefersWriteOwner(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ownerEntry *filer_pb.Entry
		wantCode   s3err.ErrorCode
	}{
		{name: "overwrite", ownerEntry: &filer_pb.Entry{Name: "object", Content: []byte("fresh")}},
		{name: "deleted", wantCode: s3err.ErrNoSuchKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &fakeLookupFiler{entry: tc.ownerEntry}
			replica := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "object", Content: []byte("stale")}}
			ownerAddr := startFakeFiler(t, owner)
			s3a := newPutTestServer(t, startFakeFiler(t, replica))
			s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
			s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

			entry, err := s3a.resolveCopySourceEntry("source", "/tmp/object", "", "")
			require.Equal(t, tc.wantCode, classifyCopySourceError(entry, err))
			if tc.wantCode == s3err.ErrNone {
				require.Equal(t, "fresh", string(entry.Content))
			}
		})
	}
}

func TestClassifyCopySourceError(t *testing.T) {
	deleteMarker := &filer_pb.Entry{
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}

	tests := []struct {
		name  string
		entry *filer_pb.Entry
		err   error
		want  s3err.ErrorCode
	}{
		{"regular entry", &filer_pb.Entry{Name: "o"}, nil, s3err.ErrNone},
		{"nil entry", nil, nil, s3err.ErrNoSuchKey},
		{"directory entry", &filer_pb.Entry{IsDirectory: true}, nil, s3err.ErrNoSuchKey},
		{"delete marker entry", deleteMarker, nil, s3err.ErrNoSuchKey},
		{"not found sentinel", nil, filer_pb.ErrNotFound, s3err.ErrNoSuchKey},
		{"wrapped not found", nil, fmt.Errorf("read %s: %w", "o", filer_pb.ErrNotFound), s3err.ErrNoSuchKey},
		{"grpc not found", nil, status.Error(codes.NotFound, "no entry"), s3err.ErrNoSuchKey},
		{"invalid version id", nil, errInvalidVersionID, s3err.ErrNoSuchKey},
		{"delete marker error", nil, ErrDeleteMarker, s3err.ErrNoSuchKey},
		{"transient unavailable", nil, status.Error(codes.Unavailable, "filer down"), s3err.ErrServiceUnavailable},
		{"store error", nil, errors.New("connection reset"), s3err.ErrInternalError},
		// A message merely mentioning the sentinel text must not downgrade a store error.
		{"sentinel text in message", nil, fmt.Errorf("peer said: %s", filer_pb.ErrNotFound.Error()), s3err.ErrInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyCopySourceError(tt.entry, tt.err); got != tt.want {
				t.Errorf("classifyCopySourceError() = %v, want %v", got, tt.want)
			}
		})
	}
}
