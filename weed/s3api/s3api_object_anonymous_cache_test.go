package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// A cache fill may return newer metadata without committing downloaded chunks.
// Keep lookup snapshots separate so the handler must authorize the RPC response.
type anonymousReadCacheFiler struct {
	*anonymousReadFiler
	cached *filer_pb.Entry
}

func (f *anonymousReadCacheFiler) CacheRemoteObjectToLocalCluster(context.Context, *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: proto.Clone(f.cached).(*filer_pb.Entry)}, nil
}

func TestAnonymousObjectACLCacheMetadata(t *testing.T) {
	for i, tc := range []struct {
		name, change                    string
		chunks, versioned, ranged, part bool
		want                            int
	}{
		{name: "uncached ACL revoked", change: "acl", want: 403},
		{name: "uncached policy tag changed", change: "tag", want: 403},
		{name: "uncached policy tag removed", change: "missing-tag", want: 403},
		{name: "uncached delete marker", change: "delete", want: 403},
		{name: "cached ACL revoked", change: "acl", chunks: true, want: 403},
		{name: "cached policy tag changed", change: "tag", chunks: true, want: 403},
		{name: "versioned cached denial clears metadata", change: "acl", chunks: true, versioned: true, want: 403},
		{name: "versioned uncached denial clears metadata", change: "tag", versioned: true, want: 403},
		{name: "part denial clears metadata", change: "acl", chunks: true, part: true, want: 403},
		{name: "uncached public origin read", want: 200},
		{name: "uncached public origin range", ranged: true, want: 206},
		{name: "uncached public origin part", part: true, want: 206},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeStreamRemoteClient{data: []byte("hello world")}
			oldMaker, existed := remote_storage.RemoteStorageClientMakers["anonymous-cache-test"]
			remote_storage.RemoteStorageClientMakers["anonymous-cache-test"] = &fakeStreamRemoteMaker{client: client}
			t.Cleanup(func() {
				if existed {
					remote_storage.RemoteStorageClientMakers["anonymous-cache-test"] = oldMaker
				} else {
					delete(remote_storage.RemoteStorageClientMakers, "anonymous-cache-test")
				}
			})
			entry := anonymousReadEntry(anonymousReadGrant("READ"))
			entry.Content = nil
			entry.RemoteEntry = &filer_pb.RemoteEntry{RemoteSize: 11}
			entry.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte("public")
			entry.Extended[s3_constants.ExtObjectLockModeKey] = []byte("GOVERNANCE")
			entry.Extended[s3_constants.ExtRetentionUntilDateKey] = []byte("1900000000")
			entry.Extended[s3_constants.ExtLegalHoldKey] = []byte("ON")
			if tc.part {
				entry.Extended[s3_constants.SeaweedFSMultipartPartsCount] = []byte("1")
				boundaries, err := json.Marshal([]PartBoundaryInfo{{PartNumber: 1, StartOffset: 0, EndOffset: 11}})
				require.NoError(t, err)
				entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries] = boundaries
			}
			cached := proto.Clone(entry).(*filer_pb.Entry)
			if tc.chunks {
				cached.Chunks = []*filer_pb.FileChunk{{FileId: "1,abc", Size: 11}}
			}
			switch tc.change {
			case "acl":
				cached.Extended[s3_constants.ExtAmzAclKey] = []byte(`[]`)
			case "tag":
				cached.Extended[s3_constants.AmzObjectTaggingPrefix+"classification"] = []byte("private")
			case "missing-tag":
				delete(cached.Extended, s3_constants.AmzObjectTaggingPrefix+"classification")
			case "delete":
				cached.Extended[s3_constants.ExtDeleteMarkerKey] = []byte("true")
			}
			f := &anonymousReadCacheFiler{anonymousReadFiler: &anonymousReadFiler{entries: map[string]*filer_pb.Entry{"/buckets/b/o": entry}}, cached: cached}
			// Remote clients are globally cached by name; isolate cases and repeated runs.
			remoteName := fmt.Sprintf("anonymous-cache-origin-%d-%d", time.Now().UnixNano(), i)
			mapping, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{"/buckets/b": {Name: remoteName, Bucket: "origin", Path: "/"}}})
			require.NoError(t, err)
			conf, err := proto.Marshal(&remote_pb.RemoteConf{Name: remoteName, Type: "anonymous-cache-test"})
			require.NoError(t, err)
			f.entries["/etc/remote/mount.mapping"] = &filer_pb.Entry{Content: mapping}
			f.entries["/etc/remote/"+remoteName+".conf"] = &filer_pb.Entry{Content: conf}
			s3a := newPutTestServer(t, startFakeFiler(t, f))
			s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			config := &BucketConfig{Name: "b", Ownership: s3_constants.OwnershipObjectWriter}
			s3a.bucketConfigCache.Set("b", config)
			s3a.policyEngine = NewBucketPolicyEngine()
			require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject*","Resource":"arn:aws:s3:::b/*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/classification":"public"}}}]}`))
			query := ""
			if tc.part {
				query = "partNumber=1"
			}
			if tc.versioned {
				config.Versioning = "Enabled"
				entry.Extended[s3_constants.ExtVersionIdKey] = []byte("v1")
				cached.Extended[s3_constants.ExtVersionIdKey] = []byte("v1")
				f.entries["/buckets/b/o.versions/v_v1"] = entry
				query = "versionId=v1"
			}
			headers := map[string]string{}
			if tc.ranged {
				headers["Range"] = "bytes=2-5"
			}
			rr := serveAnonymousRead(s3a, http.MethodGet, "o", query, headers)
			require.Equal(t, tc.want, rr.Code, rr.Body.String())
			if tc.want == 403 {
				require.Nil(t, client.gotLoc, "a denied cache snapshot must never reach the origin")
				require.NotContains(t, rr.Body.String(), "hello world")
				for _, name := range []string{"x-amz-version-id", s3_constants.AmzObjectLockMode, s3_constants.AmzObjectLockRetainUntilDate, s3_constants.AmzObjectLockLegalHold, s3_constants.AmzMpPartsCount} {
					require.Empty(t, rr.Header().Get(name), name)
				}
			} else {
				require.NotNil(t, client.gotLoc)
				if tc.ranged {
					require.Equal(t, "llo ", rr.Body.String())
					require.Equal(t, "bytes 2-5/11", rr.Header().Get("Content-Range"))
				} else {
					require.Equal(t, "hello world", rr.Body.String())
				}
			}
		})
	}
}

// An explicit zero-byte directory marker is a null-version object, unlike a
// request for the bare prefix. Both versioning states must keep that distinction.
func TestAnonymousObjectACLVersionedEmptyDirectoryMarker(t *testing.T) {
	for _, versioning := range []string{"Enabled", "Suspended"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(versioning+"/"+method, func(t *testing.T) {
				s3a, f := newAnonymousReadTestServer(t)
				config, _ := s3a.getBucketConfig("b")
				config.Versioning = versioning
				marker := &filer_pb.Entry{Name: "folder", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "application/octet-stream"}, Extended: map[string][]byte{s3_constants.ExtAmzAclKey: anonymousReadGrant("READ")}}
				f.entries["/buckets/b/folder"] = marker
				for _, query := range []string{"", "versionId=null"} {
					rr := serveAnonymousRead(s3a, method, "folder/", query, nil)
					require.Equal(t, 200, rr.Code, query+": "+rr.Body.String())
					require.Equal(t, "null", rr.Header().Get("x-amz-version-id"))
				}
				rr := serveAnonymousRead(s3a, method, "folder", "", nil)
				require.Equal(t, 404, rr.Code, "the bare prefix is still not an object")
				marker.Extended[s3_constants.ExtAmzAclKey] = []byte(`[]`)
				rr = serveAnonymousRead(s3a, method, "folder/", "", nil)
				require.Equal(t, 403, rr.Code, rr.Body.String())
				require.Empty(t, rr.Header().Get("x-amz-version-id"))
			})
		}
	}
}
