package s3api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
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

// CacheRemoteObjectToLocalCluster returns the independent post-cache metadata snapshot.
func (f *anonymousReadCacheFiler) CacheRemoteObjectToLocalCluster(context.Context, *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: proto.Clone(f.cached).(*filer_pb.Entry)}, nil
}

// anonymousReadDirectoryFiler provides the empty version history left after a marker delete.
type anonymousReadDirectoryFiler struct {
	*anonymousReadFiler
}

// ListEntries reports an empty history so recovery cannot mistake the parent for a version.
func (f *anonymousReadDirectoryFiler) ListEntries(*filer_pb.ListEntriesRequest, filer_pb.SeaweedFiler_ListEntriesServer) error {
	return nil
}

// DeleteEntry removes an exhausted version history without triggering missing-RPC retries.
func (f *anonymousReadDirectoryFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, path.Join(req.Directory, req.Name))
	return &filer_pb.DeleteEntryResponse{}, nil
}

// TestAnonymousObjectACLCacheMetadata checks cache reauthorization and permitted origin reads.
func TestAnonymousObjectACLCacheMetadata(t *testing.T) {
	for _, tc := range []struct {
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
			// Reuse one cache slot while differing configurations refresh the fake client,
			// including when a single subtest is repeated. The fake maker ignores the endpoint.
			remoteName := "anonymous-cache-origin"
			mapping, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{"/buckets/b": {Name: remoteName, Bucket: "origin", Path: "/"}}})
			require.NoError(t, err)
			conf, err := proto.Marshal(&remote_pb.RemoteConf{Name: remoteName, Type: "anonymous-cache-test", S3Endpoint: fmt.Sprintf("https://origin-%p.invalid", client)})
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

// TestAnonymousObjectACLVersionedDirectoryIdentity distinguishes slash keys from
// implicit parents, deleted markers, and objects stored under the bare prefix.
func TestAnonymousObjectACLVersionedDirectoryIdentity(t *testing.T) {
	for _, versioning := range []string{"Enabled", "Suspended"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			for _, kind := range []string{"implicit parent", "deleted marker", "regular file", "prefix object", "empty prefix object"} {
				for _, route := range []struct {
					name, query          string
					conditional, history bool
				}{{name: "latest"}, {name: "null", query: "versionId=null"}, {name: "conditional latest", conditional: true}, {name: "conditional null", query: "versionId=null", conditional: true}, {name: "empty history", history: true}} {
					t.Run(versioning+"/"+method+"/"+kind+"/"+route.name, func(t *testing.T) {
						f := &anonymousReadDirectoryFiler{anonymousReadFiler: &anonymousReadFiler{entries: make(map[string]*filer_pb.Entry)}}
						s3a := newPutTestServer(t, startFakeFiler(t, f))
						s3a.iam = &IdentityAccessManagement{isAuthEnabled: true}
						s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
						s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Ownership: s3_constants.OwnershipObjectWriter, Versioning: versioning})
						s3a.policyEngine = NewBucketPolicyEngine()
						s3a.iam.policyEngine = s3a.policyEngine
						require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject*","Resource":"arn:aws:s3:::b/folder/"}]}`))
						entry := &filer_pb.Entry{Name: "folder", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000}}
						if kind == "deleted marker" {
							// Deletion clears the marker metadata while preserving a parent with children.
							entry.Extended = make(map[string][]byte)
						} else if kind == "regular file" || kind == "prefix object" || kind == "empty prefix object" {
							entry = anonymousReadEntry([]byte(`[]`))
							entry.Name = "folder"
							if kind != "regular file" {
								entry.MarkPrefixObject()
							}
							if kind == "empty prefix object" {
								entry.Content = nil
								entry.Attributes.FileSize = 0
								entry.Attributes.Mime = ""
							}
						}
						f.entries["/buckets/b/folder"] = entry
						f.entries["/buckets/b/folder/child"] = anonymousReadEntry([]byte(`[]`))
						assertMissing := func(rr *httptest.ResponseRecorder) {
							t.Helper()
							require.Equal(t, http.StatusNotFound, rr.Code, rr.Body.String())
							for _, header := range []string{"x-amz-version-id", "Last-Modified", "ETag"} {
								require.Empty(t, rr.Header().Get(header), header)
							}
							require.NotContains(t, rr.Body.String(), "hello world")
						}
						headers := map[string]string{}
						if route.conditional {
							headers["If-Modified-Since"] = "Fri, 01 Jan 2100 00:00:00 GMT"
						}
						if route.history {
							// An empty history exercises recovery instead of the no-history fast path.
							f.entries["/buckets/b/folder/.versions"] = &filer_pb.Entry{IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte("")}}
						}
						// Equivalent filer-path spellings must not recover a different object's data.
						for _, object := range []string{"folder/", "folder//", "folder\\"} {
							assertMissing(serveAnonymousRead(s3a, method, object, route.query, headers))
						}
						if kind == "regular file" || kind == "prefix object" || kind == "empty prefix object" {
							// A slash-only policy must not authorize data belonging to the bare key.
							rr := serveAnonymousRead(s3a, method, "folder", route.query, nil)
							require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
							entry.Extended[s3_constants.ExtAmzAclKey] = anonymousReadGrant("READ")
							rr = serveAnonymousRead(s3a, method, "folder", route.query, nil)
							require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
							if method == http.MethodGet {
								require.Equal(t, string(entry.Content), rr.Body.String())
							}
						}
						// The private child remains independently protected.
						rr := serveAnonymousRead(s3a, method, "folder/child", "", nil)
						require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
						if method == http.MethodGet {
							require.Contains(t, rr.Body.String(), "AccessDenied")
						}
						// A real historical slash-key version is a file, not a regular-path marker.
						version := anonymousReadEntry([]byte(`[]`))
						version.Extended[s3_constants.ExtVersionIdKey] = []byte("v1")
						f.entries["/buckets/b/folder/.versions/v_v1"] = version
						f.entries["/buckets/b/folder/.versions"] = &filer_pb.Entry{IsDirectory: true, Extended: map[string][]byte{s3_constants.ExtLatestVersionIdKey: []byte("v1"), s3_constants.ExtLatestVersionFileNameKey: []byte("v_v1")}}
						for _, versionQuery := range []string{"versionId=v1", ""} {
							rr = serveAnonymousRead(s3a, method, "folder/", versionQuery, nil)
							require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
							require.Equal(t, "v1", rr.Header().Get("x-amz-version-id"))
							if method == http.MethodGet {
								require.Equal(t, "hello world", rr.Body.String())
							}
						}
					})
				}
			}
		}
	}
}
