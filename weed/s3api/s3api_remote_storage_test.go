package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestIsInRemoteOnly tests the IsInRemoteOnly method on filer_pb.Entry
func TestIsInRemoteOnly(t *testing.T) {
	tests := []struct {
		name     string
		entry    *filer_pb.Entry
		expected bool
	}{
		{
			name: "remote-only entry with no chunks",
			entry: &filer_pb.Entry{
				Name:   "remote-file.txt",
				Chunks: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			expected: true,
		},
		{
			name: "remote entry with chunks (cached)",
			entry: &filer_pb.Entry{
				Name: "cached-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			expected: false,
		},
		{
			name: "local file with chunks (not remote)",
			entry: &filer_pb.Entry{
				Name: "local-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				RemoteEntry: nil,
			},
			expected: false,
		},
		{
			name: "empty remote entry (size 0)",
			entry: &filer_pb.Entry{
				Name:   "empty-remote.txt",
				Chunks: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 0,
				},
			},
			expected: false,
		},
		{
			name: "no chunks but nil RemoteEntry",
			entry: &filer_pb.Entry{
				Name:        "empty-local.txt",
				Chunks:      nil,
				RemoteEntry: nil,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.entry.IsInRemoteOnly()
			assert.Equal(t, tt.expected, result,
				"IsInRemoteOnly() for %s should return %v", tt.name, tt.expected)
		})
	}
}

// TestRemoteOnlyEntryDetection tests that the streamFromVolumeServers logic
// correctly distinguishes between remote-only entries and data integrity errors
func TestRemoteOnlyEntryDetection(t *testing.T) {
	tests := []struct {
		name              string
		entry             *filer_pb.Entry
		shouldBeRemote    bool
		shouldBeDataError bool
		shouldBeEmpty     bool
	}{
		{
			name: "remote-only entry (no chunks, has remote entry)",
			entry: &filer_pb.Entry{
				Name:   "remote-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 1024,
				},
			},
			shouldBeRemote:    true,
			shouldBeDataError: false,
			shouldBeEmpty:     false,
		},
		{
			name: "data integrity error (no chunks, no remote, has size)",
			entry: &filer_pb.Entry{
				Name:   "corrupt-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: true,
			shouldBeEmpty:     false,
		},
		{
			name: "empty local file (no chunks, no remote, size 0)",
			entry: &filer_pb.Entry{
				Name:   "empty-file.txt",
				Chunks: nil,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 0,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: false,
			shouldBeEmpty:     true,
		},
		{
			name: "normal file with chunks",
			entry: &filer_pb.Entry{
				Name: "normal-file.txt",
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc123", Size: 1024, Offset: 0},
				},
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				RemoteEntry: nil,
			},
			shouldBeRemote:    false,
			shouldBeDataError: false,
			shouldBeEmpty:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := tt.entry.GetChunks()
			totalSize := int64(filer.FileSize(tt.entry))

			if len(chunks) == 0 {
				// This mirrors the logic in streamFromVolumeServers
				if tt.entry.IsInRemoteOnly() {
					assert.True(t, tt.shouldBeRemote,
						"Entry should be detected as remote-only")
				} else if totalSize > 0 && len(tt.entry.Content) == 0 {
					assert.True(t, tt.shouldBeDataError,
						"Entry should be detected as data integrity error")
				} else {
					assert.True(t, tt.shouldBeEmpty,
						"Entry should be detected as empty")
				}
			} else {
				assert.False(t, tt.shouldBeRemote, "Entry with chunks should not be remote-only")
				assert.False(t, tt.shouldBeDataError, "Entry with chunks should not be data error")
				assert.False(t, tt.shouldBeEmpty, "Entry with chunks should not be empty")
			}
		})
	}
}

// TestVersionedRemoteObjectPathBuilding tests that the path building logic
// correctly handles versioned objects stored in .versions/ directory
func TestVersionedRemoteObjectPathBuilding(t *testing.T) {
	bucketsPath := "/buckets"

	tests := []struct {
		name         string
		bucket       string
		object       string
		versionId    string
		expectedDir  string
		expectedName string
	}{
		{
			name:         "non-versioned object (empty versionId)",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "",
			expectedDir:  "/buckets/mybucket",
			expectedName: "myobject.txt",
		},
		{
			name:         "null version",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "null",
			expectedDir:  "/buckets/mybucket",
			expectedName: "myobject.txt",
		},
		{
			name:         "specific version",
			bucket:       "mybucket",
			object:       "myobject.txt",
			versionId:    "abc123",
			expectedDir:  "/buckets/mybucket/myobject.txt" + s3_constants.VersionsFolder,
			expectedName: "v_abc123",
		},
		{
			name:         "nested object with version",
			bucket:       "mybucket",
			object:       "folder/subfolder/file.txt",
			versionId:    "xyz789",
			expectedDir:  "/buckets/mybucket/folder/subfolder/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_xyz789",
		},
		{
			name:         "object with leading slash and version",
			bucket:       "mybucket",
			object:       "/path/to/file.txt",
			versionId:    "ver456",
			expectedDir:  "/buckets/mybucket/path/to/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_ver456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dir, name string

			// This mirrors the logic in cacheRemoteObjectForStreaming
			if tt.versionId != "" && tt.versionId != "null" {
				// Versioned object path
				normalizedObject := strings.TrimPrefix(removeDuplicateSlashesTest(tt.object), "/")
				dir = bucketsPath + "/" + tt.bucket + "/" + normalizedObject + s3_constants.VersionsFolder
				name = "v_" + tt.versionId
			} else {
				// Non-versioned path (simplified - just for testing)
				dir = bucketsPath + "/" + tt.bucket
				normalizedObject := strings.TrimPrefix(removeDuplicateSlashesTest(tt.object), "/")
				if idx := strings.LastIndex(normalizedObject, "/"); idx > 0 {
					dir = dir + "/" + normalizedObject[:idx]
					name = normalizedObject[idx+1:]
				} else {
					name = normalizedObject
				}
			}

			assert.Equal(t, tt.expectedDir, dir, "Directory path should match")
			assert.Equal(t, tt.expectedName, name, "Name should match")
		})
	}
}

// removeDuplicateSlashesTest is a test helper that mirrors production code
func removeDuplicateSlashesTest(s string) string {
	for strings.Contains(s, "//") {
		s = strings.ReplaceAll(s, "//", "/")
	}
	return s
}

// TestResolvedSourceVersionId pins that latest-version reads in a versioning-
// enabled bucket fall back to the entry's recorded version id when the
// request carried none, so cache paths target .versions/v_<id> correctly.
func TestResolvedSourceVersionId(t *testing.T) {
	tests := []struct {
		name      string
		requested string
		entry     *filer_pb.Entry
		expected  string
	}{
		{
			name:      "explicit request versionId wins",
			requested: "abc123",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey: []byte("ignored"),
				},
			},
			expected: "abc123",
		},
		{
			name:      "empty request falls back to entry version (latest in versioned bucket)",
			requested: "",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey: []byte("xyz789"),
				},
			},
			expected: "xyz789",
		},
		{
			name:      "empty request and pre-versioning entry stays empty",
			requested: "",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{},
			},
			expected: "",
		},
		{
			name:      "empty request and nil Extended stays empty",
			requested: "",
			entry:     &filer_pb.Entry{Extended: nil},
			expected:  "",
		},
		{
			name:      "nil entry tolerated when no request version",
			requested: "",
			entry:     nil,
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, resolvedSourceVersionId(tt.requested, tt.entry))
		})
	}
}

func TestCachedEntryHasLocalData(t *testing.T) {
	tests := []struct {
		name     string
		entry    *filer_pb.Entry
		expected bool
	}{
		{
			name:     "nil entry is not a hit",
			entry:    nil,
			expected: false,
		},
		{
			name: "entry with chunks is a hit",
			entry: &filer_pb.Entry{
				Chunks: []*filer_pb.FileChunk{{FileId: "1,abc", Size: 10}},
			},
			expected: true,
		},
		{
			name: "entry with inline content is a hit",
			entry: &filer_pb.Entry{
				Content: []byte("small file body"),
			},
			expected: true,
		},
		{
			name:     "entry with neither chunks nor content is not a hit",
			entry:    &filer_pb.Entry{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, cachedEntryHasLocalData(tt.entry))
		})
	}
}

// TestCopyObjectRemoteOnlySourceDetection guards against regressing the
// remote-only source case: such a source used to fall through to
// CopyObject's inline branch with empty Content, producing a destination
// with FileSize > 0 but no chunks.
func TestCopyObjectRemoteOnlySourceDetection(t *testing.T) {
	tests := []struct {
		name                   string
		entry                  *filer_pb.Entry
		expectRemoteOnly       bool
		expectInlineBranchHit  bool
		expectBrokenWithoutFix bool
	}{
		{
			name: "remote-only object with size and no chunks/content",
			entry: &filer_pb.Entry{
				Name: "file-1234-audio.mp3",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 16018804,
				},
				Chunks:  nil,
				Content: nil,
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 16018804,
				},
			},
			expectRemoteOnly:       true,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: true,
		},
		{
			name: "local file with chunks - copy works fine, fix does not engage",
			entry: &filer_pb.Entry{
				Name: "local.bin",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 1024,
				},
				Chunks: []*filer_pb.FileChunk{
					{FileId: "1,abc", Size: 1024, Offset: 0},
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  false,
			expectBrokenWithoutFix: false,
		},
		{
			name: "small inline file (no chunks, has Content) - hits inline branch but not broken",
			entry: &filer_pb.Entry{
				Name: "tiny.txt",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 5,
				},
				Content: []byte("hello"),
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: false,
		},
		{
			name: "remote entry already cached (has chunks) - fix does not engage",
			entry: &filer_pb.Entry{
				Name: "cached.dat",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 2048,
				},
				Chunks: []*filer_pb.FileChunk{
					{FileId: "2,def", Size: 2048, Offset: 0},
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 2048,
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  false,
			expectBrokenWithoutFix: false,
		},
		{
			// Zero-byte remote objects must not enter the cache branch:
			// IsInRemoteOnly requires RemoteSize > 0, so CopyObject's
			// pre-existing inline branch handles them with no 503.
			name: "zero-byte remote object - fix does not engage, inline branch handles it",
			entry: &filer_pb.Entry{
				Name: "empty-on-remote.txt",
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 0,
				},
				RemoteEntry: &filer_pb.RemoteEntry{
					RemoteSize: 0,
				},
			},
			expectRemoteOnly:       false,
			expectInlineBranchHit:  true,
			expectBrokenWithoutFix: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectRemoteOnly, tt.entry.IsInRemoteOnly())

			// Mirror the inline branch in s3api_object_handlers_copy.go:
			//   if entry.Attributes.FileSize == 0 || len(entry.GetChunks()) == 0
			inlineBranchHit := tt.entry.Attributes != nil &&
				(tt.entry.Attributes.FileSize == 0 || len(tt.entry.GetChunks()) == 0)
			assert.Equal(t, tt.expectInlineBranchHit, inlineBranchHit)

			// The broken shape: inline branch fires, no inline content, FileSize > 0.
			brokenWithoutFix := inlineBranchHit &&
				len(tt.entry.Content) == 0 &&
				tt.entry.Attributes != nil &&
				tt.entry.Attributes.FileSize > 0
			assert.Equal(t, tt.expectBrokenWithoutFix, brokenWithoutFix)
		})
	}
}

// fakeCacheFiler is a minimal SeaweedFiler gRPC server whose
// CacheRemoteObjectToLocalCluster behavior is driven by a callback, so tests can
// model a slow (still-caching) filer or one that returns cached chunks. The
// optional entries map serves LookupDirectoryEntry for inline-content paths
// such as the /etc/remote configuration.
type fakeCacheFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	cache   func(context.Context, *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error)
	entries map[string][]byte
}

func (f *fakeCacheFiler) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	return f.cache(ctx, req)
}

func (f *fakeCacheFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if content, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: req.Name, Content: content}}, nil
	}
	return nil, filer_pb.ErrNotFound
}

// startFakeCacheFiler serves impl on a random localhost port and returns the
// S3-style filer address whose ToGrpcAddress resolves back to that port.
func startFakeCacheFiler(t *testing.T, impl *fakeCacheFiler) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, impl)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	port := lis.Addr().(*net.TCPAddr).Port
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", port))
}

func newRemoteCacheTestServer(filerAddr pb.ServerAddress) *S3ApiServer {
	return &S3ApiServer{
		option: &S3ApiServerOption{
			Filers:         []pb.ServerAddress{filerAddr},
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
}

func remoteOnlyEntry(name string, size int64) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: size}}
}

// TestCacheRemoteObjectForStreamingTimeout pins the fix for large-file cold
// GetObject: when the filer is still caching, the streaming path returns nil
// within its bound (so the handler emits 503 + Retry-After) instead of blocking
// on the raw request context until the client gives up.
func TestCacheRemoteObjectForStreamingTimeout(t *testing.T) {
	filerAddr := startFakeCacheFiler(t, &fakeCacheFiler{
		cache: func(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
			<-ctx.Done() // never finishes within the bound
			return nil, ctx.Err()
		},
	})
	s3a := newRemoteCacheTestServer(filerAddr)

	const shortTimeout = 200 * time.Millisecond
	defer func(prev int64) { atomic.StoreInt64(&remoteCacheStreamingTimeoutNS, prev) }(atomic.LoadInt64(&remoteCacheStreamingTimeoutNS))
	atomic.StoreInt64(&remoteCacheStreamingTimeoutNS, int64(shortTimeout))

	r := httptest.NewRequest(http.MethodGet, "/mybucket/large.bin", nil)
	start := time.Now()
	got := s3a.cacheRemoteObjectForStreaming(r, remoteOnlyEntry("large.bin", 1<<30), "mybucket", "large.bin", "")
	elapsed := time.Since(start)

	assert.Nil(t, got, "uncached object must return nil so the caller maps to 503")
	assert.GreaterOrEqual(t, elapsed, shortTimeout/2, "must wait on the bounded timeout, not return early on a setup error")
	assert.Less(t, elapsed, 5*time.Second, "must not block on the full download")
	assert.NoError(t, r.Context().Err(), "request context stays alive so the caller chooses 503, not cancellation")
}

// TestCacheRemoteObjectForStreamingCached confirms that once the filer reports
// local chunks, the streaming path returns the cached entry to stream from.
func TestCacheRemoteObjectForStreamingCached(t *testing.T) {
	filerAddr := startFakeCacheFiler(t, &fakeCacheFiler{
		cache: func(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
			return &filer_pb.CacheRemoteObjectToLocalClusterResponse{
				Entry: &filer_pb.Entry{
					Name:   req.Name,
					Chunks: []*filer_pb.FileChunk{{FileId: "1,abc", Size: 100, Offset: 0}},
				},
			}, nil
		},
	})
	s3a := newRemoteCacheTestServer(filerAddr)

	r := httptest.NewRequest(http.MethodGet, "/mybucket/obj.bin", nil)
	got := s3a.cacheRemoteObjectForStreaming(r, remoteOnlyEntry("obj.bin", 100), "mybucket", "obj.bin", "")

	require.NotNil(t, got)
	assert.Len(t, got.GetChunks(), 1)
}

// fakeStreamRemoteClient records the ReadFileAsStream call and serves from an
// in-memory byte slice. The embedded nil interface panics on any other call.
type fakeStreamRemoteClient struct {
	remote_storage.RemoteStorageClient
	data      []byte
	gotLoc    *remote_pb.RemoteStorageLocation
	gotOffset int64
	gotSize   int64
}

func (c *fakeStreamRemoteClient) ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (io.ReadCloser, error) {
	c.gotLoc, c.gotOffset, c.gotSize = loc, offset, size
	end := min(offset+size, int64(len(c.data)))
	return io.NopCloser(bytes.NewReader(c.data[offset:end])), nil
}

type fakeStreamRemoteMaker struct{ client *fakeStreamRemoteClient }

func (m *fakeStreamRemoteMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return m.client, nil
}
func (m *fakeStreamRemoteMaker) HasBucket() bool { return true }

// startStreamThroughFiler serves a filer whose cache RPC always fails with
// cacheErr and whose /etc/remote mounts /buckets/mybucket on a fake origin.
func startStreamThroughFiler(t *testing.T, remoteName string, cacheErr error) pb.ServerAddress {
	mappingBytes, err := proto.Marshal(&remote_pb.RemoteStorageMapping{
		Mappings: map[string]*remote_pb.RemoteStorageLocation{
			"/buckets/mybucket": {Name: remoteName, Bucket: "origin-bucket", Path: "/data"},
		},
	})
	require.NoError(t, err)
	confBytes, err := proto.Marshal(&remote_pb.RemoteConf{Name: remoteName, Type: "faketest"})
	require.NoError(t, err)
	return startFakeCacheFiler(t, &fakeCacheFiler{
		cache: func(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
			return nil, cacheErr
		},
		entries: map[string][]byte{
			"/etc/remote/mount.mapping":           mappingBytes,
			"/etc/remote/" + remoteName + ".conf": confBytes,
		},
	})
}

var stillCachingErr = status.Error(codes.DeadlineExceeded, "still caching")

// TestS3ColdReadStreamsFromOrigin pins the stream-through path: when the filer
// reports the cache is still filling, the GET is served straight from the
// mounted origin instead of a 503 retry loop.
func TestS3ColdReadStreamsFromOrigin(t *testing.T) {
	content := []byte("0123456789")
	client := &fakeStreamRemoteClient{data: content}
	remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}

	entry := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:        "obj.bin",
			Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(content))},
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: int64(len(content))},
		}
	}

	t.Run("full read", func(t *testing.T) {
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-full", stillCachingErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, content, w.Body.Bytes())
		require.NotNil(t, client.gotLoc, "must read from the origin")
		assert.Equal(t, "origin-bucket", client.gotLoc.Bucket)
		assert.Equal(t, "/data/dir/obj.bin", client.gotLoc.Path)
		assert.Equal(t, strconv.Itoa(len(content)), w.Header().Get("Content-Length"))
	})

	t.Run("range read", func(t *testing.T) {
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-range", stillCachingErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		r.Header.Set("Range", "bytes=2-5")

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, content[2:6], w.Body.Bytes())
		assert.Equal(t, int64(2), client.gotOffset)
		assert.Equal(t, int64(4), client.gotSize)
		assert.Equal(t, fmt.Sprintf("bytes 2-5/%d", len(content)), w.Header().Get("Content-Range"))
	})

	t.Run("short origin read fails instead of silently truncating", func(t *testing.T) {
		shortClient := &fakeStreamRemoteClient{data: content[:4]}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: shortClient}
		defer func() {
			remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		}()
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-short", stillCachingErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "")

		require.Error(t, err)
		assert.Contains(t, err.Error(), io.ErrUnexpectedEOF.Error())
	})

	t.Run("version-specific read keeps 503 retry", func(t *testing.T) {
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-versioned", stillCachingErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin?versionId=v123", nil)

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "v123")

		require.Error(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Equal(t, "2", w.Header().Get("Retry-After"))
	})

	t.Run("latest read of a versioned entry keeps 503 retry", func(t *testing.T) {
		freshClient := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: freshClient}
		defer func() {
			remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		}()
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-latest-versioned", stillCachingErr))
		versionedEntry := entry()
		versionedEntry.Extended = map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v456")}
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, versionedEntry, "", "mybucket", "dir/obj.bin", "")

		require.Error(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Nil(t, freshClient.gotLoc, "the unversioned origin key must not be read for a versioned entry")
	})

	t.Run("local cache failure falls back to origin", func(t *testing.T) {
		cacheErr := status.Error(codes.Internal, "assign: no free volumes")
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-localfail", cacheErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, content, w.Body.Bytes())
	})

	t.Run("entry not found stays 404", func(t *testing.T) {
		cacheErr := status.Error(codes.NotFound, "entry vanished")
		s3a := newRemoteCacheTestServer(startStreamThroughFiler(t, "faketest-notfound", cacheErr))
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, entry(), "", "mybucket", "dir/obj.bin", "")

		require.Error(t, err)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}
