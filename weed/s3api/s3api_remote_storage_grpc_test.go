package s3api

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// stubFilerServer is a minimal filer used by the tests to drive the cache
// helpers through real gRPC. It captures CacheRemoteObjectToLocalCluster
// requests and returns whatever the test programs into cacheResponse /
// cacheError. Only the methods the cache helpers reach are implemented;
// everything else returns Unimplemented.
type stubFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu             sync.Mutex
	cacheCalls     []*filer_pb.CacheRemoteObjectToLocalClusterRequest
	cacheResponse  *filer_pb.CacheRemoteObjectToLocalClusterResponse
	cacheError     error
	cacheBlockUntil chan struct{}
}

func (s *stubFilerServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	s.mu.Lock()
	s.cacheCalls = append(s.cacheCalls, req)
	resp := s.cacheResponse
	err := s.cacheError
	block := s.cacheBlockUntil
	s.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return resp, err
}

func (s *stubFilerServer) lastCacheCall(t *testing.T) *filer_pb.CacheRemoteObjectToLocalClusterRequest {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotEmpty(t, s.cacheCalls, "expected CacheRemoteObjectToLocalCluster to be called")
	return s.cacheCalls[len(s.cacheCalls)-1]
}

// startStubFiler spins up an in-process filer gRPC server and returns an
// S3ApiServer wired to dial it. The server's responses are configured by
// mutating fields on the returned stub.
func startStubFiler(t *testing.T) (*S3ApiServer, *stubFilerServer) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	stub := &stubFilerServer{}
	grpcServer := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, stub)
	go func() { _ = grpcServer.Serve(lis) }()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			Filers:         []pb.ServerAddress{pb.NewServerAddress(host, 1, grpcPort)},
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
	return s3a, stub
}

// TestCacheRemoteObjectForCopy_EndToEnd drives cacheRemoteObjectForCopy
// through real gRPC against an in-process filer stub. Every handler that
// uses the helper goes through this same code path, so the bug class
// (silently producing a metadata-only destination on cache failure) is
// covered for any current or future caller.
func TestCacheRemoteObjectForCopy_EndToEnd(t *testing.T) {
	chunkedResponse := &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry: &filer_pb.Entry{
			Name: "object",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 16 * 1024 * 1024,
			},
			Chunks: []*filer_pb.FileChunk{
				{FileId: "1,abc", Size: 16 * 1024 * 1024, Offset: 0},
			},
		},
	}
	contentResponse := &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry: &filer_pb.Entry{
			Name: "object",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 5,
			},
			Content: []byte("hello"),
		},
	}
	stillRemoteResponse := &filer_pb.CacheRemoteObjectToLocalClusterResponse{
		Entry: &filer_pb.Entry{
			Name: "object",
			Attributes: &filer_pb.FuseAttributes{
				FileSize: 16 * 1024 * 1024,
			},
			RemoteEntry: &filer_pb.RemoteEntry{
				RemoteSize: 16 * 1024 * 1024,
			},
		},
	}

	tests := []struct {
		name              string
		response          *filer_pb.CacheRemoteObjectToLocalClusterResponse
		err               error
		expectNil         bool
		expectChunkCount  int
		expectInlineBytes int
	}{
		{
			name:             "cache returns chunked entry: helper returns it",
			response:         chunkedResponse,
			expectChunkCount: 1,
		},
		{
			name:              "cache returns inline-content entry: helper returns it (relaxed contract)",
			response:          contentResponse,
			expectInlineBytes: 5,
		},
		{
			name:      "cache returns same remote-only entry (no progress): helper returns nil so handler can 503",
			response:  stillRemoteResponse,
			expectNil: true,
		},
		{
			name:      "cache returns gRPC error: helper returns nil",
			err:       status.Error(codes.Unavailable, "filer down"),
			expectNil: true,
		},
		{
			name:      "cache returns NotFound: helper returns nil",
			err:       status.Error(codes.NotFound, "no such entry"),
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3a, stub := startStubFiler(t)
			stub.cacheResponse = tt.response
			stub.cacheError = tt.err

			got := s3a.cacheRemoteObjectForCopy(context.Background(), "mybucket", "path/to/object", "")

			if tt.expectNil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tt.expectChunkCount, len(got.GetChunks()))
			assert.Equal(t, tt.expectInlineBytes, len(got.Content))
			// The handlers' invariant: a non-nil cached entry must carry data
			// the copy code can read. cachedEntryHasLocalData is the predicate
			// the helper uses; pin it here so a future relaxation that lets
			// data-less entries through breaks this test, not production.
			assert.True(t, cachedEntryHasLocalData(got),
				"helper must not return an entry without local data")
		})
	}
}

// TestCacheRemoteObjectForCopy_VersionedPath verifies that a non-empty
// versionId routes the cache call to the .versions/v_<id> directory.
// Without this, latest-version reads in a versioning-enabled bucket
// would 503 forever (the original review feedback that drove the
// resolvedSourceVersionId helper).
func TestCacheRemoteObjectForCopy_VersionedPath(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		object       string
		versionId    string
		expectedDir  string
		expectedName string
	}{
		{
			name:         "non-versioned: bucket-relative path",
			bucket:       "mybucket",
			object:       "folder/file.txt",
			versionId:    "",
			expectedDir:  "/buckets/mybucket/folder",
			expectedName: "file.txt",
		},
		{
			name:         "null version: bucket-relative path",
			bucket:       "mybucket",
			object:       "folder/file.txt",
			versionId:    "null",
			expectedDir:  "/buckets/mybucket/folder",
			expectedName: "file.txt",
		},
		{
			name:         "specific version: .versions/v_<id> path",
			bucket:       "mybucket",
			object:       "folder/file.txt",
			versionId:    "abc123",
			expectedDir:  "/buckets/mybucket/folder/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_abc123",
		},
		{
			name:         "top-level object with version",
			bucket:       "mybucket",
			object:       "file.txt",
			versionId:    "v9",
			expectedDir:  "/buckets/mybucket/file.txt" + s3_constants.VersionsFolder,
			expectedName: "v_v9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3a, stub := startStubFiler(t)
			stub.cacheError = status.Error(codes.NotFound, "no such entry")

			_ = s3a.cacheRemoteObjectForCopy(context.Background(), tt.bucket, tt.object, tt.versionId)

			req := stub.lastCacheCall(t)
			assert.Equal(t, tt.expectedDir, req.Directory)
			assert.Equal(t, tt.expectedName, req.Name)
		})
	}
}

// TestCacheRemoteObjectForCopy_DeadlineExceeded verifies that a stuck cache
// gets unblocked by the helper's bounded timeout, returns nil, and lets the
// handler 503 instead of holding the request open indefinitely.
func TestCacheRemoteObjectForCopy_DeadlineExceeded(t *testing.T) {
	s3a, stub := startStubFiler(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	stub.cacheBlockUntil = block

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	got := s3a.cacheRemoteObjectForCopy(ctx, "mybucket", "object", "")
	assert.Nil(t, got)
}

// TestCacheRemoteObjectForStreaming_ChunksOnly pins the streaming helper's
// stricter contract: an inline-content cache result is NOT propagated,
// because streamFromVolumeServers' downstream isn't wired to read from
// inline Content here. Letting that drift is exactly what CodeRabbit
// flagged on this PR; this test makes the asymmetry executable.
func TestCacheRemoteObjectForStreaming_ChunksOnly(t *testing.T) {
	tests := []struct {
		name      string
		response  *filer_pb.CacheRemoteObjectToLocalClusterResponse
		expectNil bool
	}{
		{
			name: "chunked response is propagated",
			response: &filer_pb.CacheRemoteObjectToLocalClusterResponse{
				Entry: &filer_pb.Entry{
					Chunks: []*filer_pb.FileChunk{{FileId: "1,abc", Size: 10}},
				},
			},
			expectNil: false,
		},
		{
			name: "content-only response is NOT propagated",
			response: &filer_pb.CacheRemoteObjectToLocalClusterResponse{
				Entry: &filer_pb.Entry{
					Content: []byte("hello"),
				},
			},
			expectNil: true,
		},
		{
			name: "empty response is NOT propagated",
			response: &filer_pb.CacheRemoteObjectToLocalClusterResponse{
				Entry: &filer_pb.Entry{},
			},
			expectNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3a, stub := startStubFiler(t)
			stub.cacheResponse = tt.response

			r, err := http.NewRequest(http.MethodGet, "/mybucket/object", nil)
			require.NoError(t, err)
			entry := &filer_pb.Entry{
				RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 1024},
			}
			got := s3a.cacheRemoteObjectForStreaming(r, entry, "mybucket", "object", "")

			if tt.expectNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Greater(t, len(got.GetChunks()), 0)
			}
		})
	}
}
