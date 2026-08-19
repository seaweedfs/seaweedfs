package s3api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubReaderAt returns a fixed result, standing in for ChunkReadAt in probe unit tests.
type stubReaderAt struct {
	n   int
	err error
}

func (s stubReaderAt) ReadAtWithTime(ctx context.Context, p []byte, offset int64) (int, int64, error) {
	return s.n, 0, s.err
}

// blockingReaderAt never returns until the context is cancelled.
type blockingReaderAt struct{}

func (blockingReaderAt) ReadAtWithTime(ctx context.Context, p []byte, offset int64) (int, int64, error) {
	<-ctx.Done()
	return 0, 0, ctx.Err()
}

func TestProbeReadable(t *testing.T) {
	t.Run("readable local copy", func(t *testing.T) {
		assert.NoError(t, probeReadable(context.Background(), stubReaderAt{n: 1}, 0, time.Second))
	})

	t.Run("clean EOF counts as readable", func(t *testing.T) {
		assert.NoError(t, probeReadable(context.Background(), stubReaderAt{err: io.EOF}, 0, time.Second))
	})

	t.Run("read error surfaces", func(t *testing.T) {
		boom := errors.New("volume: connection refused")
		assert.ErrorIs(t, probeReadable(context.Background(), stubReaderAt{err: boom}, 0, time.Second), boom)
	})

	t.Run("stuck volume trips the timeout instead of blocking", func(t *testing.T) {
		start := time.Now()
		err := probeReadable(context.Background(), blockingReaderAt{}, 0, 50*time.Millisecond)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Less(t, time.Since(start), time.Second, "must not block past the probe timeout")
	})
}

func TestShouldFallBackToRemote(t *testing.T) {
	const size = int64(100)
	remoteEntry := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(size)},
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: size},
		}
	}
	server := func(enabled bool) *S3ApiServer {
		return &S3ApiServer{option: &S3ApiServerOption{LocalReadFallbackToRemote: enabled}}
	}

	t.Run("disabled", func(t *testing.T) {
		assert.False(t, server(false).shouldFallBackToRemote(remoteEntry(), size, ""))
	})

	t.Run("enabled with matching remote", func(t *testing.T) {
		assert.True(t, server(true).shouldFallBackToRemote(remoteEntry(), size, ""))
	})

	t.Run("enabled treats null versionId as unversioned", func(t *testing.T) {
		assert.True(t, server(true).shouldFallBackToRemote(remoteEntry(), size, "null"))
	})

	t.Run("versioned read cannot use the unversioned remote key", func(t *testing.T) {
		assert.False(t, server(true).shouldFallBackToRemote(remoteEntry(), size, "v123"))
	})

	t.Run("latest read that resolves to a version cannot use the unversioned key", func(t *testing.T) {
		versioned := remoteEntry()
		versioned.Extended = map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v9")}
		assert.False(t, server(true).shouldFallBackToRemote(versioned, size, ""))
	})

	t.Run("no remote entry", func(t *testing.T) {
		local := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: uint64(size)}}
		assert.False(t, server(true).shouldFallBackToRemote(local, size, ""))
	})

	t.Run("size mismatch is not served as identical bytes", func(t *testing.T) {
		assert.False(t, server(true).shouldFallBackToRemote(remoteEntry(), size+1, ""))
	})
}

// newLocalReadFallbackServer wires a real ReaderCache against the fake filer.
// The fake filer does not implement LookupVolume, so any chunk read fails,
// standing in for an unreachable/evicted volume server.
func newLocalReadFallbackServer(t *testing.T, filerAddr pb.ServerAddress, enabled bool) *S3ApiServer {
	t.Helper()
	s3a := newRemoteCacheTestServer(filerAddr)
	s3a.option.LocalReadFallbackToRemote = enabled
	s3a.option.LocalReadFallbackTimeout = 300 * time.Millisecond
	fc := wdclient.NewFilerClient(s3a.option.Filers, s3a.option.GrpcDialOption, s3a.option.DataCenter)
	s3a.filerClient = fc
	s3a.readerCache = filer.NewReaderCache(8, (*chunk_cache.TieredChunkCache)(nil), fc.GetLookupFileIdFunction(), fc)
	return s3a
}

// cachedEntry has local chunks (pointing at a volume that will fail to resolve)
// plus a RemoteEntry mirroring its size — the shape of a remote-mounted object
// whose cached copy is present in metadata but unreadable from volume servers.
func cachedEntry(content []byte) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        "obj.bin",
		Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(content))},
		Chunks:      []*filer_pb.FileChunk{{FileId: "1,0123456789ab", Size: uint64(len(content)), Offset: 0}},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: int64(len(content))},
	}
}

func TestS3CachedReadFallsBackToRemote(t *testing.T) {
	content := []byte("0123456789")

	t.Run("unreadable local copy is served from the mounted remote", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedfail", nil), true)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, cachedEntry(content), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, content, w.Body.Bytes())
		require.NotNil(t, client.gotLoc, "must read from the mounted remote")
		assert.Equal(t, "/data/dir/obj.bin", client.gotLoc.Path)
	})

	t.Run("range fallback serves the requested window from the remote", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedrange", nil), true)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		r.Header.Set("Range", "bytes=2-5")

		err := s3a.streamFromVolumeServers(w, r, cachedEntry(content), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, content[2:6], w.Body.Bytes())
		assert.Equal(t, int64(2), client.gotOffset)
		assert.Equal(t, int64(4), client.gotSize)
	})

	t.Run("disabled keeps the existing error and never reads the remote", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-nofallback", nil), false)
		w := httptest.NewRecorder()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil).WithContext(ctx)

		err := s3a.streamFromVolumeServers(w, r, cachedEntry(content), "", "mybucket", "dir/obj.bin", "")

		require.Error(t, err)
		assert.Nil(t, client.gotLoc, "must not read from the remote when fallback is disabled")
	})

	t.Run("versioned read does not fall back to the unversioned remote key", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedversioned", nil), true)
		versioned := cachedEntry(content)
		versioned.Extended = map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v456")}
		w := httptest.NewRecorder()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin?versionId=v456", nil).WithContext(ctx)

		err := s3a.streamFromVolumeServers(w, r, versioned, "", "mybucket", "dir/obj.bin", "v456")

		require.Error(t, err)
		assert.Nil(t, client.gotLoc, "a versioned read must not serve the unversioned remote object")
	})
}
