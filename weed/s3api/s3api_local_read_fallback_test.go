package s3api

import (
	"bytes"
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

	t.Run("final byte with trailing EOF counts as readable", func(t *testing.T) {
		assert.NoError(t, probeReadable(context.Background(), stubReaderAt{n: 1, err: io.EOF}, 0, time.Second))
	})

	t.Run("zero-byte EOF is unreadable", func(t *testing.T) {
		assert.Error(t, probeReadable(context.Background(), stubReaderAt{n: 0, err: io.EOF}, 0, time.Second))
	})

	t.Run("zero-byte read without error is unreadable", func(t *testing.T) {
		assert.ErrorIs(t, probeReadable(context.Background(), stubReaderAt{n: 0, err: nil}, 0, time.Second), io.ErrUnexpectedEOF)
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

// truncatedReaderAt serves the first breakAt bytes and then fails, standing in
// for a multi-chunk object whose later chunk's volume server is unreachable.
type truncatedReaderAt struct {
	data    []byte
	breakAt int64
	err     error
}

func (t truncatedReaderAt) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= t.breakAt {
		return 0, t.err
	}
	return copy(p, t.data[offset:t.breakAt]), nil
}

// failingWriter stands in for a client that disconnected mid-response.
type failingWriter struct{ err error }

func (f failingWriter) Write(p []byte) (int, error) { return 0, f.err }

// TestStreamRangeToClientFinishesFromRemote covers the window the pre-flight
// probe cannot: the byte at the requested offset reads fine, the response is
// committed, and only then does a later chunk turn out to be unreadable.
func TestStreamRangeToClientFinishesFromRemote(t *testing.T) {
	content := []byte("0123456789")
	size := int64(len(content))
	newServer := func(t *testing.T, name string, enabled bool) (*S3ApiServer, *fakeStreamRemoteClient) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		return newLocalReadFallbackServer(t, startStreamThroughFiler(t, name, nil), enabled), client
	}

	t.Run("later chunk failure is finished from the remote", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstream", true)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		local := truncatedReaderAt{data: content, breakAt: 4, err: errors.New("volume: connection refused")}

		written, err := s3a.streamRangeToClient(w, r, local, cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		require.NoError(t, err)
		assert.Equal(t, size, written)
		assert.Equal(t, content, w.Body.Bytes())
		assert.Equal(t, int64(4), client.gotOffset, "the remote must resume where the local copy stopped")
		assert.Equal(t, int64(6), client.gotSize)
	})

	t.Run("a short local read reported as clean EOF is not served truncated", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-shortread", true)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		local := truncatedReaderAt{data: content, breakAt: 7, err: io.EOF}

		written, err := s3a.streamRangeToClient(w, r, local, cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		require.NoError(t, err)
		assert.Equal(t, size, written)
		assert.Equal(t, content, w.Body.Bytes())
		assert.Equal(t, int64(7), client.gotOffset)
	})

	t.Run("range read resumes at the absolute offset", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamrange", true)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		local := truncatedReaderAt{data: content, breakAt: 5, err: errors.New("volume: connection refused")}

		written, err := s3a.streamRangeToClient(w, r, local, cachedEntry(content), "mybucket", "dir/obj.bin", 2, 6, size, "")

		require.NoError(t, err)
		assert.Equal(t, int64(6), written)
		assert.Equal(t, content[2:8], w.Body.Bytes())
		assert.Equal(t, int64(5), client.gotOffset)
		assert.Equal(t, int64(3), client.gotSize)
	})

	t.Run("disabled keeps the mid-stream error", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamoff", false)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		boom := errors.New("volume: connection refused")

		written, err := s3a.streamRangeToClient(w, r, truncatedReaderAt{data: content, breakAt: 4, err: boom}, cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		assert.ErrorIs(t, err, boom)
		assert.Equal(t, int64(4), written)
		assert.Nil(t, client.gotLoc, "must not read from the remote when fallback is disabled")
	})

	t.Run("a failed write to the client is not refetched from the remote", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamwrite", true)
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		broken := errors.New("write: broken pipe")

		written, err := s3a.streamRangeToClient(failingWriter{err: broken}, r, bytes.NewReader(content), cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		assert.ErrorIs(t, err, broken)
		assert.Zero(t, written)
		assert.Nil(t, client.gotLoc, "the client is gone; refetching from the remote is wasted work")
	})
}
