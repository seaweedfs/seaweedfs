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

// stubReaderAt stands in for ChunkReadAt in probe unit tests.
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
	server := func() *S3ApiServer {
		return &S3ApiServer{option: &S3ApiServerOption{}}
	}

	t.Run("matching remote", func(t *testing.T) {
		assert.True(t, server().shouldFallBackToRemote(remoteEntry(), size, ""))
	})

	t.Run("null versionId is treated as unversioned", func(t *testing.T) {
		assert.True(t, server().shouldFallBackToRemote(remoteEntry(), size, "null"))
	})

	t.Run("versioned read cannot use the unversioned remote key", func(t *testing.T) {
		assert.False(t, server().shouldFallBackToRemote(remoteEntry(), size, "v123"))
	})

	t.Run("latest read that resolves to a version cannot use the unversioned key", func(t *testing.T) {
		versioned := remoteEntry()
		versioned.Extended = map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v9")}
		assert.False(t, server().shouldFallBackToRemote(versioned, size, ""))
	})

	t.Run("no remote entry", func(t *testing.T) {
		local := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{FileSize: uint64(size)}}
		assert.False(t, server().shouldFallBackToRemote(local, size, ""))
	})

	t.Run("size mismatch is not served as identical bytes", func(t *testing.T) {
		assert.False(t, server().shouldFallBackToRemote(remoteEntry(), size+1, ""))
	})
}

// The fake filer does not implement LookupVolume, so every chunk read through
// this ReaderCache fails, standing in for an unreachable volume server.
func newLocalReadFallbackServer(t *testing.T, filerAddr pb.ServerAddress) *S3ApiServer {
	t.Helper()
	s3a := newRemoteCacheTestServer(filerAddr)
	fc := wdclient.NewFilerClient(s3a.option.Filers, s3a.option.GrpcDialOption, s3a.option.DataCenter)
	s3a.filerClient = fc
	s3a.readerCache = filer.NewReaderCache(8, (*chunk_cache.TieredChunkCache)(nil), fc.GetLookupFileIdFunction(), fc)
	return s3a
}

// a remote-mounted object whose cached copy is in the metadata but unreadable
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
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedfail", nil))
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
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedrange", nil))
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

	t.Run("a local-only object gets a clean 500, not a broken 200 body", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-nofallback", nil))
		local := cachedEntry(content)
		local.RemoteEntry = nil
		w := httptest.NewRecorder()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil).WithContext(ctx)

		err := s3a.streamFromVolumeServers(w, r, local, "", "mybucket", "dir/obj.bin", "")

		require.Error(t, err)
		assert.Equal(t, http.StatusInternalServerError, w.Code, "the status must not be committed before the first read succeeds")
		assert.Contains(t, w.Body.String(), "InternalError")
		assert.Nil(t, client.gotLoc, "an object that is not remote-mounted has no remote to fall back to")
	})

	t.Run("versioned read does not fall back to the unversioned remote key", func(t *testing.T) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, "faketest-cachedversioned", nil))
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

// the deferred status commit must be invisible on the success path: a readable
// local object still streams with the same status and headers as before
func TestS3LocalReadCommitsStatusOnFirstWrite(t *testing.T) {
	content := []byte("0123456789")
	newLocalServer := func(t *testing.T, name string) *S3ApiServer {
		s3a := newLocalReadFallbackServer(t, startStreamThroughFiler(t, name, nil))
		cache := chunk_cache.NewChunkCacheInMemory(16)
		cache.SetChunk("1,0123456789ab", content)
		s3a.readerCache = filer.NewReaderCache(8, cache, s3a.filerClient.GetLookupFileIdFunction(), s3a.filerClient)
		return s3a
	}
	localEntry := func() *filer_pb.Entry {
		local := cachedEntry(content)
		local.RemoteEntry = nil
		return local
	}

	t.Run("readable local object streams a 200", func(t *testing.T) {
		s3a := newLocalServer(t, "faketest-localok")
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)

		err := s3a.streamFromVolumeServers(w, r, localEntry(), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, content, w.Body.Bytes())
		assert.Equal(t, "10", w.Header().Get("Content-Length"))
	})

	t.Run("readable range streams a 206 with range headers", func(t *testing.T) {
		s3a := newLocalServer(t, "faketest-localrange")
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		r.Header.Set("Range", "bytes=2-5")

		err := s3a.streamFromVolumeServers(w, r, localEntry(), "", "mybucket", "dir/obj.bin", "")

		require.NoError(t, err)
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, content[2:6], w.Body.Bytes())
		assert.Equal(t, "bytes 2-5/10", w.Header().Get("Content-Range"))
		assert.Equal(t, "4", w.Header().Get("Content-Length"))
	})
}

// a multi-chunk object whose later chunk's volume server is unreachable
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

// a client that disconnected mid-response
type failingWriter struct{ err error }

func (f failingWriter) Write(p []byte) (int, error) { return 0, f.err }

// the window the probe cannot cover: the byte at offset reads fine, the response
// is committed, and only then does a later chunk turn out to be unreadable
func TestStreamRangeToClientFinishesFromRemote(t *testing.T) {
	content := []byte("0123456789")
	size := int64(len(content))
	newServer := func(t *testing.T, name string) (*S3ApiServer, *fakeStreamRemoteClient) {
		client := &fakeStreamRemoteClient{data: content}
		remote_storage.RemoteStorageClientMakers["faketest"] = &fakeStreamRemoteMaker{client: client}
		return newLocalReadFallbackServer(t, startStreamThroughFiler(t, name, nil)), client
	}

	t.Run("later chunk failure is finished from the remote", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstream")
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
		s3a, client := newServer(t, "faketest-shortread")
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
		s3a, client := newServer(t, "faketest-midstreamrange")
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

	t.Run("a remote overwritten since caching is not spliced onto the local prefix", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamchanged")
		client.stat = &filer_pb.RemoteEntry{RemoteSize: size, RemoteETag: "reuploaded"}
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		boom := errors.New("volume: connection refused")

		written, err := s3a.streamRangeToClient(w, r, truncatedReaderAt{data: content, breakAt: 4, err: boom}, cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		require.Error(t, err)
		assert.Equal(t, int64(4), written, "a truncated body beats one mixing two generations")
		assert.Nil(t, client.gotLoc, "must not read a remote that no longer matches what was cached")
	})

	t.Run("a local-only object keeps the mid-stream error", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamlocal")
		local := cachedEntry(content)
		local.RemoteEntry = nil
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		boom := errors.New("volume: connection refused")

		written, err := s3a.streamRangeToClient(w, r, truncatedReaderAt{data: content, breakAt: 4, err: boom}, local, "mybucket", "dir/obj.bin", 0, size, size, "")

		assert.ErrorIs(t, err, boom)
		assert.Equal(t, int64(4), written)
		assert.Nil(t, client.gotLoc, "an object that is not remote-mounted has no remote to fall back to")
	})

	t.Run("a failed write to the client is not refetched from the remote", func(t *testing.T) {
		s3a, client := newServer(t, "faketest-midstreamwrite")
		r := httptest.NewRequest(http.MethodGet, "/mybucket/dir/obj.bin", nil)
		broken := errors.New("write: broken pipe")

		written, err := s3a.streamRangeToClient(failingWriter{err: broken}, r, bytes.NewReader(content), cachedEntry(content), "mybucket", "dir/obj.bin", 0, size, size, "")

		assert.ErrorIs(t, err, broken)
		assert.Zero(t, written)
		assert.Nil(t, client.gotLoc, "the client is gone; refetching from the remote is wasted work")
	})
}
