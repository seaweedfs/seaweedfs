package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const appendRepresentationPath = "/data/append.txt"

func appendRepresentationRequest(directory, name string) *filer_pb.AppendToEntryRequest {
	return &filer_pb.AppendToEntryRequest{
		Directory: directory,
		EntryName: name,
		Chunks: []*filer_pb.FileChunk{{
			FileId: "1,01637037d7",
			Size:   4,
		}},
	}
}

func readInlineEntry(t *testing.T, entry *filer.Entry) []byte {
	t.Helper()
	reader := filer.NewFileReader(nil, entry.ToProtoEntry())
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return content
}

func TestAppendToEntryRefusesInlineContentBeforeChangingReadData(t *testing.T) {
	fs, store := newAppendLockServer(t)
	close(store.release)
	entry := newFileEntry(appendRepresentationPath, 1)
	entry.Content = []byte("inline-")
	entry.FileSize = uint64(len(entry.Content))
	store.entries[appendRepresentationPath] = entry
	want := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
	var operations []string
	store.afterOp = func(op string, _ util.FullPath) { operations = append(operations, op) }

	_, err := fs.AppendToEntry(context.Background(), appendRepresentationRequest("/data", "append.txt"))
	if err == nil || !strings.Contains(err.Error(), "cannot append chunks to an inline entry") {
		t.Fatalf("AppendToEntry inline error = %v, want a clear representation refusal", err)
	}

	stored, findErr := fs.filer.FindEntry(context.Background(), appendRepresentationPath)
	require.NoError(t, findErr)
	require.True(t, proto.Equal(stored.ToProtoEntry(), want), "refused append changed the stored entry")
	require.Equal(t, []byte("inline-"), readInlineEntry(t, stored), "the normal inline reader must keep returning the original bytes")
	require.Empty(t, operations, "refused append emitted a store operation")
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		t.Fatalf("refused append queued chunks for deletion: %v", ids)
	})
	operations = nil
	_, err = fs.AppendToEntry(context.Background(), &filer_pb.AppendToEntryRequest{Directory: "/data", EntryName: "append.txt"})
	require.NoError(t, err, "the path lock must be released after refusing the append")
}

func configureAppendRemoteMount(t *testing.T, fs *FilerServer, store *appendHoldStore, endpoint string) {
	t.Helper()
	cacheWaitMs := int32(0)
	mapping, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/b": {Name: "remote", Bucket: "origin", Path: "/", CacheWaitMs: &cacheWaitMs},
	}})
	require.NoError(t, err)
	conf, err := proto.Marshal(&remote_pb.RemoteConf{
		Name:             "remote",
		Type:             "s3",
		S3Endpoint:       endpoint,
		S3Region:         "us-east-1",
		S3AccessKey:      "append-access",
		S3SecretKey:      "append-secret",
		S3ForcePathStyle: true,
	})
	require.NoError(t, err)
	for name, content := range map[string][]byte{
		filer.REMOTE_STORAGE_MOUNT_FILE:             mapping,
		"remote" + filer.REMOTE_STORAGE_CONF_SUFFIX: conf,
	} {
		path := filer.DirectoryEtcRemote + "/" + name
		store.entries[path] = &filer.Entry{FullPath: util.FullPath(path), Content: content}
	}
	require.NoError(t, fs.filer.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(fs.filer))
}

func TestAppendToEntryRemoteRefusalKeepsTheRealRemoteReadPath(t *testing.T) {
	remoteData := []byte("remote-")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(remoteData)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(remoteData)
		}
	}))
	t.Cleanup(origin.Close)
	fs, store := newAppendLockServer(t)
	close(store.release)
	fs.option.AllowUntrustedRemoteEndpoints = true
	configureAppendRemoteMount(t, fs, store, origin.URL)

	const path = "/buckets/b/append.txt"
	entry := newFileEntry(path, 1)
	entry.FileSize = uint64(len(remoteData))
	entry.Remote = &filer_pb.RemoteEntry{StorageName: "remote", RemoteSize: int64(len(remoteData)), RemoteMtime: 1700000000}
	store.entries[path] = entry
	want := proto.Clone(entry.ToProtoEntry()).(*filer_pb.Entry)
	var operations []string
	store.afterOp = func(op string, _ util.FullPath) { operations = append(operations, op) }

	_, err := fs.AppendToEntry(context.Background(), appendRepresentationRequest("/buckets/b", "append.txt"))
	if err == nil || !strings.Contains(err.Error(), "cannot append chunks to a remote-only entry") {
		t.Fatalf("AppendToEntry remote-only error = %v, want a clear representation refusal", err)
	}

	stored, findErr := fs.filer.FindEntry(context.Background(), path)
	require.NoError(t, findErr)
	require.True(t, proto.Equal(stored.ToProtoEntry(), want), "refused append changed the stored entry")
	require.True(t, stored.IsInRemoteOnly(), "the normal remote reader must still see a remote-only entry")
	require.Equal(t, int64(len(remoteData)), stored.Remote.RemoteSize)
	require.Empty(t, operations, "refused append emitted a store operation")

	recorder := httptest.NewRecorder()
	fs.GetOrHeadHandler(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, remoteData, recorder.Body.Bytes(), "the normal remote reader must still return the original bytes")
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		t.Fatalf("refused append queued chunks for deletion: %v", ids)
	})
	operations = nil
	_, err = fs.AppendToEntry(context.Background(), &filer_pb.AppendToEntryRequest{Directory: "/buckets/b", EntryName: "append.txt"})
	require.NoError(t, err, "the path lock must be released after refusing the append")
}

type appendReadLookup struct {
	urls map[string]string
}

func (l appendReadLookup) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(_ context.Context, fileID string) ([]string, error) {
		url, ok := l.urls[fileID]
		if !ok {
			return nil, fmt.Errorf("unknown chunk %s", fileID)
		}
		return []string{url}, nil
	}
}

func TestAppendToEntryStillReadsOrdinaryChunkAppends(t *testing.T) {
	chunkData := map[string][]byte{
		"1,01637037d6": []byte("old-"),
		"1,01637037d7": []byte("new-"),
	}
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := chunkData[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(volume.Close)

	fs, store := newAppendLockServer(t)
	close(store.release)
	store.entries[appendRepresentationPath] = &filer.Entry{
		FullPath: util.FullPath(appendRepresentationPath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 4,
		},
		Chunks: []*filer_pb.FileChunk{{FileId: "1,01637037d6", Offset: 0, Size: 4}},
	}

	_, err := fs.AppendToEntry(context.Background(), appendRepresentationRequest("/data", "append.txt"))
	require.NoError(t, err)
	stored, err := fs.filer.FindEntry(context.Background(), appendRepresentationPath)
	require.NoError(t, err)
	require.Len(t, stored.GetChunks(), 2)
	require.Equal(t, int64(4), stored.GetChunks()[1].Offset)

	stream, err := filer.PrepareStreamContent(appendReadLookup{urls: map[string]string{
		"1,01637037d6": volume.URL + "/1,01637037d6",
		"1,01637037d7": volume.URL + "/1,01637037d7",
	}}, func(string) string { return "" }, stored.GetChunks(), 0, int64(filer.FileSize(stored.ToProtoEntry())))
	require.NoError(t, err)
	var got bytes.Buffer
	require.NoError(t, stream(&got))
	require.Equal(t, []byte("old-new-"), got.Bytes(), "the normal chunk reader must see both the existing and appended data")
}

func TestAppendToEntryEmptyRequestKeepsInlineEntry(t *testing.T) {
	fs, store := newAppendLockServer(t)
	close(store.release)
	entry := newFileEntry(appendRepresentationPath, 1)
	entry.Content = []byte("inline")
	entry.FileSize = uint64(len(entry.Content))
	store.entries[appendRepresentationPath] = entry

	_, err := fs.AppendToEntry(context.Background(), &filer_pb.AppendToEntryRequest{Directory: "/data", EntryName: "append.txt"})
	require.NoError(t, err)
	stored, findErr := fs.filer.FindEntry(context.Background(), appendRepresentationPath)
	require.NoError(t, findErr)
	require.Equal(t, []byte("inline"), readInlineEntry(t, stored))
	require.Empty(t, stored.GetChunks())
}
