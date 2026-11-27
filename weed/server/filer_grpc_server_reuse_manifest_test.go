package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// reuseManifestMaster assigns file ids on volume 1 and tells its clients that
// volume 1 is on volumeHost.
type reuseManifestMaster struct {
	master_pb.UnimplementedSeaweedServer
	volumeHost string
	next       atomic.Uint32
}

func (m *reuseManifestMaster) KeepConnected(stream grpc.BidiStreamingServer[master_pb.KeepConnectedRequest, master_pb.KeepConnectedResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{
		Url:       m.volumeHost,
		PublicUrl: m.volumeHost,
		NewVids:   []uint32{1},
	}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (m *reuseManifestMaster) Assign(context.Context, *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	return &master_pb.AssignResponse{
		Fid:      fmt.Sprintf("1,%x%08x", m.next.Add(1), 0xc0de),
		Count:    1,
		Location: &master_pb.Location{Url: m.volumeHost, PublicUrl: m.volumeHost},
	}, nil
}

// reuseManifestVolume keeps what the filer uploads and serves it back.
type reuseManifestVolume struct {
	mu      sync.Mutex
	blobs   map[string][]byte
	uploads int
}

func (v *reuseManifestVolume) uploadCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.uploads
}

func newReuseManifestServer(t *testing.T) (*FilerServer, *reuseManifestVolume) {
	t.Helper()
	volume := &reuseManifestVolume{blobs: map[string][]byte{}}
	volumeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The filer may spell the file id it was assigned differently.
		fid := filer.ChunkIdentity(strings.TrimPrefix(r.URL.Path, "/"))
		if r.Method == http.MethodGet {
			volume.mu.Lock()
			data, found := volume.blobs[fid]
			volume.mu.Unlock()
			if !found {
				http.NotFound(w, r)
				return
			}
			w.Write(data)
			return
		}
		multipartReader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := multipartReader.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		volume.mu.Lock()
		volume.blobs[fid] = data
		volume.uploads++
		volume.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"name":"chunk","size":%d}`, len(data))
	}))
	t.Cleanup(volumeServer.Close)
	volumeHost := strings.TrimPrefix(volumeServer.URL, "http://")

	f := newRenameTestFiler(t, newRenameTestStore())
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	masterAddr := startFakeMasterServerForLeaderLookup(t, &reuseManifestMaster{volumeHost: volumeHost})
	f.MasterClient = wdclient.NewMasterClient(dialOption, "test", cluster.FilerType, pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"master": masterAddr}))
	masterCtx, cancelMaster := context.WithCancel(context.Background())
	t.Cleanup(cancelMaster)
	go f.MasterClient.KeepConnectedToMaster(masterCtx)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	require.NotEmpty(t, f.MasterClient.GetMaster(waitCtx), "fake master did not become available")
	_, found := f.MasterClient.GetLocations(1)
	require.True(t, found, "volume 1 has no location")

	return &FilerServer{
		filer:          f,
		option:         &FilerOption{},
		grpcDialOption: dialOption,
		entryLockTable: util.NewLockTable[util.FullPath](),
	}, volume
}

// walChunks is the chunk list of a log that a client appended n records to,
// one chunk per flush.
func walChunks(n int) []*filer_pb.FileChunk {
	chunks := make([]*filer_pb.FileChunk, n)
	for i := range chunks {
		chunks[i] = &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x%08x", i+1, 0x5eed), Offset: int64(i) * 10, Size: 10, ModifiedTsNs: int64(i + 1)}
	}
	return chunks
}

func walEntry(n int) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       "wal.log",
		Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000, FileMode: 0644, Inode: 1, FileSize: uint64(n) * 10},
		Chunks:     walChunks(n),
	}
}

func walChunkIds(chunks []*filer_pb.FileChunk) []string {
	ids := make([]string, len(chunks))
	for i, chunk := range chunks {
		ids[i] = filer.ChunkIdentity(chunk.GetFileIdString())
	}
	return ids
}

// A client that keeps its own chunk list, such as the Hadoop client, lists
// every chunk it wrote on each flush. After one commit folded the first 256
// into a manifest, the next lists them flat again: the stored manifest must
// stay, not be written again under a new file id while the stored one is
// queued for deletion.
func TestCommitKeepsTheStoredManifestOfChunksListedFlat(t *testing.T) {
	const n = filer.SmallChunkManifestBatch + 10
	for _, how := range []string{"create", "update"} {
		t.Run(how, func(t *testing.T) {
			fs, volume := newReuseManifestServer(t)
			ctx := context.Background()
			resp, err := fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: "/test", Entry: walEntry(n), SkipCheckParentDirectory: true})
			require.NoError(t, err)
			require.Empty(t, resp.Error)
			first, err := fs.filer.FindEntry(ctx, "/test/wal.log")
			require.NoError(t, err)
			manifests, _ := filer.SeparateManifestChunks(first.GetChunks())
			require.Len(t, manifests, 1)
			require.Equal(t, 1, volume.uploadCount())

			if how == "create" {
				resp, err = fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: "/test", Entry: walEntry(n + 1), SkipCheckParentDirectory: true})
				require.NoError(t, err)
				require.Empty(t, resp.Error)
			} else {
				_, err = fs.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{Directory: "/test", Entry: walEntry(n + 1)})
				require.NoError(t, err)
			}

			stored, err := fs.filer.FindEntry(ctx, "/test/wal.log")
			require.NoError(t, err)
			kept, flat := filer.SeparateManifestChunks(stored.GetChunks())
			require.Equal(t, walChunkIds(manifests), walChunkIds(kept), "the stored manifest was replaced")
			require.Len(t, flat, 11)
			require.Equal(t, 1, volume.uploadCount(), "the manifest was written again")
			var queued []string
			fs.filer.FileIdDeletionQueue.Consume(func(fileIds []string) { queued = append(queued, fileIds...) })
			require.Empty(t, queued, "chunks queued for deletion")

			data, _, err := filer.ResolveChunkManifest(ctx, fs.lookupFileId, stored.GetChunks(), 0, math.MaxInt64, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, walChunkIds(walChunks(n+1)), walChunkIds(data))
		})
	}
}

// A commit that drops chunks the stored manifest holds is not given that
// manifest back: the manifest goes, and the chunks the commit lists are folded
// into a new one.
func TestCommitDropsTheStoredManifestOfChunksItNoLongerLists(t *testing.T) {
	const n = filer.SmallChunkManifestBatch + 10
	fs, volume := newReuseManifestServer(t)
	ctx := context.Background()
	resp, err := fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: "/test", Entry: walEntry(n), SkipCheckParentDirectory: true})
	require.NoError(t, err)
	require.Empty(t, resp.Error)
	first, err := fs.filer.FindEntry(ctx, "/test/wal.log")
	require.NoError(t, err)
	manifests, _ := filer.SeparateManifestChunks(first.GetChunks())
	require.Len(t, manifests, 1)

	rewritten := walEntry(n + 1)
	rewritten.Chunks[5] = &filer_pb.FileChunk{FileId: fmt.Sprintf("3,%x%08x", 9999, 0x5eed), Offset: 50, Size: 10, ModifiedTsNs: 6}
	resp, err = fs.CreateEntry(ctx, &filer_pb.CreateEntryRequest{Directory: "/test", Entry: rewritten, SkipCheckParentDirectory: true})
	require.NoError(t, err)
	require.Empty(t, resp.Error)

	stored, err := fs.filer.FindEntry(ctx, "/test/wal.log")
	require.NoError(t, err)
	kept, _ := filer.SeparateManifestChunks(stored.GetChunks())
	require.Len(t, kept, 1)
	require.NotEqual(t, walChunkIds(manifests), walChunkIds(kept))
	require.Equal(t, 2, volume.uploadCount())
	var queued []string
	fs.filer.FileIdDeletionQueue.Consume(func(fileIds []string) { queued = append(queued, fileIds...) })
	for i, fileId := range queued {
		queued[i] = filer.ChunkIdentity(fileId)
	}
	require.ElementsMatch(t, walChunkIds([]*filer_pb.FileChunk{manifests[0], walChunks(n)[5]}), queued)

	data, _, err := filer.ResolveChunkManifest(ctx, fs.lookupFileId, stored.GetChunks(), 0, math.MaxInt64, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, walChunkIds(rewritten.Chunks), walChunkIds(data))
}
