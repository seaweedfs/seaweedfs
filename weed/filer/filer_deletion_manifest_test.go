package filer

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// flakyLookupMaster answers LookupVolume with the volume server only while
// available is set, as a master does while a volume server is away.
type flakyLookupMaster struct {
	lookupOnlyMaster
	available    atomic.Bool
	onVolumeRead atomic.Pointer[func(http.ResponseWriter)]
}

func (m *flakyLookupMaster) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	if m.available.Load() {
		return m.lookupOnlyMaster.LookupVolume(ctx, req)
	}
	resp := &master_pb.LookupVolumeResponse{}
	for _, id := range req.VolumeOrFileIds {
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: id,
			Error:          "volume not found",
		})
	}
	return resp, nil
}

func newManifestDeletionFiler(t *testing.T, subChunks []*filer_pb.FileChunk) (*Filer, *flakyLookupMaster) {
	t.Helper()
	manifestBytes, err := proto.Marshal(&filer_pb.FileChunkManifest{Chunks: subChunks})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	master := &flakyLookupMaster{}
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hook := master.onVolumeRead.Load(); hook != nil {
			(*hook)(w)
		}
		_, _ = w.Write(manifestBytes)
	}))
	t.Cleanup(volume.Close)
	master.volumeUrl = strings.TrimPrefix(volume.URL, "http://")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, master)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}

	mc := wdclient.NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", cluster.FilerType, pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"m": pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))}),
	)
	connecting, stopConnecting := context.WithCancel(context.Background())
	t.Cleanup(stopConnecting)
	go mc.KeepConnectedToMaster(connecting)
	waiting, cancelWaiting := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelWaiting()
	mc.WaitUntilConnected(waiting)
	if waiting.Err() != nil {
		t.Fatal("the master client never connected to the fake master")
	}

	return &Filer{
		MasterClient:        mc,
		FileIdDeletionQueue: util.NewUnboundedQueue(),
		DeletionRetryQueue:  NewDeletionRetryQueue(),
		deletionQuit:        make(chan struct{}),
	}, master
}

func queuedDeletions(f *Filer) (ids []string) {
	f.FileIdDeletionQueue.Consume(func(fileIds []string) {
		ids = append(ids, fileIds...)
	})
	slices.Sort(ids)
	return ids
}

func makeManifestRetryDue(f *Filer, retryCount int) {
	q := f.manifestRetries()
	q.lock.Lock()
	defer q.lock.Unlock()
	for _, item := range q.heap {
		item.RetryCount = retryCount
		item.NextRetryAt = time.Now().Add(-time.Second)
	}
}

func TestDeleteChunksKeepsUnresolvedManifest(t *testing.T) {
	subChunks := []*filer_pb.FileChunk{
		{FileId: "4,02637037d6", Offset: 0, Size: 8},
		{FileId: "5,03637037d6", Offset: 8, Size: 8},
	}
	f, master := newManifestDeletionFiler(t, subChunks)
	manifest := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: 16, IsChunkManifest: true}

	f.DeleteUncommittedChunks(context.Background(), []*filer_pb.FileChunk{manifest, {FileId: "6,04637037d6", Size: 4}})
	if got := queuedDeletions(f); !slices.Equal(got, []string{"6,04637037d6"}) {
		t.Fatalf("queued %v while the manifest was unresolvable, want only the plain chunk", got)
	}
	if f.manifestRetries().Size() != 1 {
		t.Fatalf("manifest retry queue size %d, want 1", f.manifestRetries().Size())
	}

	makeManifestRetryDue(f, 1)
	f.processManifestRetries(context.Background())
	if got := queuedDeletions(f); len(got) != 0 {
		t.Fatalf("queued %v while the manifest volume is still away", got)
	}

	master.available.Store(true)
	makeManifestRetryDue(f, 2)
	f.processManifestRetries(context.Background())
	want := []string{"3,01637037d6", "4,02637037d6", "5,03637037d6"}
	if got := queuedDeletions(f); !slices.Equal(got, want) {
		t.Fatalf("queued %v after the manifest resolved, want %v", got, want)
	}
	if f.manifestRetries().Size() != 0 {
		t.Fatalf("manifest retry queue size %d after resolving, want 0", f.manifestRetries().Size())
	}
}

func TestDeleteChunksDropsManifestAfterMaxRetries(t *testing.T) {
	f, _ := newManifestDeletionFiler(t, []*filer_pb.FileChunk{{FileId: "4,02637037d6", Size: 8}})
	manifest := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: 8, IsChunkManifest: true}

	f.DeleteUncommittedChunks(context.Background(), []*filer_pb.FileChunk{manifest})
	makeManifestRetryDue(f, MaxRetryAttempts+1)
	f.processManifestRetries(context.Background())
	if got := queuedDeletions(f); !slices.Equal(got, []string{"3,01637037d6"}) {
		t.Fatalf("queued %v after giving up, want the manifest needle alone", got)
	}
	if f.manifestRetries().Size() != 0 {
		t.Fatalf("manifest retry queue size %d after giving up, want 0", f.manifestRetries().Size())
	}
}

func fsyncMarked(f *Filer, ids ...string) bool {
	f.deletionLedgerLock.Lock()
	defer f.deletionLedgerLock.Unlock()
	for _, id := range ids {
		if _, ok := f.fsyncDeletions[id]; !ok {
			return false
		}
	}
	return true
}

func TestManifestRetryKeepsFsyncUpgrade(t *testing.T) {
	f, master := newManifestDeletionFiler(t, []*filer_pb.FileChunk{{FileId: "4,02637037d6", Size: 8}})
	manifest := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: 8, IsChunkManifest: true}

	f.doDeleteChunks(context.Background(), []*filer_pb.FileChunk{manifest}, false)
	f.doDeleteChunks(context.Background(), []*filer_pb.FileChunk{manifest}, true)

	master.available.Store(true)
	makeManifestRetryDue(f, 1)
	f.processManifestRetries(context.Background())
	if !fsyncMarked(f, "3,01637037d6", "4,02637037d6") {
		t.Fatal("a durable delete queued behind a non-durable one lost its fsync mark")
	}
}

func TestManifestRetryKeepsFsyncUpgradeInFlight(t *testing.T) {
	f, master := newManifestDeletionFiler(t, []*filer_pb.FileChunk{{FileId: "4,02637037d6", Size: 8}})
	manifest := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: 8, IsChunkManifest: true}

	f.doDeleteChunks(context.Background(), []*filer_pb.FileChunk{manifest}, false)
	makeManifestRetryDue(f, 1)
	q := f.manifestRetries()
	ready, _ := q.GetReadyItems(DeletionRetryBatchSize)
	if len(ready) != 1 {
		t.Fatalf("got %d ready manifest retries, want 1", len(ready))
	}

	master.available.Store(true)
	// The first read sees the durable delete arrive; once that attempt has
	// queued the manifest needle, the volume server no longer has it.
	var reads atomic.Int32
	upgrade := func(w http.ResponseWriter) {
		if reads.Add(1) == 1 {
			q.addOrUpdate(manifest.FileId, "durable delete", 0, manifest, true)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}
	master.onVolumeRead.Store(&upgrade)
	f.retryManifest(context.Background(), q, ready[0])
	if !fsyncMarked(f, "3,01637037d6", "4,02637037d6") {
		t.Fatal("a durable delete that arrived in flight lost its fsync mark")
	}
	if q.Size() != 0 || len(q.itemIndex) != 0 {
		t.Fatalf("manifest retry queue still holds %d items", len(q.itemIndex))
	}
}

func TestManifestRetryStalledReadIsBounded(t *testing.T) {
	f, master := newManifestDeletionFiler(t, []*filer_pb.FileChunk{{FileId: "4,02637037d6", Size: 8}})
	manifest := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: 8, IsChunkManifest: true}

	f.doDeleteChunks(context.Background(), []*filer_pb.FileChunk{manifest}, false)
	master.available.Store(true)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stall := func(w http.ResponseWriter) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release
	}
	master.onVolumeRead.Store(&stall)

	defer func(old time.Duration) { manifestRetryTimeout = old }(manifestRetryTimeout)
	manifestRetryTimeout = 200 * time.Millisecond
	makeManifestRetryDue(f, 1)
	done := make(chan struct{})
	go func() {
		f.processManifestRetries(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("a stalled manifest read held up the retry loop")
	}
	if got := queuedDeletions(f); len(got) != 0 {
		t.Fatalf("queued %v after a stalled manifest read", got)
	}
	if f.manifestRetries().Size() != 1 {
		t.Fatalf("manifest retry queue size %d, want the manifest requeued", f.manifestRetries().Size())
	}
}

func TestManifestRetryPassLeavesRestQueued(t *testing.T) {
	f, master := newManifestDeletionFiler(t, []*filer_pb.FileChunk{{FileId: "4,02637037d6", Size: 8}})
	for _, fid := range []string{"3,01637037d6", "3,05637037d6", "3,06637037d6"} {
		f.doDeleteChunks(context.Background(), []*filer_pb.FileChunk{{FileId: fid, Size: 8, IsChunkManifest: true}}, false)
	}
	master.available.Store(true)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stall := func(w http.ResponseWriter) {
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release
	}
	master.onVolumeRead.Store(&stall)

	defer func(old time.Duration) { manifestRetryTimeout = old }(manifestRetryTimeout)
	manifestRetryTimeout = 200 * time.Millisecond
	makeManifestRetryDue(f, 1)
	f.processManifestRetries(context.Background())

	q := f.manifestRetries()
	q.lock.Lock()
	defer q.lock.Unlock()
	untried := 0
	for _, item := range q.heap {
		if item.RetryCount == 1 {
			untried++
		}
	}
	if len(q.heap) != 3 || untried == 0 {
		t.Fatalf("after one stalled pass: %d queued, %d untried; want all 3 queued and some untried", len(q.heap), untried)
	}
}
