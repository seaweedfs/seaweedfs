package filer

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type fsyncRecordingVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	mu       sync.Mutex
	fsync    map[string][]bool
	failOnce map[string]string
	failCode int32
	inFlight func(req *volume_server_pb.BatchDeleteRequest)
}

func (v *fsyncRecordingVolumeServer) BatchDelete(_ context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {
	if v.inFlight != nil {
		v.inFlight(req)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	resp := &volume_server_pb.BatchDeleteResponse{}
	for _, fid := range req.FileIds {
		v.fsync[fid] = append(v.fsync[fid], req.Fsync)
		if msg, ok := v.failOnce[fid]; ok {
			delete(v.failOnce, fid)
			code := v.failCode
			if code == 0 {
				code = http.StatusServiceUnavailable
			}
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{FileId: fid, Status: code, Error: msg})
			continue
		}
		resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{FileId: fid, Status: http.StatusAccepted})
	}
	return resp, nil
}

func (v *fsyncRecordingVolumeServer) calls(fid string) []bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]bool(nil), v.fsync[fid]...)
}

func startFsyncRecordingVolumeServer(t *testing.T) (*fsyncRecordingVolumeServer, func([]string) (map[string]*operation.LookupResult, error)) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	v := &fsyncRecordingVolumeServer{fsync: map[string][]bool{}, failOnce: map[string]string{}}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, v)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	port := lis.Addr().(*net.TCPAddr).Port
	location := operation.Location{Url: "127.0.0.1:" + strconv.Itoa(port), GrpcPort: port}
	lookup := func(vids []string) (map[string]*operation.LookupResult, error) {
		m := make(map[string]*operation.LookupResult, len(vids))
		for _, vid := range vids {
			m[vid] = &operation.LookupResult{VolumeOrFileId: vid, Locations: []operation.Location{location}}
		}
		return m, nil
	}
	return v, lookup
}

func newFsyncRuleTestFiler(t *testing.T) *Filer {
	t.Helper()
	f := newLedgerTestFiler(newStubFilerStore())
	f.GrpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	f.FilerConf = NewFilerConf()
	if err := f.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: "/buckets/", Fsync: proto.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestDeleteChunksFsyncFollowsPathRule(t *testing.T) {
	withPersistedConfig(t, false)
	v, lookup := startFsyncRecordingVolumeServer(t)
	f := newFsyncRuleTestFiler(t)
	ctx := context.Background()

	f.DeleteChunks(ctx, util.FullPath("/buckets/b/obj"), []*filer_pb.FileChunk{{FileId: "3,01637037d6"}})
	f.DeleteChunks(ctx, util.FullPath("/other/file"), []*filer_pb.FileChunk{{FileId: "3,02637037d6"}})
	f.DeleteUncommittedChunks(ctx, []*filer_pb.FileChunk{{FileId: "3,03637037d6"}})
	f.FlushFileIdDeletionQueue(ctx, lookup)

	for fid, want := range map[string]bool{"3,01637037d6": true, "3,02637037d6": false, "3,03637037d6": false} {
		got := v.calls(fid)
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s: BatchDelete fsync calls %v, want [%v]", fid, got, want)
		}
	}
	if n := f.pendingDeletionCount(); n != 0 {
		t.Errorf("pending deletions %d, want 0", n)
	}
	if ids := f.fsyncDeletionIds([]string{"3,01637037d6"}); len(ids) != 0 {
		t.Errorf("fsync mark kept after the delete was confirmed: %v", ids)
	}
}

func TestDeleteChunksFsyncSurvivesRetry(t *testing.T) {
	withPersistedConfig(t, false)
	v, lookup := startFsyncRecordingVolumeServer(t)
	f := newFsyncRuleTestFiler(t)
	ctx := context.Background()
	fid := "4,01637037d6"
	v.failOnce[fid] = "volume 4 is read only"

	f.DeleteChunks(ctx, util.FullPath("/buckets/b/obj"), []*filer_pb.FileChunk{{FileId: fid}})
	f.queueDeletions(false, fid)
	f.FlushFileIdDeletionQueue(ctx, lookup)
	if f.DeletionRetryQueue.Size() != 1 {
		t.Fatalf("retry queue size %d, want 1", f.DeletionRetryQueue.Size())
	}

	f.DeletionRetryQueue.lock.Lock()
	f.DeletionRetryQueue.itemIndex[fid].NextRetryAt = time.Now().Add(-time.Second)
	f.DeletionRetryQueue.lock.Unlock()
	ready, _ := f.DeletionRetryQueue.GetReadyItems(DeletionRetryBatchSize)
	f.processRetryBatch(ready, lookup)

	if got := v.calls(fid); len(got) != 2 || !got[0] || !got[1] {
		t.Fatalf("BatchDelete fsync calls %v, want [true true]", got)
	}
	if n := f.pendingDeletionCount(); n != 0 {
		t.Errorf("pending deletions %d, want 0", n)
	}
}

// readLedgerAsOlderFiler decodes the ledger the way a filer without fsync
// marks does: a JSON array, or a {parts, gen} manifest over its part keys.
func readLedgerAsOlderFiler(t *testing.T, f *Filer) []string {
	t.Helper()
	ctx := context.Background()
	key := f.deletionLedgerKey()
	raw, err := f.Store.KvGet(ctx, []byte(key))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	var ids []string
	if json.Unmarshal(raw, &ids) == nil {
		return ids
	}
	var manifest struct {
		Parts int `json:"parts"`
		Gen   int `json:"gen,omitempty"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("ledger %q is neither an array nor a manifest: %v", raw, err)
	}
	for i := 0; i < manifest.Parts; i++ {
		part, err := f.Store.KvGet(ctx, deletionLedgerGenPartKey(key, manifest.Gen, i))
		if err != nil {
			t.Fatalf("read part %d: %v", i, err)
		}
		var partIds []string
		if err := json.Unmarshal(part, &partIds); err != nil {
			t.Fatalf("part %d: %v", i, err)
		}
		ids = append(ids, partIds...)
	}
	return ids
}

func TestDeletionLedgerKeepsFsyncMarks(t *testing.T) {
	for _, n := range []int{1, 4000} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			withPersistedConfig(t, true)
			store := newStubFilerStore()
			f := newLedgerTestFiler(store)
			var plain, synced []string
			for i := 0; i < n; i++ {
				plain = append(plain, fmt.Sprintf("7,%08xbeefcafe", i))
				synced = append(synced, fmt.Sprintf("8,%08xbeefcafe", i))
			}
			f.queueDeletions(false, plain...)
			f.queueDeletions(true, synced...)
			f.snapshotDeletionLedger()

			if got := readLedgerAsOlderFiler(t, f); len(got) != 2*n || !contains(got, plain[0]) || !contains(got, synced[n-1]) {
				t.Fatalf("older filer reads %d ids, want all %d", len(got), 2*n)
			}

			f2 := newLedgerTestFiler(store)
			if !f2.reloadDeletionLedger() {
				t.Fatalf("ledger should reload")
			}
			if got := f2.pendingDeletionCount(); got != 2*n {
				t.Fatalf("recovered %d ids, want %d", got, 2*n)
			}
			if got := f2.fsyncDeletionIds(append(append([]string(nil), plain...), synced...)); len(got) != n {
				t.Fatalf("recovered %d fsync marks, want %d", len(got), n)
			}
			if got := f2.fsyncDeletionIds(plain); len(got) != 0 {
				t.Fatalf("plain ids came back marked: %d", len(got))
			}

			for _, id := range synced {
				f2.forgetDeletion(id, true)
			}
			f2.snapshotDeletionLedger()
			f3 := newLedgerTestFiler(store)
			f3.reloadDeletionLedger()
			if got := f3.fsyncDeletionIds(synced); len(got) != 0 || f3.pendingDeletionCount() != n {
				t.Fatalf("after forgetting the marked ids: %d marks, %d pending", len(got), f3.pendingDeletionCount())
			}
			if n == 1 {
				if _, ok := readPersistedLedger(t, f2); !ok {
					t.Fatalf("a ledger without marks should be a single array again")
				}
			}
		})
	}
}

func TestDeletionLedgerClaimKeepsFsyncMarks(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	fOld := newLedgerTestFiler(store)
	fOld.Dlm = lock_manager.NewDistributedLockManager("filer-old:8888")
	fOld.queueDeletions(false, "1,01")
	fOld.queueDeletions(true, "1,02")
	fOld.snapshotDeletionLedger()

	fNew := newLedgerTestFiler(store)
	fNew.Dlm = lock_manager.NewDistributedLockManager("filer-new:9999")
	if !fNew.reloadDeletionLedger() {
		t.Fatalf("reload should claim the stranded ledger")
	}
	if got := fNew.fsyncDeletionIds([]string{"1,01", "1,02"}); len(got) != 1 {
		t.Fatalf("claimed fsync marks %v, want only 1,02", got)
	}

	fNext := newLedgerTestFiler(store)
	fNext.Dlm = lock_manager.NewDistributedLockManager("filer-new:9999")
	fNext.reloadDeletionLedger()
	if _, ok := fNext.fsyncDeletionIds([]string{"1,02"})["1,02"]; !ok {
		t.Fatalf("claimed fsync mark was not persisted under the new key")
	}
}

func TestQueueDeletionsMarksBeforePublishing(t *testing.T) {
	f := newLedgerTestFiler(newStubFilerStore())
	const n = 20000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			f.queueDeletions(true, fmt.Sprintf("9,%08xbeefcafe", i))
		}
	}()
	seen := 0
	for seen < n {
		f.FileIdDeletionQueue.Consume(func(ids []string) {
			marked := f.fsyncDeletionIds(ids)
			for _, id := range ids {
				if _, ok := marked[id]; !ok {
					t.Errorf("%s reached the deletion queue before its fsync mark", id)
				}
			}
			seen += len(ids)
		})
		if t.Failed() {
			break
		}
	}
	<-done
}

func TestRecursiveDeleteFsyncFollowsChildPaths(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	for prefix, fsync := range map[string]bool{"/data/": true, "/mix/important": true, "/keep": true, "/keep/tmp-": false} {
		if err := f.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{LocationPrefix: prefix, Fsync: proto.Bool(fsync)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	for _, dir := range []string{"/data", "/data/sub", "/mix", "/keep"} {
		if err := f.CreateEntry(ctx, &Entry{FullPath: util.FullPath(dir), Attr: Attr{Mode: os.ModeDir | 0755}}, nil, false, false, nil, true, 255); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		"/data/a":            "5,01637037d6",
		"/data/sub/b":        "5,02637037d6",
		"/mix/important.txt": "5,03637037d6",
		"/mix/other.txt":     "5,04637037d6",
		"/keep/y":            "5,05637037d6",
		"/keep/tmp-x":        "5,06637037d6",
	}
	for path, fid := range files {
		entry := &Entry{FullPath: util.FullPath(path), Attr: Attr{Mode: 0644, Mtime: time.Now()}, Chunks: []*filer_pb.FileChunk{{FileId: fid}}}
		if err := f.CreateEntry(ctx, entry, nil, false, false, nil, true, 255); err != nil {
			t.Fatal(err)
		}
	}

	for _, dir := range []string{"/data", "/mix", "/keep"} {
		if err := f.DeleteEntryMetaAndData(ctx, util.FullPath(dir), true, false, true, false, nil, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}

	if n := f.pendingDeletionCount(); n != len(files) {
		t.Fatalf("pending deletions %d, want %d", n, len(files))
	}
	var fids []string
	for _, fid := range files {
		fids = append(fids, fid)
	}
	marked := f.fsyncDeletionIds(fids)
	for path, fid := range files {
		_, got := marked[fid]
		if want := path != "/mix/other.txt" && path != "/keep/tmp-x"; got != want {
			t.Errorf("%s: fsync mark %v, want %v", path, got, want)
		}
	}
}

func TestPlainDeleteKeepsANewerFsyncMark(t *testing.T) {
	withPersistedConfig(t, false)
	v, lookup := startFsyncRecordingVolumeServer(t)
	f := newFsyncRuleTestFiler(t)
	ctx := context.Background()
	fid := "6,01637037d6"
	var once sync.Once
	v.inFlight = func(*volume_server_pb.BatchDeleteRequest) {
		once.Do(func() {
			f.DeleteChunks(ctx, util.FullPath("/buckets/b/obj"), []*filer_pb.FileChunk{{FileId: fid}})
		})
	}

	f.DeleteChunks(ctx, util.FullPath("/other/file"), []*filer_pb.FileChunk{{FileId: fid}})
	f.FlushFileIdDeletionQueue(ctx, lookup)
	if _, ok := f.fsyncDeletionIds([]string{fid})[fid]; !ok || f.pendingDeletionCount() != 1 {
		t.Fatalf("a plain delete dropped the fsync request queued while it ran")
	}

	f.FlushFileIdDeletionQueue(ctx, lookup)
	if got := v.calls(fid); len(got) != 2 || got[0] || !got[1] {
		t.Fatalf("BatchDelete fsync calls %v, want [false true]", got)
	}
	if n := f.pendingDeletionCount(); n != 0 {
		t.Errorf("pending deletions %d, want 0", n)
	}
}

func TestDurableDeleteRetriesAServerError(t *testing.T) {
	withPersistedConfig(t, false)
	v, lookup := startFsyncRecordingVolumeServer(t)
	v.failCode = http.StatusInternalServerError
	f := newFsyncRuleTestFiler(t)
	ctx := context.Background()
	durable, plain := "7,01637037d6", "7,02637037d6"
	v.failOnce[durable] = "sync /data/7.dat: input/output error"
	v.failOnce[plain] = "sync /data/7.dat: input/output error"

	f.DeleteChunks(ctx, util.FullPath("/buckets/b/obj"), []*filer_pb.FileChunk{{FileId: durable}})
	f.DeleteChunks(ctx, util.FullPath("/other/file"), []*filer_pb.FileChunk{{FileId: plain}})
	f.FlushFileIdDeletionQueue(ctx, lookup)

	f.DeletionRetryQueue.lock.Lock()
	_, durableQueued := f.DeletionRetryQueue.itemIndex[durable]
	_, plainQueued := f.DeletionRetryQueue.itemIndex[plain]
	f.DeletionRetryQueue.lock.Unlock()
	if !durableQueued || plainQueued {
		t.Fatalf("retry queued: durable %v, plain %v; want durable only", durableQueued, plainQueued)
	}
	if _, ok := f.fsyncDeletionIds([]string{durable})[durable]; !ok || f.pendingDeletionCount() != 1 {
		t.Fatalf("the failed durable delete lost its pending record or fsync mark")
	}
}
