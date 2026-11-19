package filer

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

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
}

func (v *fsyncRecordingVolumeServer) BatchDelete(_ context.Context, req *volume_server_pb.BatchDeleteRequest) (*volume_server_pb.BatchDeleteResponse, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	resp := &volume_server_pb.BatchDeleteResponse{}
	for _, fid := range req.FileIds {
		v.fsync[fid] = append(v.fsync[fid], req.Fsync)
		if msg, ok := v.failOnce[fid]; ok {
			delete(v.failOnce, fid)
			resp.Results = append(resp.Results, &volume_server_pb.DeleteResult{FileId: fid, Status: http.StatusServiceUnavailable, Error: msg})
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
