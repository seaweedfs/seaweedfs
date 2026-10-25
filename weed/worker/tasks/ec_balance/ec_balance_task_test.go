package ec_balance

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestECBalanceTaskRejectsOutOfRangeShardIds(t *testing.T) {
	// ShardId is a uint8; an unchecked id like 259 would alias shard 3 and
	// copy/delete a real, unrelated shard.
	params := &worker_pb.TaskParams{
		VolumeId: 7,
		Sources:  []*worker_pb.TaskSource{{Node: "src:8080", ShardIds: []uint32{259}}},
		Targets:  []*worker_pb.TaskTarget{{Node: "dst:8080", ShardIds: []uint32{259}}},
	}
	task := NewECBalanceTask("t1", 7, "c1", nil)

	if err := task.Validate(params); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("Validate: expected out-of-range rejection, got: %v", err)
	}
	if err := task.Execute(context.Background(), params); err == nil || !strings.Contains(err.Error(), "out of range") {
		t.Fatalf("Execute: expected out-of-range rejection, got: %v", err)
	}
}

// A refusal against the pinned disk convicts only that disk, not the node:
// the task retries once letting the server pick a disk, and the move completes.
func TestECBalanceTaskRetriesServerPickOnPinnedDiskRefusal(t *testing.T) {
	source := startFakeEcVolumeServer(t, 7, 5)
	target := startFakeEcVolumeServer(t, 7)
	target.refuseCopyOnDisk = 2

	task := NewECBalanceTask("t2", 7, "c1",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	params := &worker_pb.TaskParams{
		VolumeId: 7,
		Sources:  []*worker_pb.TaskSource{{Node: source.address(), ShardIds: []uint32{5}}},
		Targets:  []*worker_pb.TaskTarget{{Node: target.address(), DiskId: 2, ShardIds: []uint32{5}}},
	}

	if err := task.Execute(context.Background(), params); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := target.copyDiskIds; len(got) != 2 || got[0] != 2 || got[1] != 0 {
		t.Fatalf("expected copies to disk 2 then server-picked 0, got %v", got)
	}
	if source.has(7, 5) {
		t.Fatalf("source still holds shard 5")
	}
	if !target.has(7, 5) {
		t.Fatalf("target never received shard 5")
	}
}
