package ec_balance

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
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
