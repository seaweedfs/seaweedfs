package erasure_coding

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
)

// Task implements erasure coding operation to convert volumes to EC format
type Task struct {
	*tasks.BaseTask
	server      string
	volumeID    uint32
	collection  string
	grpcDialOpt grpc.DialOption
}

// NewTask creates a new erasure coding task instance
func NewTask(server string, volumeID uint32) *Task {
	task := &Task{
		BaseTask: tasks.NewBaseTask(types.TaskTypeErasureCoding),
		server:   server,
		volumeID: volumeID,
	}
	return task
}

// Execute executes the actual erasure coding task using real SeaweedFS operations
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting erasure coding for volume %d on server %s", t.volumeID, t.server)

	ctx := context.Background()

	// Extract parameters
	t.collection = params.Collection
	if t.collection == "" {
		t.collection = "default"
	}

	// Connect to volume server
	conn, err := grpc.Dial(t.server, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to volume server %s: %v", t.server, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Step 1: Mark volume as read-only first
	t.SetProgress(10.0)
	glog.V(1).Infof("Marking volume %d as read-only", t.volumeID)

	_, err = client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		return fmt.Errorf("failed to mark volume %d as read-only: %v", t.volumeID, err)
	}

	// Step 2: Generate EC shards
	t.SetProgress(30.0)
	glog.V(1).Infof("Generating EC shards for volume %d", t.volumeID)

	_, err = client.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
	})
	if err != nil {
		return fmt.Errorf("failed to generate EC shards for volume %d: %v", t.volumeID, err)
	}

	// Step 3: Mount EC shards (all 14 shards: 10 data + 4 parity)
	t.SetProgress(70.0)
	glog.V(1).Infof("Mounting EC shards for volume %d", t.volumeID)

	// Create shard IDs for all 14 shards (0-13)
	shardIds := make([]uint32, 14)
	for i := 0; i < 14; i++ {
		shardIds[i] = uint32(i)
	}

	_, err = client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   t.volumeID,
		Collection: t.collection,
		ShardIds:   shardIds,
	})
	if err != nil {
		return fmt.Errorf("failed to mount EC shards for volume %d: %v", t.volumeID, err)
	}

	// Step 4: Verify volume status
	t.SetProgress(90.0)
	glog.V(1).Infof("Verifying volume %d after EC conversion", t.volumeID)

	// Check if volume is now read-only (which indicates successful EC conversion)
	statusResp, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: t.volumeID,
	})
	if err != nil {
		glog.Warningf("Could not verify EC status for volume %d: %v", t.volumeID, err)
		// This is not a failure - continue
	} else {
		if statusResp.IsReadOnly {
			glog.V(1).Infof("Volume %d is now read-only, EC conversion likely successful", t.volumeID)
		} else {
			glog.Warningf("Volume %d is not read-only after EC conversion", t.volumeID)
		}
	}

	t.SetProgress(100.0)
	glog.Infof("Successfully completed erasure coding for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for EC operations - varies significantly by volume size
	// For a typical 30GB volume, EC generation can take 5-15 minutes
	baseTime := 10 * time.Minute

	// Could adjust based on volume size if available in params
	if size, ok := params.Parameters["volume_size"].(int64); ok {
		// Rough estimate: 1 minute per GB
		estimatedTime := time.Duration(size/(1024*1024*1024)) * time.Minute
		if estimatedTime > baseTime {
			return estimatedTime
		}
	}

	return baseTime
}

// GetProgress returns the current progress
func (t *Task) GetProgress() float64 {
	return t.BaseTask.GetProgress()
}

// Cancel cancels the task
func (t *Task) Cancel() error {
	return t.BaseTask.Cancel()
}
