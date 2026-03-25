package delete_empty

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// DeleteEmptyTask deletes a single empty volume from a volume server.
// It only proceeds if the volume is still empty at execution time (onlyEmpty=true),
// so it is safe to run concurrently with write traffic.
type DeleteEmptyTask struct {
	*base.BaseTask
	server         string
	volumeID       uint32
	collection     string
	grpcDialOption grpc.DialOption
}

// NewDeleteEmptyTask creates a new delete_empty task instance
func NewDeleteEmptyTask(id string, server string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *DeleteEmptyTask {
	return &DeleteEmptyTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeDeleteEmpty),
		server:         server,
		volumeID:       volumeID,
		collection:     collection,
		grpcDialOption: grpcDialOption,
	}
}

// Execute implements the Task interface
func (t *DeleteEmptyTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":  t.volumeID,
		"server":     t.server,
		"collection": t.collection,
	}).Info("Starting compaction task")

	t.ReportProgress(10.0)

	if err := t.verifyVolumeIsEmpty(ctx); err != nil {
		return err
	}

	t.ReportProgress(50.0)

	if err := t.deleteVolume(ctx); err != nil {
		return err
	}

	t.ReportProgress(100.0)
	glog.Infof("COMPACTION: successfully deleted empty volume %d from %s", t.volumeID, t.server)
	return nil
}

// Validate implements the Task interface
func (t *DeleteEmptyTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}
	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}
	if len(params.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	return nil
}

// EstimateTime implements the Task interface
func (t *DeleteEmptyTask) EstimateTime(_ *worker_pb.TaskParams) time.Duration {
	return 5 * time.Second
}

// verifyVolumeIsEmpty re-checks the volume size before deletion to guard
// against a race where new data arrived between detection and execution.
func (t *DeleteEmptyTask) verifyVolumeIsEmpty(ctx context.Context) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			resp, err := client.VacuumVolumeCheck(checkCtx, &volume_server_pb.VacuumVolumeCheckRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("failed to check volume %d status: %w", t.volumeID, err)
			}

			// Re-verify the volume is still empty: a non-zero GarbageRatio means
			// the volume has received data since detection. Abort to be safe;
			// the real server-side guard is onlyEmpty=true in VolumeDelete.
			glog.V(2).Infof("COMPACTION: pre-delete check on volume %d: garbage_ratio=%.4f", t.volumeID, resp.GarbageRatio)
			if resp.GarbageRatio > 0 {
				return fmt.Errorf("pre-delete check failed: volume %d is not empty (garbage_ratio=%.4f)", t.volumeID, resp.GarbageRatio)
			}
			return nil
		})
}

// deleteVolume calls VolumeDelete with onlyEmpty=true so the server will
// refuse to delete the volume if it has received data since detection.
func (t *DeleteEmptyTask) deleteVolume(ctx context.Context) error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			_, err := client.VolumeDelete(deleteCtx, &volume_server_pb.VolumeDeleteRequest{
				VolumeId:  t.volumeID,
				OnlyEmpty: true,
			})
			if err != nil {
				return fmt.Errorf("failed to delete empty volume %d from %s: %w", t.volumeID, t.server, err)
			}

			return nil
		})
}
