package balance

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// BalanceTask implements the Task interface
type BalanceTask struct {
	*base.BaseTask
	server     string
	volumeID   uint32
	collection string
	progress   float64
}

// NewBalanceTask creates a new balance task instance
func NewBalanceTask(id string, server string, volumeID uint32, collection string) *BalanceTask {
	return &BalanceTask{
		BaseTask:   base.NewBaseTask(id, types.TaskTypeBalance),
		server:     server,
		volumeID:   volumeID,
		collection: collection,
	}
}

// Execute implements the Task interface
func (t *BalanceTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		return fmt.Errorf("balance parameters are required")
	}

	// Get source and destination from unified arrays
	if len(params.Sources) == 0 {
		return fmt.Errorf("source is required for balance task")
	}
	if len(params.Targets) == 0 {
		return fmt.Errorf("target is required for balance task")
	}

	sourceNode := params.Sources[0].Node
	destNode := params.Targets[0].Node

	if sourceNode == "" {
		return fmt.Errorf("source node is required for balance task")
	}
	if destNode == "" {
		return fmt.Errorf("destination node is required for balance task")
	}

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":   t.volumeID,
		"source":      sourceNode,
		"destination": destNode,
		"collection":  t.collection,
	}).Info("Starting balance task - moving volume")

	sourceServer := pb.ServerAddress(sourceNode)
	targetServer := pb.ServerAddress(destNode)
	volumeId := needle.VolumeId(t.volumeID)

	// Step 1: Mark volume readonly
	t.ReportProgress(10.0)
	t.GetLogger().Info("Marking volume readonly for move")
	if err := t.markVolumeReadonly(sourceServer, volumeId); err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}

	// Step 2: Copy volume to destination
	t.ReportProgress(20.0)
	t.GetLogger().Info("Copying volume to destination")
	lastAppendAtNs, err := t.copyVolume(sourceServer, targetServer, volumeId)
	if err != nil {
		return fmt.Errorf("failed to copy volume: %v", err)
	}

	// Step 3: Mount volume on target and mark it readonly
	t.ReportProgress(60.0)
	t.GetLogger().Info("Mounting volume on target server")
	if err := t.mountVolume(targetServer, volumeId); err != nil {
		return fmt.Errorf("failed to mount volume on target: %v", err)
	}

	// Step 4: Tail for updates
	t.ReportProgress(70.0)
	t.GetLogger().Info("Syncing final updates")
	if err := t.tailVolume(sourceServer, targetServer, volumeId, lastAppendAtNs); err != nil {
		glog.Warningf("Tail operation failed (may be normal): %v", err)
	}

	// Step 5: Delete from source
	t.ReportProgress(90.0)
	t.GetLogger().Info("Deleting volume from source server")
	if err := t.deleteVolume(sourceServer, volumeId); err != nil {
		return fmt.Errorf("failed to delete volume from source: %v", err)
	}

	t.ReportProgress(100.0)
	glog.Infof("Balance task completed successfully: volume %d moved from %s to %s",
		t.volumeID, t.server, destNode)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *BalanceTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	balanceParams := params.GetBalanceParams()
	if balanceParams == nil {
		return fmt.Errorf("balance parameters are required")
	}

	if params.VolumeId != t.volumeID {
		return fmt.Errorf("volume ID mismatch: expected %d, got %d", t.volumeID, params.VolumeId)
	}

	// Validate that at least one source matches our server
	found := false
	for _, source := range params.Sources {
		if source.Node == t.server {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("no source matches expected server %s", t.server)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *BalanceTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 14 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *BalanceTask) GetProgress() float64 {
	return t.progress
}

// Helper methods for real balance operations

// markVolumeReadonly marks the volume readonly
func (t *BalanceTask) markVolumeReadonly(server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
			})
			return err
		})
}

// copyVolume copies volume from source to target server
func (t *BalanceTask) copyVolume(sourceServer, targetServer pb.ServerAddress, volumeId needle.VolumeId) (uint64, error) {
	var lastAppendAtNs uint64

	err := operation.WithVolumeServerClient(true, targetServer, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
				VolumeId:       uint32(volumeId),
				SourceDataNode: string(sourceServer),
			})
			if err != nil {
				return err
			}

			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					}
					return recvErr
				}

				if resp.LastAppendAtNs != 0 {
					lastAppendAtNs = resp.LastAppendAtNs
				} else {
					// Report copy progress
					glog.V(1).Infof("Volume %d copy progress: %s", volumeId,
						util.BytesToHumanReadable(uint64(resp.ProcessedBytes)))
				}
			}

			return nil
		})

	return lastAppendAtNs, err
}

// mountVolume mounts the volume on the target server
func (t *BalanceTask) mountVolume(server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{
				VolumeId: uint32(volumeId),
			})
			return err
		})
}

// tailVolume syncs remaining updates from source to target
func (t *BalanceTask) tailVolume(sourceServer, targetServer pb.ServerAddress, volumeId needle.VolumeId, sinceNs uint64) error {
	return operation.WithVolumeServerClient(true, targetServer, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeTailReceiver(context.Background(), &volume_server_pb.VolumeTailReceiverRequest{
				VolumeId:           uint32(volumeId),
				SinceNs:            sinceNs,
				IdleTimeoutSeconds: 60, // 1 minute timeout
				SourceVolumeServer: string(sourceServer),
			})
			return err
		})
}

// unmountVolume unmounts the volume from the server
func (t *BalanceTask) unmountVolume(server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeUnmount(context.Background(), &volume_server_pb.VolumeUnmountRequest{
				VolumeId: uint32(volumeId),
			})
			return err
		})
}

// deleteVolume deletes the volume from the server
func (t *BalanceTask) deleteVolume(server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
				VolumeId:  uint32(volumeId),
				OnlyEmpty: false,
			})
			return err
		})
}
