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
	server         string
	volumeID       uint32
	collection     string
	progress       float64
	grpcDialOption grpc.DialOption
}

// NewBalanceTask creates a new balance task instance
func NewBalanceTask(id string, server string, volumeID uint32, collection string, grpcDialOption grpc.DialOption) *BalanceTask {
	return &BalanceTask{
		BaseTask:       base.NewBaseTask(id, types.TaskTypeBalance),
		server:         server,
		volumeID:       volumeID,
		collection:     collection,
		grpcDialOption: grpcDialOption,
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
	if err := t.markVolumeReadonly(ctx, sourceServer, volumeId); err != nil {
		return fmt.Errorf("failed to mark volume readonly: %v", err)
	}
	// Restore source writability if any subsequent step fails, so the
	// source volume is not left permanently readonly on abort.
	sourceMarkedReadonly := true
	defer func() {
		if sourceMarkedReadonly {
			if wErr := t.markVolumeWritable(context.Background(), sourceServer, volumeId); wErr != nil {
				glog.Warningf("failed to restore volume %d writability on %s: %v", volumeId, sourceServer, wErr)
			}
		}
	}()

	// Step 2: Read source volume size before copy (for post-copy verification)
	t.ReportProgress(15.0)
	sourceStatus, err := t.readVolumeFileStatus(ctx, sourceServer, volumeId)
	if err != nil {
		return fmt.Errorf("failed to read source volume status: %v", err)
	}

	// Step 3: Copy volume to destination (VolumeCopy also mounts the volume)
	t.ReportProgress(20.0)
	t.GetLogger().Info("Copying volume to destination")
	lastAppendAtNs, err := t.copyVolume(ctx, sourceServer, targetServer, volumeId)
	if err != nil {
		return fmt.Errorf("failed to copy volume: %v", err)
	}

	// Step 4: Tail for updates
	t.ReportProgress(70.0)
	t.GetLogger().Info("Syncing final updates")
	if err := t.tailVolume(ctx, sourceServer, targetServer, volumeId, lastAppendAtNs); err != nil {
		glog.Warningf("Tail operation failed (may be normal): %v", err)
	}

	// Step 5: Verify the volume on target before deleting source.
	// This is a critical safety check — once the source is deleted, data loss
	// is irreversible. We verify the target has the volume with matching size.
	t.ReportProgress(85.0)
	t.GetLogger().Info("Verifying volume on target before deleting source")
	targetStatus, err := t.readVolumeFileStatus(ctx, targetServer, volumeId)
	if err != nil {
		return fmt.Errorf("aborting: cannot verify volume %d on target %s before deleting source: %v", volumeId, targetServer, err)
	}
	if targetStatus.DatFileSize != sourceStatus.DatFileSize {
		return fmt.Errorf("aborting: volume %d .dat size mismatch — source %d bytes, target %d bytes",
			volumeId, sourceStatus.DatFileSize, targetStatus.DatFileSize)
	}
	if targetStatus.FileCount != sourceStatus.FileCount {
		return fmt.Errorf("aborting: volume %d file count mismatch — source %d, target %d",
			volumeId, sourceStatus.FileCount, targetStatus.FileCount)
	}

	// Step 6: Delete from source — after this, the move is committed.
	// Clear the readonly flag so the defer doesn't try to restore writability.
	t.ReportProgress(90.0)
	t.GetLogger().Info("Deleting volume from source server")
	if err := t.deleteVolume(ctx, sourceServer, volumeId); err != nil {
		return fmt.Errorf("failed to delete volume from source: %v", err)
	}
	sourceMarkedReadonly = false

	t.ReportProgress(100.0)
	glog.Infof("Balance task completed successfully: volume %d moved from %s to %s",
		t.volumeID, sourceNode, destNode)
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

// markVolumeReadonly marks the volume readonly on the source server.
func (t *BalanceTask) markVolumeReadonly(ctx context.Context, server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
			})
			return err
		})
}

// markVolumeWritable restores the volume to writable on the source server.
func (t *BalanceTask) markVolumeWritable(ctx context.Context, server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{
				VolumeId: uint32(volumeId),
			})
			return err
		})
}

// copyVolume copies volume from source to target server.
func (t *BalanceTask) copyVolume(ctx context.Context, sourceServer, targetServer pb.ServerAddress, volumeId needle.VolumeId) (uint64, error) {
	var lastAppendAtNs uint64

	err := operation.WithVolumeServerClient(true, targetServer, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			stream, err := client.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
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

// tailVolume syncs remaining updates from source to target.
func (t *BalanceTask) tailVolume(ctx context.Context, sourceServer, targetServer pb.ServerAddress, volumeId needle.VolumeId, sinceNs uint64) error {
	return operation.WithVolumeServerClient(true, targetServer, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
				VolumeId:           uint32(volumeId),
				SinceNs:            sinceNs,
				IdleTimeoutSeconds: 60, // 1 minute timeout
				SourceVolumeServer: string(sourceServer),
			})
			return err
		})
}

// readVolumeFileStatus reads the volume's file status (sizes, file count) from a server.
func (t *BalanceTask) readVolumeFileStatus(ctx context.Context, server pb.ServerAddress, volumeId needle.VolumeId) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	var resp *volume_server_pb.ReadVolumeFileStatusResponse
	err := operation.WithVolumeServerClient(false, server, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			var err error
			resp, err = client.ReadVolumeFileStatus(ctx,
				&volume_server_pb.ReadVolumeFileStatusRequest{
					VolumeId: uint32(volumeId),
				})
			return err
		})
	return resp, err
}

// deleteVolume deletes the volume from the server.
func (t *BalanceTask) deleteVolume(ctx context.Context, server pb.ServerAddress, volumeId needle.VolumeId) error {
	return operation.WithVolumeServerClient(false, server, t.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			_, err := client.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
				VolumeId:  uint32(volumeId),
				OnlyEmpty: false,
			})
			return err
		})
}
