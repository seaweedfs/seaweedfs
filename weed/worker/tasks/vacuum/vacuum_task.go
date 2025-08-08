package vacuum

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
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/seaweedfs/seaweedfs/weed/worker/types/base"
	"google.golang.org/grpc"
)

// VacuumTask implements the Task interface
type VacuumTask struct {
	*base.BaseTask
	server           string
	volumeID         uint32
	collection       string
	garbageThreshold float64
	progress         float64
}

// NewVacuumTask creates a new unified vacuum task instance
func NewVacuumTask(id string, server string, volumeID uint32, collection string) *VacuumTask {
	return &VacuumTask{
		BaseTask:         base.NewBaseTask(id, types.TaskTypeVacuum),
		server:           server,
		volumeID:         volumeID,
		collection:       collection,
		garbageThreshold: 0.3, // Default 30% threshold
	}
}

// Execute implements the UnifiedTask interface
func (t *VacuumTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
	}

	t.garbageThreshold = vacuumParams.GarbageThreshold

	t.GetLogger().WithFields(map[string]interface{}{
		"volume_id":         t.volumeID,
		"server":            t.server,
		"collection":        t.collection,
		"garbage_threshold": t.garbageThreshold,
	}).Info("Starting vacuum task")

	// Step 1: Check volume status and garbage ratio
	t.ReportProgress(10.0)
	t.GetLogger().Info("Checking volume status")
	eligible, currentGarbageRatio, err := t.checkVacuumEligibility()
	if err != nil {
		return fmt.Errorf("failed to check vacuum eligibility: %v", err)
	}

	if !eligible {
		t.GetLogger().WithFields(map[string]interface{}{
			"current_garbage_ratio": currentGarbageRatio,
			"required_threshold":    t.garbageThreshold,
		}).Info("Volume does not meet vacuum criteria, skipping")
		t.ReportProgress(100.0)
		return nil
	}

	// Step 2: Perform vacuum operation
	t.ReportProgress(50.0)
	t.GetLogger().WithFields(map[string]interface{}{
		"garbage_ratio": currentGarbageRatio,
		"threshold":     t.garbageThreshold,
	}).Info("Performing vacuum operation")

	if err := t.performVacuum(); err != nil {
		return fmt.Errorf("failed to perform vacuum: %v", err)
	}

	// Step 3: Verify vacuum results
	t.ReportProgress(90.0)
	t.GetLogger().Info("Verifying vacuum results")
	if err := t.verifyVacuumResults(); err != nil {
		glog.Warningf("Vacuum verification failed: %v", err)
		// Don't fail the task - vacuum operation itself succeeded
	}

	t.ReportProgress(100.0)
	glog.Infof("Vacuum task completed successfully: volume %d from %s (garbage ratio was %.2f%%)",
		t.volumeID, t.server, currentGarbageRatio*100)
	return nil
}

// Validate implements the UnifiedTask interface
func (t *VacuumTask) Validate(params *worker_pb.TaskParams) error {
	if params == nil {
		return fmt.Errorf("task parameters are required")
	}

	vacuumParams := params.GetVacuumParams()
	if vacuumParams == nil {
		return fmt.Errorf("vacuum parameters are required")
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

	if vacuumParams.GarbageThreshold < 0 || vacuumParams.GarbageThreshold > 1.0 {
		return fmt.Errorf("invalid garbage threshold: %f (must be between 0.0 and 1.0)", vacuumParams.GarbageThreshold)
	}

	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *VacuumTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Basic estimate based on simulated steps
	return 14 * time.Second // Sum of all step durations
}

// GetProgress returns current progress
func (t *VacuumTask) GetProgress() float64 {
	return t.progress
}

// Helper methods for real vacuum operations

// checkVacuumEligibility checks if the volume meets vacuum criteria
func (t *VacuumTask) checkVacuumEligibility() (bool, float64, error) {
	var garbageRatio float64

	err := operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("failed to check volume vacuum status: %v", err)
			}

			garbageRatio = resp.GarbageRatio

			return nil
		})

	if err != nil {
		return false, 0, err
	}

	eligible := garbageRatio >= t.garbageThreshold
	glog.V(1).Infof("Volume %d garbage ratio: %.2f%%, threshold: %.2f%%, eligible: %v",
		t.volumeID, garbageRatio*100, t.garbageThreshold*100, eligible)

	return eligible, garbageRatio, nil
}

// performVacuum executes the actual vacuum operation
func (t *VacuumTask) performVacuum() error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			// Step 1: Compact the volume
			t.GetLogger().Info("Compacting volume")
			stream, err := client.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("vacuum compact failed: %v", err)
			}

			// Read compact progress
			for {
				resp, recvErr := stream.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					}
					return fmt.Errorf("vacuum compact stream error: %v", recvErr)
				}
				glog.V(2).Infof("Volume %d compact progress: %d bytes processed", t.volumeID, resp.ProcessedBytes)
			}

			// Step 2: Commit the vacuum
			t.GetLogger().Info("Committing vacuum operation")
			_, err = client.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("vacuum commit failed: %v", err)
			}

			// Step 3: Cleanup old files
			t.GetLogger().Info("Cleaning up vacuum files")
			_, err = client.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("vacuum cleanup failed: %v", err)
			}

			glog.V(1).Infof("Volume %d vacuum operation completed successfully", t.volumeID)
			return nil
		})
}

// verifyVacuumResults checks the volume status after vacuum
func (t *VacuumTask) verifyVacuumResults() error {
	return operation.WithVolumeServerClient(false, pb.ServerAddress(t.server), grpc.WithInsecure(),
		func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
				VolumeId: t.volumeID,
			})
			if err != nil {
				return fmt.Errorf("failed to verify vacuum results: %v", err)
			}

			postVacuumGarbageRatio := resp.GarbageRatio

			glog.V(1).Infof("Volume %d post-vacuum garbage ratio: %.2f%%",
				t.volumeID, postVacuumGarbageRatio*100)

			return nil
		})
}
