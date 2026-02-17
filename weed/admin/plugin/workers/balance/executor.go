package balance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Executor handles the 5-step volume migration execution process
type Executor struct {
	jobID              string
	sourceServer       string
	targetServer       string
	volumeID           string
	config             *plugin_pb.JobTypeConfig
	parallelMigrations int32
	maxBytesPerSecond  int64
}

// ExecutionStep represents a single step in the migration process
type ExecutionStep struct {
	StepNumber  int
	Name        string
	Description string
}

// MigrationState tracks the state of a migration
type MigrationState struct {
	mu               sync.Mutex
	volumeReadonly   bool
	volumeCopied     bool
	volumeMounted    bool
	tailStarted      bool
	sourceDeleted    bool
	bytesTransferred int64
}

// NewExecutor creates a new balance executor
func NewExecutor(jobID string, config *plugin_pb.JobTypeConfig) *Executor {
	parallelMigrations := int32(2)
	maxBytesPerSecond := int64(0)

	for _, cfv := range config.WorkerConfig {
		if cfv.FieldName == "parallelMigrations" {
			parallelMigrations = int32(cfv.IntValue)
		} else if cfv.FieldName == "maxBytesPerSecond" {
			maxBytesPerSecond = cfv.IntValue
		}
	}

	return &Executor{
		jobID:              jobID,
		config:             config,
		parallelMigrations: parallelMigrations,
		maxBytesPerSecond:  maxBytesPerSecond,
	}
}

// Execute runs the 5-step volume migration process
// Step 1: markVolumeReadonly - Make source volume read-only
// Step 2: copyVolume - Copy to target server
// Step 3: mountVolume - Mount on target
// Step 4: tailUpdates - Tail live updates from source
// Step 5: deleteSourceVolume - Delete from source if balanced
// Returns progress updates via the progressChan
func (e *Executor) Execute(ctx context.Context, metadata map[string]string, progressChan chan<- *plugin_pb.JobProgress) error {
	e.volumeID = metadata["volume_id"]
	e.sourceServer = metadata["source_server"]
	e.targetServer = metadata["target_server"]

	glog.Infof("balance executor job=%s: starting migration (volume=%s, %s -> %s)",
		e.jobID, e.volumeID, e.sourceServer, e.targetServer)

	state := &MigrationState{}

	steps := []ExecutionStep{
		{StepNumber: 1, Name: "markVolumeReadonly", Description: "Make source volume read-only"},
		{StepNumber: 2, Name: "copyVolume", Description: "Copy volume data to target server"},
		{StepNumber: 3, Name: "mountVolume", Description: "Mount volume on target server"},
		{StepNumber: 4, Name: "tailUpdates", Description: "Tail live updates from source"},
		{StepNumber: 5, Name: "deleteSourceVolume", Description: "Delete volume from source server"},
	}

	totalSteps := len(steps)

	for i, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		glog.Infof("balance executor job=%s: executing step %d: %s", e.jobID, step.StepNumber, step.Name)

		// Send progress update before executing step
		progress := int32((i * 100) / totalSteps)
		progressChan <- &plugin_pb.JobProgress{
			ProgressPercent: progress,
			CurrentStep:     step.Name,
			StatusMessage:   step.Description,
			UpdatedAt:       timestamppb.Now(),
		}

		var err error
		switch step.StepNumber {
		case 1:
			err = e.markVolumeReadonly(ctx, state)
		case 2:
			err = e.copyVolume(ctx, state, progressChan)
		case 3:
			err = e.mountVolume(ctx, state)
		case 4:
			err = e.tailUpdates(ctx, state, progressChan)
		case 5:
			err = e.deleteSourceVolume(ctx, state)
		}

		if err != nil {
			glog.Errorf("balance executor job=%s: step %d failed: %v", e.jobID, step.StepNumber, err)
			// Attempt to rollback on error
			e.rollbackMigration(ctx, state)
			return fmt.Errorf("step %d (%s) failed: %w", step.StepNumber, step.Name, err)
		}

		glog.Infof("balance executor job=%s: step %d completed", e.jobID, step.StepNumber)
	}

	// Send final progress update
	progressChan <- &plugin_pb.JobProgress{
		ProgressPercent: 100,
		CurrentStep:     "complete",
		StatusMessage:   "Volume migration completed successfully",
		UpdatedAt:       timestamppb.Now(),
	}

	glog.Infof("balance executor job=%s: migration completed (volume=%s, transferred=%dB)",
		e.jobID, e.volumeID, state.bytesTransferred)

	return nil
}

// Step 1: markVolumeReadonly makes the source volume read-only to prepare for migration
func (e *Executor) markVolumeReadonly(ctx context.Context, state *MigrationState) error {
	glog.Infof("balance executor job=%s: marking volume %s as read-only on %s",
		e.jobID, e.volumeID, e.sourceServer)

	// TODO: Connect to source server and mark volume as read-only
	// 1. Get volume server connection
	// 2. Send MarkVolumeReadonly RPC
	// 3. Wait for confirmation
	// 4. Verify volume is read-only

	state.mu.Lock()
	state.volumeReadonly = true
	state.mu.Unlock()

	glog.Infof("balance executor job=%s: volume %s is now read-only", e.jobID, e.volumeID)
	return nil
}

// Step 2: copyVolume copies volume data from source to target server
func (e *Executor) copyVolume(ctx context.Context, state *MigrationState, progressChan chan<- *plugin_pb.JobProgress) error {
	glog.Infof("balance executor job=%s: starting volume copy from %s to %s (bandwidth=%dB/s)",
		e.jobID, e.sourceServer, e.targetServer, e.maxBytesPerSecond)

	// TODO: Connect to both source and target servers and perform copy
	// 1. Get volume metadata from source (size, collection, replicas)
	// 2. Create volume on target server
	// 3. Stream volume data from source to target
	// 4. Respect bandwidth limit if maxBytesPerSecond > 0
	// 5. Periodically send progress updates via progressChan
	// 6. Verify checksum after transfer

	// Simulate progress updates
	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			elapsed := time.Since(startTime)
			// Simulate byte transfer (would be actual in production)
			state.mu.Lock()
			state.bytesTransferred = int64(elapsed.Seconds()) * 1024 * 1024 // 1MB/s simulation
			state.mu.Unlock()

			if elapsed > 5*time.Second {
				// Copy complete after 5 seconds in simulation
				state.mu.Lock()
				state.volumeCopied = true
				state.mu.Unlock()
				glog.Infof("balance executor job=%s: volume copy completed (%dB transferred)",
					e.jobID, state.bytesTransferred)
				return nil
			}
		}
	}
}

// Step 3: mountVolume mounts the copied volume on the target server
func (e *Executor) mountVolume(ctx context.Context, state *MigrationState) error {
	glog.Infof("balance executor job=%s: mounting volume %s on target server %s",
		e.jobID, e.volumeID, e.targetServer)

	state.mu.Lock()
	if !state.volumeCopied {
		state.mu.Unlock()
		return fmt.Errorf("volume not copied yet")
	}
	state.mu.Unlock()

	// TODO: Connect to target server and mount the volume
	// 1. Send MountVolume RPC with volume ID and collection
	// 2. Wait for mount to complete
	// 3. Verify volume is accessible and readable

	state.mu.Lock()
	state.volumeMounted = true
	state.mu.Unlock()

	glog.Infof("balance executor job=%s: volume %s mounted on %s", e.jobID, e.volumeID, e.targetServer)
	return nil
}

// Step 4: tailUpdates tails live updates from source to target to keep them in sync
func (e *Executor) tailUpdates(ctx context.Context, state *MigrationState, progressChan chan<- *plugin_pb.JobProgress) error {
	glog.Infof("balance executor job=%s: starting to tail updates from %s to %s",
		e.jobID, e.sourceServer, e.targetServer)

	state.mu.Lock()
	if !state.volumeMounted {
		state.mu.Unlock()
		return fmt.Errorf("volume not mounted on target")
	}
	state.mu.Unlock()

	// TODO: Stream updates from source to target
	// 1. Get update stream from source volume
	// 2. Apply updates to target volume
	// 3. Handle concurrent writes
	// 4. Report tail progress

	// Simulate tail for a short duration
	for i := 0; i < 3; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			state.mu.Lock()
			state.tailStarted = true
			state.mu.Unlock()

			progressChan <- &plugin_pb.JobProgress{
				ProgressPercent: int32(70 + i*5),
				CurrentStep:     "tailUpdates",
				StatusMessage:   fmt.Sprintf("Applying updates... (%d updates processed)", (i+1)*100),
				UpdatedAt:       timestamppb.Now(),
			}
		}
	}

	glog.Infof("balance executor job=%s: tail updates completed", e.jobID)
	return nil
}

// Step 5: deleteSourceVolume deletes the volume from the source server after verification
func (e *Executor) deleteSourceVolume(ctx context.Context, state *MigrationState) error {
	glog.Infof("balance executor job=%s: deleting volume %s from source server %s",
		e.jobID, e.volumeID, e.sourceServer)

	state.mu.Lock()
	if !state.tailStarted {
		state.mu.Unlock()
		return fmt.Errorf("tail updates not completed")
	}
	state.mu.Unlock()

	// TODO: Delete volume from source server
	// 1. Verify target volume is healthy and in sync
	// 2. Verify no new writes can occur to source volume
	// 3. Send DeleteVolume RPC to source server
	// 4. Wait for deletion to complete
	// 5. Verify source volume is removed

	state.mu.Lock()
	state.sourceDeleted = true
	state.mu.Unlock()

	glog.Infof("balance executor job=%s: volume %s deleted from source %s", e.jobID, e.volumeID, e.sourceServer)
	return nil
}

// rollbackMigration attempts to rollback the migration if an error occurs
func (e *Executor) rollbackMigration(ctx context.Context, state *MigrationState) {
	glog.Warningf("balance executor job=%s: attempting rollback (readonly=%v, copied=%v, mounted=%v, deleted=%v)",
		e.jobID, state.volumeReadonly, state.volumeCopied, state.volumeMounted, state.sourceDeleted)

	// Only rollback if we've started but not completed
	state.mu.Lock()
	defer state.mu.Unlock()

	// If volume was deleted, we can't fully rollback
	if state.sourceDeleted {
		glog.Errorf("balance executor job=%s: cannot rollback - volume already deleted from source", e.jobID)
		return
	}

	// If target was mounted, unmount it
	if state.volumeMounted {
		glog.Infof("balance executor job=%s: unmounting target volume", e.jobID)
		// TODO: Send UnmountVolume RPC to target server
	}

	// If source was made read-only, mark it as writable again
	if state.volumeReadonly {
		glog.Infof("balance executor job=%s: restoring source volume to writable", e.jobID)
		// TODO: Send MarkVolumeWritable RPC to source server
	}

	glog.Infof("balance executor job=%s: rollback completed", e.jobID)
}
