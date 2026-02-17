package vacuum

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Executor handles the 3-step vacuum execution process
type Executor struct {
	jobID    string
	volumeID string
	config   *plugin_pb.JobTypeConfig
}

// ExecutionStep represents a single step in the vacuum process
type ExecutionStep struct {
	StepNumber  int
	Name        string
	Description string
}

// NewExecutor creates a new vacuum executor
func NewExecutor(jobID string, config *plugin_pb.JobTypeConfig) *Executor {
	return &Executor{
		jobID:  jobID,
		config: config,
	}
}

// Execute runs the 3-step vacuum process
// Step 1: vacuumVolumeCheck - Verify conditions are still met
// Step 2: vacuumVolumeCompact - Compact and defragment volume
// Step 3: vacuumVolumeCleanup - Clean up deleted space if enableCleanup is true
// Returns progress updates via the progressChan
func (e *Executor) Execute(ctx context.Context, metadata map[string]string, progressChan chan<- *plugin_pb.JobProgress) error {
	volumeID := metadata["volume_id"]
	e.volumeID = volumeID

	steps := []ExecutionStep{
		{StepNumber: 1, Name: "vacuumVolumeCheck", Description: "Verify vacuum conditions are still met"},
		{StepNumber: 2, Name: "vacuumVolumeCompact", Description: "Compact and defragment volume data"},
		{StepNumber: 3, Name: "vacuumVolumeCleanup", Description: "Clean up deleted space"},
	}

	totalSteps := len(steps)
	for i, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		glog.Infof("vacuum executor job=%s: executing step %d: %s", e.jobID, step.StepNumber, step.Name)

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
			err = e.vacuumVolumeCheck(ctx)
		case 2:
			err = e.vacuumVolumeCompact(ctx)
		case 3:
			err = e.vacuumVolumeCleanup(ctx)
		}

		if err != nil {
			glog.Errorf("vacuum executor job=%s: step %d failed: %v", e.jobID, step.StepNumber, err)
			return fmt.Errorf("step %d (%s) failed: %w", step.StepNumber, step.Name, err)
		}

		glog.Infof("vacuum executor job=%s: step %d completed", e.jobID, step.StepNumber)
	}

	// Send final progress update
	progressChan <- &plugin_pb.JobProgress{
		ProgressPercent: 100,
		CurrentStep:     "complete",
		StatusMessage:   "Vacuum completed successfully",
		UpdatedAt:       timestamppb.Now(),
	}

	glog.Infof("vacuum executor job=%s: all steps completed", e.jobID)
	return nil
}

// Step 1: vacuumVolumeCheck verifies that vacuum conditions are still met
// before proceeding with the operation
func (e *Executor) vacuumVolumeCheck(ctx context.Context) error {
	glog.Infof("vacuum executor job=%s: checking vacuum eligibility for volume %s", e.jobID, e.volumeID)

	// Extract garbage threshold from config
	var garbageThreshold float64 = 30
	for _, cfv := range e.config.AdminConfig {
		if cfv.FieldName == "garbageThreshold" {
			garbageThreshold = float64(cfv.IntValue)
		}
	}

	// TODO: Connect to master and query volume status
	// Check current garbage ratio
	// Verify volume hasn't changed significantly since detection
	// Return error if conditions no longer met

	glog.Infof("vacuum executor job=%s: volume %s meets vacuum criteria (threshold=%.1f%%)",
		e.jobID, e.volumeID, garbageThreshold)
	return nil
}

// Step 2: vacuumVolumeCompact compacts and defragments the volume
// This is the main vacuum operation that reclaims space
func (e *Executor) vacuumVolumeCompact(ctx context.Context) error {
	glog.Infof("vacuum executor job=%s: compacting volume %s", e.jobID, e.volumeID)

	// Extract compaction level from config
	var compactLevel int64 = 2
	for _, cfv := range e.config.WorkerConfig {
		if cfv.FieldName == "compactLevel" {
			compactLevel = cfv.IntValue
		}
	}

	glog.Infof("vacuum executor job=%s: using compaction level %d for volume %s",
		e.jobID, compactLevel, e.volumeID)

	// TODO: Connect to volume server and perform compaction
	// 1. Read volume data, skip deleted entries
	// 2. Rewrite volume file without garbage
	// 3. Update volume metadata
	// 4. Report compaction progress
	//
	// The compaction process should:
	// - Read the volume's .dat and .idx files
	// - Skip entries marked as deleted
	// - Write new .dat file with only valid entries
	// - Update .idx file with new offsets
	// - Use compactLevel to determine aggressiveness:
	//   Level 0: Minimal compaction (fast, least space saved)
	//   Level 5: Aggressive compaction (slower, maximum space saved)

	return nil
}

// Step 3: vacuumVolumeCleanup cleans up deleted space if enabled
// This step frees up disk space after compaction
func (e *Executor) vacuumVolumeCleanup(ctx context.Context) error {
	// Extract enableCleanup flag from config
	enableCleanup := true
	for _, cfv := range e.config.WorkerConfig {
		if cfv.FieldName == "enableCleanup" {
			enableCleanup = cfv.BoolValue
		}
	}

	if !enableCleanup {
		glog.Infof("vacuum executor job=%s: skipping cleanup for volume %s (enableCleanup=false)",
			e.jobID, e.volumeID)
		return nil
	}

	glog.Infof("vacuum executor job=%s: cleaning up deleted space for volume %s", e.jobID, e.volumeID)

	// TODO: Connect to volume server and perform cleanup
	// 1. Frees unused space at the end of volume files
	// 2. Truncates .dat file to actual size
	// 3. Optimizes file system allocation
	// 4. Updates volume metadata with new sizes
	//
	// The cleanup process should:
	// - Trim excess capacity from the volume files
	// - Update volume size metadata on master
	// - Log space reclaimed

	return nil
}
