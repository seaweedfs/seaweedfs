package erasure_coding

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Executor handles the 6-step erasure coding execution process
type Executor struct {
	jobID    string
	volumeID string
	config   *plugin_pb.JobTypeConfig
}

// NewExecutor creates a new erasure coding executor
func NewExecutor(jobID string, config *plugin_pb.JobTypeConfig) *Executor {
	return &Executor{
		jobID:  jobID,
		config: config,
	}
}

// ExecutionStep represents a single step in the EC process
type ExecutionStep struct {
	StepNumber  int
	Name        string
	Description string
}

// Execute runs the 6-step erasure coding process
// Returns progress updates via the progressChan
func (e *Executor) Execute(ctx context.Context, metadata map[string]string, progressChan chan<- *plugin_pb.JobProgress) error {
	volumeID := metadata["volume_id"]
	e.volumeID = volumeID

	steps := []ExecutionStep{
		{StepNumber: 1, Name: "markVolumeReadonly", Description: "Mark volume as read-only"},
		{StepNumber: 2, Name: "copyVolumeFilesToWorker", Description: "Copy volume files to worker"},
		{StepNumber: 3, Name: "generateEcShards", Description: "Generate erasure coding shards"},
		{StepNumber: 4, Name: "distributeEcShards", Description: "Distribute shards to data nodes"},
		{StepNumber: 5, Name: "mountEcShards", Description: "Mount shards on target nodes"},
		{StepNumber: 6, Name: "deleteOriginalVolume", Description: "Delete original volume"},
	}

	totalSteps := len(steps)
	for i, step := range steps {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		glog.Infof("erasure_coding executor job=%s: executing step %d: %s", e.jobID, step.StepNumber, step.Name)

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
			err = e.markVolumeReadonly(ctx)
		case 2:
			err = e.copyVolumeFilesToWorker(ctx)
		case 3:
			err = e.generateEcShards(ctx)
		case 4:
			err = e.distributeEcShards(ctx)
		case 5:
			err = e.mountEcShards(ctx)
		case 6:
			err = e.deleteOriginalVolume(ctx)
		}

		if err != nil {
			glog.Errorf("erasure_coding executor job=%s: step %d failed: %v", e.jobID, step.StepNumber, err)
			return fmt.Errorf("step %d (%s) failed: %w", step.StepNumber, step.Name, err)
		}

		glog.Infof("erasure_coding executor job=%s: step %d completed", e.jobID, step.StepNumber)
	}

	// Send final progress update
	progressChan <- &plugin_pb.JobProgress{
		ProgressPercent: 100,
		CurrentStep:     "complete",
		StatusMessage:   "Erasure coding completed successfully",
		UpdatedAt:       timestamppb.Now(),
	}

	glog.Infof("erasure_coding executor job=%s: all steps completed", e.jobID)
	return nil
}

// Step 1: markVolumeReadonly marks the volume as read-only
func (e *Executor) markVolumeReadonly(ctx context.Context) error {
	glog.Infof("erasure_coding executor job=%s: marking volume %s as read-only", e.jobID, e.volumeID)

	// TODO: Connect to master and mark volume as read-only
	// This prevents new writes during encoding

	return nil
}

// Step 2: copyVolumeFilesToWorker copies volume data to the worker
func (e *Executor) copyVolumeFilesToWorker(ctx context.Context) error {
	glog.Infof("erasure_coding executor job=%s: copying volume files for %s", e.jobID, e.volumeID)

	// TODO: Transfer volume files from storage nodes to worker
	// This includes the .idx and .dat files

	return nil
}

// Step 3: generateEcShards generates erasure coding shards
func (e *Executor) generateEcShards(ctx context.Context) error {
	glog.Infof("erasure_coding executor job=%s: generating EC shards for volume %s", e.jobID, e.volumeID)

	// Extract config values
	var ecM, ecN, stripeSize int64
	for _, cfv := range e.config.WorkerConfig {
		if cfv.FieldName == "ec_m" {
			ecM = cfv.IntValue
		} else if cfv.FieldName == "ec_n" {
			ecN = cfv.IntValue
		} else if cfv.FieldName == "stripe_size" {
			stripeSize = cfv.IntValue
		}
	}

	if ecM == 0 {
		ecM = 10
	}
	if ecN == 0 {
		ecN = 4
	}
	if stripeSize == 0 {
		stripeSize = 65536
	}

	glog.Infof("erasure_coding executor job=%s: encoding with M=%d, N=%d, stripe_size=%d", e.jobID, ecM, ecN, stripeSize)

	// TODO: Use libReedSolomon or similar library to generate EC shards
	// This creates M data shards and N parity shards

	return nil
}

// Step 4: distributeEcShards distributes shards to data nodes
func (e *Executor) distributeEcShards(ctx context.Context) error {
	glog.Infof("erasure_coding executor job=%s: distributing EC shards to destination nodes", e.jobID)

	// Extract destination nodes from config
	var destinationNodes []string
	for _, cfv := range e.config.AdminConfig {
		if cfv.FieldName == "destination_data_nodes" {
			if cfv.StringValue != "" {
				destinationNodes = append(destinationNodes, cfv.StringValue)
			}
		}
	}

	glog.Infof("erasure_coding executor job=%s: distributing to %d destination nodes", e.jobID, len(destinationNodes))

	// TODO: Send shards to each destination data node
	// Balance shards across available nodes

	return nil
}

// Step 5: mountEcShards mounts the shards on their target nodes
func (e *Executor) mountEcShards(ctx context.Context) error {
	glog.Infof("erasure_coding executor job=%s: mounting EC shards", e.jobID)

	// TODO: Notify data nodes to mount the EC shards
	// Register shards with master server

	return nil
}

// Step 6: deleteOriginalVolume deletes the original volume if configured
func (e *Executor) deleteOriginalVolume(ctx context.Context) error {
	// Check if delete_source is enabled
	var deleteSource bool
	for _, cfv := range e.config.AdminConfig {
		if cfv.FieldName == "delete_source" {
			deleteSource = cfv.BoolValue
		}
	}

	if !deleteSource {
		glog.Infof("erasure_coding executor job=%s: skipping deletion of original volume (delete_source=false)", e.jobID)
		return nil
	}

	glog.Infof("erasure_coding executor job=%s: deleting original volume %s", e.jobID, e.volumeID)

	// TODO: Delete the original volume from the master
	// This frees up space on the storage nodes

	return nil
}
