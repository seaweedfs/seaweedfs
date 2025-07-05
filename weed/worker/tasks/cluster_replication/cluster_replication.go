package cluster_replication

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements cluster replication operation to replicate data between clusters
type Task struct {
	*tasks.BaseTask
	server             string
	volumeID           uint32
	collection         string
	destinationCluster string
}

// NewTask creates a new cluster replication task instance
func NewTask(server string, volumeID uint32, collection string, destinationCluster string) *Task {
	task := &Task{
		BaseTask:           tasks.NewBaseTask(types.TaskTypeClusterReplication),
		server:             server,
		volumeID:           volumeID,
		collection:         collection,
		destinationCluster: destinationCluster,
	}
	return task
}

// Execute executes the cluster replication task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting cluster replication task for volume %d on server %s (collection: %s, destination: %s)",
		t.volumeID, t.server, t.collection, t.destinationCluster)

	// Simulate cluster replication operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Connecting to destination cluster", 2 * time.Second, 10},
		{"Analyzing replication state", 1 * time.Second, 20},
		{"Transferring data", 12 * time.Second, 80},
		{"Verifying replication", 2 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("cluster replication task cancelled")
		}

		glog.V(1).Infof("Cluster replication task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Cluster replication task completed for volume %d on server %s", t.volumeID, t.server)
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
	// destination_cluster parameter could be validated from params.Parameters
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for cluster replication operation
	baseTime := 60 * time.Second

	// Could adjust based on volume size or network conditions
	return baseTime
}

// Factory creates cluster replication task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new cluster replication task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeClusterReplication,
			[]string{"cluster_replication", "storage", "disaster_recovery"},
			"Replicate data between clusters for disaster recovery",
		),
	}
}

// Create creates a new cluster replication task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Extract destination cluster from parameters
	destinationCluster := ""
	if params.Parameters != nil {
		if dest, ok := params.Parameters["destination_cluster"].(string); ok {
			destinationCluster = dest
		}
	}

	task := NewTask(params.Server, params.VolumeID, params.Collection, destinationCluster)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// Register registers the cluster replication task with the given registry
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register(types.TaskTypeClusterReplication, factory)
	glog.V(1).Infof("Registered cluster replication task type")
}
