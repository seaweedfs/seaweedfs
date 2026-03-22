package delete_empty

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func makeWorker(id string, hasCapability bool, currentLoad, maxConcurrent int) *types.WorkerData {
	caps := []types.TaskType{}
	if hasCapability {
		caps = append(caps, types.TaskTypeCompaction)
	}
	return &types.WorkerData{
		ID:            id,
		Status:        "active",
		Capabilities:  caps,
		CurrentLoad:   currentLoad,
		MaxConcurrent: maxConcurrent,
	}
}

func makeCompactionTask() *types.TaskInput {
	return makeTask(types.TaskTypeCompaction)
}

func makeTask(taskType types.TaskType) *types.TaskInput {
	return &types.TaskInput{
		Type: taskType,
	}
}

func schedulingConfig(maxConcurrent int) *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:       true,
			MaxConcurrent: maxConcurrent,
		},
		QuietForSeconds: 3600,
	}
}

func TestScheduling_AvailableWorkerWithCapability(t *testing.T) {
	task := makeCompactionTask()
	workers := []*types.WorkerData{
		makeWorker("worker-1", true, 0, 2),
	}

	result := Scheduling(task, nil, workers, schedulingConfig(1))
	if !result {
		t.Error("Expected Scheduling=true with capable idle worker")
	}
}

func TestScheduling_NoWorkersWithCapability(t *testing.T) {
	task := makeCompactionTask()
	workers := []*types.WorkerData{
		makeWorker("worker-1", false, 0, 2), // no delete_empty capability
	}

	result := Scheduling(task, nil, workers, schedulingConfig(1))
	if result {
		t.Error("Expected Scheduling=false when no worker has capability")
	}
}

func TestScheduling_WorkerAtCapacity(t *testing.T) {
	task := makeCompactionTask()
	workers := []*types.WorkerData{
		makeWorker("worker-1", true, 2, 2), // CurrentLoad == MaxConcurrent
	}

	result := Scheduling(task, nil, workers, schedulingConfig(1))
	if result {
		t.Error("Expected Scheduling=false when worker is at capacity")
	}
}

func TestScheduling_AtConcurrencyLimit(t *testing.T) {
	task := makeCompactionTask()
	// Already running 1 compaction task, config max is 1
	running := []*types.TaskInput{
		makeCompactionTask(),
	}
	workers := []*types.WorkerData{
		makeWorker("worker-1", true, 0, 4),
	}

	result := Scheduling(task, running, workers, schedulingConfig(1))
	if result {
		t.Error("Expected Scheduling=false when at concurrency limit (1/1)")
	}
}

func TestScheduling_BelowConcurrencyLimit(t *testing.T) {
	task := makeCompactionTask()
	// 1 running, max is 2 → still room
	running := []*types.TaskInput{
		makeCompactionTask(),
	}
	workers := []*types.WorkerData{
		makeWorker("worker-1", true, 0, 4),
	}

	result := Scheduling(task, running, workers, schedulingConfig(2))
	if !result {
		t.Error("Expected Scheduling=true when below concurrency limit (1/2)")
	}
}

func TestScheduling_OtherTaskTypesDoNotCountAgainstLimit(t *testing.T) {
	task := makeTask(types.TaskTypeDeleteEmpty)
	// Running vacuum tasks should not count against delete_empty limit
	running := []*types.TaskInput{
		makeTask(types.TaskTypeVacuum),
		makeTask(types.TaskTypeBalance),
	}
	workers := []*types.WorkerData{
		makeWorker("worker-1", true, 0, 4),
	}

	result := Scheduling(task, running, workers, schedulingConfig(1))
	if !result {
		t.Error("Expected Scheduling=true: other task types don't count against delete_empty limit")
	}
}

func TestScheduling_NoWorkers(t *testing.T) {
	task := makeCompactionTask()

	result := Scheduling(task, nil, nil, schedulingConfig(1))
	if result {
		t.Error("Expected Scheduling=false with no workers")
	}
}

func TestScheduling_MultipleWorkers_OnlyOneCapable(t *testing.T) {
	task := makeCompactionTask()
	workers := []*types.WorkerData{
		makeWorker("worker-1", false, 0, 4),
		makeWorker("worker-2", false, 0, 4),
		makeWorker("worker-3", true, 1, 4), // capable and has room
	}

	result := Scheduling(task, nil, workers, schedulingConfig(2))
	if !result {
		t.Error("Expected Scheduling=true: worker-3 is capable and has room")
	}
}
