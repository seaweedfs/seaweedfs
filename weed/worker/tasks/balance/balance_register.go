package balance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Auto-register this task when the package is imported
func init() {
	RegisterBalanceTask()
}

// RegisterBalanceTask registers the balance task with the new architecture
func RegisterBalanceTask() {
	// Create configuration instance
	config := NewDefaultConfig()

	// Create complete task definition
	taskDef := &base.TaskDefinition{
		Type:         types.TaskTypeBalance,
		Name:         "balance",
		DisplayName:  "Volume Balance",
		Description:  "Balances volume distribution across servers",
		Icon:         "fas fa-balance-scale text-warning",
		Capabilities: []string{"balance", "distribution"},

		Config:         config,
		ConfigSpec:     GetConfigSpec(),
		CreateTask:     CreateTask,
		DetectionFunc:  Detection,
		ScanInterval:   30 * time.Minute,
		SchedulingFunc: Scheduling,
		MaxConcurrent:  1,
		RepeatInterval: 2 * time.Hour,
	}

	// Register everything with a single function call!
	base.RegisterTask(taskDef)
}
