package plugin

import (
	"sync"
	"time"
)

// SchedulerLane identifies an independent scheduling track. Each lane runs
// its own goroutine, maintains its own detection timing, and acquires its
// own admin lock so that workloads in different lanes never block each other.
type SchedulerLane string

const (
	// LaneDefault handles volume management operations (vacuum, balance,
	// erasure coding) and admin scripts. It is the fallback lane for any
	// job type that is not explicitly mapped elsewhere.
	LaneDefault SchedulerLane = "default"

	// LaneIceberg handles table-bucket Iceberg compaction and maintenance.
	LaneIceberg SchedulerLane = "iceberg"

	// LaneLifecycle handles S3 object store lifecycle management
	// (expiration, transition, abort incomplete multipart uploads).
	LaneLifecycle SchedulerLane = "lifecycle"
)

// AllLanes returns every defined scheduler lane in a stable order.
func AllLanes() []SchedulerLane {
	return []SchedulerLane{LaneDefault, LaneIceberg, LaneLifecycle}
}

// laneIdleSleep maps each lane to its default idle sleep duration.
// Each lane can sleep for a different amount when no work is detected,
// independent of the per-job-type DetectionInterval.
var laneIdleSleep = map[SchedulerLane]time.Duration{
	LaneDefault:   61 * time.Second,
	LaneIceberg:   61 * time.Second,
	LaneLifecycle: 5 * time.Minute,
}

// LaneIdleSleep returns the idle sleep duration for the given lane,
// falling back to defaultSchedulerIdleSleep if the lane is unknown.
func LaneIdleSleep(lane SchedulerLane) time.Duration {
	if d, ok := laneIdleSleep[lane]; ok {
		return d
	}
	return defaultSchedulerIdleSleep
}

// jobTypeLaneMap is the hardcoded mapping from job type to scheduler lane.
// Job types not present here are assigned to LaneDefault.
var jobTypeLaneMap = map[string]SchedulerLane{
	// Volume management (default lane)
	"vacuum":         LaneDefault,
	"volume_balance": LaneDefault,
	"ec_balance":     LaneDefault,
	"erasure_coding": LaneDefault,
	"admin_script":   LaneDefault,

	// Iceberg table maintenance
	"iceberg_maintenance": LaneIceberg,

	// S3 lifecycle management
	"s3_lifecycle": LaneLifecycle,
}

// JobTypeLane returns the scheduler lane for the given job type.
// Unknown job types are assigned to LaneDefault.
func JobTypeLane(jobType string) SchedulerLane {
	if lane, ok := jobTypeLaneMap[jobType]; ok {
		return lane
	}
	return LaneDefault
}

// LaneJobTypes returns the set of known job types assigned to the given lane.
func LaneJobTypes(lane SchedulerLane) []string {
	var result []string
	for jobType, l := range jobTypeLaneMap {
		if l == lane {
			result = append(result, jobType)
		}
	}
	return result
}

// schedulerLaneState holds the per-lane runtime state used by the scheduler.
type schedulerLaneState struct {
	lane   SchedulerLane
	wakeCh chan struct{}

	loopMu sync.Mutex
	loop   schedulerLoopState

	// Per-lane execution reservation pool. Each lane tracks how many
	// execution slots it has reserved on each worker independently,
	// so lanes cannot starve each other.
	execMu sync.Mutex
	execRes map[string]int
}

// newLaneState creates a schedulerLaneState for the given lane.
func newLaneState(lane SchedulerLane) *schedulerLaneState {
	return &schedulerLaneState{
		lane:    lane,
		wakeCh:  make(chan struct{}, 1),
		execRes: make(map[string]int),
	}
}
