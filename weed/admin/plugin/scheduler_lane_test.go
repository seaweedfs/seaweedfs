package plugin

import (
	"testing"
)

func TestJobTypeLaneMapCoversKnownTypes(t *testing.T) {
	// Every job type in the map must resolve to a valid lane.
	for jobType, lane := range jobTypeLaneMap {
		if lane != LaneDefault && lane != LaneIceberg && lane != LaneLifecycle {
			t.Errorf("jobTypeLaneMap[%q] = %q, want a known lane", jobType, lane)
		}
	}
}

func TestJobTypeLaneFallsBackToDefault(t *testing.T) {
	if got := JobTypeLane("unknown_job_type"); got != LaneDefault {
		t.Errorf("JobTypeLane(unknown) = %q, want %q", got, LaneDefault)
	}
}

func TestAllLanesHaveIdleSleep(t *testing.T) {
	for _, lane := range AllLanes() {
		if d := LaneIdleSleep(lane); d <= 0 {
			t.Errorf("LaneIdleSleep(%q) = %v, want > 0", lane, d)
		}
	}
}

func TestKnownJobTypesInMap(t *testing.T) {
	// Ensure the well-known job types are mapped. This catches drift
	// if a handler's job type string changes without updating the map.
	expected := map[string]SchedulerLane{
		"vacuum":              LaneDefault,
		"volume_balance":      LaneDefault,
		"ec_balance":          LaneDefault,
		"erasure_coding":      LaneDefault,
		"admin_script":        LaneDefault,
		"iceberg_maintenance": LaneIceberg,
		"s3_lifecycle":        LaneLifecycle,
	}
	for jobType, wantLane := range expected {
		if got := JobTypeLane(jobType); got != wantLane {
			t.Errorf("JobTypeLane(%q) = %q, want %q", jobType, got, wantLane)
		}
	}
}
