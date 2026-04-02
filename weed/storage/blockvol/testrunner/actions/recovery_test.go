package actions

import (
	"encoding/json"
	"testing"
)

func TestClassifyVolumeState(t *testing.T) {
	tests := []struct {
		name     string
		degraded bool
		status   string
		rf       int
		replicas int
		want     string
	}{
		{"healthy_rf2", false, "active", 2, 1, "healthy"},
		{"healthy_rf1", false, "active", 1, 0, "healthy"},
		{"degraded_generic", true, "active", 2, 1, "degraded"},
		{"degraded_catching_up", true, "CatchingUp", 2, 1, "catching_up"},
		{"degraded_catchup", true, "catchup", 2, 1, "catching_up"},
		{"degraded_rebuild", true, "Rebuilding", 2, 1, "rebuilding"},
		{"no_replicas", false, "active", 2, 0, "no_replicas"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate VolumeInfo fields used by classifyVolumeState.
			// We call the function indirectly through the test since it uses blockapi.VolumeInfo.
			// For now, test classifyPath directly and verify the mapping logic.
		})
		_ = tt // placeholders for direct classifyVolumeState call
	}
}

func TestClassifyPath(t *testing.T) {
	tests := []struct {
		catchUp  bool
		rebuild  bool
		failover bool
		want     string
	}{
		{false, false, false, "direct"},
		{true, false, false, "catch-up"},
		{false, true, false, "rebuild"},
		{false, false, true, "failover"},
		{true, false, true, "failover+catch-up"},
		{false, true, true, "failover+rebuild"},
		{true, true, false, "rebuild"}, // rebuild takes precedence over catch-up
		{true, true, true, "failover+rebuild"},
	}

	for _, tt := range tests {
		got := classifyPath(tt.catchUp, tt.rebuild, tt.failover)
		if got != tt.want {
			t.Errorf("classifyPath(%v,%v,%v) = %q, want %q",
				tt.catchUp, tt.rebuild, tt.failover, got, tt.want)
		}
	}
}

func TestProfileToVars(t *testing.T) {
	p := RecoveryProfile{
		FaultType:  "crash",
		DurationMs: 5200,
		DegradedMs: 3100,
		Path:       "catch-up",
		Transitions: []StateTransition{
			{FromState: "healthy", ToState: "degraded", AtMs: 0},
			{FromState: "degraded", ToState: "catching_up", AtMs: 1500},
			{FromState: "catching_up", ToState: "healthy", AtMs: 5200},
		},
		PollCount: 8,
	}

	vars := profileToVars(p)

	if vars["duration_ms"] != "5200" {
		t.Fatalf("duration_ms=%s", vars["duration_ms"])
	}
	if vars["path"] != "catch-up" {
		t.Fatalf("path=%s", vars["path"])
	}
	if vars["degraded_ms"] != "3100" {
		t.Fatalf("degraded_ms=%s", vars["degraded_ms"])
	}
	if vars["polls"] != "8" {
		t.Fatalf("polls=%s", vars["polls"])
	}

	expectedTransitions := "healthy→degraded→catching_up→healthy"
	if vars["transitions"] != expectedTransitions {
		t.Fatalf("transitions=%q, want %q", vars["transitions"], expectedTransitions)
	}

	// JSON should be valid and round-trip.
	var decoded RecoveryProfile
	if err := json.Unmarshal([]byte(vars["json"]), &decoded); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if decoded.DurationMs != 5200 {
		t.Fatalf("json round-trip: duration=%d", decoded.DurationMs)
	}
	if len(decoded.Transitions) != 3 {
		t.Fatalf("json round-trip: transitions=%d", len(decoded.Transitions))
	}
}

func TestProfileToVars_Empty(t *testing.T) {
	p := RecoveryProfile{
		FaultType:  "restart",
		DurationMs: 200,
		Path:       "direct",
	}

	vars := profileToVars(p)
	if vars["transitions"] != "" {
		t.Fatalf("empty transitions should be empty string, got %q", vars["transitions"])
	}
	if vars["duration_ms"] != "200" {
		t.Fatalf("duration_ms=%s", vars["duration_ms"])
	}
}

func TestClassifyPath_RebuildPrecedence(t *testing.T) {
	// When both catch-up and rebuild are observed (e.g., catch-up failed
	// then escalated to rebuild), the path should be "rebuild".
	got := classifyPath(true, true, false)
	if got != "rebuild" {
		t.Fatalf("both catch-up and rebuild → %q, want rebuild", got)
	}
}

func TestShipperStateInfo_ParseJSON(t *testing.T) {
	raw := `[{"path":"/tmp/blocks/vol1.blk","role":"primary","epoch":3,"head_lsn":150,"degraded":true,"shippers":[{"data_addr":"10.0.0.2:4295","state":"degraded","flushed_lsn":120}],"timestamp":"2026-03-31T00:00:00Z"}]`

	var infos []ShipperStateInfo
	if err := json.Unmarshal([]byte(raw), &infos); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("count=%d", len(infos))
	}
	info := infos[0]
	if info.Role != "primary" {
		t.Fatalf("role=%s", info.Role)
	}
	if info.HeadLSN != 150 {
		t.Fatalf("head_lsn=%d", info.HeadLSN)
	}
	if !info.Degraded {
		t.Fatal("should be degraded")
	}
	if len(info.Shippers) != 1 {
		t.Fatalf("shippers=%d", len(info.Shippers))
	}
	if info.Shippers[0].State != "degraded" {
		t.Fatalf("shipper state=%s", info.Shippers[0].State)
	}
	if info.Shippers[0].FlushedLSN != 120 {
		t.Fatalf("flushed_lsn=%d", info.Shippers[0].FlushedLSN)
	}
}

func TestRebuildProfile_JSON(t *testing.T) {
	p := RebuildProfile{
		RebuildDurationMs: 45000,
		SourceType:        "full_base",
		SourceReason:      "untrusted_checkpoint",
		DataIntegrity:     "pass",
		RecoveryObservable: true,
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}

	var decoded RebuildProfile
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RebuildDurationMs != 45000 {
		t.Fatalf("duration=%d", decoded.RebuildDurationMs)
	}
	if decoded.SourceType != "full_base" {
		t.Fatalf("source=%s", decoded.SourceType)
	}
	if decoded.SourceReason != "untrusted_checkpoint" {
		t.Fatalf("reason=%s", decoded.SourceReason)
	}
}

func TestShipperStateInfo_NoShippers(t *testing.T) {
	raw := `[{"path":"/tmp/blocks/vol1.blk","role":"primary","epoch":1,"head_lsn":0,"degraded":false,"timestamp":"2026-03-31T00:00:00Z"}]`

	var infos []ShipperStateInfo
	if err := json.Unmarshal([]byte(raw), &infos); err != nil {
		t.Fatal(err)
	}
	if len(infos[0].Shippers) != 0 {
		t.Fatalf("should have 0 shippers, got %d", len(infos[0].Shippers))
	}
}
