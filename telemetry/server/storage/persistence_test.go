package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data", "telemetry-state.json")

	// promauto registers on the global registry, so the whole test uses a
	// single storage instance.
	s := NewPrometheusStorage()

	if err := s.SaveStateIfDirty(path); err != nil {
		t.Fatalf("clean save: %v", err)
	}
	if _, err := s.LoadState(path); err != nil {
		t.Fatalf("load with no state file: %v", err)
	}

	report := &proto.TelemetryData{
		TopologyId:        "test-cluster-1",
		Version:           "4.40",
		Os:                "linux/amd64",
		VolumeServerCount: 5,
		TotalDiskBytes:    123456789,
		TotalVolumeCount:  42,
		FilerCount:        2,
		BrokerCount:       1,
		Timestamp:         time.Now().Unix(),
	}
	if err := s.StoreTelemetry(report); err != nil {
		t.Fatalf("store: %v", err)
	}
	receivedAt := s.instances[report.TopologyId].ReceivedAt

	if err := s.SaveStateIfDirty(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	if s.dirty {
		t.Fatal("dirty flag not cleared after save")
	}

	// Simulate a restart: wipe the in-memory map, then restore.
	s.instances = make(map[string]*telemetryData)
	n, err := s.LoadState(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if n != 1 {
		t.Fatalf("loaded %d instances, want 1", n)
	}

	got, ok := s.instances[report.TopologyId]
	if !ok {
		t.Fatal("instance missing after load")
	}
	if !got.ReceivedAt.Equal(receivedAt) {
		t.Errorf("ReceivedAt not preserved: got %v, want %v", got.ReceivedAt, receivedAt)
	}
	if got.TelemetryData.TotalDiskBytes != report.TotalDiskBytes ||
		got.TelemetryData.Version != report.Version ||
		got.TelemetryData.VolumeServerCount != report.VolumeServerCount {
		t.Errorf("fields not preserved: got %+v", got.TelemetryData)
	}

	// Load must not mark state dirty.
	if s.dirty {
		t.Error("dirty flag set after load")
	}
}
