package topology

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestMaxVolumeIdFileRestore(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "max_volume_id")

	topo := NewTopology("topo", nil, 0, 5, false)
	topo.SetMaxVolumeIdFile(file)
	if err := topo.PersistMaxVolumeId(needle.VolumeId(41)); err != nil {
		t.Fatalf("persist: %v", err)
	}

	restarted := NewTopology("topo", nil, 0, 5, false)
	restarted.SetMaxVolumeIdFile(file)
	if got := restarted.GetMaxVolumeId(); got != needle.VolumeId(41) {
		t.Fatalf("restored max volume id = %d, want 41", got)
	}
}

func TestMaxVolumeIdFileRestoresTopologyId(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "max_volume_id")

	topo := NewTopology("topo", nil, 0, 5, false)
	topo.SetTopologyId("cluster-a")
	topo.SetMaxVolumeIdFile(file)
	if err := topo.PersistMaxVolumeId(needle.VolumeId(7)); err != nil {
		t.Fatalf("persist: %v", err)
	}

	restarted := NewTopology("topo", nil, 0, 5, false)
	restarted.SetMaxVolumeIdFile(file)
	if got := restarted.GetTopologyId(); got != "cluster-a" {
		t.Fatalf("restored topology id = %q, want cluster-a", got)
	}
}

func TestMaxVolumeIdFileRewritesOnTopologyIdChange(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "max_volume_id")

	topo := NewTopology("topo", nil, 0, 5, false)
	topo.SetMaxVolumeIdFile(file)
	if err := topo.PersistMaxVolumeId(needle.VolumeId(7)); err != nil {
		t.Fatalf("persist: %v", err)
	}
	topo.SetTopologyId("cluster-b")
	if err := topo.PersistMaxVolumeId(needle.VolumeId(7)); err != nil {
		t.Fatalf("persist same bound with new topology id: %v", err)
	}

	restarted := NewTopology("topo", nil, 0, 5, false)
	restarted.SetMaxVolumeIdFile(file)
	if got := restarted.GetTopologyId(); got != "cluster-b" {
		t.Fatalf("restored topology id = %q, want cluster-b", got)
	}
}

func TestMaxVolumeIdFileMonotonic(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "max_volume_id")

	topo := NewTopology("topo", nil, 0, 5, false)
	topo.SetMaxVolumeIdFile(file)
	if err := topo.PersistMaxVolumeId(needle.VolumeId(10)); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := topo.PersistMaxVolumeId(needle.VolumeId(5)); err != nil {
		t.Fatalf("persist lower: %v", err)
	}
	if got := topo.lastPersistedMaxVolumeId.Load(); got != 10 {
		t.Fatalf("lastPersisted = %d, want 10", got)
	}

	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("mirror file is empty")
	}
}

func TestMaxVolumeIdFileCorrupt(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "max_volume_id")
	if err := os.WriteFile(file, []byte("{not json"), 0644); err != nil {
		t.Fatal(err)
	}

	topo := NewTopology("topo", nil, 0, 5, false)
	topo.SetMaxVolumeIdFile(file)
	if got := topo.GetMaxVolumeId(); got != 0 {
		t.Fatalf("corrupt file seeded bound %d, want 0", got)
	}
}
