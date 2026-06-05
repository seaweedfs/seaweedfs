package weed_server

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"testing"

	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// writeLegacySnapshot writes a seaweedfs/raft (legacy) snapshot carrying the
// given FSM state, in the on-disk format readLegacyRaftSnapshotState expects.
func writeLegacySnapshot(t *testing.T, dataDir string, maxVolumeId needle.VolumeId, topologyId string) {
	t.Helper()
	snapshotDir := path.Join(dataDir, "snapshot")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(topology.MaxVolumeIdCommand{MaxVolumeId: maxVolumeId, TopologyId: topologyId})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(struct {
		State json.RawMessage `json:"state"`
	}{State: state})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path.Join(snapshotDir, "0_1.ss"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%08x\n", crc32.ChecksumIEEE(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(body); err != nil {
		t.Fatal(err)
	}
}

func openStableStore(t *testing.T, dataDir string) *boltdb.BoltStore {
	t.Helper()
	sdb, err := boltdb.NewBoltStore(filepath.Join(dataDir, sdbFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sdb.Close() })
	return sdb
}

func newTestTopology() *topology.Topology {
	return topology.NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
}

func TestMigrateLegacyRaftState_ImportsIdentityOnce(t *testing.T) {
	dir := t.TempDir()
	writeLegacySnapshot(t, dir, 4242, "cluster-abc")

	sdb := openStableStore(t, dir)
	topo := newTestTopology()

	migrateLegacyRaftStateIfNeeded(sdb, dir, topo)

	if got := topo.GetTopologyId(); got != "cluster-abc" {
		t.Fatalf("TopologyId = %q, want cluster-abc", got)
	}
	if got := topo.GetMaxVolumeId(); got != needle.VolumeId(4242) {
		t.Fatalf("MaxVolumeId = %d, want 4242", got)
	}
	if v, err := sdb.Get([]byte(legacyMigrationKey)); err != nil || len(v) == 0 {
		t.Fatalf("migration marker not set: v=%q err=%v", v, err)
	}

	// Second run on a fresh topology must NOT re-import: the marker gates it.
	topo2 := newTestTopology()
	migrateLegacyRaftStateIfNeeded(sdb, dir, topo2)
	if got := topo2.GetTopologyId(); got != "" {
		t.Fatalf("re-import after marker set: TopologyId = %q, want empty", got)
	}
}

func TestMigrateLegacyRaftState_NoLegacyJustMarks(t *testing.T) {
	dir := t.TempDir()
	sdb := openStableStore(t, dir)
	topo := newTestTopology()

	migrateLegacyRaftStateIfNeeded(sdb, dir, topo)

	if got := topo.GetTopologyId(); got != "" {
		t.Fatalf("TopologyId = %q, want empty (nothing to import)", got)
	}
	if v, err := sdb.Get([]byte(legacyMigrationKey)); err != nil || len(v) == 0 {
		t.Fatalf("migration marker not set on fresh install: v=%q err=%v", v, err)
	}
}

func TestLegacyRaftStateExists(t *testing.T) {
	dir := t.TempDir()
	if legacyRaftStateExists(dir) {
		t.Fatal("empty dir reported as having legacy state")
	}
	if err := os.WriteFile(path.Join(dir, "log"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if !legacyRaftStateExists(dir) {
		t.Fatal("legacy log file not detected")
	}
}
