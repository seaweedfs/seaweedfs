package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// fileExists is a tiny test helper for asserting file presence.
func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be absent, got err=%v", path, err)
	}
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

// A crash after the .idx was already renamed away but before the .cpx->.idx
// rename committed leaves only .cpd + .cpx + .cpc on disk. The directory
// pre-pass must roll the swap FORWARD: finish the renames and produce a
// loadable, writable volume holding the compacted needle count. Before the
// fix there was no marker and the loader simply discarded the orphan temp
// files, losing the entire compacted generation.
func TestReconcileRollForwardMarkerOnly(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapLevelDb, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	const liveCount = 6
	for i := uint64(1); i <= liveCount; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Delete two so the compacted generation is strictly smaller than the source.
	for _, id := range []uint64{2, 5} {
		if _, err := v.deleteNeedle2(newEmptyNeedle(id)); err != nil {
			t.Fatalf("delete %d: %v", id, err)
		}
	}
	expectedLive := uint64(liveCount - 2)

	// Generate a valid .cpd/.cpx pair, then close the volume.
	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("compact by index: %v", err)
	}
	v.Close()

	cpd := filepath.Join(dir, "1.cpd")
	cpx := filepath.Join(dir, "1.cpx")
	cpc := filepath.Join(dir, "1.cpc")
	datPath := filepath.Join(dir, "1.dat")
	idxPath := filepath.Join(dir, "1.idx")
	mustExist(t, cpd)
	mustExist(t, cpx)

	// Simulate the mid-commit crash: the original .dat/.idx are gone and only
	// the compacted temp files plus the commit marker survive.
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove dat: %v", err)
	}
	if err := os.Remove(idxPath); err != nil {
		t.Fatalf("remove idx: %v", err)
	}
	os.RemoveAll(filepath.Join(dir, "1.ldb"))
	if err := os.WriteFile(cpc, nil, 0644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	// The directory pre-pass should roll the commit forward.
	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()
	location.loadExistingVolumes(NeedleMapLevelDb, 0)

	mustNotExist(t, cpd)
	mustNotExist(t, cpx)
	mustNotExist(t, cpc)
	mustExist(t, datPath)
	mustExist(t, idxPath)

	reloaded, found := location.FindVolume(needle.VolumeId(1))
	if !found {
		t.Fatalf("volume not loaded after roll-forward")
	}
	if reloaded.IsReadOnly() {
		t.Fatalf("rolled-forward volume is read-only")
	}
	if got := reloaded.FileCount(); got != expectedLive {
		t.Fatalf("rolled-forward volume has %d needles, want compacted count %d", got, expectedLive)
	}
}

// A crash AFTER .cpd->.dat but BEFORE .cpx->.idx leaves a compacted .dat, a
// stale .idx, and only .cpx + .cpc. The roll-forward must finish the pending
// .cpx->.idx rename, not abandon it -- abandoning pairs the fresh .dat with the
// stale .idx, which is index corruption.
func TestReconcileRollForwardPartialRename(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapLevelDb, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	const liveCount = 6
	for i := uint64(1); i <= liveCount; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	for _, id := range []uint64{2, 5} {
		if _, err := v.deleteNeedle2(newEmptyNeedle(id)); err != nil {
			t.Fatalf("delete %d: %v", id, err)
		}
	}
	expectedLive := uint64(liveCount - 2)
	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("compact by index: %v", err)
	}
	v.Close()

	cpd := filepath.Join(dir, "1.cpd")
	cpx := filepath.Join(dir, "1.cpx")
	cpc := filepath.Join(dir, "1.cpc")
	datPath := filepath.Join(dir, "1.dat")
	idxPath := filepath.Join(dir, "1.idx")

	// Crash after the .dat rename committed but before the .idx one: .cpd has
	// become .dat, the stale pre-compact .idx survives, .cpx and the marker remain.
	os.RemoveAll(filepath.Join(dir, "1.ldb"))
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove stale dat: %v", err)
	}
	if err := os.Rename(cpd, datPath); err != nil {
		t.Fatalf("rename cpd->dat: %v", err)
	}
	if err := os.WriteFile(cpc, nil, 0644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	mustNotExist(t, cpd)
	mustExist(t, cpx)
	mustExist(t, idxPath)

	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()
	location.loadExistingVolumes(NeedleMapLevelDb, 0)

	mustNotExist(t, cpx)
	mustNotExist(t, cpc)
	mustExist(t, datPath)
	mustExist(t, idxPath)

	reloaded, found := location.FindVolume(needle.VolumeId(1))
	if !found {
		t.Fatalf("volume not loaded after partial roll-forward")
	}
	if got := reloaded.FileCount(); got != expectedLive {
		t.Fatalf("volume has %d needles, want compacted count %d (pending .cpx->.idx was not rolled forward)", got, expectedLive)
	}
}

// With no .cpc marker, leftover .cpd/.cpx are an abandoned generation. The
// pre-pass must roll BACK by deleting them, leaving the live .dat/.idx intact.
func TestReconcileRollBackNoMarker(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := uint64(1); i <= 4; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("compact by index: %v", err)
	}
	v.Close()

	cpd := filepath.Join(dir, "1.cpd")
	cpx := filepath.Join(dir, "1.cpx")
	datPath := filepath.Join(dir, "1.dat")
	idxPath := filepath.Join(dir, "1.idx")
	mustExist(t, cpd)
	mustExist(t, cpx)
	// No .cpc marker exists.

	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()
	location.loadExistingVolumes(NeedleMapInMemory, 0)

	mustNotExist(t, cpd)
	mustNotExist(t, cpx)
	mustExist(t, datPath)
	mustExist(t, idxPath)

	reloaded, found := location.FindVolume(needle.VolumeId(1))
	if !found {
		t.Fatalf("volume not loaded after roll-back")
	}
	if got := reloaded.FileCount(); got != 4 {
		t.Fatalf("rolled-back volume has %d needles, want original 4", got)
	}
}

// A runtime reload (SIGHUP -> LoadNewVolumes -> reconcileCompactStates) must
// NOT roll back an in-flight vacuum of an already-loaded volume: its .cpd/.cpx
// are live, not crash leftovers. The pre-pass only reconciles vids not loaded.
func TestReconcileSkipsLoadedVolumeMidVacuum(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := uint64(1); i <= 4; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	v.Close()

	// Load the volume as a running server would (populates l.volumes).
	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()
	location.loadExistingVolumes(NeedleMapInMemory, 0)
	loaded, found := location.FindVolume(needle.VolumeId(1))
	if !found {
		t.Fatalf("volume not loaded")
	}

	// In-flight vacuum: .cpd/.cpx written, commit (.cpc) not yet reached.
	if err := loaded.CompactByIndex(nil); err != nil {
		t.Fatalf("compact by index: %v", err)
	}
	cpd := filepath.Join(dir, "1.cpd")
	cpx := filepath.Join(dir, "1.cpx")
	mustExist(t, cpd)
	mustExist(t, cpx)

	// The reload pre-pass must leave the loaded volume's in-flight temp files alone.
	location.reconcileCompactStates()
	mustExist(t, cpd)
	mustExist(t, cpx)
}

// applyCompactSwap must abort without touching the live .dat/.idx when the
// .cpd/.cpx temp files are missing (a stale or duplicate commit). Before the
// fix a late commit on Windows would RemoveAll the .dat/.idx first and then
// fail the rename, destroying the volume.
func TestApplyCompactSwapMissingTempFilesPreservesLive(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	v.Close()

	datPath := filepath.Join(dir, "1.dat")
	idxPath := filepath.Join(dir, "1.idx")
	mustExist(t, datPath)
	mustExist(t, idxPath)
	datBefore, _ := os.Stat(datPath)

	// No .cpd/.cpx present: the swap must refuse.
	if err := v.applyCompactSwap(); err == nil {
		t.Fatalf("applyCompactSwap should error when .cpd/.cpx are missing")
	}

	mustExist(t, datPath)
	mustExist(t, idxPath)
	if datAfter, _ := os.Stat(datPath); datAfter.Size() != datBefore.Size() {
		t.Fatalf("live .dat changed size: before=%d after=%d", datBefore.Size(), datAfter.Size())
	}
}

// Deleting a volume and recreating the same id must not leave a stale .cpc
// behind for the next load to roll forward against. removeVolumeFiles must
// sweep the marker along with the rest.
func TestDestroyRemovesCommitMarker(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Strand a commit marker and its temp files as a crashed commit would.
	cpc := filepath.Join(dir, "1.cpc")
	cpd := filepath.Join(dir, "1.cpd")
	cpx := filepath.Join(dir, "1.cpx")
	for _, p := range []string{cpc, cpd, cpx} {
		if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	if err := v.Destroy(false, false); err != nil {
		t.Fatalf("destroy: %v", err)
	}

	mustNotExist(t, cpc)
	mustNotExist(t, cpd)
	mustNotExist(t, cpx)

	// Recreate the same id and load: there must be no marker to roll forward.
	v2, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("recreate volume: %v", err)
	}
	defer v2.Close()

	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()
	location.reconcileCompactStates()
	mustNotExist(t, cpc)
}
