package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The offset key is a persistence contract: a changed form orphans saved
// checkpoints, so the historical forms must survive verbatim and the
// target-scoped form must stay stable.
func TestGetSignaturePrefixByPath_KeyForms(t *testing.T) {
	cases := []struct {
		sourcePath, targetPath, want string
	}{
		// historical forms, kept so existing deployments resume unchanged
		{"/", "/", "sync."},
		{"/data", "/", "sync./data"},
		// the target path participates once it deviates from "/"
		{"/", "/backup-a", "sync.\x00/backup-a"},
		{"/data", "/backup-a", "sync./data\x00/backup-a"},
	}
	for _, tc := range cases {
		if got := getSignaturePrefixByPath(tc.sourcePath, tc.targetPath); got != tc.want {
			t.Errorf("getSignaturePrefixByPath(%q, %q) = %q, want %q", tc.sourcePath, tc.targetPath, got, tc.want)
		}
	}
}

// The NUL separator keeps the key injective: a source directory whose name
// spells out a separator must not alias a different (source, target) pair.
func TestGetSignaturePrefixByPath_NoAliasing(t *testing.T) {
	if a, b := getSignaturePrefixByPath("/archive=>/backup", "/"), getSignaturePrefixByPath("/archive", "/backup"); a == b {
		t.Errorf("source path spelling a separator aliases key %q", a)
	}
}

// Two syncs from the same source filer and path to different directories on
// the same target filer must keep separate checkpoints, or the running one
// pushes the shared offset past events the other never processed.
func TestGetSignaturePrefixByPath_DistinctTargets(t *testing.T) {
	a := getSignaturePrefixByPath("/", "/backup-a")
	b := getSignaturePrefixByPath("/", "/backup-b")
	if a == b {
		t.Errorf("different target paths share offset key %q", a)
	}
	if root := getSignaturePrefixByPath("/", "/"); a == root {
		t.Errorf("non-root target path shares offset key %q with the root form", a)
	}
}

// Over the real KV wire path: a sync that predates target-scoped keys resumes
// from the historical key, and once each sync checkpoints under its own key
// they no longer disturb each other.
func TestSyncOffset_HistoricalFallbackThenIndependence(t *testing.T) {
	filerAddr, dial := startKvFiler(t)
	const sourceSig = int32(12345)
	historical := getSignaturePrefixByPath("/data", "/")
	prefixA := getSignaturePrefixByPath("/data", "/backup-a")
	prefixB := getSignaturePrefixByPath("/data", "/backup-b")

	// pre-upgrade state: both syncs shared the historical key
	if err := setOffset(dial, filerAddr, historical, sourceSig, 111); err != nil {
		t.Fatalf("seed historical offset: %v", err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixA, sourceSig, historical, sourceSig); err != nil || got != 111 {
		t.Fatalf("A fallback read = (%d, %v), want (111, nil)", got, err)
	}

	// A checkpoints under its own key; B still resumes from the historical one
	if err := setOffset(dial, filerAddr, prefixA, sourceSig, 222); err != nil {
		t.Fatalf("write A offset: %v", err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixA, sourceSig, historical, sourceSig); err != nil || got != 222 {
		t.Fatalf("A read = (%d, %v), want (222, nil)", got, err)
	}
	if got, err := getOffsetWithFallback(dial, filerAddr, prefixB, sourceSig, historical, sourceSig); err != nil || got != 111 {
		t.Fatalf("B read = (%d, %v), want (111, nil)", got, err)
	}
}

// TestSyncOffset_StaleWriteDropped: a checkpoint write issued before a write
// that already landed under the same key is stale and must not apply, while
// a deliberately regressed value issued later — a resume floor or an
// operator seed — still does.
func TestSyncOffset_StaleWriteDropped(t *testing.T) {
	filerAddr, dial := startKvFiler(t)
	prefix := getSignaturePrefixByPath("/data", "/backup-stale")
	sourceSig := int32(77)

	if err := setOffset(dial, filerAddr, prefix, sourceSig, 300); err != nil {
		t.Fatalf("seed offset: %v", err)
	}

	// Pin the checkpoint past the next write's issuance seq, simulating a
	// write that was issued before a newer one already landed.
	syncKey := []byte(prefix + "____")
	util.Uint32toBytes(syncKey[len(prefix):len(prefix)+4], uint32(sourceSig))
	mapKey := string(filerAddr) + "\x00" + string(syncKey)
	offsetCheckpointWrites.Lock()
	checkpoint := offsetCheckpointWrites.byKey[mapKey]
	offsetCheckpointWrites.Unlock()
	if checkpoint == nil {
		t.Fatal("no checkpoint state for the seeded key")
	}
	checkpoint.Lock()
	checkpoint.lastSeq = offsetWriteSeq.Load() + 1000
	checkpoint.Unlock()

	if err := setOffset(dial, filerAddr, prefix, sourceSig, 100); err != nil {
		t.Fatalf("stale write: %v", err)
	}
	if got, err := getOffset(dial, filerAddr, prefix, sourceSig); err != nil || got != 300 {
		t.Fatalf("after stale write = (%d, %v), want (300, nil)", got, err)
	}

	// a later-issued regression is deliberate and must apply
	checkpoint.Lock()
	checkpoint.lastSeq = 0
	checkpoint.Unlock()
	if err := setOffset(dial, filerAddr, prefix, sourceSig, 199); err != nil {
		t.Fatalf("floored write: %v", err)
	}
	if got, err := getOffset(dial, filerAddr, prefix, sourceSig); err != nil || got != 199 {
		t.Fatalf("after floored write = (%d, %v), want (199, nil)", got, err)
	}
}
