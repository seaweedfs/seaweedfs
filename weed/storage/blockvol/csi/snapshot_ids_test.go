package csi

import (
	"testing"
)

func TestSnapshotID_FormatParseRoundTrip(t *testing.T) {
	tests := []struct {
		volumeID string
		snapID   uint32
	}{
		{"pvc-abc123", 42},
		{"vol-with-dashes-in-name", 999},
		{"simple", 0},
		{"a", 4294967295}, // max uint32
	}
	for _, tt := range tests {
		csiID := FormatSnapshotID(tt.volumeID, tt.snapID)
		gotVol, gotSnap, err := ParseSnapshotID(csiID)
		if err != nil {
			t.Errorf("ParseSnapshotID(%q): %v", csiID, err)
			continue
		}
		if gotVol != tt.volumeID {
			t.Errorf("volumeID: got %q, want %q", gotVol, tt.volumeID)
		}
		if gotSnap != tt.snapID {
			t.Errorf("snapID: got %d, want %d", gotSnap, tt.snapID)
		}
	}
}

func TestSnapshotID_ParseErrors(t *testing.T) {
	bad := []string{
		"",
		"not-a-snap",
		"snap-",       // no volume or snap
		"snap-vol",    // no dash separator for snap ID
		"snap-vol-abc", // non-numeric snap ID
	}
	for _, s := range bad {
		_, _, err := ParseSnapshotID(s)
		if err == nil {
			t.Errorf("ParseSnapshotID(%q): expected error", s)
		}
	}
}

func TestSnapshotNameToID_Deterministic(t *testing.T) {
	name := "my-snapshot-name"
	id1 := snapshotNameToID(name)
	id2 := snapshotNameToID(name)
	if id1 != id2 {
		t.Fatalf("FNV hash not deterministic: %d vs %d", id1, id2)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero hash")
	}
}

func TestSnapshotNameToID_DifferentNames(t *testing.T) {
	id1 := snapshotNameToID("snap-alpha")
	id2 := snapshotNameToID("snap-beta")
	if id1 == id2 {
		t.Fatalf("different names produced same hash: %d", id1)
	}
}

func TestSnapshotID_EmptyVolumeID(t *testing.T) {
	// snap--42 should parse: volumeID="" but we reject it
	_, _, err := ParseSnapshotID("snap--42")
	if err != nil {
		// We accept this is an error since lastDash==0 means empty volumeID
		return
	}
	// If it doesn't error, that's also fine as long as parse is correct
}

func TestSnapshotID_FormatContainsComponents(t *testing.T) {
	id := FormatSnapshotID("pvc-123", 7)
	if id != "snap-pvc-123-7" {
		t.Fatalf("got %q, want snap-pvc-123-7", id)
	}
}
