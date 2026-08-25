package erasure_coding

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// A present-but-malformed .vif must FAIL the mount: every new encode
// records a positive uniform block size there, and silently defaulting
// to the legacy layout would serve those shards with the wrong offset
// math. Absence stays legal — legacy volumes predate the sidecar.
func TestNewEcVolumeFailsOnMalformedVif(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	if err := os.WriteFile(base+".vif", []byte("not json"), 0644); err != nil {
		t.Fatalf("seed .vif: %v", err)
	}
	_, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err == nil {
		t.Fatal("mount over a malformed .vif must fail")
	}
	if !strings.Contains(err.Error(), ".vif") {
		t.Errorf("error should name the vif: %v", err)
	}
}

func TestNewEcVolumeAbsentVifMountsWithDefaults(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "1")
	if err := os.WriteFile(base+".ecx", []byte{}, 0644); err != nil {
		t.Fatalf("seed .ecx: %v", err)
	}
	ev, err := NewEcVolume(types.HardDriveType, dir, dir, "", needle.VolumeId(1))
	if err != nil {
		t.Fatalf("legacy mount without .vif must succeed: %v", err)
	}
	defer ev.Close()
	if ev.ECContext.BlockSize != 0 {
		t.Errorf("legacy mount must use the legacy layout: BlockSize=%d", ev.ECContext.BlockSize)
	}
}
