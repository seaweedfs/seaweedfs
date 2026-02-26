package pluginworkers

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// WriteTestVolumeFiles creates a minimal .dat/.idx pair for the given volume.
func WriteTestVolumeFiles(t *testing.T, baseDir string, volumeID uint32, datSize int) (string, string) {
	t.Helper()

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("create volume dir: %v", err)
	}

	datPath := filepath.Join(baseDir, volumeFilename(volumeID, ".dat"))
	idxPath := filepath.Join(baseDir, volumeFilename(volumeID, ".idx"))

	data := make([]byte, datSize)
	rng := rand.New(rand.NewSource(99))
	_, _ = rng.Read(data)
	if err := os.WriteFile(datPath, data, 0644); err != nil {
		t.Fatalf("write dat file: %v", err)
	}

	entry := make([]byte, types.NeedleMapEntrySize)
	idEnd := types.NeedleIdSize
	offsetEnd := idEnd + types.OffsetSize
	sizeEnd := offsetEnd + types.SizeSize

	types.NeedleIdToBytes(entry[:idEnd], types.NeedleId(1))
	types.OffsetToBytes(entry[idEnd:offsetEnd], types.ToOffset(0))
	types.SizeToBytes(entry[offsetEnd:sizeEnd], types.Size(datSize))

	if err := os.WriteFile(idxPath, entry, 0644); err != nil {
		t.Fatalf("write idx file: %v", err)
	}

	return datPath, idxPath
}

func volumeFilename(volumeID uint32, ext string) string {
	return fmt.Sprintf("%d%s", volumeID, ext)
}
