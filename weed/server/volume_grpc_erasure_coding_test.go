package weed_server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func TestCheckEcVolumeStatusCountOnlyDataShards(t *testing.T) {
	tempDir := t.TempDir()
	dataDir := filepath.Join(tempDir, "data")
	idxDir := filepath.Join(tempDir, "idx")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.MkdirAll(idxDir, 0o755); err != nil {
		t.Fatalf("mkdir idx dir: %v", err)
	}

	baseName := "7"
	filesToCreate := []string{
		filepath.Join(dataDir, baseName+".ec00"),
		filepath.Join(dataDir, baseName+".ec09"),
		filepath.Join(dataDir, baseName+".ec13"),
		filepath.Join(idxDir, baseName+".ecx"),
		filepath.Join(idxDir, baseName+".ecj"),
		filepath.Join(idxDir, baseName+".idx"),
	}
	for _, fileName := range filesToCreate {
		if err := os.WriteFile(fileName, []byte("x"), 0o644); err != nil {
			t.Fatalf("create %s: %v", fileName, err)
		}
	}

	location := &storage.DiskLocation{
		Directory:    dataDir,
		IdxDirectory: idxDir,
	}

	hasEcxFile, hasIdxFile, shardCount, err := checkEcVolumeStatus(baseName, location)
	if err != nil {
		t.Fatalf("checkEcVolumeStatus: %v", err)
	}

	if !hasEcxFile {
		t.Fatalf("expected hasEcxFile=true")
	}
	if !hasIdxFile {
		t.Fatalf("expected hasIdxFile=true")
	}
	if shardCount != 3 {
		t.Fatalf("expected shardCount=3, got %d", shardCount)
	}
}
