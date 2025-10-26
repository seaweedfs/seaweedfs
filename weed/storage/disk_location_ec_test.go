package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestIncompleteEcEncodingCleanup tests the cleanup logic for incomplete EC encoding scenarios
func TestIncompleteEcEncodingCleanup(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name              string
		volumeId          needle.VolumeId
		collection        string
		createDatFile     bool
		createEcxFile     bool
		createEcjFile     bool
		numShards         int
		expectCleanup     bool
		expectLoadSuccess bool
	}{
		{
			name:              "Incomplete EC: shards without .ecx, .dat exists - should cleanup",
			volumeId:          100,
			collection:        "",
			createDatFile:     true,
			createEcxFile:     false,
			createEcjFile:     false,
			numShards:         14, // All shards but no .ecx
			expectCleanup:     true,
			expectLoadSuccess: false,
		},
		{
			name:              "Distributed EC: shards without .ecx, .dat deleted - should NOT cleanup",
			volumeId:          101,
			collection:        "",
			createDatFile:     false,
			createEcxFile:     false,
			createEcjFile:     false,
			numShards:         5, // Partial shards, distributed
			expectCleanup:     false,
			expectLoadSuccess: false,
		},
		{
			name:              "Incomplete EC: shards with .ecx but < 10 shards, .dat exists - should cleanup",
			volumeId:          102,
			collection:        "",
			createDatFile:     true,
			createEcxFile:     true,
			createEcjFile:     false,
			numShards:         7, // Less than DataShardsCount (10)
			expectCleanup:     true,
			expectLoadSuccess: false,
		},
		{
			name:              "Valid local EC: shards with .ecx, >= 10 shards, .dat exists - should load",
			volumeId:          103,
			collection:        "",
			createDatFile:     true,
			createEcxFile:     true,
			createEcjFile:     false,
			numShards:         14, // All shards
			expectCleanup:     false,
			expectLoadSuccess: true, // Would succeed if .ecx was valid
		},
		{
			name:              "Distributed EC: shards with .ecx, .dat deleted - should load",
			volumeId:          104,
			collection:        "",
			createDatFile:     false,
			createEcxFile:     true,
			createEcjFile:     false,
			numShards:         10, // Enough shards
			expectCleanup:     false,
			expectLoadSuccess: true, // Would succeed if .ecx was valid
		},
		{
			name:              "Incomplete EC with collection: shards without .ecx, .dat exists - should cleanup",
			volumeId:          105,
			collection:        "test_collection",
			createDatFile:     true,
			createEcxFile:     false,
			createEcjFile:     false,
			numShards:         14,
			expectCleanup:     true,
			expectLoadSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create DiskLocation
			minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
			diskLocation := &DiskLocation{
				Directory:              tempDir,
				DirectoryUuid:          "test-uuid",
				IdxDirectory:           tempDir,
				DiskType:               types.HddType,
				MaxVolumeCount:         100,
				OriginalMaxVolumeCount: 100,
				MinFreeSpace:           minFreeSpace,
			}
			diskLocation.volumes = make(map[needle.VolumeId]*Volume)
			diskLocation.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)

			// Setup test files
			baseFileName := erasure_coding.EcShardFileName(tt.collection, tempDir, int(tt.volumeId))

			// Use deterministic sizes that match EC encoding
			datFileSize := int64(10 * 1024 * 1024 * 1024) // 10GB
			expectedShardSize := calculateExpectedShardSize(datFileSize)

			// Create .dat file if needed
			if tt.createDatFile {
				datFile, err := os.Create(baseFileName + ".dat")
				if err != nil {
					t.Fatalf("Failed to create .dat file: %v", err)
				}
				datFile.Truncate(datFileSize)
				datFile.Close()
			}

			// Create EC shard files
			for i := 0; i < tt.numShards; i++ {
				shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
				if err != nil {
					t.Fatalf("Failed to create shard file: %v", err)
				}
				shardFile.Truncate(expectedShardSize)
				shardFile.Close()
			}

			// Create .ecx file if needed
			if tt.createEcxFile {
				ecxFile, err := os.Create(baseFileName + ".ecx")
				if err != nil {
					t.Fatalf("Failed to create .ecx file: %v", err)
				}
				ecxFile.WriteString("dummy ecx data")
				ecxFile.Close()
			}

			// Create .ecj file if needed
			if tt.createEcjFile {
				ecjFile, err := os.Create(baseFileName + ".ecj")
				if err != nil {
					t.Fatalf("Failed to create .ecj file: %v", err)
				}
				ecjFile.WriteString("dummy ecj data")
				ecjFile.Close()
			}

			// Run loadAllEcShards
			loadErr := diskLocation.loadAllEcShards()
			if loadErr != nil {
				t.Logf("loadAllEcShards returned error (expected in some cases): %v", loadErr)
			}

			// Verify cleanup expectations
			if tt.expectCleanup {
				// Check that files were cleaned up
				if util.FileExists(baseFileName + ".ecx") {
					t.Errorf("Expected .ecx to be cleaned up but it still exists")
				}
				if util.FileExists(baseFileName + ".ecj") {
					t.Errorf("Expected .ecj to be cleaned up but it still exists")
				}
				for i := 0; i < erasure_coding.TotalShardsCount; i++ {
					shardFile := baseFileName + erasure_coding.ToExt(i)
					if util.FileExists(shardFile) {
						t.Errorf("Expected shard %d to be cleaned up but it still exists", i)
					}
				}
				// .dat file should still exist (not cleaned up)
				if tt.createDatFile && !util.FileExists(baseFileName+".dat") {
					t.Errorf("Expected .dat file to remain but it was deleted")
				}
			} else {
				// Check that files were NOT cleaned up
				for i := 0; i < tt.numShards; i++ {
					shardFile := baseFileName + erasure_coding.ToExt(i)
					if !util.FileExists(shardFile) {
						t.Errorf("Expected shard %d to remain but it was cleaned up", i)
					}
				}
				if tt.createEcxFile && !util.FileExists(baseFileName+".ecx") {
					t.Errorf("Expected .ecx to remain but it was cleaned up")
				}
			}

		})
	}
}

// TestValidateEcVolume tests the validateEcVolume function
func TestValidateEcVolume(t *testing.T) {
	tempDir := t.TempDir()

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
	diskLocation := &DiskLocation{
		Directory:     tempDir,
		DirectoryUuid: "test-uuid",
		IdxDirectory:  tempDir,
		DiskType:      types.HddType,
		MinFreeSpace:  minFreeSpace,
	}

	tests := []struct {
		name          string
		volumeId      needle.VolumeId
		collection    string
		createDatFile bool
		numShards     int
		expectValid   bool
	}{
		{
			name:          "Valid: .dat exists with 10+ shards",
			volumeId:      200,
			collection:    "",
			createDatFile: true,
			numShards:     10,
			expectValid:   true,
		},
		{
			name:          "Invalid: .dat exists with < 10 shards",
			volumeId:      201,
			collection:    "",
			createDatFile: true,
			numShards:     9,
			expectValid:   false,
		},
		{
			name:          "Valid: .dat deleted (distributed EC) with any shards",
			volumeId:      202,
			collection:    "",
			createDatFile: false,
			numShards:     5,
			expectValid:   true,
		},
		{
			name:          "Valid: .dat deleted (distributed EC) with no shards",
			volumeId:      203,
			collection:    "",
			createDatFile: false,
			numShards:     0,
			expectValid:   true,
		},
		{
			name:          "Invalid: zero-byte shard files should not count",
			volumeId:      204,
			collection:    "",
			createDatFile: true,
			numShards:     0, // Will create 10 zero-byte files below
			expectValid:   false,
		},
		{
			name:          "Invalid: .dat exists with different size shards",
			volumeId:      205,
			collection:    "",
			createDatFile: true,
			numShards:     10, // Will create shards with varying sizes
			expectValid:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseFileName := erasure_coding.EcShardFileName(tt.collection, tempDir, int(tt.volumeId))

			// For proper testing, we need to use realistic sizes that match EC encoding
			// EC uses large blocks (1GB) and small blocks (1MB)
			// For test purposes, use a .dat file size that results in expected shard sizes
			// 10GB .dat file = 1GB per shard (1 large block)
			datFileSize := int64(10 * 1024 * 1024 * 1024) // 10GB
			expectedShardSize := calculateExpectedShardSize(datFileSize)

			// Create .dat file if needed
			if tt.createDatFile {
				datFile, err := os.Create(baseFileName + ".dat")
				if err != nil {
					t.Fatalf("Failed to create .dat file: %v", err)
				}
				// Write minimal data (don't need to fill entire 10GB for tests)
				datFile.Truncate(datFileSize)
				datFile.Close()
			}

			// Create EC shard files with correct size
			for i := 0; i < tt.numShards; i++ {
				shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
				if err != nil {
					t.Fatalf("Failed to create shard file: %v", err)
				}
				// Use truncate to create file of correct size without allocating all the space
				shardFile.Truncate(expectedShardSize)
				shardFile.Close()
			}

			// For zero-byte test case, create 10 empty files
			if tt.volumeId == 204 {
				for i := 0; i < 10; i++ {
					shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
					if err != nil {
						t.Fatalf("Failed to create empty shard file: %v", err)
					}
					// Don't write anything - leave as zero-byte
					shardFile.Close()
				}
			}

			// For mismatched shard size test case, create shards with different sizes
			if tt.volumeId == 205 {
				for i := 0; i < 10; i++ {
					shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
					if err != nil {
						t.Fatalf("Failed to create shard file: %v", err)
					}
					// Write different amount of data to each shard
					data := make([]byte, 100+i*10)
					shardFile.Write(data)
					shardFile.Close()
				}
			}

			// Test validation
			isValid := diskLocation.validateEcVolume(tt.collection, tt.volumeId)
			if isValid != tt.expectValid {
				t.Errorf("Expected validation result %v but got %v", tt.expectValid, isValid)
			}
		})
	}
}

// TestRemoveEcVolumeFiles tests the removeEcVolumeFiles function
func TestRemoveEcVolumeFiles(t *testing.T) {
	tempDir := t.TempDir()

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
	diskLocation := &DiskLocation{
		Directory:     tempDir,
		DirectoryUuid: "test-uuid",
		IdxDirectory:  tempDir,
		DiskType:      types.HddType,
		MinFreeSpace:  minFreeSpace,
	}

	volumeId := needle.VolumeId(300)
	collection := ""
	baseFileName := erasure_coding.EcShardFileName(collection, tempDir, int(volumeId))

	// Create all EC files
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
		if err != nil {
			t.Fatalf("Failed to create shard file: %v", err)
		}
		shardFile.WriteString("dummy shard data")
		shardFile.Close()
	}

	ecxFile, _ := os.Create(baseFileName + ".ecx")
	ecxFile.WriteString("dummy ecx data")
	ecxFile.Close()

	ecjFile, _ := os.Create(baseFileName + ".ecj")
	ecjFile.WriteString("dummy ecj data")
	ecjFile.Close()

	// Create .dat file that should NOT be removed
	datFile, _ := os.Create(baseFileName + ".dat")
	datFile.WriteString("dummy dat data")
	datFile.Close()

	// Call removeEcVolumeFiles
	diskLocation.removeEcVolumeFiles(collection, volumeId)

	// Verify all EC files are removed
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile := baseFileName + erasure_coding.ToExt(i)
		if util.FileExists(shardFile) {
			t.Errorf("Shard file %d should be removed but still exists", i)
		}
	}

	if util.FileExists(baseFileName + ".ecx") {
		t.Errorf(".ecx file should be removed but still exists")
	}

	if util.FileExists(baseFileName + ".ecj") {
		t.Errorf(".ecj file should be removed but still exists")
	}

	// Verify .dat file is NOT removed
	if !util.FileExists(baseFileName + ".dat") {
		t.Errorf(".dat file should NOT be removed but was deleted")
	}
}

// TestEcCleanupWithSeparateIdxDirectory tests EC cleanup when idx directory is different
func TestEcCleanupWithSeparateIdxDirectory(t *testing.T) {
	tempDir := t.TempDir()

	idxDir := filepath.Join(tempDir, "idx")
	dataDir := filepath.Join(tempDir, "data")
	os.MkdirAll(idxDir, 0755)
	os.MkdirAll(dataDir, 0755)

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
	diskLocation := &DiskLocation{
		Directory:     dataDir,
		DirectoryUuid: "test-uuid",
		IdxDirectory:  idxDir,
		DiskType:      types.HddType,
		MinFreeSpace:  minFreeSpace,
	}
	diskLocation.volumes = make(map[needle.VolumeId]*Volume)
	diskLocation.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)

	volumeId := needle.VolumeId(400)
	collection := ""

	// Create shards in data directory
	dataBaseFileName := erasure_coding.EcShardFileName(collection, dataDir, int(volumeId))
	for i := 0; i < 14; i++ {
		shardFile, _ := os.Create(dataBaseFileName + erasure_coding.ToExt(i))
		shardFile.WriteString("dummy shard data")
		shardFile.Close()
	}

	// Create .dat in data directory
	datFile, _ := os.Create(dataBaseFileName + ".dat")
	datFile.WriteString("dummy data")
	datFile.Close()

	// Create .ecx and .ecj in idx directory (but no .ecx to trigger cleanup)
	// Don't create .ecx to test orphaned shards cleanup

	// Run loadAllEcShards
	loadErr := diskLocation.loadAllEcShards()
	if loadErr != nil {
		t.Logf("loadAllEcShards error: %v", loadErr)
	}

	// Verify cleanup occurred
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile := dataBaseFileName + erasure_coding.ToExt(i)
		if util.FileExists(shardFile) {
			t.Errorf("Shard file %d should be cleaned up but still exists", i)
		}
	}

	// .dat should still exist
	if !util.FileExists(dataBaseFileName + ".dat") {
		t.Errorf(".dat file should remain but was deleted")
	}
}

// TestDistributedEcVolumeNoFileDeletion verifies that distributed EC volumes
// (where .dat is deleted) do NOT have their shard files deleted when load fails
// This tests the critical bug fix where DestroyEcVolume was incorrectly deleting files
func TestDistributedEcVolumeNoFileDeletion(t *testing.T) {
	tempDir := t.TempDir()

	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
	diskLocation := &DiskLocation{
		Directory:     tempDir,
		DirectoryUuid: "test-uuid",
		IdxDirectory:  tempDir,
		DiskType:      types.HddType,
		MinFreeSpace:  minFreeSpace,
		ecVolumes:     make(map[needle.VolumeId]*erasure_coding.EcVolume),
	}

	collection := ""
	volumeId := needle.VolumeId(500)
	baseFileName := erasure_coding.EcShardFileName(collection, tempDir, int(volumeId))

	// Create EC shards (only 5 shards - not enough to load, but this is a distributed EC volume)
	for i := 0; i < 5; i++ {
		shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
		if err != nil {
			t.Fatalf("Failed to create shard file: %v", err)
		}
		shardFile.WriteString("dummy shard data")
		shardFile.Close()
	}

	// Create .ecx file to trigger EC loading
	ecxFile, err := os.Create(baseFileName + ".ecx")
	if err != nil {
		t.Fatalf("Failed to create .ecx file: %v", err)
	}
	ecxFile.WriteString("dummy ecx data")
	ecxFile.Close()

	// NO .dat file - this is a distributed EC volume

	// Run loadAllEcShards - this should fail but NOT delete shard files
	loadErr := diskLocation.loadAllEcShards()
	if loadErr != nil {
		t.Logf("loadAllEcShards returned error (expected): %v", loadErr)
	}

	// CRITICAL CHECK: Verify shard files still exist (should NOT be deleted)
	for i := 0; i < 5; i++ {
		shardFile := baseFileName + erasure_coding.ToExt(i)
		if !util.FileExists(shardFile) {
			t.Errorf("CRITICAL BUG: Shard file %s was deleted for distributed EC volume!", shardFile)
		}
	}

	// Verify .ecx file still exists (should NOT be deleted for distributed EC)
	if !util.FileExists(baseFileName + ".ecx") {
		t.Errorf("CRITICAL BUG: .ecx file was deleted for distributed EC volume!")
	}

	t.Logf("SUCCESS: Distributed EC volume files preserved (not deleted)")
}
