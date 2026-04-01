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

// closeEcVolumes closes all EC volumes in the given DiskLocation to release file handles.
func closeEcVolumes(dl *DiskLocation) {
	for _, ecVol := range dl.ecVolumes {
		ecVol.Close()
	}
}

// TestIncompleteEcEncodingCleanup tests the cleanup logic for incomplete EC encoding scenarios
func TestIncompleteEcEncodingCleanup(t *testing.T) {
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
			// Use per-subtest temp directory for stronger isolation
			tempDir := t.TempDir()

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

			// Use deterministic but small size: 10MB .dat => 1MB per shard
			datFileSize := int64(10 * 1024 * 1024) // 10MB
			expectedShardSize := calculateExpectedShardSize(datFileSize)

			// Create .dat file if needed
			if tt.createDatFile {
				datFile, err := os.Create(baseFileName + ".dat")
				if err != nil {
					t.Fatalf("Failed to create .dat file: %v", err)
				}
				if err := datFile.Truncate(datFileSize); err != nil {
					t.Fatalf("Failed to truncate .dat file: %v", err)
				}
				if err := datFile.Close(); err != nil {
					t.Fatalf("Failed to close .dat file: %v", err)
				}
			}

			// Create EC shard files
			for i := 0; i < tt.numShards; i++ {
				shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
				if err != nil {
					t.Fatalf("Failed to create shard file: %v", err)
				}
				if err := shardFile.Truncate(expectedShardSize); err != nil {
					t.Fatalf("Failed to truncate shard file: %v", err)
				}
				if err := shardFile.Close(); err != nil {
					t.Fatalf("Failed to close shard file: %v", err)
				}
			}

			// Create .ecx file if needed
			if tt.createEcxFile {
				ecxFile, err := os.Create(baseFileName + ".ecx")
				if err != nil {
					t.Fatalf("Failed to create .ecx file: %v", err)
				}
				if _, err := ecxFile.WriteString("dummy ecx data"); err != nil {
					ecxFile.Close()
					t.Fatalf("Failed to write .ecx file: %v", err)
				}
				if err := ecxFile.Close(); err != nil {
					t.Fatalf("Failed to close .ecx file: %v", err)
				}
			}

			// Create .ecj file if needed
			if tt.createEcjFile {
				ecjFile, err := os.Create(baseFileName + ".ecj")
				if err != nil {
					t.Fatalf("Failed to create .ecj file: %v", err)
				}
				if _, err := ecjFile.WriteString("dummy ecj data"); err != nil {
					ecjFile.Close()
					t.Fatalf("Failed to write .ecj file: %v", err)
				}
				if err := ecjFile.Close(); err != nil {
					t.Fatalf("Failed to close .ecj file: %v", err)
				}
			}

			// Run loadAllEcShards
			loadErr := diskLocation.loadAllEcShards(nil)
			if loadErr != nil {
				t.Logf("loadAllEcShards returned error (expected in some cases): %v", loadErr)
			}

			// Close EC volumes before idempotency test to avoid leaking file handles
			closeEcVolumes(diskLocation)
			diskLocation.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)

			// Test idempotency - running again should not cause issues
			loadErr2 := diskLocation.loadAllEcShards(nil)
			if loadErr2 != nil {
				t.Logf("Second loadAllEcShards returned error: %v", loadErr2)
			}
			t.Cleanup(func() {
				closeEcVolumes(diskLocation)
			})

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

			// Verify load expectations
			if tt.expectLoadSuccess {
				if diskLocation.EcShardCount() == 0 {
					t.Errorf("Expected EC shards to be loaded for volume %d", tt.volumeId)
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
			// For test purposes, use a small .dat file size that still exercises the logic
			// 10MB .dat file = 1MB per shard (one small batch, fast and deterministic)
			datFileSize := int64(10 * 1024 * 1024) // 10MB
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
				if err := shardFile.Truncate(expectedShardSize); err != nil {
					shardFile.Close()
					t.Fatalf("Failed to truncate shard file: %v", err)
				}
				if err := shardFile.Close(); err != nil {
					t.Fatalf("Failed to close shard file: %v", err)
				}
			}

			// For zero-byte test case, create empty files for all data shards
			if tt.volumeId == 204 {
				for i := 0; i < erasure_coding.DataShardsCount; i++ {
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
				for i := 0; i < erasure_coding.DataShardsCount; i++ {
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
	tests := []struct {
		name           string
		separateIdxDir bool
	}{
		{"Same directory for data and index", false},
		{"Separate idx directory", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()

			var dataDir, idxDir string
			if tt.separateIdxDir {
				dataDir = filepath.Join(tempDir, "data")
				idxDir = filepath.Join(tempDir, "idx")
				os.MkdirAll(dataDir, 0755)
				os.MkdirAll(idxDir, 0755)
			} else {
				dataDir = tempDir
				idxDir = tempDir
			}

			minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}
			diskLocation := &DiskLocation{
				Directory:     dataDir,
				DirectoryUuid: "test-uuid",
				IdxDirectory:  idxDir,
				DiskType:      types.HddType,
				MinFreeSpace:  minFreeSpace,
			}

			volumeId := needle.VolumeId(300)
			collection := ""
			dataBaseFileName := erasure_coding.EcShardFileName(collection, dataDir, int(volumeId))
			idxBaseFileName := erasure_coding.EcShardFileName(collection, idxDir, int(volumeId))

			// Create all EC shard files in data directory
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				shardFile, err := os.Create(dataBaseFileName + erasure_coding.ToExt(i))
				if err != nil {
					t.Fatalf("Failed to create shard file: %v", err)
				}
				if _, err := shardFile.WriteString("dummy shard data"); err != nil {
					shardFile.Close()
					t.Fatalf("Failed to write shard file: %v", err)
				}
				if err := shardFile.Close(); err != nil {
					t.Fatalf("Failed to close shard file: %v", err)
				}
			}

			// Create .ecx file in idx directory
			ecxFile, err := os.Create(idxBaseFileName + ".ecx")
			if err != nil {
				t.Fatalf("Failed to create .ecx file: %v", err)
			}
			if _, err := ecxFile.WriteString("dummy ecx data"); err != nil {
				ecxFile.Close()
				t.Fatalf("Failed to write .ecx file: %v", err)
			}
			if err := ecxFile.Close(); err != nil {
				t.Fatalf("Failed to close .ecx file: %v", err)
			}

			// Create .ecj file in idx directory
			ecjFile, err := os.Create(idxBaseFileName + ".ecj")
			if err != nil {
				t.Fatalf("Failed to create .ecj file: %v", err)
			}
			if _, err := ecjFile.WriteString("dummy ecj data"); err != nil {
				ecjFile.Close()
				t.Fatalf("Failed to write .ecj file: %v", err)
			}
			if err := ecjFile.Close(); err != nil {
				t.Fatalf("Failed to close .ecj file: %v", err)
			}

			// Create .dat file in data directory (should NOT be removed)
			datFile, err := os.Create(dataBaseFileName + ".dat")
			if err != nil {
				t.Fatalf("Failed to create .dat file: %v", err)
			}
			if _, err := datFile.WriteString("dummy dat data"); err != nil {
				datFile.Close()
				t.Fatalf("Failed to write .dat file: %v", err)
			}
			if err := datFile.Close(); err != nil {
				t.Fatalf("Failed to close .dat file: %v", err)
			}

			// Call removeEcVolumeFiles
			diskLocation.removeEcVolumeFiles(collection, volumeId)

			// Verify all EC shard files are removed from data directory
			for i := 0; i < erasure_coding.TotalShardsCount; i++ {
				shardFile := dataBaseFileName + erasure_coding.ToExt(i)
				if util.FileExists(shardFile) {
					t.Errorf("Shard file %d should be removed but still exists", i)
				}
			}

			// Verify .ecx file is removed from idx directory
			if util.FileExists(idxBaseFileName + ".ecx") {
				t.Errorf(".ecx file should be removed but still exists")
			}

			// Verify .ecj file is removed from idx directory
			if util.FileExists(idxBaseFileName + ".ecj") {
				t.Errorf(".ecj file should be removed but still exists")
			}

			// Verify .dat file is NOT removed from data directory
			if !util.FileExists(dataBaseFileName + ".dat") {
				t.Errorf(".dat file should NOT be removed but was deleted")
			}
		})
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

	// Create shards in data directory (shards only go to Directory, not IdxDirectory)
	dataBaseFileName := erasure_coding.EcShardFileName(collection, dataDir, int(volumeId))
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile, err := os.Create(dataBaseFileName + erasure_coding.ToExt(i))
		if err != nil {
			t.Fatalf("Failed to create shard file: %v", err)
		}
		if _, err := shardFile.WriteString("dummy shard data"); err != nil {
			t.Fatalf("Failed to write shard file: %v", err)
		}
		if err := shardFile.Close(); err != nil {
			t.Fatalf("Failed to close shard file: %v", err)
		}
	}

	// Create .dat in data directory
	datFile, err := os.Create(dataBaseFileName + ".dat")
	if err != nil {
		t.Fatalf("Failed to create .dat file: %v", err)
	}
	if _, err := datFile.WriteString("dummy data"); err != nil {
		t.Fatalf("Failed to write .dat file: %v", err)
	}
	if err := datFile.Close(); err != nil {
		t.Fatalf("Failed to close .dat file: %v", err)
	}

	// Do not create .ecx: trigger orphaned-shards cleanup when .dat exists

	// Run loadAllEcShards
	loadErr := diskLocation.loadAllEcShards(nil)
	if loadErr != nil {
		t.Logf("loadAllEcShards error: %v", loadErr)
	}
	t.Cleanup(func() {
		closeEcVolumes(diskLocation)
	})

	// Verify cleanup occurred in data directory (shards)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFile := dataBaseFileName + erasure_coding.ToExt(i)
		if util.FileExists(shardFile) {
			t.Errorf("Shard file %d should be cleaned up but still exists", i)
		}
	}

	// Verify .dat in data directory still exists (only EC files are cleaned up)
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

	// Create EC shards (only 5 shards - less than DataShardsCount, but OK for distributed EC)
	numDistributedShards := 5
	for i := 0; i < numDistributedShards; i++ {
		shardFile, err := os.Create(baseFileName + erasure_coding.ToExt(i))
		if err != nil {
			t.Fatalf("Failed to create shard file: %v", err)
		}
		if _, err := shardFile.WriteString("dummy shard data"); err != nil {
			shardFile.Close()
			t.Fatalf("Failed to write shard file: %v", err)
		}
		if err := shardFile.Close(); err != nil {
			t.Fatalf("Failed to close shard file: %v", err)
		}
	}

	// Create .ecx file to trigger EC loading
	ecxFile, err := os.Create(baseFileName + ".ecx")
	if err != nil {
		t.Fatalf("Failed to create .ecx file: %v", err)
	}
	if _, err := ecxFile.WriteString("dummy ecx data"); err != nil {
		ecxFile.Close()
		t.Fatalf("Failed to write .ecx file: %v", err)
	}
	if err := ecxFile.Close(); err != nil {
		t.Fatalf("Failed to close .ecx file: %v", err)
	}

	// NO .dat file - this is a distributed EC volume

	// Run loadAllEcShards - this should fail but NOT delete shard files
	loadErr := diskLocation.loadAllEcShards(nil)
	if loadErr != nil {
		t.Logf("loadAllEcShards returned error (expected): %v", loadErr)
	}
	t.Cleanup(func() {
		closeEcVolumes(diskLocation)
	})

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
