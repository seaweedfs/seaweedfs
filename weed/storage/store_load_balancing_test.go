package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newTestStore creates a test store with the specified number of directories
func newTestStore(t *testing.T, numDirs int) *Store {
	tempDir := t.TempDir()

	var dirs []string
	var maxCounts []int32
	var minFreeSpaces []util.MinFreeSpace
	var diskTypes []types.DiskType

	for i := 0; i < numDirs; i++ {
		dir := filepath.Join(tempDir, "dir"+strconv.Itoa(i))
		os.MkdirAll(dir, 0755)
		dirs = append(dirs, dir)
		maxCounts = append(maxCounts, 100) // high limit
		minFreeSpaces = append(minFreeSpaces, util.MinFreeSpace{})
		diskTypes = append(diskTypes, types.HardDriveType)
	}

	store := NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "",
		dirs, maxCounts, minFreeSpaces, "", NeedleMapInMemory, diskTypes, 3)

	// Consume channel messages to prevent blocking
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-store.NewVolumesChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})

	return store
}

func TestLocalVolumesLen(t *testing.T) {
	testCases := []struct {
		name               string
		totalVolumes       int
		remoteVolumes      int
		expectedLocalCount int
	}{
		{
			name:               "all local volumes",
			totalVolumes:       5,
			remoteVolumes:      0,
			expectedLocalCount: 5,
		},
		{
			name:               "all remote volumes",
			totalVolumes:       5,
			remoteVolumes:      5,
			expectedLocalCount: 0,
		},
		{
			name:               "mixed local and remote",
			totalVolumes:       10,
			remoteVolumes:      3,
			expectedLocalCount: 7,
		},
		{
			name:               "no volumes",
			totalVolumes:       0,
			remoteVolumes:      0,
			expectedLocalCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			diskLocation := &DiskLocation{
				volumes: make(map[needle.VolumeId]*Volume),
			}

			// Add volumes
			for i := 0; i < tc.totalVolumes; i++ {
				vol := &Volume{
					Id:         needle.VolumeId(i + 1),
					volumeInfo: &volume_server_pb.VolumeInfo{},
				}

				// Mark some as remote
				if i < tc.remoteVolumes {
					vol.hasRemoteFile = true
					vol.volumeInfo.Files = []*volume_server_pb.RemoteFile{
						{BackendType: "s3", BackendId: "test", Key: "test-key"},
					}
				}

				diskLocation.volumes[vol.Id] = vol
			}

			result := diskLocation.LocalVolumesLen()

			if result != tc.expectedLocalCount {
				t.Errorf("Expected LocalVolumesLen() = %d; got %d (total: %d, remote: %d)",
					tc.expectedLocalCount, result, tc.totalVolumes, tc.remoteVolumes)
			}
		})
	}
}

func TestVolumeLoadBalancing(t *testing.T) {
	testCases := []struct {
		name              string
		locations         []locationSetup
		expectedLocations []int // which location index should get each volume
	}{
		{
			name: "even distribution across empty locations",
			locations: []locationSetup{
				{localVolumes: 0, remoteVolumes: 0},
				{localVolumes: 0, remoteVolumes: 0},
				{localVolumes: 0, remoteVolumes: 0},
			},
			expectedLocations: []int{0, 1, 2, 0, 1, 2}, // round-robin
		},
		{
			name: "prefers location with fewer local volumes",
			locations: []locationSetup{
				{localVolumes: 5, remoteVolumes: 0},
				{localVolumes: 2, remoteVolumes: 0},
				{localVolumes: 8, remoteVolumes: 0},
			},
			expectedLocations: []int{1, 1, 1}, // all go to location 1 (has fewest)
		},
		{
			name: "ignores remote volumes in count",
			locations: []locationSetup{
				{localVolumes: 2, remoteVolumes: 10}, // 2 local, 10 remote
				{localVolumes: 5, remoteVolumes: 0},  // 5 local
				{localVolumes: 3, remoteVolumes: 0},  // 3 local
			},
			// expectedLocations: []int{0, 0, 2}
			// Explanation:
			// 1. Initial local counts: [2, 5, 3]. First volume goes to location 0 (2 local, ignoring 10 remote).
			// 2. New local counts: [3, 5, 3]. Second volume goes to location 0 (first with min count 3).
			// 3. New local counts: [4, 5, 3]. Third volume goes to location 2 (3 local < 4 local).
			expectedLocations: []int{0, 0, 2},
		},
		{
			name: "balances when some locations have remote volumes",
			locations: []locationSetup{
				{localVolumes: 1, remoteVolumes: 5},
				{localVolumes: 1, remoteVolumes: 0},
				{localVolumes: 0, remoteVolumes: 3},
			},
			// expectedLocations: []int{2, 0, 1}
			// Explanation:
			// 1. Initial local counts: [1, 1, 0]. First volume goes to location 2 (0 local).
			// 2. New local counts: [1, 1, 1]. Second volume goes to location 0 (first with min count 1).
			// 3. New local counts: [2, 1, 1]. Third volume goes to location 1 (next with min count 1).
			expectedLocations: []int{2, 0, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test store with multiple directories
			store := newTestStore(t, len(tc.locations))

			// Pre-populate locations with volumes
			for locIdx, setup := range tc.locations {
				location := store.Locations[locIdx]
				vidCounter := 1000 + locIdx*100 // unique volume IDs per location

				// Add local volumes
				for i := 0; i < setup.localVolumes; i++ {
					vol := createTestVolume(needle.VolumeId(vidCounter), false)
					location.SetVolume(vol.Id, vol)
					vidCounter++
				}

				// Add remote volumes
				for i := 0; i < setup.remoteVolumes; i++ {
					vol := createTestVolume(needle.VolumeId(vidCounter), true)
					location.SetVolume(vol.Id, vol)
					vidCounter++
				}
			}

			// Create volumes and verify they go to expected locations
			for i, expectedLoc := range tc.expectedLocations {
				volumeId := needle.VolumeId(i + 1)

				err := store.AddVolume(volumeId, "", NeedleMapInMemory, "000", "",
					0, needle.GetCurrentVersion(), 0, types.HardDriveType, 3)

				if err != nil {
					t.Fatalf("Failed to add volume %d: %v", volumeId, err)
				}

				// Find which location got the volume
				actualLoc := -1
				for locIdx, location := range store.Locations {
					if _, found := location.FindVolume(volumeId); found {
						actualLoc = locIdx
						break
					}
				}

				if actualLoc != expectedLoc {
					t.Errorf("Volume %d: expected location %d, got location %d",
						volumeId, expectedLoc, actualLoc)

					// Debug info
					for locIdx, loc := range store.Locations {
						localCount := loc.LocalVolumesLen()
						totalCount := loc.VolumesLen()
						t.Logf("  Location %d: %d local, %d total", locIdx, localCount, totalCount)
					}
				}
			}
		})
	}
}

// Helper types and functions
type locationSetup struct {
	localVolumes  int
	remoteVolumes int
}

func createTestVolume(vid needle.VolumeId, isRemote bool) *Volume {
	vol := &Volume{
		Id:         vid,
		SuperBlock: super_block.SuperBlock{},
		volumeInfo: &volume_server_pb.VolumeInfo{},
	}

	if isRemote {
		vol.hasRemoteFile = true
		vol.volumeInfo.Files = []*volume_server_pb.RemoteFile{
			{BackendType: "s3", BackendId: "test", Key: "remote-key-" + strconv.Itoa(int(vid))},
		}
	}

	return vol
}
