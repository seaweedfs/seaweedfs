package storage

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestScrubVolumeData(t *testing.T) {
	testCases := []struct {
		name      string
		dataPath  string
		indexPath string
		version   needle.Version
		want      int64
		wantErrs  []error
	}{
		{
			name:      "healthy volume",
			dataPath:  "./test_files/healthy_volume.dat",
			indexPath: "./test_files/healthy_volume.idx",
			version:   needle.Version3,
			want:      27,
			wantErrs:  []error{},
		},
		{
			name:      "bitrot volume",
			dataPath:  "./test_files/bitrot_volume.dat",
			indexPath: "./test_files/bitrot_volume.idx",
			version:   needle.Version3,
			want:      27,
			wantErrs: []error{
				fmt.Errorf("needle 3 on volume 0: invalid CRC for needle 3 (got 0b243a0d, want 4af853fb), data on disk corrupted"),
				fmt.Errorf("needle 48 on volume 0: invalid CRC for needle 30 (got 3c40e8d5, want 5077fea1), data on disk corrupted"),
				fmt.Errorf("data file size for volume 0 (942864) doesn't match the size for 27 needles read (942856)"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			datFile, err := os.OpenFile(tc.dataPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("failed to open data file: %v", err)
			}
			defer datFile.Close()

			idxFile, err := os.OpenFile(tc.indexPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("failed to open index file: %v", err)
			}
			defer idxFile.Close()

			idxStat, err := idxFile.Stat()
			if err != nil {
				t.Fatalf("failed to stat index file: %v", err)
			}

			v := Volume{
				volumeInfo: &volume_server_pb.VolumeInfo{
					Version: uint32(tc.version),
				},
			}

			got, gotErrs := v.scrubVolumeData(backend.NewDiskFile(datFile), idxFile, idxStat.Size())

			if got != tc.want {
				t.Errorf("expected %d files processed, got %d", tc.want, got)
			}
			if !reflect.DeepEqual(gotErrs, tc.wantErrs) {
				t.Errorf("expected errors %v, got %v", tc.wantErrs, gotErrs)
			}
		})
	}
}

// TestVerifyIndexFitsInDat ensures the structural startup check rejects an
// .idx that references bytes past the end of the .dat — the corruption shape
// described in issue #8928. The pre-existing tail check only inspects the
// last 10 entries, so this guards against deeper-in-the-file rot.
func TestVerifyIndexFitsInDat(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	// A handful of healthy needles establishes a baseline .dat/.idx.
	for i := 1; i <= 4; i++ {
		n := newRandomNeedle(uint64(i))
		if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	// Sanity: a healthy volume passes the new structural check.
	idxFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx: %v", err)
	}
	if err := verifyIndexFitsInDat(idxFile, v.DataBackend, v.Version()); err != nil {
		idxFile.Close()
		t.Fatalf("healthy volume should pass structural check, got %v", err)
	}
	idxFile.Close()

	// Inject a dangling entry and rerun: now the check must fire.
	datSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	bogusOffset := types.ToOffset(datSize + 4*1024*1024)
	if err := v.nm.Put(types.Uint64ToNeedleId(9999), bogusOffset, types.Size(1024)); err != nil {
		t.Fatalf("inject dangling idx entry: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx after inject: %v", err)
	}

	idxFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("reopen .idx: %v", err)
	}
	defer idxFile.Close()
	err = verifyIndexFitsInDat(idxFile, v.DataBackend, v.Version())
	if err == nil {
		t.Fatalf("structural check should have failed for dangling idx entry")
	}
}
