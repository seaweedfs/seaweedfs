package storage

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
