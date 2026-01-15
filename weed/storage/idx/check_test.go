package idx

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestCheckIndexFile(t *testing.T) {
	testCases := []struct {
		name      string
		indexPath string
		version   needle.Version
		want      int64
		wantErrs  []error
	}{
		{
			name:      "healthy index",
			indexPath: "./test_files/simple_index.idx",
			version:   needle.Version3,
			want:      161,
			wantErrs:  []error{},
		},
		{
			name:      "damaged index, overlapping needles",
			indexPath: "./test_files/simple_index_damaged_1.idx",
			version:   needle.Version3,
			want:      120,
			wantErrs: []error{
				fmt.Errorf("offset 0 for needle 32287180425496915 (#120) overlaps previous needle at 58386488"),
				fmt.Errorf("expected an index file of size 2521, got 1920"),
			},
		},
		{
			name:      "damaged index, inconsistent needles",
			indexPath: "./test_files/simple_index_damaged_2.idx",
			version:   needle.Version3,
			want:      2,
			wantErrs: []error{
				fmt.Errorf("offset 191330304 for needle 4096 (#2) doesn't match expected 747304"),
				fmt.Errorf("expected an index file of size 2574, got 32"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			icx, err := os.OpenFile(tc.indexPath, os.O_RDONLY, 0)
			if err != nil {
				t.Errorf("failed to open index file: %v", err)
			}
			icxStat, err := icx.Stat()
			if err != nil {
				t.Errorf("failed to stat index file: %v", err)
			}
			defer icx.Close()

			got, gotErrs := CheckIndexFile(icx, icxStat.Size(), tc.version)

			if got != tc.want {
				t.Errorf("expected %d files processed, got %d", tc.want, got)
			}
			if !reflect.DeepEqual(gotErrs, tc.wantErrs) {
				t.Errorf("expected errors %v, got %v", tc.wantErrs, gotErrs)
			}
		})
	}
}
