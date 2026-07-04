package idx

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func idxEntryBytes(id types.NeedleId, offset types.Offset, size types.Size) []byte {
	b := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
	types.NeedleIdToBytes(b[0:types.NeedleIdSize], id)
	types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], offset)
	types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:], size)
	return b
}

// An offset-0 logical tombstone (remote-tier delete) occupies no physical .dat
// extent, so it must not be flagged as overlapping a real needle. The tombstone
// row is still counted for the index-size check.
func TestCheckIndexFile_IgnoresOffset0Tombstone(t *testing.T) {
	var buf []byte
	buf = append(buf, idxEntryBytes(types.NeedleId(1), types.ToOffset(8), types.Size(100))...)
	buf = append(buf, idxEntryBytes(types.NeedleId(2), types.ToOffset(0), types.TombstoneFileSize)...)

	count, errs := CheckIndexFile(bytes.NewReader(buf), int64(len(buf)), needle.Version3)
	if count != 2 {
		t.Errorf("tombstone row must still count: got %d", count)
	}
	if len(errs) != 0 {
		t.Errorf("offset-0 tombstone must not overlap: %v", errs)
	}
}

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
			name:      "healthy index with deleted files",
			indexPath: "./test_files/deleted_files.idx",
			version:   needle.Version3,
			want:      230,
			wantErrs:  []error{},
		},
		{
			name:      "damaged index (bitrot)",
			indexPath: "./test_files/simple_index_bitrot.idx",
			version:   needle.Version3,
			want:      161,
			wantErrs: []error{
				fmt.Errorf("needle 3544668469065756977 (#2) at [6602459528-7427766999] overlaps needle 49 at [6602459528-7427766999]"),
				fmt.Errorf("expected an index file of size 2577, got 2576"),
			},
		},
		{
			name:      "damaged index (truncated)",
			indexPath: "./test_files/simple_index_truncated.idx",
			version:   needle.Version3,
			want:      158,
			wantErrs: []error{
				fmt.Errorf("expected an index file of size 2540, got 2528"),
			},
		},
		{
			name:      "healthy EC index with deleted files",
			indexPath: "./test_files/deleted_files.ecx",
			version:   needle.Version3,
			want:      116,
			wantErrs:  []error{},
		},
		{
			name:      "damaged EC index (bitrot)",
			indexPath: "./test_files/deleted_files_bitrot.ecx",
			version:   needle.Version3,
			want:      116,
			wantErrs: []error{
				fmt.Errorf("needle 3223857 (#110) at [6602459528-7427767055] overlaps needle 12593 at [6601933184-7407907279]"),
				fmt.Errorf("needle 3544668469065757234 (#43) at [6737203600-7579354079] overlaps needle 3223857 at [6602459528-7427767055]"),
				fmt.Errorf("needle 3421236 (#112) at [7006693800-7899362591] overlaps needle 3544668469065757234 at [6737203600-7579354079]"),
				fmt.Errorf("needle 310 (#113) at [7276179888-8185702583] overlaps needle 3421236 at [7006693800-7899362591]"),
				fmt.Errorf("needle 7089336938131513954 (#52) at [13204919056-13205053935] overlaps needle 27410143614427489 at [13070174984-14703946887]"),
				fmt.Errorf("needle 25186 (#50) at [13204919056-14855533967] overlaps needle 7089336938131513954 at [13204919056-13205053935]"),
				fmt.Errorf("needle 7089336938131513954 (#51) at [13204919056-14855533967] overlaps needle 25186 at [13204919056-14855533967]"),
				fmt.Errorf("expected an index file of size 1857, got 1856"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := os.OpenFile(tc.indexPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("failed to open index file: %v", err)
			}
			defer idx.Close()

			idxStat, err := idx.Stat()
			if err != nil {
				t.Fatalf("failed to stat index file: %v", err)
			}

			got, gotErrs := CheckIndexFile(idx, idxStat.Size(), tc.version)

			if got != tc.want {
				t.Errorf("expected %d files processed, got %d", tc.want, got)
			}
			if !reflect.DeepEqual(gotErrs, tc.wantErrs) {
				t.Errorf("expected errors %v, got %v", tc.wantErrs, gotErrs)
			}
		})
	}
}
