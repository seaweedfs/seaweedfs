package needle_map

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func BenchmarkMemDb(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		nm := NewMemDb()

		nid := types.NeedleId(345)
		offset := types.Offset{
			OffsetHigher: types.OffsetHigher{},
			OffsetLower:  types.OffsetLower{},
		}
		nm.Set(nid, offset, 324)
		nm.Close()
	}

}
