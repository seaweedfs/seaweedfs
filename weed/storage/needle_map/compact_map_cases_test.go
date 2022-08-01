//go:build 5BytesOffset
// +build 5BytesOffset

package needle_map

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func Test5bytesIndexLoading(t *testing.T) {

	indexFile, ie := os.OpenFile("../../../test/data/187.idx", os.O_RDWR|os.O_RDONLY, 0644)
	if ie != nil {
		log.Fatalln(ie)
	}
	defer indexFile.Close()
	m, rowCount := loadNewNeedleMap(indexFile)

	println("total entries:", rowCount)

	key := types.NeedleId(0x671b905) // 108116229

	needle, found := m.Get(types.NeedleId(0x671b905))

	fmt.Printf("%v key:%v offset:%v size:%v\n", found, key, needle.Offset, needle.Size)

	assert.Equal(t, int64(12884911892)*8, needle.Offset.ToActualOffset(), "offset")

}
