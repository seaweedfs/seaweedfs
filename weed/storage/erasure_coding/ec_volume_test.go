package erasure_coding

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestPositioning(t *testing.T) {

	ecxFile, err := os.OpenFile("389.ecx", os.O_RDONLY, 0)
	if err != nil {
		t.Errorf("failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	stat, _ := ecxFile.Stat()
	fileSize := stat.Size()

	tests := []struct {
		needleId string
		offset   int64
		size     int
	}{
		{needleId: "0f0edb92", offset: 31300679656, size: 1167},
		{needleId: "0ef7d7f8", offset: 11513014944, size: 66044},
	}

	for _, test := range tests {
		needleId, _ := types.ParseNeedleId(test.needleId)
		offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
		assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")
		fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)
	}

	needleId, _ := types.ParseNeedleId("0f087622")
	offset, size, err := SearchNeedleFromSortedIndex(ecxFile, fileSize, needleId, nil)
	assert.Equal(t, nil, err, "SearchNeedleFromSortedIndex")
	fmt.Printf("offset: %d size: %d\n", offset.ToActualOffset(), size)

	var shardEcdFileSize int64 = 1118830592 // 1024*1024*1024*3
	intervals := LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shardEcdFileSize, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, needle.CurrentVersion)))

	for _, interval := range intervals {
		shardId, shardOffset := interval.ToShardIdAndOffset(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
		fmt.Printf("interval: %+v, shardId: %d, shardOffset: %d\n", interval, shardId, shardOffset)
	}

}
