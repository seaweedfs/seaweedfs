package erasure_coding

import (
	"os"
	"testing"

	"github.com/klauspost/reedsolomon"
)

func TestEncodingDecoding(t *testing.T) {
	largeBlockSize := int64(10000)
	smallBlockSize := int64(100)
	bufferSize := 50

	file, err := os.OpenFile("1.dat", os.O_RDONLY, 0)
	if err != nil {
		t.Logf("failed to open dat file: %v", err)
	}

	fi, err := file.Stat()
	if err != nil {
		t.Logf("failed to stat dat file: %v", err)
	}

	remainingSize := fi.Size()
	var processedSize int64

	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		t.Logf("failed to create encoder: %v", err)
	}

	buffers := make([][]byte, DataShardsCount+ParityShardsCount)

	for i, _ := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	for remainingSize > largeBlockSize*DataShardsCount {
		encodeData(file, enc, processedSize, largeBlockSize, buffers)
		remainingSize -= largeBlockSize * DataShardsCount
		processedSize += largeBlockSize * DataShardsCount
	}

	for remainingSize > 0 {
		encodeData(file, enc, processedSize, smallBlockSize, buffers)
		remainingSize -= smallBlockSize * DataShardsCount
		processedSize += smallBlockSize * DataShardsCount
	}

}

