package erasure_coding

import (
	"fmt"
	"os"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/klauspost/reedsolomon"
)

func TestEncodingDecoding(t *testing.T) {
	largeBlockSize := int64(10000)
	smallBlockSize := int64(100)
	bufferSize := 50
	baseFileName := "1"

	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		t.Logf("failed to open dat file: %v", err)
	}

	fi, err := file.Stat()
	if err != nil {
		t.Logf("failed to stat dat file: %v", err)
	}

	err = encodeDatFile(fi.Size(), err, baseFileName, bufferSize, largeBlockSize, file, smallBlockSize)
	if err != nil {
		t.Logf("failed to stat dat file: %v", err)
	}
	file.Close()

	err = writeSortedEcxFiles(baseFileName)
	if err != nil {
		t.Logf("writeSortedEcxFiles: %v", err)
	}

	err = validateFiles(baseFileName)
	if err != nil {
		t.Logf("writeSortedEcxFiles: %v", err)
	}

}

func encodeDatFile(remainingSize int64, err error, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {
	var processedSize int64
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}
	buffers := make([][]byte, DataShardsCount+ParityShardsCount)
	outputs, err := openEcFiles(baseFileName)
	defer closeEcFiles(outputs)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	for i, _ := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}
	for remainingSize > largeBlockSize*DataShardsCount {
		err = encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs)
		if err != nil {
			return fmt.Errorf("failed to encode large chunk data: %v", err)
		}
		remainingSize -= largeBlockSize * DataShardsCount
		processedSize += largeBlockSize * DataShardsCount
	}
	for remainingSize > 0 {
		encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %v", err)
		}
		remainingSize -= smallBlockSize * DataShardsCount
		processedSize += smallBlockSize * DataShardsCount
	}
	return nil
}

func writeSortedEcxFiles(baseFileName string) (e error) {

	var indexFile *os.File
	if indexFile, e = os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644); e != nil {
		return fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, e)
	}

	cm := needle_map.NewCompactMap()
	storage.WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})

	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	defer ecxFile.Close()

	err = cm.AscendingVisit(func(value needle_map.NeedleValue) error {
		bytes := value.ToBytes()
		_, writeErr := ecxFile.Write(bytes)
		return writeErr
	})

	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}

	return nil
}

func validateFiles(baseFileName string) error {
	return nil

}
