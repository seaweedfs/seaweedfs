package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/klauspost/reedsolomon"
)

const (
	DataShardsCount             = 10
	ParityShardsCount           = 4
	TotalShardsCount            = DataShardsCount + ParityShardsCount
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB
	ErasureCodingSmallBlockSize = 1024 * 1024        // 1MB
)

// WriteSortedEcxFile generates .ecx file from existing .idx file
// all keys are sorted in ascending order
func WriteSortedEcxFile(baseFileName string) (e error) {

	cm, err := readCompactMap(baseFileName)
	if err != nil {
		return fmt.Errorf("readCompactMap: %v", err)
	}

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

// WriteEcFiles generates .ec01 ~ .ec14 files
func WriteEcFiles(baseFileName string) error {
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
}

func ToExt(ecIndex int) string {
	return fmt.Sprintf(".ec%02d", ecIndex)
}

func generateEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64) error {
	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %v", err)
	}
	err = encodeDatFile(fi.Size(), err, baseFileName, bufferSize, largeBlockSize, file, smallBlockSize)
	if err != nil {
		return fmt.Errorf("encodeDatFile: %v", err)
	}
	return nil
}

func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	bufferSize := int64(len(buffers[0]))
	batchCount := blockSize / bufferSize
	if blockSize%bufferSize != 0 {
		glog.Fatalf("unexpected block size %d buffer size %d", blockSize, bufferSize)
	}

	for b := int64(0); b < batchCount; b++ {
		err := encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs)
		if err != nil {
			return err
		}
	}

	return nil
}

func openEcFiles(baseFileName string, forRead bool) (files []*os.File, err error) {
	for i := 0; i < TotalShardsCount; i++ {
		fname := baseFileName + ToExt(i)
		openOption := os.O_TRUNC | os.O_CREATE | os.O_WRONLY
		if forRead {
			openOption = os.O_RDONLY
		}
		f, err := os.OpenFile(fname, openOption, 0644)
		if err != nil {
			return files, fmt.Errorf("failed to open file %s: %v", fname, err)
		}
		files = append(files, f)
	}
	return
}

func closeEcFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}

func encodeDataOneBatch(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	// read data into buffers
	for i := 0; i < DataShardsCount; i++ {
		n, err := file.ReadAt(buffers[i], startOffset+blockSize*int64(i))
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if n < len(buffers[i]) {
			for t := len(buffers[i]) - 1; t >= n; t-- {
				buffers[i][t] = 0
			}
		}
	}

	err := enc.Encode(buffers)
	if err != nil {
		return err
	}

	for i := 0; i < TotalShardsCount; i++ {
		_, err := outputs[i].Write(buffers[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func encodeDatFile(remainingSize int64, err error, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {
	var processedSize int64
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}
	buffers := make([][]byte, TotalShardsCount)
	outputs, err := openEcFiles(baseFileName, false)
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

func readCompactMap(baseFileName string) (*needle_map.CompactMap, error) {
	indexFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, err)
	}
	defer indexFile.Close()

	cm := needle_map.NewCompactMap()
	err = idx.WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}
