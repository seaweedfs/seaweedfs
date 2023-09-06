package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DataShardsCount             = 10
	ParityShardsCount           = 4
	TotalShardsCount            = DataShardsCount + ParityShardsCount
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB
	ErasureCodingSmallBlockSize = 1024 * 1024        // 1MB
)

// WriteSortedFileFromIdx generates .ecx file from existing .idx file
// all keys are sorted in ascending order
func WriteSortedFileFromIdx(baseFileName string, ext string) (e error) {

	nm, err := readNeedleMap(baseFileName)
	if nm != nil {
		defer nm.Close()
	}
	if err != nil {
		return fmt.Errorf("readNeedleMap: %v", err)
	}

	ecxFile, err := os.OpenFile(baseFileName+ext, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		bytes := value.ToBytes()
		_, writeErr := ecxFile.Write(bytes)
		return writeErr
	})

	if err != nil {
		return fmt.Errorf("failed to visit idx file: %v", err)
	}

	return nil
}

// WriteEcFiles generates .ec00 ~ .ec13 files
func WriteEcFiles(baseFileName string) error {
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
}

func RebuildEcFiles(baseFileName string) ([]uint32, error) {
	return generateMissingEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
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

	glog.V(0).Infof("encodeDatFile %s.dat size:%d", baseFileName, fi.Size())
	err = encodeDatFile(fi.Size(), baseFileName, bufferSize, largeBlockSize, file, smallBlockSize)
	if err != nil {
		return fmt.Errorf("encodeDatFile: %v", err)
	}
	return nil
}

func generateMissingEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64) (generatedShardIds []uint32, err error) {

	shardHasData := make([]bool, TotalShardsCount)
	inputFiles := make([]*os.File, TotalShardsCount)
	outputFiles := make([]*os.File, TotalShardsCount)
	for shardId := 0; shardId < TotalShardsCount; shardId++ {
		shardFileName := baseFileName + ToExt(shardId)
		if util.FileExists(shardFileName) {
			shardHasData[shardId] = true
			inputFiles[shardId], err = os.OpenFile(shardFileName, os.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			defer inputFiles[shardId].Close()
		} else {
			outputFiles[shardId], err = os.OpenFile(shardFileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			defer outputFiles[shardId].Close()
			generatedShardIds = append(generatedShardIds, uint32(shardId))
		}
	}

	err = rebuildEcFiles(shardHasData, inputFiles, outputFiles)
	if err != nil {
		return nil, fmt.Errorf("rebuildEcFiles: %v", err)
	}
	return
}

func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	bufferSize := int64(len(buffers[0]))
	if bufferSize == 0 {
		glog.Fatal("unexpected zero buffer size")
	}

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

func encodeDatFile(remainingSize int64, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {

	var processedSize int64

	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}

	buffers := make([][]byte, TotalShardsCount)
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	outputs, err := openEcFiles(baseFileName, false)
	defer closeEcFiles(outputs)
	if err != nil {
		return fmt.Errorf("failed to open ec files %s: %v", baseFileName, err)
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
		err = encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %v", err)
		}
		remainingSize -= smallBlockSize * DataShardsCount
		processedSize += smallBlockSize * DataShardsCount
	}
	return nil
}

func rebuildEcFiles(shardHasData []bool, inputFiles []*os.File, outputFiles []*os.File) error {

	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}

	buffers := make([][]byte, TotalShardsCount)
	for i := range buffers {
		if shardHasData[i] {
			buffers[i] = make([]byte, ErasureCodingSmallBlockSize)
		}
	}

	var startOffset int64
	var inputBufferDataSize int
	for {

		// read the input data from files
		for i := 0; i < TotalShardsCount; i++ {
			if shardHasData[i] {
				n, _ := inputFiles[i].ReadAt(buffers[i], startOffset)
				if n == 0 {
					return nil
				}
				if inputBufferDataSize == 0 {
					inputBufferDataSize = n
				}
				if inputBufferDataSize != n {
					return fmt.Errorf("ec shard size expected %d actual %d", inputBufferDataSize, n)
				}
			} else {
				buffers[i] = nil
			}
		}

		// encode the data
		err = enc.Reconstruct(buffers)
		if err != nil {
			return fmt.Errorf("reconstruct: %v", err)
		}

		// write the data to output files
		for i := 0; i < TotalShardsCount; i++ {
			if !shardHasData[i] {
				n, _ := outputFiles[i].WriteAt(buffers[i][:inputBufferDataSize], startOffset)
				if inputBufferDataSize != n {
					return fmt.Errorf("fail to write to %s", outputFiles[i].Name())
				}
			}
		}
		startOffset += int64(inputBufferDataSize)
	}

}

func readNeedleMap(baseFileName string) (*needle_map.MemDb, error) {
	indexFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, err)
	}
	defer indexFile.Close()

	cm := needle_map.NewMemDb()
	err = idx.WalkIndexFile(indexFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}
