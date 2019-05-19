package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/klauspost/reedsolomon"
)

const (
	DataShardsCount             = 10
	ParityShardsCount           = 4
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB
	ErasureCodingSmallBlockSize = 1024 * 1024        // 1MB
)

func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File) error {

	bufferSize := int64(len(buffers[0]))
	batchCount := blockSize/bufferSize
	if blockSize%bufferSize!=0 {
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

func openEcFiles(baseFileName string, forRead bool) (files []*os.File, err error){
	for i := 0; i< DataShardsCount+ParityShardsCount; i++{
		fname := fmt.Sprintf("%s.ec%02d", baseFileName, i+1)
		openOption := os.O_TRUNC|os.O_CREATE|os.O_WRONLY
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

func closeEcFiles(files []*os.File){
	for _, f := range files{
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

	for i := 0; i < DataShardsCount+ParityShardsCount; i++ {
		_, err := outputs[i].Write(buffers[i])
		if err != nil {
			return err
		}
	}

	return nil
}
