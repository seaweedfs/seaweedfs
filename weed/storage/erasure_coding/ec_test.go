package erasure_coding

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/klauspost/reedsolomon"
)

const (
	largeBlockSize = 10000
	smallBlockSize = 100
)

func TestEncodingDecoding(t *testing.T) {
	bufferSize := 50
	baseFileName := "1"

	err := generateEcFiles(baseFileName, bufferSize, largeBlockSize, smallBlockSize)
	if err != nil {
		t.Logf("generateEcFiles: %v", err)
	}

	err = writeSortedEcxFiles(baseFileName)
	if err != nil {
		t.Logf("writeSortedEcxFiles: %v", err)
	}

	err = validateFiles(baseFileName)
	if err != nil {
		t.Logf("writeSortedEcxFiles: %v", err)
	}

	removeGeneratedFiles(baseFileName)

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

func encodeDatFile(remainingSize int64, err error, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64) error {
	var processedSize int64
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %v", err)
	}
	buffers := make([][]byte, DataShardsCount+ParityShardsCount)
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

func writeSortedEcxFiles(baseFileName string) (e error) {

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

func validateFiles(baseFileName string) error {
	cm, err := readCompactMap(baseFileName)
	if err != nil {
		return fmt.Errorf("readCompactMap: %v", err)
	}

	datFile, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %v", err)
	}
	defer datFile.Close()

	fi, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %v", err)
	}

	ecFiles, err := openEcFiles(baseFileName, true)
	defer closeEcFiles(ecFiles)

	err = cm.AscendingVisit(func(value needle_map.NeedleValue) error {
		return assertSame(datFile, fi.Size(), ecFiles, value.Offset, value.Size)
	})
	if err != nil {
		return fmt.Errorf("failed to check ec files: %v", err)
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
	err = storage.WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		if !offset.IsZero() && size != types.TombstoneFileSize {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}

func assertSame(datFile *os.File, datSize int64, ecFiles []*os.File, offset types.Offset, size uint32) error {

	data, err := readDatFile(datFile, offset, size)
	if err != nil {
		return fmt.Errorf("failed to read dat file: %v", err)
	}

	ecData, err := readEcFile(datSize, ecFiles, offset, size)
	if err != nil {
		return fmt.Errorf("failed to read ec file: %v", err)
	}

	if bytes.Compare(data, ecData) != 0 {
		return fmt.Errorf("unexpected data read")
	}

	return nil
}

func readDatFile(datFile *os.File, offset types.Offset, size uint32) ([]byte, error) {

	data := make([]byte, size)
	n, err := datFile.ReadAt(data, offset.ToAcutalOffset())
	if err != nil {
		return nil, fmt.Errorf("failed to ReadAt dat file: %v", err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("unexpected read size %d, expected %d", n, size)
	}
	return data, nil
}

func readEcFile(datSize int64, ecFiles []*os.File, offset types.Offset, size uint32) (data []byte, err error) {

	intervals := locateData(largeBlockSize, smallBlockSize, datSize, offset.ToAcutalOffset(), size)

	nLargeBlockRows := int(datSize / (largeBlockSize * DataShardsCount))

	for i, interval := range intervals {
		if d, e := readOneInterval(interval, ecFiles, nLargeBlockRows); e != nil {
			return nil, e
		} else {
			if i == 0 {
				data = d
			} else {
				data = append(data, d...)
			}
		}
	}

	return data, nil
}

func readOneInterval(interval Interval, ecFiles []*os.File, nLargeBlockRows int) (data []byte, err error) {
	ecFileOffset := interval.innerBlockOffset
	rowIndex := interval.blockIndex / DataShardsCount
	if interval.isLargeBlock {
		ecFileOffset += int64(rowIndex) * largeBlockSize
	} else {
		ecFileOffset += int64(nLargeBlockRows)*largeBlockSize + int64(rowIndex)*smallBlockSize
	}

	ecFileIndex := interval.blockIndex % DataShardsCount

	data = make([]byte, interval.size)
	err = readFromFile(ecFiles[ecFileIndex], data, ecFileOffset)
	{ // do some ec testing
		ecData, err := readFromOtherEcFiles(ecFiles, ecFileIndex, ecFileOffset, interval.size)
		if err != nil {
			return nil, fmt.Errorf("ec reconstruct error: %v", err)
		}
		if bytes.Compare(data, ecData) != 0 {
			return nil, fmt.Errorf("ec compare error")
		}
	}
	return
}

func readFromOtherEcFiles(ecFiles []*os.File, ecFileIndex int, ecFileOffset int64, size uint32) (data []byte, err error) {
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %v", err)
	}

	bufs := make([][]byte, DataShardsCount+ParityShardsCount)
	for i := 0; i < DataShardsCount; {
		n := int(rand.Int31n(DataShardsCount + ParityShardsCount))
		if n == ecFileIndex || bufs[n] != nil {
			continue
		}
		bufs[n] = make([]byte, size)
		i++
	}

	for i, buf := range bufs {
		if buf == nil {
			continue
		}
		err = readFromFile(ecFiles[i], buf, ecFileOffset)
		if err != nil {
			return
		}
	}

	if err = enc.ReconstructData(bufs); err != nil {
		return nil, err
	}

	return bufs[ecFileIndex], nil
}

func readFromFile(file *os.File, data []byte, ecFileOffset int64) (err error) {
	_, err = file.ReadAt(data, ecFileOffset)
	return
}

func removeGeneratedFiles(baseFileName string) {
	for i := 0; i < DataShardsCount+ParityShardsCount; i++ {
		fname := fmt.Sprintf("%s.ec%02d", baseFileName, i+1)
		os.Remove(fname)
	}
	os.Remove(baseFileName+".ecx")
}

func TestLocateData(t *testing.T) {
	intervals := locateData(largeBlockSize, smallBlockSize, DataShardsCount*largeBlockSize+1, DataShardsCount*largeBlockSize, 1)
	if len(intervals) != 1 {
		t.Errorf("unexpected interval size %d", len(intervals))
	}
	if !intervals[0].sameAs(Interval{0, 0, 1, false}) {
		t.Errorf("unexpected interval %+v", intervals[0])
	}

	intervals = locateData(largeBlockSize, smallBlockSize, DataShardsCount*largeBlockSize+1, DataShardsCount*largeBlockSize/2+100, DataShardsCount*largeBlockSize+1-DataShardsCount*largeBlockSize/2-100)
	fmt.Printf("%+v\n", intervals)
}

func (this Interval) sameAs(that Interval) bool {
	return this.isLargeBlock == that.isLargeBlock &&
		this.innerBlockOffset == that.innerBlockOffset &&
		this.blockIndex == that.blockIndex &&
		this.size == that.size
}
