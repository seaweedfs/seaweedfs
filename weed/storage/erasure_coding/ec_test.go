package erasure_coding

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/klauspost/reedsolomon"

	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
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

	err = WriteSortedFileFromIdx(baseFileName, ".ecx")
	if err != nil {
		t.Logf("WriteSortedFileFromIdx: %v", err)
	}

	err = validateFiles(baseFileName)
	if err != nil {
		t.Logf("WriteSortedFileFromIdx: %v", err)
	}

	removeGeneratedFiles(baseFileName)

}

func validateFiles(baseFileName string) error {
	nm, err := readNeedleMap(baseFileName)
	defer nm.Close()
	if err != nil {
		return fmt.Errorf("readNeedleMap: %v", err)
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

	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		return assertSame(datFile, fi.Size(), ecFiles, value.Offset, value.Size)
	})
	if err != nil {
		return fmt.Errorf("failed to check ec files: %v", err)
	}
	return nil
}

func assertSame(datFile *os.File, datSize int64, ecFiles []*os.File, offset types.Offset, size types.Size) error {

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

func readDatFile(datFile *os.File, offset types.Offset, size types.Size) ([]byte, error) {

	data := make([]byte, size)
	n, err := datFile.ReadAt(data, offset.ToActualOffset())
	if err != nil {
		return nil, fmt.Errorf("failed to ReadAt dat file: %v", err)
	}
	if n != int(size) {
		return nil, fmt.Errorf("unexpected read size %d, expected %d", n, size)
	}
	return data, nil
}

func readEcFile(datSize int64, ecFiles []*os.File, offset types.Offset, size types.Size) (data []byte, err error) {

	intervals := LocateData(largeBlockSize, smallBlockSize, datSize, offset.ToActualOffset(), size)

	for i, interval := range intervals {
		if d, e := readOneInterval(interval, ecFiles); e != nil {
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

func readOneInterval(interval Interval, ecFiles []*os.File) (data []byte, err error) {

	ecFileIndex, ecFileOffset := interval.ToShardIdAndOffset(largeBlockSize, smallBlockSize)

	data = make([]byte, interval.Size)
	err = readFromFile(ecFiles[ecFileIndex], data, ecFileOffset)
	{ // do some ec testing
		ecData, err := readFromOtherEcFiles(ecFiles, int(ecFileIndex), ecFileOffset, interval.Size)
		if err != nil {
			return nil, fmt.Errorf("ec reconstruct error: %v", err)
		}
		if bytes.Compare(data, ecData) != 0 {
			return nil, fmt.Errorf("ec compare error")
		}
	}
	return
}

func readFromOtherEcFiles(ecFiles []*os.File, ecFileIndex int, ecFileOffset int64, size types.Size) (data []byte, err error) {
	enc, err := reedsolomon.New(DataShardsCount, ParityShardsCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %v", err)
	}

	bufs := make([][]byte, TotalShardsCount)
	for i := 0; i < DataShardsCount; {
		n := int(rand.Int31n(TotalShardsCount))
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
		fname := fmt.Sprintf("%s.ec%02d", baseFileName, i)
		os.Remove(fname)
	}
	os.Remove(baseFileName + ".ecx")
}

func TestLocateData(t *testing.T) {
	intervals := LocateData(largeBlockSize, smallBlockSize, DataShardsCount*largeBlockSize+1, DataShardsCount*largeBlockSize, 1)
	if len(intervals) != 1 {
		t.Errorf("unexpected interval size %d", len(intervals))
	}
	if !intervals[0].sameAs(Interval{0, 0, 1, false, 1}) {
		t.Errorf("unexpected interval %+v", intervals[0])
	}

	intervals = LocateData(largeBlockSize, smallBlockSize, DataShardsCount*largeBlockSize+1, DataShardsCount*largeBlockSize/2+100, DataShardsCount*largeBlockSize+1-DataShardsCount*largeBlockSize/2-100)
	fmt.Printf("%+v\n", intervals)
}

func (this Interval) sameAs(that Interval) bool {
	return this.IsLargeBlock == that.IsLargeBlock &&
		this.InnerBlockOffset == that.InnerBlockOffset &&
		this.BlockIndex == that.BlockIndex &&
		this.Size == that.Size
}
