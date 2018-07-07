package storage

import (
	"fmt"
	"os"
	"github.com/willf/bloom"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"encoding/binary"
)

type mapMetric struct {
	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(deletedByteCount)
	mm.DeletionCounter++
}

func (mm *mapMetric) logPut(key uint64, oldSize uint32, newSize uint32) {
	if key > mm.MaximumFileKey {
		mm.MaximumFileKey = key
	}
	mm.FileCounter++
	mm.FileByteCounter = mm.FileByteCounter + uint64(newSize)
	if oldSize > 0 {
		mm.DeletionCounter++
		mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(oldSize)
	}
}

func (mm mapMetric) ContentSize() uint64 {
	return mm.FileByteCounter
}
func (mm mapMetric) DeletedSize() uint64 {
	return mm.DeletionByteCounter
}
func (mm mapMetric) FileCount() int {
	return mm.FileCounter
}
func (mm mapMetric) DeletedCount() int {
	return mm.DeletionCounter
}
func (mm mapMetric) MaxFileKey() uint64 {
	return mm.MaximumFileKey
}

func newNeedleMapMetricFromIndexFile(r *os.File) (mm *mapMetric, err error) {
	mm = &mapMetric{}
	var bf *bloom.BloomFilter
	buf := make([]byte, 8)
	err = reverseWalkIndexFile(r, func(entryCount int64) {
		bf = bloom.NewWithEstimates(uint(entryCount), 0.001)
	}, func(key uint64, offset, size uint32) error {

		if key > mm.MaximumFileKey {
			mm.MaximumFileKey = key
		}

		binary.BigEndian.PutUint64(buf, key)
		if size != TombstoneFileSize {
			mm.FileByteCounter += uint64(size)
		}

		if !bf.Test(buf) {
			mm.FileCounter++
			bf.Add(buf)
		} else {
			// deleted file
			mm.DeletionCounter++
			if size != TombstoneFileSize {
				// previously already deleted file
				mm.DeletionByteCounter += uint64(size)
			}
		}
		return nil
	})
	return
}

func reverseWalkIndexFile(r *os.File, initFn func(entryCount int64), fn func(key uint64, offset, size uint32) error) error {
	fi, err := r.Stat()
	if err != nil {
		return fmt.Errorf("file %s stat error: %v", r.Name(), err)
	}
	fileSize := fi.Size()
	if fileSize%NeedleIndexSize != 0 {
		return fmt.Errorf("unexpected file %s size: %d", r.Name(), fileSize)
	}

	initFn(fileSize / NeedleIndexSize)

	bytes := make([]byte, NeedleIndexSize)
	for readerOffset := fileSize - NeedleIndexSize; readerOffset >= 0; readerOffset -= NeedleIndexSize {
		count, e := r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		key, offset, size := idxFileEntry(bytes)
		if e = fn(key, offset, size); e != nil {
			return e
		}
	}
	return nil
}
