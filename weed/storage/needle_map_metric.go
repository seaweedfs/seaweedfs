package storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/willf/bloom"
	"os"
)

type mapMetric struct {
	DeletionCounter     int      `json:"DeletionCounter"`
	FileCounter         int      `json:"FileCounter"`
	DeletionByteCounter uint64   `json:"DeletionByteCounter"`
	FileByteCounter     uint64   `json:"FileByteCounter"`
	MaximumFileKey      NeedleId `json:"MaxFileKey"`
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(deletedByteCount)
	mm.DeletionCounter++
}

func (mm *mapMetric) logPut(key NeedleId, oldSize uint32, newSize uint32) {
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
func (mm mapMetric) MaxFileKey() NeedleId {
	return mm.MaximumFileKey
}

func newNeedleMapMetricFromIndexFile(r *os.File) (mm *mapMetric, err error) {
	mm = &mapMetric{}
	var bf *bloom.BloomFilter
	buf := make([]byte, NeedleIdSize)
	err = reverseWalkIndexFile(r, func(entryCount int64) {
		bf = bloom.NewWithEstimates(uint(entryCount), 0.001)
	}, func(key NeedleId, offset Offset, size uint32) error {

		if key > mm.MaximumFileKey {
			mm.MaximumFileKey = key
		}
		NeedleIdToBytes(buf, key)
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

func reverseWalkIndexFile(r *os.File, initFn func(entryCount int64), fn func(key NeedleId, offset Offset, size uint32) error) error {
	fi, err := r.Stat()
	if err != nil {
		return fmt.Errorf("file %s stat error: %v", r.Name(), err)
	}
	fileSize := fi.Size()
	if fileSize%NeedleEntrySize != 0 {
		return fmt.Errorf("unexpected file %s size: %d", r.Name(), fileSize)
	}

	initFn(fileSize / NeedleEntrySize)

	bytes := make([]byte, NeedleEntrySize)
	for readerOffset := fileSize - NeedleEntrySize; readerOffset >= 0; readerOffset -= NeedleEntrySize {
		count, e := r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		key, offset, size := IdxFileEntry(bytes)
		if e = fn(key, offset, size); e != nil {
			return e
		}
	}
	return nil
}
