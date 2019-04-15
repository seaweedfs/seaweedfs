package storage

import (
	"fmt"
	"os"
	"sync/atomic"

	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/willf/bloom"
)

type mapMetric struct {
	DeletionCounter     uint32 `json:"DeletionCounter"`
	FileCounter         uint32 `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	mm.LogDeletionCounter(deletedByteCount)
}

func (mm *mapMetric) logPut(key NeedleId, oldSize uint32, newSize uint32) {
	mm.MaybeSetMaxFileKey(key)
	mm.LogFileCounter(newSize)
	if oldSize > 0 && oldSize != TombstoneFileSize {
		mm.LogDeletionCounter(oldSize)
	}
}
func (mm mapMetric) LogFileCounter(newSize uint32) {
	atomic.AddUint32(&mm.FileCounter, 1)
	atomic.AddUint64(&mm.FileByteCounter, uint64(newSize))
}
func (mm mapMetric) LogDeletionCounter(oldSize uint32) {
	if oldSize > 0 {
		atomic.AddUint32(&mm.DeletionCounter, 1)
		atomic.AddUint64(&mm.DeletionByteCounter, uint64(oldSize))
	}
}
func (mm mapMetric) ContentSize() uint64 {
	return atomic.LoadUint64(&mm.FileByteCounter)
}
func (mm mapMetric) DeletedSize() uint64 {
	return atomic.LoadUint64(&mm.DeletionByteCounter)
}
func (mm mapMetric) FileCount() int {
	return int(atomic.LoadUint32(&mm.FileCounter))
}
func (mm mapMetric) DeletedCount() int {
	return int(atomic.LoadUint32(&mm.DeletionCounter))
}
func (mm mapMetric) MaxFileKey() NeedleId {
	t := uint64(mm.MaximumFileKey)
	return NeedleId(t)
}
func (mm mapMetric) MaybeSetMaxFileKey(key NeedleId) {
	if key > mm.MaxFileKey() {
		atomic.StoreUint64(&mm.MaximumFileKey, uint64(key))
	}
}


func newNeedleMapMetricFromIndexFile(r *os.File) (mm *mapMetric, err error) {
	mm = &mapMetric{}
	var bf *bloom.BloomFilter
	buf := make([]byte, NeedleIdSize)
	err = reverseWalkIndexFile(r, func(entryCount int64) {
		bf = bloom.NewWithEstimates(uint(entryCount), 0.001)
	}, func(key NeedleId, offset Offset, size uint32) error {

		mm.MaybeSetMaxFileKey(key)
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

	entryCount := fileSize / NeedleEntrySize
	initFn(entryCount)

	batchSize := int64(1024 * 4)

	bytes := make([]byte, NeedleEntrySize*batchSize)
	nextBatchSize := entryCount % batchSize
	if nextBatchSize == 0 {
		nextBatchSize = batchSize
	}
	remainingCount := entryCount - nextBatchSize

	for remainingCount >= 0 {
		_, e := r.ReadAt(bytes[:NeedleEntrySize*nextBatchSize], NeedleEntrySize*remainingCount)
		// glog.V(0).Infoln("file", r.Name(), "readerOffset", NeedleEntrySize*remainingCount, "count", count, "e", e)
		if e != nil {
			return e
		}
		for i := int(nextBatchSize) - 1; i >= 0; i-- {
			key, offset, size := IdxFileEntry(bytes[i*NeedleEntrySize : i*NeedleEntrySize+NeedleEntrySize])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		nextBatchSize = batchSize
		remainingCount -= nextBatchSize
	}
	return nil
}
