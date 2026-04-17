package storage

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	boom "github.com/tylertreat/BoomFilters"
)

type mapMetric struct {
	DeletionCounter     uint32 `json:"DeletionCounter"`
	FileCounter         uint32 `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
	// MaximumNeedleEnd is the largest (offset.ToActualOffset() +
	// GetActualSize(size, version)) seen during the index walk. It is used
	// at volume load to verify that no .idx entry references bytes past
	// the end of the .dat — the deeper-than-tail corruption shape from
	// issue #8928 — without paying for a second linear scan of the index.
	MaximumNeedleEnd int64 `json:"MaxNeedleEnd"`
}

func (mm *mapMetric) logDelete(deletedByteCount Size) {
	if mm == nil {
		return
	}
	mm.LogDeletionCounter(deletedByteCount)
}

func (mm *mapMetric) logPut(key NeedleId, oldSize Size, newSize Size) {
	if mm == nil {
		return
	}
	mm.MaybeSetMaxFileKey(key)
	mm.LogFileCounter(newSize)
	if oldSize > 0 && oldSize.IsValid() {
		mm.LogDeletionCounter(oldSize)
	}
}
func (mm *mapMetric) LogFileCounter(newSize Size) {
	if mm == nil {
		return
	}
	atomic.AddUint32(&mm.FileCounter, 1)
	atomic.AddUint64(&mm.FileByteCounter, uint64(newSize))
}
func (mm *mapMetric) LogDeletionCounter(oldSize Size) {
	if mm == nil {
		return
	}
	if oldSize > 0 {
		atomic.AddUint32(&mm.DeletionCounter, 1)
		atomic.AddUint64(&mm.DeletionByteCounter, uint64(oldSize))
	}
}
func (mm *mapMetric) ContentSize() uint64 {
	if mm == nil {
		return 0
	}
	return atomic.LoadUint64(&mm.FileByteCounter)
}
func (mm *mapMetric) DeletedSize() uint64 {
	if mm == nil {
		return 0
	}
	return atomic.LoadUint64(&mm.DeletionByteCounter)
}
func (mm *mapMetric) FileCount() int {
	if mm == nil {
		return 0
	}
	return int(atomic.LoadUint32(&mm.FileCounter))
}
func (mm *mapMetric) DeletedCount() int {
	if mm == nil {
		return 0
	}
	return int(atomic.LoadUint32(&mm.DeletionCounter))
}
func (mm *mapMetric) MaxFileKey() NeedleId {
	if mm == nil {
		return 0
	}
	t := uint64(mm.MaximumFileKey)
	return Uint64ToNeedleId(t)
}
func (mm *mapMetric) MaybeSetMaxFileKey(key NeedleId) {
	if mm == nil {
		return
	}
	if key > mm.MaxFileKey() {
		atomic.StoreUint64(&mm.MaximumFileKey, uint64(key))
	}
}

// MaybeSetMaxNeedleEnd updates MaximumNeedleEnd if the supplied entry's
// (offset + actual size) is larger than what we have seen so far. Skips
// deleted/zero-offset entries because they don't reserve space in .dat.
func (mm *mapMetric) MaybeSetMaxNeedleEnd(offset Offset, size Size, version needle.Version) {
	if mm == nil || offset.IsZero() || !size.IsValid() {
		return
	}
	end := offset.ToActualOffset() + needle.GetActualSize(size, version)
	if end > atomic.LoadInt64(&mm.MaximumNeedleEnd) {
		atomic.StoreInt64(&mm.MaximumNeedleEnd, end)
	}
}

func (mm *mapMetric) MaxNeedleEnd() int64 {
	if mm == nil {
		return 0
	}
	return atomic.LoadInt64(&mm.MaximumNeedleEnd)
}

func needleMapMetricFromIndexFile(r *os.File, mm *mapMetric, version needle.Version) error {
	var bf *boom.BloomFilter
	buf := make([]byte, NeedleIdSize)
	err := reverseWalkIndexFile(r, func(entryCount int64) {
		bf = boom.NewBloomFilter(uint(entryCount), 0.001)
	}, func(key NeedleId, offset Offset, size Size) error {

		mm.MaybeSetMaxFileKey(key)
		mm.MaybeSetMaxNeedleEnd(offset, size, version)
		NeedleIdToBytes(buf, key)
		if size.IsValid() {
			mm.FileByteCounter += uint64(size)
		}

		mm.FileCounter++
		if !bf.TestAndAdd(buf) {
			// if !size.IsValid(), then this file is deleted already
			if !size.IsValid() {
				mm.DeletionCounter++
			}
		} else {
			// deleted file
			mm.DeletionCounter++
			if size.IsValid() {
				// previously already deleted file
				mm.DeletionByteCounter += uint64(size)
			}
		}
		return nil
	})
	return err
}

func newNeedleMapMetricFromIndexFile(r *os.File, version needle.Version) (mm *mapMetric, err error) {
	mm = &mapMetric{}
	err = needleMapMetricFromIndexFile(r, mm, version)
	return
}

func reverseWalkIndexFile(r *os.File, initFn func(entryCount int64), fn func(key NeedleId, offset Offset, size Size) error) error {
	fi, err := r.Stat()
	if err != nil {
		return fmt.Errorf("file %s stat error: %v", r.Name(), err)
	}
	fileSize := fi.Size()
	if fileSize%NeedleMapEntrySize != 0 {
		return fmt.Errorf("unexpected file %s size: %d", r.Name(), fileSize)
	}

	entryCount := fileSize / NeedleMapEntrySize
	initFn(entryCount)

	batchSize := int64(1024 * 4)

	bytes := make([]byte, NeedleMapEntrySize*batchSize)
	nextBatchSize := entryCount % batchSize
	if nextBatchSize == 0 {
		nextBatchSize = batchSize
	}
	remainingCount := entryCount - nextBatchSize

	for remainingCount >= 0 {
		n, e := r.ReadAt(bytes[:NeedleMapEntrySize*nextBatchSize], NeedleMapEntrySize*remainingCount)
		// glog.V(0).Infoln("file", r.Name(), "readerOffset", NeedleMapEntrySize*remainingCount, "count", count, "e", e)
		if e == io.EOF && n == int(NeedleMapEntrySize*nextBatchSize) {
			e = nil
		}
		if e != nil {
			return e
		}
		for i := int(nextBatchSize) - 1; i >= 0; i-- {
			key, offset, size := idx.IdxFileEntry(bytes[i*NeedleMapEntrySize : i*NeedleMapEntrySize+NeedleMapEntrySize])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		nextBatchSize = batchSize
		remainingCount -= nextBatchSize
	}
	return nil
}
