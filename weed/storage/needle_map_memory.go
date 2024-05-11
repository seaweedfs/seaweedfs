package storage

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type NeedleMap struct {
	baseNeedleMapper
	m needle_map.NeedleValueMap
}

func NewCompactNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: needle_map.NewCompactMap(),
	}
	nm.indexFile = file
	stat, err := file.Stat()
	if err != nil {
		glog.Fatalf("stat file %s: %v", file.Name(), err)
	}
	nm.indexFileOffset = stat.Size()
	return nm
}

func LoadCompactNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewCompactNeedleMap(file)
	return doLoading(file, nm)
}

func doLoading(file *os.File, nm *NeedleMap) (*NeedleMap, error) {
	e := idx.WalkIndexFile(file, 0, func(key NeedleId, offset Offset, size Size) error {
		nm.MaybeSetMaxFileKey(key)
		if !offset.IsZero() && size.IsValid() {
			nm.FileCounter++
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			oldOffset, oldSize := nm.m.Set(NeedleId(key), offset, size)
			if !oldOffset.IsZero() && oldSize.IsValid() {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(NeedleId(key))
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infof("max file key: %v count: %d deleted: %d for file: %s", nm.MaxFileKey(), nm.FileCount(), nm.DeletedCount(), file.Name())
	return nm, e
}

func (nm *NeedleMap) Put(key NeedleId, offset Offset, size Size) error {
	_, oldSize := nm.m.Set(NeedleId(key), offset, size)
	nm.logPut(key, oldSize, size)
	return nm.appendToIndexFile(key, offset, size)
}
func (nm *NeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	element, ok = nm.m.Get(NeedleId(key))
	return
}
func (nm *NeedleMap) Delete(key NeedleId, offset Offset) error {
	deletedBytes := nm.m.Delete(NeedleId(key))
	nm.logDelete(deletedBytes)
	return nm.appendToIndexFile(key, offset, TombstoneFileSize)
}
func (nm *NeedleMap) Close() {
	if nm.indexFile == nil {
		return
	}
	indexFileName := nm.indexFile.Name()
	if err := nm.indexFile.Sync(); err != nil {
		glog.Warningf("sync file %s failed, %v", indexFileName, err)
	}
	_ = nm.indexFile.Close()
}
func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}

func (nm *NeedleMap) UpdateNeedleMap(v *Volume, indexFile *os.File, opts *opt.Options, ldbTimeout int64) error {
	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	defer func() {
		if v.tmpNm != nil {
			v.tmpNm.Close()
			v.tmpNm = nil
		}
	}()
	nm.indexFile = indexFile
	stat, err := indexFile.Stat()
	if err != nil {
		glog.Fatalf("stat file %s: %v", indexFile.Name(), err)
		return err
	}
	nm.indexFileOffset = stat.Size()
	v.nm = nm
	v.tmpNm = nil
	return nil
}

func (nm *NeedleMap) DoOffsetLoading(v *Volume, indexFile *os.File, startFrom uint64) error {
	glog.V(0).Infof("loading idx from offset %d for file: %s", startFrom, indexFile.Name())
	e := idx.WalkIndexFile(indexFile, startFrom, func(key NeedleId, offset Offset, size Size) error {
		nm.MaybeSetMaxFileKey(key)
		nm.FileCounter++
		if !offset.IsZero() && size.IsValid() {
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			oldOffset, oldSize := nm.m.Set(NeedleId(key), offset, size)
			if !oldOffset.IsZero() && oldSize.IsValid() {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(NeedleId(key))
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})

	return e
}
