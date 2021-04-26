package needle_map

import (
	"fmt"
	"io"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

//This map uses in memory level db
type MemDb struct {
	db *leveldb.DB
}

func NewMemDb() *MemDb {
	opts := &opt.Options{}

	var err error
	t := &MemDb{}
	if t.db, err = leveldb.Open(storage.NewMemStorage(), opts); err != nil {
		glog.V(0).Infof("MemDb fails to open: %v", err)
		return nil
	}

	return t
}

func (cm *MemDb) Set(key NeedleId, offset Offset, size Size) error {

	bytes := ToBytes(key, offset, size)

	if err := cm.db.Put(bytes[0:NeedleIdSize], bytes[NeedleIdSize:NeedleIdSize+OffsetSize+SizeSize], nil); err != nil {
		return fmt.Errorf("failed to write temp leveldb: %v", err)
	}
	return nil
}

func (cm *MemDb) Delete(key NeedleId) error {
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes, key)
	return cm.db.Delete(bytes, nil)

}
func (cm *MemDb) Get(key NeedleId) (*NeedleValue, bool) {
	bytes := make([]byte, NeedleIdSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	data, err := cm.db.Get(bytes, nil)
	if err != nil || len(data) != OffsetSize+SizeSize {
		return nil, false
	}
	offset := BytesToOffset(data[0:OffsetSize])
	size := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])
	return &NeedleValue{Key: key, Offset: offset, Size: size}, true
}

// Visit visits all entries or stop if any error when visiting
func (cm *MemDb) AscendingVisit(visit func(NeedleValue) error) (ret error) {
	iter := cm.db.NewIterator(nil, nil)
	for iter.Next() {
		key := BytesToNeedleId(iter.Key())
		data := iter.Value()
		offset := BytesToOffset(data[0:OffsetSize])
		size := BytesToSize(data[OffsetSize : OffsetSize+SizeSize])

		needle := NeedleValue{Key: key, Offset: offset, Size: size}
		ret = visit(needle)
		if ret != nil {
			return
		}
	}
	iter.Release()
	ret = iter.Error()

	return
}

func (cm *MemDb) SaveToIdx(idxName string) (ret error) {
	idxFile, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer idxFile.Close()

	return cm.AscendingVisit(func(value NeedleValue) error {
		if value.Offset.IsZero() || value.Size.IsDeleted() {
			return nil
		}
		_, err := idxFile.Write(value.ToBytes())
		return err
	})

}

func (cm *MemDb) LoadFromIdx(idxName string) (ret error) {
	idxFile, err := os.OpenFile(idxName, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer idxFile.Close()

	return cm.LoadFromReaderAt(idxFile)

}

func (cm *MemDb) LoadFromReaderAt(readerAt io.ReaderAt) (ret error) {

	return idx.WalkIndexFile(readerAt, func(key NeedleId, offset Offset, size Size) error {
		if offset.IsZero() || size.IsDeleted() {
			return cm.Delete(key)
		}
		return cm.Set(key, offset, size)
	})

}

func (cm *MemDb) Close() {
	cm.db.Close()
}
