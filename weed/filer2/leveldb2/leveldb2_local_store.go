package leveldb

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &LevelDB2Store{})
}

var (
	_ = filer2.FilerLocalStore(&LevelDB2Store{})
)

func (store *LevelDB2Store) UpdateOffset(filer string, lastTsNs int64) error {

	value := make([]byte, 8)
	util.Uint64toBytes(value, uint64(lastTsNs))

	err := store.dbs[0].Put([]byte("meta"+filer), value, nil)

	if err != nil {
		return fmt.Errorf("UpdateOffset %s : %v", filer, err)
	}

	println("UpdateOffset", filer, "lastTsNs", lastTsNs)

	return nil
}

func (store *LevelDB2Store) ReadOffset(filer string) (lastTsNs int64, err error) {

	value, err := store.dbs[0].Get([]byte("meta"+filer), nil)

	if err != nil {
		return 0, fmt.Errorf("ReadOffset %s : %v", filer, err)
	}

	lastTsNs = int64(util.BytesToUint64(value))

	println("ReadOffset", filer, "lastTsNs", lastTsNs)

	return
}
