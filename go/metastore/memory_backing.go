package metastore

import (
	"fmt"
	"path"
)

//this is for testing only

type MetaStoreMemoryBacking struct {
	m map[string][]byte
}

func NewMetaStoreMemoryBacking() MetaStoreMemoryBacking {
	mms := MetaStoreMemoryBacking{}
	mms.m = make(map[string][]byte)
	return mms
}

func (mms MetaStoreMemoryBacking) Set(val []byte, elem ...string) error {
	mms.m[path.Join(elem...)] = val
	return nil
}

func (mms MetaStoreMemoryBacking) Get(elem ...string) (val []byte, err error) {
	var ok bool
	val, ok = mms.m[path.Join(elem...)]
	if !ok {
		return nil, fmt.Errorf("Missing value for %s", path.Join(elem...))
	}
	return
}

func (mms MetaStoreMemoryBacking) Has(elem ...string) (ok bool) {
	_, ok = mms.m[path.Join(elem...)]
	return
}
