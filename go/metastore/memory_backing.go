package metastore

import (
	"fmt"
)

//this is for testing only

type MetaStoreMemoryBacking struct {
	m map[string]string
}

func NewMetaStoreMemoryBacking() *MetaStoreMemoryBacking {
	mms := &MetaStoreMemoryBacking{}
	mms.m = make(map[string]string)
	return mms
}

func (mms MetaStoreMemoryBacking) Set(path, val string) error {
	mms.m[path] = val
	return nil
}

func (mms MetaStoreMemoryBacking) Get(path string) (val string, err error) {
	var ok bool
	val, ok = mms.m[path]
	if !ok {
		return "", fmt.Errorf("Missing value for %s", path)
	}
	return
}

func (mms MetaStoreMemoryBacking) Has(path string) (ok bool) {
	_, ok = mms.m[path]
	return
}
