package metastore

import (
	"code.google.com/p/weed-fs/go/util"
	"errors"
	"path"
)

type MetaStoreBacking interface {
	Get(elem ...string) ([]byte, error)
	Set(val []byte, elem ...string) error
  Has(elem ...string) bool
}

type MetaStore struct {
	MetaStoreBacking
}

func (m *MetaStore) SetUint64(val uint64, elem ...string) error {
	b := make([]byte, 8)
	util.Uint64toBytes(b, val)
	return m.Set(b, elem...)
}

func (m *MetaStore) GetUint64(elem ...string) (val uint64, err error) {
	if b, e := m.Get(elem...); e == nil && len(b) == 8 {
		val = util.BytesToUint64(b)
	} else {
		if e != nil {
			return 0, e
		}
		err = errors.New("Not found value for " + path.Join(elem...))
	}
	return
}
