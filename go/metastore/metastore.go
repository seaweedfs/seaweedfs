package metastore

import (
	"errors"
	"strconv"
)

type MetaStoreBacking interface {
	Get(path string) (string, error)
	Set(path, val string) error
	Has(path string) bool
}

type MetaStore struct {
	MetaStoreBacking
}

func (m *MetaStore) SetUint64(path string, val uint64) error {
	return m.Set(path, strconv.FormatUint(val, 10))
}

func (m *MetaStore) GetUint64(path string) (val uint64, err error) {
	if b, e := m.Get(path); e == nil {
		val, err = strconv.ParseUint(b, 10, 64)
		return
	} else {
		if e != nil {
			return 0, e
		}
		err = errors.New("Not found value for " + path)
	}
	return
}
