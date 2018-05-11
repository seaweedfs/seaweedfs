package embedded

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type EmbeddedStore struct {
	db *leveldb.DB
}

func NewEmbeddedStore(dir string) (filer *EmbeddedStore, err error) {
	filer = &EmbeddedStore{}
	if filer.db, err = leveldb.OpenFile(dir, nil); err != nil {
		return
	}
	return
}

func (filer *EmbeddedStore) CreateFile(filePath string, fid string) (err error) {
	return nil
}

func (filer *EmbeddedStore) Mkdir(filePath string) (err error) {
	return nil
}
