package metastore

import (
	"io/ioutil"
	"os"
	"path"
)

// store data on disk, enough for most cases

type MetaStoreFileBacking struct {
}

func NewMetaStoreFileBacking() MetaStoreFileBacking {
	mms := MetaStoreFileBacking{}
	return mms
}

func (mms MetaStoreFileBacking) Set(val []byte, elem ...string) error {
	return ioutil.WriteFile(path.Join(elem...), val, 0644)
}

func (mms MetaStoreFileBacking) Get(elem ...string) (val []byte, err error) {
	return ioutil.ReadFile(path.Join(elem...))
}

func (mms MetaStoreFileBacking) Has(elem ...string) (ok bool) {
	seqFile, se := os.OpenFile(path.Join(elem...), os.O_RDONLY, 0644)
	if se != nil {
		return false
	}
	defer seqFile.Close()
	return true
}
