package metastore

import (
	"io/ioutil"
	"os"
)

// store data on disk, enough for most cases

type MetaStoreFileBacking struct {
}

func NewMetaStoreFileBacking() *MetaStoreFileBacking {
	mms := &MetaStoreFileBacking{}
	return mms
}

func (mms *MetaStoreFileBacking) Set(path, val string) error {
	return ioutil.WriteFile(path, []byte(val), 0644)
}

func (mms *MetaStoreFileBacking) Get(path string) (string, error) {
	val, e := ioutil.ReadFile(path)
	return string(val), e
}

func (mms *MetaStoreFileBacking) Has(path string) (ok bool) {
	seqFile, se := os.OpenFile(path, os.O_RDONLY, 0644)
	if se != nil {
		return false
	}
	defer seqFile.Close()
	return true
}
