package storage

import (
	"github.com/tgulacsi/go-cdb"
	"io"
	"log"
	"os"
	"pkg/util"
	"strings"
)

type CdbMap struct {
	db        *cdb.Cdb
	transient []byte
	Filename  string
}

// Opens the CDB file and servers as a needle map
func NewCdbMap(filename string) (*CdbMap, error) {
	m, err := cdb.Open(filename)
	if err != nil {
		return nil, err
	}
	return &CdbMap{db: m, transient: make([]byte, 8),
		Filename: filename}, nil
}

// writes the content of the index file to a CDB and returns that
func NewCdbMapFromIndex(indexFile *os.File) (*CdbMap, error) {
	nm := indexFile.Name()
	nm = nm[strings.LastIndex(nm, ".")+1:] + "cdb"

	var (
		key    uint64
		offset uint32
		ok     bool
	)
	deleted := make(map[uint64]bool, 16)
	gatherDeletes := func(buf []byte) error {
		key = util.BytesToUint64(buf[:8])
		offset = util.BytesToUint32(buf[8:12])
		if offset > 0 {
			if _, ok = deleted[key]; ok { //undelete
				delete(deleted, key)
			}
		} else {
			deleted[key] = true
		}
		return nil
	}
	if err := readIndexFile(indexFile, gatherDeletes); err != nil {
		return nil, err
	}

	w, err := cdb.NewWriter(nm)
	if err != nil {
		return nil, err
	}
	iterFun := func(buf []byte) error {
		key = util.BytesToUint64(buf[:8])
		if _, ok = deleted[key]; !ok {
			w.PutPair(buf[:8], buf[8:16])
		}
		return nil
	}
	indexFile.Seek(0, 0)
	err = readIndexFile(indexFile, iterFun)
	w.Close()
	if err != nil {
		return nil, err
	}

	return NewCdbMap(nm)
}

func (m *CdbMap) Get(key Key) (element *NeedleValue, ok bool) {
	util.Uint64toBytes(m.transient, uint64(key))
	data, err := m.db.Data(m.transient)
	if err != nil {
		if err == io.EOF {
			return nil, false
		}
		log.Printf("error getting %s: %s", key, err)
		return nil, false
	}
	return &NeedleValue{Key: key,
		Offset: util.BytesToUint32(data[:4]),
		Size:   util.BytesToUint32(data[4:8]),
	}, true
}

func (m *CdbMap) Walk(pedestrian func(*NeedleValue) error) (err error) {
	r, err := os.Open(m.Filename)
	if err != nil {
		return err
	}
	defer r.Close()

	iterFunc := func(elt cdb.Element) error {
		return pedestrian(&NeedleValue{
			Key:    Key(util.BytesToUint64(elt.Key[:8])),
			Offset: util.BytesToUint32(elt.Data[:4]),
			Size:   util.BytesToUint32(elt.Data[4:8]),
		})
	}
	return cdb.DumpMap(r, iterFunc)
}
