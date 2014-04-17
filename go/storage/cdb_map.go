package storage

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"os"
	"path/filepath"
)

// CDB-backed read-only needle map
type cdbMap struct {
	c1, c2   *cdb.Cdb
	fn1, fn2 string
	mapMetric
}

const maxCdbRecCount = 100000000

var errReadOnly = errors.New("cannot modify a read-only map")

// opens the cdb file(s) (base.cdb OR base.1.cdb AND base.2.cdb)
// in case of two files, the metric (at key 'M') must be in base.2.cdb
func OpenCdbMap(fileName string) (m *cdbMap, err error) {
	m = new(cdbMap)
	if m.c1, err = cdb.Open(fileName); err == nil {
		m.fn1 = fileName
		err = getMetric(m.c1, &m.mapMetric)
		return
	}
	if os.IsNotExist(err) {
		bn, ext := baseFilename(fileName)
		m.fn1 = bn + ".1" + ext
		if m.c1, err = cdb.Open(m.fn1); err != nil {
			return nil, err
		}
		m.fn2 = bn + ".2" + ext
		if m.c2, err = cdb.Open(m.fn2); err != nil {
			return nil, err
		}
		err = getMetric(m.c2, &m.mapMetric)
		return
	}
	return nil, err
}

func (m *cdbMap) Put(key uint64, offset uint32, size uint32) (int, error) {
	return -1, errReadOnly
}
func (m *cdbMap) Delete(key uint64) error {
	return errReadOnly
}

func (m *cdbMap) Close() {
	if m.c2 != nil {
		m.c2.Close()
		m.c2 = nil
	}
	if m.c1 != nil {
		m.c1.Close()
		m.c1 = nil
	}
}

func (m *cdbMap) Destroy() error {
	return errors.New("Can not delete readonly volumes")
}

func (m cdbMap) ContentSize() uint64 {
	return m.FileByteCounter
}
func (m cdbMap) DeletedSize() uint64 {
	return m.DeletionByteCounter
}
func (m cdbMap) FileCount() int {
	return m.FileCounter
}
func (m *cdbMap) DeletedCount() int {
	return m.DeletionCounter
}
func (m *cdbMap) MaxFileKey() uint64 {
	return m.MaximumFileKey
}

func getMetric(c *cdb.Cdb, m *mapMetric) error {
	data, err := c.Data([]byte{'M'})
	if err != nil {
		return err
	}
	return json.Unmarshal(data, m)
}

func (m cdbMap) Get(key uint64) (element *NeedleValue, ok bool) {
	var (
		data []byte
		k    []byte = make([]byte, 8)
		err  error
	)
	util.Uint64toBytes(k, key)
	if data, err = m.c1.Data(k); err != nil || data == nil {
		if m.c2 == nil {
			return nil, false
		}
		if data, err = m.c2.Data(k); err != nil || data == nil {
			return nil, false
		}
	}
	return &NeedleValue{Key: Key(key), Offset: util.BytesToUint32(data[:4]),
		Size: util.BytesToUint32(data[4:])}, true
}

func (m cdbMap) Visit(visit func(NeedleValue) error) (err error) {
	fh, err := os.Open(m.fn1)
	if err != nil {
		return fmt.Errorf("cannot open %s: %s", m.fn1, err)
	}
	defer fh.Close()
	walk := func(elt cdb.Element) error {
		if len(elt.Key) != 8 {
			return nil
		}
		return visit(NeedleValue{Key: Key(util.BytesToUint64(elt.Key)),
			Offset: util.BytesToUint32(elt.Data[:4]),
			Size:   util.BytesToUint32(elt.Data[4:8])})
	}
	if err = cdb.DumpMap(fh, walk); err != nil {
		return err
	}
	if m.c2 == nil {
		return nil
	}
	fh.Close()
	if fh, err = os.Open(m.fn2); err != nil {
		return fmt.Errorf("cannot open %s: %s", m.fn2, err)
	}
	return cdb.DumpMap(fh, walk)
}

// converts an .idx index to a cdb
func ConvertIndexToCdb(cdbName string, index *os.File) error {
	idx, err := LoadNeedleMap(index)
	if err != nil {
		return fmt.Errorf("error loading needle map %s: %s", index.Name(), err)
	}
	defer idx.Close()
	return DumpNeedleMapToCdb(cdbName, idx)
}

// dumps a NeedleMap into a cdb
func DumpNeedleMapToCdb(cdbName string, nm *NeedleMap) error {
	tempnam := cdbName + "t"
	fnames := make([]string, 1, 2)
	adder, closer, err := openTempCdb(tempnam)
	if err != nil {
		return fmt.Errorf("error creating factory: %s", err)
	}
	fnames[0] = tempnam

	elt := cdb.Element{Key: make([]byte, 8), Data: make([]byte, 8)}

	fcount := uint64(0)
	walk := func(key uint64, offset, size uint32) error {
		if fcount >= maxCdbRecCount {
			if err = closer(); err != nil {
				return err
			}
			tempnam = cdbName + "t2"
			if adder, closer, err = openTempCdb(tempnam); err != nil {
				return fmt.Errorf("error creating second factory: %s", err)
			}
			fnames = append(fnames, tempnam)
			fcount = 0
		}
		util.Uint64toBytes(elt.Key, key)
		util.Uint32toBytes(elt.Data[:4], offset)
		util.Uint32toBytes(elt.Data[4:], size)
		fcount++
		return adder(elt)
	}
	// and write out the cdb from there
	err = nm.Visit(func(nv NeedleValue) error {
		return walk(uint64(nv.Key), nv.Offset, nv.Size)
	})
	if err != nil {
		closer()
		return fmt.Errorf("error walking index %v: %s", nm, err)
	}
	// store fileBytes
	data, e := json.Marshal(nm.mapMetric)
	if e != nil {
		return fmt.Errorf("error marshaling metric %v: %s", nm.mapMetric, e)
	}
	if err = adder(cdb.Element{Key: []byte{'M'}, Data: data}); err != nil {
		return err
	}
	if err = closer(); err != nil {
		return err
	}

	os.Remove(cdbName)
	if len(fnames) == 1 {
		return os.Rename(fnames[0], cdbName)
	}
	bn, ext := baseFilename(cdbName)
	if err = os.Rename(fnames[0], bn+".1"+ext); err != nil {
		return err
	}
	return os.Rename(fnames[1], bn+".2"+ext)
}

func openTempCdb(fileName string) (cdb.AdderFunc, cdb.CloserFunc, error) {
	fh, err := os.Create(fileName)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create cdb file %s: %s", fileName, err.Error())
	}
	adder, closer, err := cdb.MakeFactory(fh)
	if err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("error creating factory: %s", err.Error())
	}
	return adder, func() error {
		if e := closer(); e != nil {
			fh.Close()
			return e
		}
		fh.Close()
		return nil
	}, nil
}

// returns filename without extension, and the extension
func baseFilename(fileName string) (string, string) {
	ext := filepath.Ext(fileName)
	return fileName[:len(fileName)-len(ext)], ext
}
