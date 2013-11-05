package sequence

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/metastore"
	"encoding/gob"
	"path"
	"sync"
)

const (
	FileIdSaveInterval = 10000
)

type Sequencer interface {
	NextFileId(count int) (uint64, int)
}
type SequencerImpl struct {
	dir      string
	fileName string

	volumeLock   sync.Mutex
	sequenceLock sync.Mutex

	FileIdSequence uint64
	fileIdCounter  uint64

	metaStore *metastore.MetaStore
}

func NewSequencer(dirname string, filename string) (m *SequencerImpl) {
	m = &SequencerImpl{dir: dirname, fileName: filename}
	m.metaStore = &metastore.MetaStore{metastore.NewMetaStoreFileBacking()}

	if !m.metaStore.Has(m.dir, m.fileName+".seq") {
		m.FileIdSequence = FileIdSaveInterval
		glog.V(0).Infoln("Setting file id sequence", m.FileIdSequence)
	} else {
		var err error
		if m.FileIdSequence, err = m.metaStore.GetUint64(m.dir, m.fileName+".seq"); err != nil {
			if data, err := m.metaStore.Get(m.dir, m.fileName+".seq"); err == nil {
				m.FileIdSequence = decode(data)
        glog.V(0).Infoln("Decoding old version of FileIdSequence", m.FileIdSequence)
			} else {
				glog.V(0).Infof("No existing FileIdSequence: %s", err)
			}
		} else {
			glog.V(0).Infoln("Loading file id sequence", m.FileIdSequence)
		}
		//in case the server stops between intervals
	}
	return
}

//count should be 1 or more
func (m *SequencerImpl) NextFileId(count int) (uint64, int) {
	if count <= 0 {
		return 0, 0
	}
	m.sequenceLock.Lock()
	defer m.sequenceLock.Unlock()
	if m.fileIdCounter < uint64(count) {
		m.fileIdCounter = FileIdSaveInterval
		m.FileIdSequence += FileIdSaveInterval
		m.saveSequence()
	}
	m.fileIdCounter = m.fileIdCounter - uint64(count)
	return m.FileIdSequence - m.fileIdCounter - uint64(count), count
}
func (m *SequencerImpl) saveSequence() {
	glog.V(0).Infoln("Saving file id sequence", m.FileIdSequence, "to", path.Join(m.dir, m.fileName+".seq"))
	if e := m.metaStore.SetUint64(m.FileIdSequence, m.dir, m.fileName+".seq"); e != nil {
		glog.Fatalf("Sequence id Save [ERROR] %s", e)
	}
}

//decode are for backward compatible purpose
func decode(input []byte) uint64 {
	var x uint64
	b := bytes.NewReader(input)
	decoder := gob.NewDecoder(b)
	if e := decoder.Decode(&x); e == nil {
		return x
	}
	return 0
}
