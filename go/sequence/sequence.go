package sequence

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/metastore"
	"encoding/gob"
	"sync"
)

const (
	FileIdSaveInterval = 10000
)

type Sequencer interface {
	NextFileId(count int) (uint64, int)
}
type SequencerImpl struct {
	fileFullPath string

	volumeLock   sync.Mutex
	sequenceLock sync.Mutex

	FileIdSequence uint64
	fileIdCounter  uint64

	metaStore *metastore.MetaStore
}

func NewFileSequencer(filepath string) (m *SequencerImpl) {
	m = &SequencerImpl{fileFullPath: filepath}
	m.metaStore = &metastore.MetaStore{metastore.NewMetaStoreFileBacking()}
	m.initilize()
	return
}

//func NewEtcdSequencer(etcdCluster string) (m *SequencerImpl) {
//	m = &SequencerImpl{fileFullPath: "/weedfs/default/sequence"}
//	m.metaStore = &metastore.MetaStore{metastore.NewMetaStoreEtcdBacking(etcdCluster)}
//	m.initilize()
//	return
//}

func (m *SequencerImpl) initilize() {
	if !m.metaStore.Has(m.fileFullPath) {
		m.FileIdSequence = FileIdSaveInterval
		glog.V(0).Infoln("Setting file id sequence", m.FileIdSequence)
	} else {
		var err error
		if m.FileIdSequence, err = m.metaStore.GetUint64(m.fileFullPath); err != nil {
			if data, err := m.metaStore.Get(m.fileFullPath); err == nil {
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
	glog.V(0).Infoln("Saving file id sequence", m.FileIdSequence, "to", m.fileFullPath)
	if e := m.metaStore.SetUint64(m.fileFullPath, m.FileIdSequence); e != nil {
		glog.Fatalf("Sequence id Save [ERROR] %s", e)
	}
}

//decode are for backward compatible purpose
func decode(input string) uint64 {
	var x uint64
	b := bytes.NewReader([]byte(input))
	decoder := gob.NewDecoder(b)
	if e := decoder.Decode(&x); e == nil {
		return x
	}
	return 0
}
