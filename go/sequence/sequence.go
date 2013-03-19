package sequence

import (
	"encoding/gob"
	"log"
	"os"
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
}

func NewSequencer(dirname string, filename string) (m *SequencerImpl) {
	m = &SequencerImpl{dir: dirname, fileName: filename}

	seqFile, se := os.OpenFile(path.Join(m.dir, m.fileName+".seq"), os.O_RDONLY, 0644)
	if se != nil {
		m.FileIdSequence = FileIdSaveInterval
		log.Println("Setting file id sequence", m.FileIdSequence)
	} else {
		decoder := gob.NewDecoder(seqFile)
		defer seqFile.Close()
		if se = decoder.Decode(&m.FileIdSequence); se != nil {
			log.Printf("error decoding FileIdSequence: %s", se)
			m.FileIdSequence = FileIdSaveInterval
			log.Println("Setting file id sequence", m.FileIdSequence)
		} else {
			log.Println("Loading file id sequence", m.FileIdSequence, "=>", m.FileIdSequence+FileIdSaveInterval)
			m.FileIdSequence += FileIdSaveInterval
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
	log.Println("Saving file id sequence", m.FileIdSequence, "to", path.Join(m.dir, m.fileName+".seq"))
	seqFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".seq"), os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Sequence File Save [ERROR] %s\n", e)
	}
	defer seqFile.Close()
	encoder := gob.NewEncoder(seqFile)
	if e = encoder.Encode(m.FileIdSequence); e != nil {
		log.Fatalf("Sequence File Save [ERROR] %s\n", e)
	}
}
