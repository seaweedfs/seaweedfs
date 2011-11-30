package directory

import (
	"gob"
	"os"
	"log"
)

type Mapper struct {
	dir              string
	fileName         string
	Virtual2physical map[uint32][]uint32
}

func NewMapper(dirname string, filename string) (m *Mapper) {
	m = new(Mapper)
	m.dir = dirname
	m.fileName = filename
	log.Println("Loading virtual to physical:", m.dir, "/", m.fileName)
	dataFile, e := os.OpenFile(m.dir+string(os.PathSeparator)+m.fileName+".map", os.O_RDONLY, 0644)
	if e != nil {
		log.Fatalf("Mapping File Read [ERROR] %s\n", e)
	} else {
		m.Virtual2physical = make(map[uint32][]uint32)
		decoder := gob.NewDecoder(dataFile)
		decoder.Decode(m.Virtual2physical)
        dataFile.Close()
	}
	return
}
func (m *Mapper) Get(vid uint32) []uint32 {
	return m.Virtual2physical[vid]
}
func (m *Mapper) Add(vid uint32, pids ...uint32) {
	m.Virtual2physical[vid] = append(m.Virtual2physical[vid], pids...)
}
func (m *Mapper) Save() {
	log.Println("Saving virtual to physical:", m.dir, "/", m.fileName)
	dataFile, e := os.OpenFile(m.dir+string(os.PathSeparator)+m.fileName+".map", os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Mapping File Save [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	m.Virtual2physical = make(map[uint32][]uint32)
	encoder := gob.NewEncoder(dataFile)
	encoder.Encode(m.Virtual2physical)
}
