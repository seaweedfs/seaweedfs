package directory

import (
	"gob"
	"os"
	"rand"
	"log"
	"store"
)

type Machine struct {
	Server       string //<server name/ip>[:port]
	PublicServer string
}

func NewMachine(server, publicServer string) (m *Machine) {
	m = new(Machine)
	m.Server, m.PublicServer = server, publicServer
	return
}

type Mapper struct {
	dir              string
	fileName         string
	Virtual2physical map[uint32][]*Machine
}

func NewMapper(dirname string, filename string) (m *Mapper) {
	m = new(Mapper)
	m.dir = dirname
	m.fileName = filename
	log.Println("Loading virtual to physical:", m.dir, "/", m.fileName)
	dataFile, e := os.OpenFile(m.dir+string(os.PathSeparator)+m.fileName+".map", os.O_RDONLY, 0644)
	m.Virtual2physical = make(map[uint32][]*Machine)
	if e != nil {
		log.Println("Mapping File Read [ERROR]", e)
	} else {
		decoder := gob.NewDecoder(dataFile)
		decoder.Decode(m.Virtual2physical)
		dataFile.Close()
	}
	return
}
func (m *Mapper) PickForWrite() []*Machine {
	vid := uint32(rand.Intn(len(m.Virtual2physical)))
	return m.Virtual2physical[vid]
}
func (m *Mapper) Get(vid uint32) []*Machine {
	return m.Virtual2physical[vid]
}
func (m *Mapper) Add(machine *Machine, volumes []store.VolumeStat) {
	for _, v := range volumes {
		existing := m.Virtual2physical[uint32(v.Id)]
		found := false
		for _, entry := range existing {
			if machine == entry {
				found = true
				break
			}
		}
		if !found {
			m.Virtual2physical[uint32(v.Id)] = append(existing, machine)
		}
        log.Println(v.Id, "=>", machine.Server)
	}
}
func (m *Mapper) Save() {
	log.Println("Saving virtual to physical:", m.dir, "/", m.fileName)
	dataFile, e := os.OpenFile(m.dir+string(os.PathSeparator)+m.fileName+".map", os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Mapping File Save [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	m.Virtual2physical = make(map[uint32][]*Machine)
	encoder := gob.NewEncoder(dataFile)
	encoder.Encode(m.Virtual2physical)
}
