package directory

import (
	"gob"
	"os"
	"path"
	"rand"
	"log"
	"storage"
)

type Machine struct {
	Server       string //<server name/ip>[:port]
	PublicServer string
}
type Mapper struct {
    dir              string
    FileName         string
    Id2Machine map[uint32][]*Machine
    LastId uint32
}

func NewMachine(server, publicServer string) (m *Machine) {
	m = new(Machine)
	m.Server, m.PublicServer = server, publicServer
	return
}

func NewMapper(dirname string, filename string) (m *Mapper) {
	m = new(Mapper)
	m.dir = dirname
	m.FileName = filename
	log.Println("Loading virtual to physical:", path.Join(m.dir,m.FileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir,m.FileName+".map"), os.O_RDONLY, 0644)
	m.Id2Machine = make(map[uint32][]*Machine)
	if e != nil {
		log.Println("Mapping File Read", e)
	} else {
		decoder := gob.NewDecoder(dataFile)
        decoder.Decode(m.LastId)
		decoder.Decode(m.Id2Machine)
		dataFile.Close()
	}
	return
}
func (m *Mapper) PickForWrite() []*Machine {
	vid := uint32(rand.Intn(len(m.Id2Machine)))
	return m.Id2Machine[vid]
}
func (m *Mapper) Get(vid uint32) []*Machine {
	return m.Id2Machine[vid]
}
func (m *Mapper) Add(machine *Machine, volumes []storage.VolumeStat) {
    log.Println("Adding store node", machine.Server)
	for _, v := range volumes {
		existing := m.Id2Machine[uint32(v.Id)]
		found := false
		for _, entry := range existing {
			if machine == entry {
				found = true
				break
			}
		}
		if !found {
			m.Id2Machine[uint32(v.Id)] = append(existing, machine)
		}
        log.Println(v.Id, "=>", machine.Server)
	}
	m.Save()
}
func (m *Mapper) Save() {
	log.Println("Saving virtual to physical:", path.Join(m.dir,m.FileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir,m.FileName+".map"), os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Mapping File Save [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	m.Id2Machine = make(map[uint32][]*Machine)
	encoder := gob.NewEncoder(dataFile)
    encoder.Encode(m.LastId)
	encoder.Encode(m.Id2Machine)
}
