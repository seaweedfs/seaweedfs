package directory

import (
	"gob"
	"os"
	"path"
	"rand"
	"log"
	"storage"
	"sync"
)

const (
	ChunkSizeLimit     = 1 * 1000 * 1000 * 1000 //1G, can not be more than max(uint32)*8
	FileIdSaveInterval = 10000
)

type MachineInfo struct {
	Url       string //<server name/ip>[:port]
	PublicUrl string
}
type Machine struct {
	Server   MachineInfo
	Volumes  []storage.VolumeInfo
}

type Mapper struct {
	dir      string
	fileName string

	lock          sync.Mutex
	Machines      []*Machine
	vid2machineId map[uint32]int
	Writers       []int // transient array of Writers volume id

	FileIdSequence uint64
	fileIdCounter  uint64
}

func NewMachine(server, publicUrl string, volumes []storage.VolumeInfo) *Machine {
	return &Machine{Server: MachineInfo{Url: server, PublicUrl: publicUrl}, Volumes: volumes}
}

func NewMapper(dirname string, filename string) (m *Mapper) {
	m = &Mapper{dir: dirname, fileName: filename}
	m.vid2machineId = make(map[uint32]int)
	m.Writers = *new([]int)
	m.Machines = *new([]*Machine)

	seqFile, se := os.OpenFile(path.Join(m.dir, m.fileName+".seq"), os.O_RDONLY, 0644)
	if se != nil {
		m.FileIdSequence = FileIdSaveInterval
		log.Println("Setting file id sequence", m.FileIdSequence)
	} else {
		decoder := gob.NewDecoder(seqFile)
		defer seqFile.Close()
		decoder.Decode(&m.FileIdSequence)
		log.Println("Loading file id sequence", m.FileIdSequence, "=>", m.FileIdSequence+FileIdSaveInterval)
		//in case the server stops between intervals
		m.FileIdSequence += FileIdSaveInterval
	}
	return
}
func (m *Mapper) PickForWrite() (string, MachineInfo) {
	machine := m.Machines[m.Writers[rand.Intn(len(m.Writers))]]
	vid := machine.Volumes[rand.Intn(len(machine.Volumes))].Id
	return NewFileId(vid, m.NextFileId(), rand.Uint32()).String(), machine.Server
}
func (m *Mapper) NextFileId() uint64 {
	if m.fileIdCounter <= 0 {
		m.fileIdCounter = FileIdSaveInterval
		m.saveSequence()
	}
	m.fileIdCounter--
	return m.FileIdSequence - m.fileIdCounter
}
func (m *Mapper) Get(vid uint32) *Machine {
	return m.Machines[m.vid2machineId[vid]]
}
func (m *Mapper) Add(machine Machine){
	//check existing machine, linearly
	m.lock.Lock()
	foundExistingMachineId := -1
	for index, entry := range m.Machines {
		if machine.Server.Url == entry.Server.Url {
			foundExistingMachineId = index
			break
		}
	}
	machineId := foundExistingMachineId
	if machineId < 0 {
		machineId = len(m.Machines)
		m.Machines = append(m.Machines, &machine)
	}else{
	    m.Machines[machineId] = &machine
	}
	m.lock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		//log.Println("Setting volume", v.Id, "to", machine.Server.Url)
		m.vid2machineId[v.Id] = machineId
	}
	//setting Writers, copy-on-write because of possible updating
	var writers []int
	for machine_index, machine_entry := range m.Machines {
		for _, v := range machine_entry.Volumes {
			if v.Size < ChunkSizeLimit {
				writers = append(writers, machine_index)
			}
		}
	}
	m.Writers = writers
}
func (m *Mapper) saveSequence() {
	log.Println("Saving file id sequence", m.FileIdSequence, "to", path.Join(m.dir, m.fileName+".seq"))
	seqFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".seq"), os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Sequence File Save [ERROR] %s\n", e)
	}
	defer seqFile.Close()
	encoder := gob.NewEncoder(seqFile)
	encoder.Encode(m.FileIdSequence)
}
