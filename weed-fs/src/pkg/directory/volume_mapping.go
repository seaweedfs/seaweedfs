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
	Capacity int
}

type Mapper struct {
	dir      string
	fileName string
	capacity int

	lock          sync.Mutex
	Machines      []*Machine
	vid2machineId map[uint32]int
	Writers       []int // transient array of Writers volume id

	GlobalVolumeSequence uint32

	FileIdSequence uint64
	fileIdCounter  uint64
}

func NewMachine(server, publicServer string, volumes []storage.VolumeInfo, capacity int) *Machine {
	return &Machine{Server: MachineInfo{Url: server, PublicUrl: publicServer}, Volumes: volumes, Capacity: capacity}
}

func NewMapper(dirname string, filename string, capacity int) (m *Mapper) {
	m = &Mapper{dir: dirname, fileName: filename, capacity: capacity}
	log.Println("Loading volume id to maching mapping:", path.Join(m.dir, m.fileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".map"), os.O_RDONLY, 0644)
	m.vid2machineId = make(map[uint32]int)
	m.Writers = *new([]int)
	if e != nil {
		log.Println("Mapping File Read", e)
		m.Machines = *new([]*Machine)
	} else {
		decoder := gob.NewDecoder(dataFile)
		defer dataFile.Close()
		decoder.Decode(&m.Machines)
		decoder.Decode(&m.GlobalVolumeSequence)

		//add to vid2machineId map, and Writers array
		for machine_index, machine := range m.Machines {
			for _, v := range machine.Volumes {
				m.vid2machineId[v.Id] = machine_index
				if v.Size < ChunkSizeLimit {
					m.Writers = append(m.Writers, machine_index)
				}
			}
		}
		log.Println("Loaded mapping size", len(m.Machines))
	}

	seqFile, se := os.OpenFile(path.Join(m.dir, m.fileName+".seq"), os.O_RDONLY, 0644)
	if se != nil {
		m.FileIdSequence = FileIdSaveInterval
        log.Println("Setting file id sequence", m.FileIdSequence)
	} else {
		decoder := gob.NewDecoder(seqFile)
		defer seqFile.Close()
		decoder.Decode(&m.FileIdSequence)
        log.Println("Loading file id sequence", m.FileIdSequence, "=>", m.FileIdSequence + FileIdSaveInterval)
		//in case the server stops between intervals
		m.FileIdSequence += FileIdSaveInterval
	}
	return
}
func (m *Mapper) PickForWrite() (string, MachineInfo) {
	machine := m.Machines[m.Writers[rand.Intn(len(m.Writers))]]
	vid := machine.Volumes[rand.Intn(len(machine.Volumes))].Id
	return NewFileId(vid,m.NextFileId(),rand.Uint32()).String(), machine.Server
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
func (m *Mapper) Add(machine Machine) []uint32 {
	log.Println("Adding existing", machine.Server.Url, len(machine.Volumes), "volumes to dir", len(m.Machines))
	log.Println("Adding      new ", machine.Server.Url, machine.Capacity-len(machine.Volumes), "volumes to dir", len(m.Machines))
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
	}

	//generate new volumes
	vids := new([]uint32)
	for vid, i := m.GlobalVolumeSequence, len(machine.Volumes); i < machine.Capacity; i, vid = i+1, vid+1 {
		newVolume := storage.VolumeInfo{Id: vid, Size: 0}
		machine.Volumes = append(machine.Volumes, newVolume)
		m.vid2machineId[vid] = machineId
		log.Println("Adding volume", vid, "from", machine.Server.Url)
		*vids = append(*vids, vid)
		m.GlobalVolumeSequence = vid + 1
	}

	m.Save()
	m.lock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		log.Println("Setting volume", v.Id, "to", machine.Server.Url)
		m.vid2machineId[v.Id] = machineId
		if v.Size < ChunkSizeLimit {
			m.Writers = append(m.Writers, machineId)
		}
	}
	//setting Writers, copy-on-write because of possible updating
	var Writers []int
	for machine_index, machine_entry := range m.Machines {
		for _, v := range machine_entry.Volumes {
			if v.Size < ChunkSizeLimit {
				Writers = append(Writers, machine_index)
			}
		}
	}
	m.Writers = Writers

	log.Println("Machines:", len(m.Machines), "Volumes:", len(m.vid2machineId), "Writable:", len(m.Writers))
	return *vids
}
func (m *Mapper) Save() {
	log.Println("Saving virtual to physical:", path.Join(m.dir, m.fileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".map"), os.O_CREATE|os.O_WRONLY, 0644)
	if e != nil {
		log.Fatalf("Mapping File Save [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	encoder := gob.NewEncoder(dataFile)
	encoder.Encode(m.Machines)
	encoder.Encode(m.GlobalVolumeSequence)
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
