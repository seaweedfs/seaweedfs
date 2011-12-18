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
	ChunkSizeLimit = 1 * 1024 * 1024 * 1024 //1G, can not be more than max(uint32)*8
)

type MachineInfo struct {
    Server       string //<server name/ip>[:port]
    PublicServer string
}
type Machine struct {
    MachineInfo
	Server       string //<server name/ip>[:port]
	PublicServer string
	Volumes      []storage.VolumeInfo
	Capacity     int
}

type Mapper struct {
	dir      string
	fileName string
	capacity int

	lock          sync.Mutex
	Machines      []*Machine
	vid2machineId map[uint64]int
	Writers       []int // transient array of Writers volume id

	GlobalVolumeSequence uint64
}

func NewMachine(server, publicServer string, volumes []storage.VolumeInfo, capacity int) (m *Machine) {
	m = new(Machine)
	m.Server, m.PublicServer, m.Volumes, m.Capacity = server, publicServer, volumes, capacity
	return
}

func NewMapper(dirname string, filename string, capacity int) (m *Mapper) {
	m = new(Mapper)
	m.dir, m.fileName, m.capacity = dirname, filename, capacity
	log.Println("Loading volume id to maching mapping:", path.Join(m.dir, m.fileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".map"), os.O_RDONLY, 0644)
    m.vid2machineId = make(map[uint64]int)
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
	return
}
func (m *Mapper) PickForWrite() map[string]string {
	vid := rand.Intn(len(m.Writers))
	return map[string]string{
	  "server":m.Machines[m.Writers[vid]].Server,
      "url":m.Machines[m.Writers[vid]].PublicServer,
	}
}
func (m *Mapper) Get(vid uint64) *Machine {
	return m.Machines[m.vid2machineId[vid]]
}
func (m *Mapper) Add(machine Machine) []uint64 {
	log.Println("Adding existing", machine.Server, len(machine.Volumes), "volumes to dir", len(m.Machines))
	log.Println("Adding      new ", machine.Server, machine.Capacity-len(machine.Volumes), "volumes to dir", len(m.Machines))
	//check existing machine, linearly
	m.lock.Lock()
	foundExistingMachineId := -1
	for index, entry := range m.Machines {
		if machine.Server == entry.Server {
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
	vids := new([]uint64)
	for vid, i := m.GlobalVolumeSequence, len(machine.Volumes); i < machine.Capacity; i, vid = i+1, vid+1 {
		newVolume := *new(storage.VolumeInfo)
		newVolume.Id, newVolume.Size = vid, 0
		machine.Volumes = append(machine.Volumes, newVolume)
		m.vid2machineId[vid] = machineId
		log.Println("Adding volume", vid, "from", machine.Server)
		*vids = append(*vids, vid)
		m.GlobalVolumeSequence = vid + 1
	}

	m.Save()
	m.lock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		log.Println("Setting volume", v.Id, "to", machine.Server)
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
