package directory

import (
	"gob"
	"os"
	"path"
	"rand"
	"log"
	"storage"
	"strconv"
	"sync"
)

const (
	FileIdSaveInterval = 10000
)

type MachineInfo struct {
	Url       string //<server name/ip>[:port]
	PublicUrl string
}
type Machine struct {
	Server  MachineInfo
	Volumes []storage.VolumeInfo
}

type Mapper struct {
	dir      string
	fileName string

	lock          sync.Mutex
	Machines      []*Machine
	vid2machineId map[uint32]int //machineId is +1 of the index of []*Machine, to detect not found entries
	Writers       []uint32 // transient array of Writers volume id

	FileIdSequence uint64
	fileIdCounter  uint64

	volumeSizeLimit uint64
}

func NewMachine(server, publicUrl string, volumes []storage.VolumeInfo) *Machine {
	return &Machine{Server: MachineInfo{Url: server, PublicUrl: publicUrl}, Volumes: volumes}
}

func NewMapper(dirname string, filename string, volumeSizeLimit uint64) (m *Mapper) {
	m = &Mapper{dir: dirname, fileName: filename}
	m.vid2machineId = make(map[uint32]int)
	m.volumeSizeLimit = volumeSizeLimit
	m.Writers = *new([]uint32)
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
func (m *Mapper) PickForWrite() (string, MachineInfo, os.Error) {
	len_writers := len(m.Writers)
	if len_writers <= 0 {
		log.Println("No more writable volumes!")
		return "", m.Machines[rand.Intn(len(m.Machines))].Server, os.NewError("No more writable volumes!")
	}
	vid := m.Writers[rand.Intn(len_writers)]
	machine_id := m.vid2machineId[vid]
	if machine_id > 0 {
		machine := m.Machines[machine_id-1]
		return NewFileId(vid, m.NextFileId(), rand.Uint32()).String(), machine.Server, nil
	}
	return "", m.Machines[rand.Intn(len(m.Machines))].Server, os.NewError("Strangely vid " + strconv.Uitoa64(uint64(vid)) + " is on no machine!")
}
func (m *Mapper) NextFileId() uint64 {
	if m.fileIdCounter <= 0 {
		m.fileIdCounter = FileIdSaveInterval
		m.FileIdSequence += FileIdSaveInterval
		m.saveSequence()
	}
	m.fileIdCounter--
	return m.FileIdSequence - m.fileIdCounter
}
func (m *Mapper) Get(vid uint32) (*Machine, os.Error) {
	machineId := m.vid2machineId[vid]
	if machineId <= 0 {
		return nil, os.NewError("invalid volume id " + strconv.Uitob64(uint64(vid), 10))
	}
	return m.Machines[machineId-1], nil
}
func (m *Mapper) Add(machine Machine) {
	//check existing machine, linearly
    //log.Println("Adding machine", machine.Server.Url)
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
	} else {
		m.Machines[machineId] = &machine
	}
	m.lock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		m.vid2machineId[v.Id] = machineId + 1 //use base 1 indexed, to detect not found cases
	}
	//setting Writers, copy-on-write because of possible updating, this needs some future work!
	var writers []uint32
	for _, machine_entry := range m.Machines {
		for _, v := range machine_entry.Volumes {
			if uint64(v.Size) < m.volumeSizeLimit {
				writers = append(writers, v.Id)
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
