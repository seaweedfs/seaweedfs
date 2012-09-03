package directory

import (
	"errors"
	"log"
	"math/rand"
	"pkg/sequence"
	"pkg/storage"
	"pkg/util"
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
	volumeLock    sync.Mutex
	Machines      []*Machine
	vid2machineId map[storage.VolumeId]int //machineId is +1 of the index of []*Machine, to detect not found entries
	Writers       []storage.VolumeId       // transient array of Writers volume id

	volumeSizeLimit uint64

	sequence sequence.Sequencer
}

func NewMachine(server, publicUrl string, volumes []storage.VolumeInfo) *Machine {
	return &Machine{Server: MachineInfo{Url: server, PublicUrl: publicUrl}, Volumes: volumes}
}

func NewMapper(dirname string, filename string, volumeSizeLimit uint64) (m *Mapper) {
	m = &Mapper{}
	m.vid2machineId = make(map[storage.VolumeId]int)
	m.volumeSizeLimit = volumeSizeLimit
	m.Writers = *new([]storage.VolumeId)
	m.Machines = *new([]*Machine)

	m.sequence = sequence.NewSequencer(dirname, filename)

	return
}
func (m *Mapper) PickForWrite(c string) (string, int, *MachineInfo, error) {
	len_writers := len(m.Writers)
	if len_writers <= 0 {
		log.Println("No more writable volumes!")
		return "", 0, nil, errors.New("No more writable volumes!")
	}
	vid := m.Writers[rand.Intn(len_writers)]
	machine_id := m.vid2machineId[vid]
	if machine_id > 0 {
		machine := m.Machines[machine_id-1]
		fileId, count := m.sequence.NextFileId(util.ParseInt(c, 1))
		if count == 0 {
			return "", 0, &m.Machines[rand.Intn(len(m.Machines))].Server, errors.New("Strange count:" + c)
		}
		return NewFileId(vid, fileId, rand.Uint32()).String(), count, &machine.Server, nil
	}
	return "", 0, &m.Machines[rand.Intn(len(m.Machines))].Server, errors.New("Strangely vid " + vid.String() + " is on no machine!")
}
func (m *Mapper) Get(vid storage.VolumeId) (*Machine, error) {
	machineId := m.vid2machineId[vid]
	if machineId <= 0 {
		return nil, errors.New("invalid volume id " + vid.String())
	}
	return m.Machines[machineId-1], nil
}
func (m *Mapper) Add(machine Machine) {
	//check existing machine, linearly
	//log.Println("Adding machine", machine.Server.Url)
	m.volumeLock.Lock()
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
	m.volumeLock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		m.vid2machineId[v.Id] = machineId + 1 //use base 1 indexed, to detect not found cases
	}
	//setting Writers, copy-on-write because of possible updating, this needs some future work!
	var writers []storage.VolumeId
	for _, machine_entry := range m.Machines {
		for _, v := range machine_entry.Volumes {
			if uint64(v.Size) < m.volumeSizeLimit {
				writers = append(writers, v.Id)
			}
		}
	}
	m.Writers = writers
}
