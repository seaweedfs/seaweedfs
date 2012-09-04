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

type Machine struct {
	Volumes   []storage.VolumeInfo
	Url       string //<server name/ip>[:port]
	PublicUrl string
}

type Mapper struct {
	volumeLock    sync.Mutex
	Machines      map[string]*Machine
	vid2machineId map[storage.VolumeId]*Machine //machineId is +1 of the index of []*Machine, to detect not found entries
	Writers       []storage.VolumeId            // transient array of Writers volume id

	volumeSizeLimit uint64

	sequence sequence.Sequencer
}

func NewMachine(server, publicUrl string, volumes []storage.VolumeInfo) *Machine {
	return &Machine{Url: server, PublicUrl: publicUrl, Volumes: volumes}
}

func NewMapper(dirname string, filename string, volumeSizeLimit uint64) (m *Mapper) {
	m = &Mapper{}
	m.vid2machineId = make(map[storage.VolumeId]*Machine)
	m.volumeSizeLimit = volumeSizeLimit
	m.Writers = *new([]storage.VolumeId)
	m.Machines = make(map[string]*Machine)

	m.sequence = sequence.NewSequencer(dirname, filename)

	return
}
func (m *Mapper) PickForWrite(c string) (string, int, *Machine, error) {
	len_writers := len(m.Writers)
	if len_writers <= 0 {
		log.Println("No more writable volumes!")
		return "", 0, nil, errors.New("No more writable volumes!")
	}
	vid := m.Writers[rand.Intn(len_writers)]
	machine := m.vid2machineId[vid]
	if machine != nil {
		fileId, count := m.sequence.NextFileId(util.ParseInt(c, 1))
		if count == 0 {
			return "", 0, nil, errors.New("Strange count:" + c)
		}
		return NewFileId(vid, fileId, rand.Uint32()).String(), count, machine, nil
	}
	return "", 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
}
func (m *Mapper) Get(vid storage.VolumeId) (*Machine, error) {
	machine := m.vid2machineId[vid]
	if machine == nil {
		return nil, errors.New("invalid volume id " + vid.String())
	}
	return machine, nil
}
func (m *Mapper) Add(machine *Machine) {
	//check existing machine, linearly
	//log.Println("Adding machine", machine.Server.Url)
	m.volumeLock.Lock()
	m.Machines[machine.Url] = machine
	m.volumeLock.Unlock()

	//add to vid2machineId map, and Writers array
	for _, v := range machine.Volumes {
		m.vid2machineId[v.Id] = machine
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
