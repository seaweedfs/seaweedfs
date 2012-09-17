package directory

import (
	"errors"
	"log"
	"math/rand"
	"pkg/sequence"
	"pkg/storage"
	"pkg/util"
	"time"
)

type Machine struct {
	C1Volumes   []storage.VolumeInfo
	Url       string //<server name/ip>[:port]
	PublicUrl string
	LastSeen  int64 // unix time in seconds
}

type Mapper struct {
	Machines     map[string]*Machine
	vid2machines map[storage.VolumeId][]*Machine
	Writers      []storage.VolumeId // transient array of Writers volume id
	pulse        int64

	volumeSizeLimit uint64

	sequence sequence.Sequencer
}

func NewMachine(server, publicUrl string, volumes []storage.VolumeInfo, lastSeen int64) *Machine {
	return &Machine{Url: server, PublicUrl: publicUrl, C1Volumes: volumes, LastSeen: lastSeen}
}

func NewMapper(dirname string, filename string, volumeSizeLimit uint64, pulse int) (m *Mapper) {
	m = &Mapper{}
	m.vid2machines = make(map[storage.VolumeId][]*Machine)
	m.volumeSizeLimit = volumeSizeLimit
	m.Writers = *new([]storage.VolumeId)
	m.Machines = make(map[string]*Machine)
	m.pulse = int64(pulse)

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
	machines := m.vid2machines[vid]
	if machines != nil && len(machines)>0 {
		fileId, count := m.sequence.NextFileId(util.ParseInt(c, 1))
		if count == 0 {
			return "", 0, nil, errors.New("Strange count:" + c)
		}
		//always use the first server to write
		return NewFileId(vid, fileId, rand.Uint32()).String(), count, machines[0], nil
	}
	return "", 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
}
func (m *Mapper) Get(vid storage.VolumeId) ([]*Machine, error) {
	machines := m.vid2machines[vid]
	if machines == nil {
		return nil, errors.New("invalid volume id " + vid.String())
	}
	return machines, nil
}
func (m *Mapper) Add(machine *Machine) {
	m.Machines[machine.Url] = machine
	//add to vid2machine map, and Writers array
	for _, v := range machine.C1Volumes {
		list := m.vid2machines[v.Id]
		found := false
		for index, entry := range list {
			if machine.Url == entry.Url {
				list[index] = machine
				found = true
			}
		}
		if !found {
			m.vid2machines[v.Id] = append(m.vid2machines[v.Id], machine)
		}
	}
	m.refreshWritableVolumes()
}
func (m *Mapper) remove(machine *Machine) {
	delete(m.Machines, machine.Url)
	for _, v := range machine.C1Volumes {
    list := m.vid2machines[v.Id]
    foundIndex := -1
    for index, entry := range list {
      if machine.Url == entry.Url {
        foundIndex = index
      }
    }
    m.vid2machines[v.Id] = deleteFromSlice(foundIndex,m.vid2machines[v.Id])
	}
}
func deleteFromSlice(i int, slice []*Machine) []*Machine{
    switch i {
        case -1://do nothing
        case 0: slice = slice[1:]
        case len(slice)-1: slice = slice[:len(slice)-1]
        default: slice = append(slice[:i], slice[i+1:]...)
    }
    return slice
}

func (m *Mapper) StartRefreshWritableVolumes() {
	go func() {
		for {
			m.refreshWritableVolumes()
			time.Sleep(time.Duration(float32(m.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
}

func (m *Mapper) refreshWritableVolumes() {
	freshThreshHold := time.Now().Unix() - 3*m.pulse //3 times of sleep interval
	//setting Writers, copy-on-write because of possible updating, this needs some future work!
	var writers []storage.VolumeId
	for _, machine_entry := range m.Machines {
		if machine_entry.LastSeen > freshThreshHold {
			for _, v := range machine_entry.C1Volumes {
				if uint64(v.Size) < m.volumeSizeLimit {
					writers = append(writers, v.Id)
				}
			}
		} else {
			log.Println("Warning! DataNode", machine_entry.Url, "last seen is", time.Now().Unix()-machine_entry.LastSeen, "seconds ago!")
			m.remove(machine_entry)
		}
	}
	m.Writers = writers
}
