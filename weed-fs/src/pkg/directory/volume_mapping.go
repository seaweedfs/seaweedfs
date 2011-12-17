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
	CanWrite     bool
}
type Mapper struct {
	dir      string
	fileName string
	capacity int
	Machines [][]Machine //initial version only support one copy per machine
	writers  [][]Machine // transient value to lookup writers fast
}

func NewMachine(server, publicServer string) (m *Machine) {
	m = new(Machine)
	m.Server, m.PublicServer = server, publicServer
	return
}

func NewMapper(dirname string, filename string, capacity int) (m *Mapper) {
	m = new(Mapper)
	m.dir, m.fileName, m.capacity = dirname, filename, capacity
	log.Println("Loading volume id to maching mapping:", path.Join(m.dir, m.fileName+".map"))
	dataFile, e := os.OpenFile(path.Join(m.dir, m.fileName+".map"), os.O_RDONLY, 0644)
	m.Machines = *new([][]Machine)
	m.writers = *new([][]Machine)
	if e != nil {
		log.Println("Mapping File Read", e)
	} else {
		decoder := gob.NewDecoder(dataFile)
		decoder.Decode(m.Machines)
		for _, list := range m.Machines {
			//TODO: what if a list has mixed readers and writers? Now it's treated as readonly
			allCanWrite := false
			for _, entry := range list {
				allCanWrite = allCanWrite && entry.CanWrite
			}
			if allCanWrite {
				m.writers = append(m.writers, list)
			}
		}
		dataFile.Close()
        log.Println("Loaded mapping size", len(m.Machines))
	}
	return
}
func (m *Mapper) PickForWrite() []Machine {
	vid := rand.Intn(len(m.Machines))
	return m.Machines[vid]
}
func (m *Mapper) Get(vid int) []Machine {
	return m.Machines[vid]
}
func (m *Mapper) Add(machine Machine, volumes []storage.VolumeStat, capacity int) []int {
	log.Println("Adding existing", machine.Server, len(volumes), "volumes to dir", len(m.Machines))
    log.Println("Adding      new ", machine.Server, capacity - len(volumes), "volumes to dir", len(m.Machines))
	maxId := len(m.Machines)-1
	for _, v := range volumes {
		if maxId < int(v.Id) {
			maxId = int(v.Id)
		}
	}
	for i := len(m.Machines); i <= maxId; i++ {
		m.Machines = append(m.Machines, nil)
	}
    log.Println("Machine list now is", len(m.Machines))
	for _, v := range volumes {
		found := false
		existing := m.Machines[v.Id]
		for _, entry := range existing {
			if machine.Server == entry.Server {
				found = true
				break
			}
		}
		if !found {
			m.Machines[v.Id] = append(existing, machine)
			log.Println("Setting volume", v.Id, "to", machine.Server)
		}
	}

    vids := new([]int)
	for vid,i := len(m.Machines),len(volumes); i < capacity; i,vid=i+1,vid+1 {
		list := new([]Machine)
		*list = append(*list, machine)
		m.Machines = append(m.Machines, *list)
		log.Println("Adding volume", vid, "from", machine.Server)
		*vids = append(*vids, vid)
	}

	m.Save()
	log.Println("Dir size =>", len(m.Machines))
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
}
