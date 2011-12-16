package storage

import (
	"log"
	"io/ioutil"
	"json"
	"strings"
	"strconv"
	"url"
)

type Store struct {
	volumes      map[uint64]*Volume
	dir          string
	Port       int
	PublicServer string
}
type VolumeStat struct {
	Id     uint64 "id"
	Status int    "status" //0:read, 1:write
}

func NewStore(port int, publicServer, dirname string) (s *Store) {
	s = new(Store)
	s.Port, s.PublicServer, s.dir = port, publicServer, dirname
	s.volumes = make(map[uint64]*Volume)

	counter := uint64(0)
	files, _ := ioutil.ReadDir(dirname)
	for _, f := range files {
		if f.IsDirectory() || !strings.HasSuffix(f.Name, ".dat") {
			continue
		}
		id, err := strconv.Atoui64(f.Name[:-4])
		if err == nil {
			continue
		}
		s.volumes[counter] = NewVolume(s.dir, id)
		counter++
	}
	log.Println("Store started on dir:", dirname, "with", counter, "existing volumes")
	return
}

func (s *Store) Join(mserver string) {
	stats := make([]*VolumeStat, len(s.volumes))
	for k, _ := range s.volumes {
		s := new(VolumeStat)
		s.Id, s.Status = k, 1
		stats = append(stats, s)
	}
	bytes, _ := json.Marshal(stats)
	values := make(url.Values)
	values.Add("port", strconv.Itoa(s.Port))
    values.Add("publicServer", s.PublicServer)
	values.Add("volumes", string(bytes))
	post("http://"+mserver+"/dir/join", values)
}
func (s *Store) Close() {
	for _, v := range s.volumes {
		v.Close()
	}
}
func (s *Store) Write(i uint64, n *Needle) {
	s.volumes[i].write(n)
}
func (s *Store) Read(i uint64, n *Needle) {
	s.volumes[i].read(n)
}
