package storage

import (
	"log"
	"json"
	"os"
	"strings"
	"strconv"
	"url"
	"util"
)

type Store struct {
	volumes      map[uint64]*Volume
	dir          string
	Port         int
	PublicUrl string
}
type VolumeInfo struct {
	Id   uint32
	Size int64
}

func NewStore(port int, publicUrl, dirname string, volumeListString string) (s *Store) {
	s = &Store{Port: port, PublicUrl: publicUrl, dir: dirname}
	s.volumes = make(map[uint64]*Volume)
		
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
		    id_string := range_string
            id, err := strconv.Atoui64(id_string)
            if err != nil {
                log.Println("Volume Id", id_string, "is not a valid unsigned integer! Skipping it...")
                continue
            }
            s.volumes[id] = NewVolume(s.dir, uint32(id))
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.Atoui64(pair[0])
			if start_err != nil {
				log.Println("Volume Id", pair[0], "is not a valid unsigned integer! Skipping it...")
				continue
			}
            end, end_err := strconv.Atoui64(pair[1])
            if end_err != nil {
                log.Println("Volume Id", pair[1], "is not a valid unsigned integer! Skipping it...")
                continue
            }
            for id := start; id<=end; id++ {
				s.volumes[id] = NewVolume(s.dir, uint32(id))
			}
		}
	}
	log.Println("Store started on dir:", dirname, "with", len(s.volumes), "existing volumes")
	return
}

func (s *Store) Status() *[]*VolumeInfo {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size = uint32(k), v.Size()
		*stats = append(*stats, s)
	}
	return stats
}
func (s *Store) Join(mserver string) {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size = uint32(k), v.Size()
		*stats = append(*stats, s)
	}
	bytes, _ := json.Marshal(stats)
	values := make(url.Values)
	values.Add("port", strconv.Itoa(s.Port))
	values.Add("publicUrl", s.PublicUrl)
	values.Add("volumes", string(bytes))
	log.Println("Exiting volumes", string(bytes))
	util.Post("http://"+mserver+"/dir/join", values)
}
func (s *Store) Close() {
	for _, v := range s.volumes {
		v.Close()
	}
}
func (s *Store) Write(i uint64, n *Needle) uint32 {
	return s.volumes[i].write(n)
}
func (s *Store) Read(i uint64, n *Needle) (int, os.Error) {
	return s.volumes[i].read(n)
}
