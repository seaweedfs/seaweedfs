package storage

import (
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"strconv"
	"strings"
	"pkg/util"
)

type Store struct {
	volumes   map[VolumeId]*Volume
	dir       string
	Port      int
	PublicUrl string
}

func NewStore(port int, publicUrl, dirname string, volumeListString string) (s *Store) {
	s = &Store{Port: port, PublicUrl: publicUrl, dir: dirname}
	s.volumes = make(map[VolumeId]*Volume)

	s.AddVolume(volumeListString)

	log.Println("Store started on dir:", dirname, "with", len(s.volumes), "volumes")
	return
}
func (s *Store) AddVolume(volumeListString string) error {
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := strconv.ParseUint(id_string, 10, 64)
			if err != nil {
				return errors.New("Volume Id " + id_string + " is not a valid unsigned integer!")
			}
			s.addVolume(VolumeId(id))
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return errors.New("Volume Start Id" + pair[0] + " is not a valid unsigned integer!")
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return errors.New("Volume End Id" + pair[1] + " is not a valid unsigned integer!")
			}
			for id := start; id <= end; id++ {
				s.addVolume(VolumeId(id))
			}
		}
	}
	return nil
}
func (s *Store) addVolume(vid VolumeId) error {
	if s.volumes[vid] != nil {
		return errors.New("Volume Id " + vid.String() + " already exists!")
	}
	s.volumes[vid] = NewVolume(s.dir, vid)
	return nil
}
func (s *Store) Status() *[]*VolumeInfo {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size = VolumeId(k), v.Size()
		*stats = append(*stats, s)
	}
	return stats
}
func (s *Store) Join(mserver string) {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size = VolumeId(k), v.Size()
		*stats = append(*stats, s)
	}
	bytes, _ := json.Marshal(stats)
	values := make(url.Values)
	values.Add("port", strconv.Itoa(s.Port))
	values.Add("publicUrl", s.PublicUrl)
	values.Add("volumes", string(bytes))
	util.Post("http://"+mserver+"/dir/join", values)
}
func (s *Store) Close() {
	for _, v := range s.volumes {
		v.Close()
	}
}
func (s *Store) Write(i VolumeId, n *Needle) uint32 {
	return s.volumes[i].write(n)
}
func (s *Store) Delete(i VolumeId, n *Needle) uint32 {
	return s.volumes[i].delete(n)
}
func (s *Store) Read(i VolumeId, n *Needle) (int, error) {
	return s.volumes[i].read(n)
}
