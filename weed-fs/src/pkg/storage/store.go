package storage

import (
	"encoding/json"
	"errors"
	"log"
	"net/url"
	"pkg/util"
	"strconv"
	"strings"
)

type Store struct {
	volumes   map[VolumeId]*Volume
	dir       string
	Port      int
	PublicUrl string
	Limit     StorageLimit
}

func NewStore(port int, publicUrl, dirname string, volumeListString string) (s *Store) {
	s = &Store{Port: port, PublicUrl: publicUrl, dir: dirname}
	s.volumes = make(map[VolumeId]*Volume)

	s.AddVolume(volumeListString, "00")

	log.Println("Store started on dir:", dirname, "with", len(s.volumes), "volumes", volumeListString)
	return
}
func (s *Store) AddVolume(volumeListString string, replicationType string) (e error) {
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := strconv.ParseUint(id_string, 10, 64)
			if err != nil {
				return errors.New("Volume Id " + id_string + " is not a valid unsigned integer!")
			}
			e = s.addVolume(VolumeId(id), NewReplicationType(replicationType))
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
				if err := s.addVolume(VolumeId(id), NewReplicationType(replicationType)); err != nil {
				  e = err
				}
			}
		}
	}
	return e
}
func (s *Store) addVolume(vid VolumeId, replicationType ReplicationType) error {
	if s.volumes[vid] != nil {
		return errors.New("Volume Id " + vid.String() + " already exists!")
	}
  log.Println("In dir", s.dir, "adds volume = ", vid, ", replicationType =", replicationType)
	s.volumes[vid] = NewVolume(s.dir, vid, replicationType)
	return nil
}
func (s *Store) Status() *[]*VolumeInfo {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size, s.RepType = VolumeId(k), v.Size(), v.replicaType
		*stats = append(*stats, s)
	}
	return stats
}
func (s *Store) Join(mserver string) {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := new(VolumeInfo)
		s.Id, s.Size, s.RepType = VolumeId(k), v.Size(), v.replicaType
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
	v := s.volumes[i]
	if v != nil {
		return v.write(n)
	}
	return 0
}
func (s *Store) Delete(i VolumeId, n *Needle) uint32 {
	v := s.volumes[i]
	if v != nil {
		return v.delete(n)
	}
	return 0
}
func (s *Store) Read(i VolumeId, n *Needle) (int, error) {
	v := s.volumes[i]
	if v != nil {
		return v.read(n)
	}
	return 0, errors.New("Not Found")
}

type VolumeLocations struct {
	Vid       VolumeId
	Locations []string
}

func (s *Store) SetVolumeLocations(volumeLocationList []VolumeLocations) error {
	for _, volumeLocations := range volumeLocationList {
		vid := volumeLocations.Vid
		v := s.volumes[vid]
		if v != nil {
			v.locations = volumeLocations.Locations
		}
	}
	return nil
}
