package storage

import (
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/url"
	"strconv"
	"strings"
)

type Store struct {
	volumes        map[VolumeId]*Volume
	dir            string
	Port           int
	Ip             string
	PublicUrl      string
	MaxVolumeCount int

	masterNode      string
	connected       bool
	volumeSizeLimit uint64 //read from the master

}

func NewStore(port int, ip, publicUrl, dirname string, maxVolumeCount int) (s *Store) {
	s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl, dir: dirname, MaxVolumeCount: maxVolumeCount}
	s.volumes = make(map[VolumeId]*Volume)
	s.loadExistingVolumes()

	log.Println("Store started on dir:", dirname, "with", len(s.volumes), "volumes")
	return
}
func (s *Store) AddVolume(volumeListString string, replicationType string) error {
	rt, e := NewReplicationTypeFromString(replicationType)
	if e != nil {
		return e
	}
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := NewVolumeId(id_string)
			if err != nil {
				return errors.New("Volume Id " + id_string + " is not a valid unsigned integer!")
			}
			e = s.addVolume(VolumeId(id), rt)
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
				if err := s.addVolume(VolumeId(id), rt); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) addVolume(vid VolumeId, replicationType ReplicationType) (err error) {
	if s.volumes[vid] != nil {
		return errors.New("Volume Id " + vid.String() + " already exists!")
	}
	log.Println("In dir", s.dir, "adds volume =", vid, ", replicationType =", replicationType)
	s.volumes[vid], err = NewVolume(s.dir, vid, replicationType)
	return err
}

func (s *Store) CheckCompactVolume(volumeIdString string, garbageThresholdString string) (error, bool) {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return errors.New("Volume Id " + volumeIdString + " is not a valid unsigned integer!"), false
	}
	garbageThreshold, e := strconv.ParseFloat(garbageThresholdString, 32)
	if e != nil {
		return errors.New("garbageThreshold " + garbageThresholdString + " is not a valid float number!"), false
	}
	return nil, garbageThreshold < s.volumes[vid].garbageLevel()
}
func (s *Store) CompactVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return errors.New("Volume Id " + volumeIdString + " is not a valid unsigned integer!")
	}
	return s.volumes[vid].compact()
}
func (s *Store) CommitCompactVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return errors.New("Volume Id " + volumeIdString + " is not a valid unsigned integer!")
	}
	return s.volumes[vid].commitCompact()
}
func (s *Store) loadExistingVolumes() {
	if dirs, err := ioutil.ReadDir(s.dir); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
				base := name[:len(name)-len(".dat")]
				if vid, err := NewVolumeId(base); err == nil {
					if s.volumes[vid] == nil {
						if v, e := NewVolume(s.dir, vid, CopyNil); e == nil {
							s.volumes[vid] = v
							log.Println("In dir", s.dir, "read volume =", vid, "replicationType =", v.ReplicaType, "version =", v.Version(), "size =", v.Size())
						}
					}
				}
			}
		}
	}
}
func (s *Store) Status() []*VolumeInfo {
	var stats []*VolumeInfo
	for k, v := range s.volumes {
		s := &VolumeInfo{Id: VolumeId(k), Size: v.ContentSize(),
			RepType: v.ReplicaType, Version: v.Version(), FileCount: v.nm.fileCounter,
			DeleteCount: v.nm.deletionCounter, DeletedByteCount: v.nm.deletionByteCounter,
			ReadOnly: v.readOnly}
		stats = append(stats, s)
	}
	return stats
}

type JoinResult struct {
	VolumeSizeLimit uint64
}

func (s *Store) SetMaster(mserver string) {
	s.masterNode = mserver
}
func (s *Store) Join() error {
	stats := new([]*VolumeInfo)
	for k, v := range s.volumes {
		s := &VolumeInfo{Id: VolumeId(k), Size: uint64(v.Size()),
			RepType: v.ReplicaType, Version: v.Version(), FileCount: v.nm.fileCounter,
			DeleteCount: v.nm.deletionCounter, DeletedByteCount: v.nm.deletionByteCounter,
			ReadOnly: v.readOnly}
		*stats = append(*stats, s)
	}
	bytes, _ := json.Marshal(stats)
	values := make(url.Values)
	if !s.connected {
		values.Add("init", "true")
	}
	values.Add("port", strconv.Itoa(s.Port))
	values.Add("ip", s.Ip)
	values.Add("publicUrl", s.PublicUrl)
	values.Add("volumes", string(bytes))
	values.Add("maxVolumeCount", strconv.Itoa(s.MaxVolumeCount))
	jsonBlob, err := util.Post("http://"+s.masterNode+"/dir/join", values)
	if err != nil {
		return err
	}
	var ret JoinResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	s.volumeSizeLimit = ret.VolumeSizeLimit
	s.connected = true
	return nil
}
func (s *Store) Close() {
	for _, v := range s.volumes {
		v.Close()
	}
}
func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) {
	if v := s.volumes[i]; v != nil {
		if v.readOnly {
		  err = errors.New("Volume " + i.String() + " is read only!")
		  return
		} else {
			size, err = v.write(n)
			if err != nil && s.volumeSizeLimit < v.ContentSize()+uint64(size) && s.volumeSizeLimit >= v.ContentSize() {
				log.Println("volume", i, "size is", v.ContentSize(), "close to", s.volumeSizeLimit)
				if err = s.Join(); err != nil {
					log.Printf("error with Join: %s", err)
				}
			}
		}
		return
	}
	log.Println("volume", i, "not found!")
  err = errors.New("Volume " + i.String() + " not found!")
	return
}
func (s *Store) Delete(i VolumeId, n *Needle) (uint32, error) {
	if v := s.volumes[i]; v != nil && !v.readOnly {
		return v.delete(n)
	}
	return 0, nil
}
func (s *Store) Read(i VolumeId, n *Needle) (int, error) {
	if v := s.volumes[i]; v != nil {
		return v.read(n)
	}
	return 0, errors.New("Not Found")
}
func (s *Store) GetVolume(i VolumeId) *Volume {
	return s.volumes[i]
}

func (s *Store) HasVolume(i VolumeId) bool {
	_, ok := s.volumes[i]
	return ok
}
