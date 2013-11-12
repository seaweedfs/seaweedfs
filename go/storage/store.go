package storage

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
)

type DiskLocation struct {
	directory      string
	maxVolumeCount int
	volumes        map[VolumeId]*Volume
}
type Store struct {
	Port            int
	Ip              string
	PublicUrl       string
	locations       []*DiskLocation
	masterNode      string
	dataCenter      string //optional informaton, overwriting master setting if exists
	rack            string //optional information, overwriting master setting if exists
	connected       bool
	volumeSizeLimit uint64 //read from the master
}

func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int) (s *Store) {
	s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl}
	s.locations = make([]*DiskLocation, 0)
	for i := 0; i < len(dirnames); i++ {
		location := &DiskLocation{directory: dirnames[i], maxVolumeCount: maxVolumeCounts[i]}
		location.volumes = make(map[VolumeId]*Volume)
		location.loadExistingVolumes()
		s.locations = append(s.locations, location)
	}
	return
}
func (s *Store) AddVolume(volumeListString string, collection string, replicationType string) error {
	rt, e := NewReplicationTypeFromString(replicationType)
	if e != nil {
		return e
	}
	for _, range_string := range strings.Split(volumeListString, ",") {
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := NewVolumeId(id_string)
			if err != nil {
				return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", id_string)
			}
			e = s.addVolume(VolumeId(id), collection, rt)
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return fmt.Errorf("Volume Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return fmt.Errorf("Volume End Id %s is not a valid unsigned integer!", pair[1])
			}
			for id := start; id <= end; id++ {
				if err := s.addVolume(VolumeId(id), collection, rt); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) findVolume(vid VolumeId) *Volume {
	for _, location := range s.locations {
		if v, found := location.volumes[vid]; found {
			return v
		}
	}
	return nil
}
func (s *Store) findFreeLocation() (ret *DiskLocation) {
	max := 0
	for _, location := range s.locations {
		currentFreeCount := location.maxVolumeCount - len(location.volumes)
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}
func (s *Store) addVolume(vid VolumeId, collection string, replicationType ReplicationType) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %s already exists!", vid)
	}
	if location := s.findFreeLocation(); location != nil {
		glog.V(0).Infoln("In dir", location.directory, "adds volume =", vid, ", collection =", collection, ", replicationType =", replicationType)
		if volume, err := NewVolume(location.directory, collection, vid, replicationType); err == nil {
			location.volumes[vid] = volume
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) CheckCompactVolume(volumeIdString string, garbageThresholdString string) (error, bool) {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString), false
	}
	garbageThreshold, e := strconv.ParseFloat(garbageThresholdString, 32)
	if e != nil {
		return fmt.Errorf("garbageThreshold %s is not a valid float number!", garbageThresholdString), false
	}
	if v := s.findVolume(vid); v != nil {
		return nil, garbageThreshold < v.garbageLevel()
	}
	return fmt.Errorf("volume id %s is not found during check compact!", vid), false
}
func (s *Store) CompactVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		return v.Compact()
	}
	return fmt.Errorf("volume id %s is not found during compact!", vid)
}
func (s *Store) CommitCompactVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		return v.commitCompact()
	}
	return fmt.Errorf("volume id %s is not found during commit compact!", vid)
}
func (s *Store) FreezeVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		if v.readOnly {
			return fmt.Errorf("Volume %s is already read-only", volumeIdString)
		}
		return v.freeze()
	}
	return fmt.Errorf("volume id %s is not found during freeze!", vid)
}
func (l *DiskLocation) loadExistingVolumes() {
	if dirs, err := ioutil.ReadDir(l.directory); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
				collection := ""
				base := name[:len(name)-len(".dat")]
				i := strings.Index(base, "_")
				if i > 0 {
					collection, base = base[0:i], base[i+1:]
				}
				if vid, err := NewVolumeId(base); err == nil {
					if l.volumes[vid] == nil {
						if v, e := NewVolume(l.directory, collection, vid, CopyNil); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infoln("data file", l.directory+"/"+name, "replicationType =", v.ReplicaType, "version =", v.Version(), "size =", v.Size())
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.directory, "with", len(l.volumes), "volumes", "max", l.maxVolumeCount)
}
func (s *Store) Status() []*VolumeInfo {
	var stats []*VolumeInfo
	for _, location := range s.locations {
		for k, v := range location.volumes {
			s := &VolumeInfo{Id: VolumeId(k), Size: v.ContentSize(),
				Collection:       v.Collection,
				RepType:          v.ReplicaType,
				Version:          v.Version(),
				FileCount:        v.nm.FileCount(),
				DeleteCount:      v.nm.DeletedCount(),
				DeletedByteCount: v.nm.DeletedSize(),
				ReadOnly:         v.readOnly}
			stats = append(stats, s)
		}
	}
	return stats
}

type JoinResult struct {
	VolumeSizeLimit uint64
}

func (s *Store) SetMaster(mserver string) {
	s.masterNode = mserver
}
func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *Store) SetRack(rack string) {
	s.rack = rack
}
func (s *Store) Join() error {
	stats := new([]*VolumeInfo)
	maxVolumeCount := 0
	for _, location := range s.locations {
		maxVolumeCount = maxVolumeCount + location.maxVolumeCount
		for k, v := range location.volumes {
			s := &VolumeInfo{Id: VolumeId(k), Size: uint64(v.Size()),
				Collection:       v.Collection,
				RepType:          v.ReplicaType,
				Version:          v.Version(),
				FileCount:        v.nm.FileCount(),
				DeleteCount:      v.nm.DeletedCount(),
				DeletedByteCount: v.nm.DeletedSize(),
				ReadOnly:         v.readOnly}
			*stats = append(*stats, s)
		}
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
	values.Add("maxVolumeCount", strconv.Itoa(maxVolumeCount))
	values.Add("dataCenter", s.dataCenter)
	values.Add("rack", s.rack)
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
	for _, location := range s.locations {
		for _, v := range location.volumes {
			v.Close()
		}
	}
}
func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) {
	if v := s.findVolume(i); v != nil {
		if v.readOnly {
			err = fmt.Errorf("Volume %s is read only!", i)
			return
		} else {
			if s.volumeSizeLimit >= v.ContentSize()+uint64(size) {
				size, err = v.write(n)
			} else {
				err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", s.volumeSizeLimit, v.ContentSize())
			}
			if err != nil && s.volumeSizeLimit < v.ContentSize()+uint64(size) && s.volumeSizeLimit >= v.ContentSize() {
				glog.V(0).Infoln("volume", i, "size is", v.ContentSize(), "close to", s.volumeSizeLimit)
				if err = s.Join(); err != nil {
					glog.V(0).Infoln("error with Join:", err)
				}
			}
		}
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("Volume %s not found!", i)
	return
}
func (s *Store) Delete(i VolumeId, n *Needle) (uint32, error) {
	if v := s.findVolume(i); v != nil && !v.readOnly {
		return v.delete(n)
	}
	return 0, nil
}
func (s *Store) Read(i VolumeId, n *Needle) (int, error) {
	if v := s.findVolume(i); v != nil {
		return v.read(n)
	}
	return 0, fmt.Errorf("Volume %s not found!", i)
}
func (s *Store) GetVolume(i VolumeId) *Volume {
	return s.findVolume(i)
}

func (s *Store) HasVolume(i VolumeId) bool {
	v := s.findVolume(i)
	return v != nil
}
