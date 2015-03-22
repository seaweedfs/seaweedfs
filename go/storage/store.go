package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/operation"
	"github.com/chrislusf/weed-fs/go/security"
	"github.com/chrislusf/weed-fs/go/util"
	"github.com/golang/protobuf/proto"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[VolumeId]*Volume
}

func (mn *DiskLocation) reset() {
}

type MasterNodes struct {
	nodes    []string
	lastNode int
}

func (mn *MasterNodes) String() string {
	return fmt.Sprintf("nodes:%v, lastNode:%d", mn.nodes, mn.lastNode)
}

func NewMasterNodes(bootstrapNode string) (mn *MasterNodes) {
	mn = &MasterNodes{nodes: []string{bootstrapNode}, lastNode: -1}
	return
}
func (mn *MasterNodes) reset() {
	if len(mn.nodes) > 1 && mn.lastNode > 0 {
		mn.lastNode = -mn.lastNode
	}
}
func (mn *MasterNodes) findMaster() (string, error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.lastNode < 0 {
		for _, m := range mn.nodes {
			if masters, e := operation.ListMasters(m); e == nil {
				if len(masters) == 0 {
					continue
				}
				mn.nodes = masters
				mn.lastNode = rand.Intn(len(mn.nodes))
				glog.V(2).Info("current master node is :", mn.nodes[mn.lastNode])
				break
			}
		}
	}
	if mn.lastNode < 0 {
		return "", errors.New("No master node available!")
	}
	return mn.nodes[mn.lastNode], nil
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	Ip              string
	Port            int
	PublicUrl       string
	Locations       []*DiskLocation
	dataCenter      string //optional informaton, overwriting master setting if exists
	rack            string //optional information, overwriting master setting if exists
	connected       bool
	volumeSizeLimit uint64 //read from the master
	masterNodes     *MasterNodes
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, PublicUrl:%s, dataCenter:%s, rack:%s, connected:%v, volumeSizeLimit:%d, masterNodes:%s", s.Ip, s.Port, s.PublicUrl, s.dataCenter, s.rack, s.connected, s.volumeSizeLimit, s.masterNodes)
	return
}

func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int) (s *Store) {
	s = &Store{Port: port, Ip: ip, PublicUrl: publicUrl}
	s.Locations = make([]*DiskLocation, 0)
	for i := 0; i < len(dirnames); i++ {
		location := &DiskLocation{Directory: dirnames[i], MaxVolumeCount: maxVolumeCounts[i]}
		location.volumes = make(map[VolumeId]*Volume)
		location.loadExistingVolumes()
		s.Locations = append(s.Locations, location)
	}
	return
}
func (s *Store) AddVolume(volumeListString string, collection string, replicaPlacement string, ttlString string) error {
	rt, e := NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}
	ttl, e := ReadTTL(ttlString)
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
			e = s.addVolume(VolumeId(id), collection, rt, ttl)
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
				if err := s.addVolume(VolumeId(id), collection, rt, ttl); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		for k, v := range location.volumes {
			if v.Collection == collection {
				e = v.Destroy()
				if e != nil {
					return
				}
				delete(location.volumes, k)
			}
		}
	}
	return
}
func (s *Store) DeleteVolume(volumes map[VolumeId]*Volume, v *Volume) (e error) {
	e = v.Destroy()
	if e != nil {
		return
	}
	delete(volumes, v.Id)
	return
}
func (s *Store) findVolume(vid VolumeId) *Volume {
	for _, location := range s.Locations {
		if v, found := location.volumes[vid]; found {
			return v
		}
	}
	return nil
}
func (s *Store) findFreeLocation() (ret *DiskLocation) {
	max := 0
	for _, location := range s.Locations {
		currentFreeCount := location.MaxVolumeCount - len(location.volumes)
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}
func (s *Store) addVolume(vid VolumeId, collection string, replicaPlacement *ReplicaPlacement, ttl *TTL) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}
	if location := s.findFreeLocation(); location != nil {
		glog.V(0).Infof("In dir %s adds volume:%v collection:%s replicaPlacement:%v ttl:%v",
			location.Directory, vid, collection, replicaPlacement, ttl)
		if volume, err := NewVolume(location.Directory, collection, vid, replicaPlacement, ttl); err == nil {
			location.volumes[vid] = volume
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) FreezeVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		if v.readOnly {
			return fmt.Errorf("Volume %s is already read-only", volumeIdString)
		}
		return v.freeze()
	}
	return fmt.Errorf("volume id %d is not found during freeze", vid)
}
func (l *DiskLocation) loadExistingVolumes() {
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
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
						if v, e := NewVolume(l.Directory, collection, vid, nil, nil); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s", l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), v.Size(), v.Ttl.String())
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes", "max", l.MaxVolumeCount)
}
func (s *Store) Status() []*VolumeInfo {
	var stats []*VolumeInfo
	for _, location := range s.Locations {
		for k, v := range location.volumes {
			s := &VolumeInfo{Id: VolumeId(k), Size: v.ContentSize(),
				Collection:       v.Collection,
				ReplicaPlacement: v.ReplicaPlacement,
				Version:          v.Version(),
				FileCount:        v.nm.FileCount(),
				DeleteCount:      v.nm.DeletedCount(),
				DeletedByteCount: v.nm.DeletedSize(),
				ReadOnly:         v.readOnly,
				Ttl:              v.Ttl}
			stats = append(stats, s)
		}
	}
	return stats
}

func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *Store) SetRack(rack string) {
	s.rack = rack
}

func (s *Store) SetBootstrapMaster(bootstrapMaster string) {
	s.masterNodes = NewMasterNodes(bootstrapMaster)
}
func (s *Store) Join() (masterNode string, secretKey security.Secret, e error) {
	masterNode, e = s.masterNodes.findMaster()
	if e != nil {
		return
	}
	var volumeMessages []*operation.VolumeInformationMessage
	maxVolumeCount := 0
	var maxFileKey uint64
	for _, location := range s.Locations {
		maxVolumeCount = maxVolumeCount + location.MaxVolumeCount
		for k, v := range location.volumes {
			if maxFileKey < v.nm.MaxFileKey() {
				maxFileKey = v.nm.MaxFileKey()
			}
			if !v.expired(s.volumeSizeLimit) {
				volumeMessage := &operation.VolumeInformationMessage{
					Id:               proto.Uint32(uint32(k)),
					Size:             proto.Uint64(uint64(v.Size())),
					Collection:       proto.String(v.Collection),
					FileCount:        proto.Uint64(uint64(v.nm.FileCount())),
					DeleteCount:      proto.Uint64(uint64(v.nm.DeletedCount())),
					DeletedByteCount: proto.Uint64(v.nm.DeletedSize()),
					ReadOnly:         proto.Bool(v.readOnly),
					ReplicaPlacement: proto.Uint32(uint32(v.ReplicaPlacement.Byte())),
					Version:          proto.Uint32(uint32(v.Version())),
					Ttl:              proto.Uint32(v.Ttl.ToUint32()),
				}
				volumeMessages = append(volumeMessages, volumeMessage)
			} else {
				if v.exiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
					s.DeleteVolume(location.volumes, v)
					glog.V(0).Infoln("volume", v.Id, "is deleted.")
				} else {
					glog.V(0).Infoln("volume", v.Id, "is expired.")
				}
			}
		}
	}

	joinMessage := &operation.JoinMessage{
		IsInit:         proto.Bool(!s.connected),
		Ip:             proto.String(s.Ip),
		Port:           proto.Uint32(uint32(s.Port)),
		PublicUrl:      proto.String(s.PublicUrl),
		MaxVolumeCount: proto.Uint32(uint32(maxVolumeCount)),
		MaxFileKey:     proto.Uint64(maxFileKey),
		DataCenter:     proto.String(s.dataCenter),
		Rack:           proto.String(s.rack),
		Volumes:        volumeMessages,
	}

	data, err := proto.Marshal(joinMessage)
	if err != nil {
		return "", "", err
	}

	jsonBlob, err := util.PostBytes("http://"+masterNode+"/dir/join", data)
	if err != nil {
		s.masterNodes.reset()
		return "", "", err
	}
	var ret operation.JoinResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return masterNode, "", err
	}
	if ret.Error != "" {
		return masterNode, "", errors.New(ret.Error)
	}
	s.volumeSizeLimit = ret.VolumeSizeLimit
	secretKey = security.Secret(ret.SecretKey)
	s.connected = true
	return
}
func (s *Store) Close() {
	for _, location := range s.Locations {
		for _, v := range location.volumes {
			v.Close()
		}
	}
}
func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) {
	if v := s.findVolume(i); v != nil {
		if v.readOnly {
			err = fmt.Errorf("Volume %d is read only", i)
			return
		}
		if MaxPossibleVolumeSize >= v.ContentSize()+uint64(size) {
			size, err = v.write(n)
		} else {
			err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", s.volumeSizeLimit, v.ContentSize())
		}
		if s.volumeSizeLimit < v.ContentSize()+3*uint64(size) {
			glog.V(0).Infoln("volume", i, "size", v.ContentSize(), "will exceed limit", s.volumeSizeLimit)
			if _, _, e := s.Join(); e != nil {
				glog.V(0).Infoln("error when reporting size:", e)
			}
		}
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("Volume %d not found!", i)
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
	return 0, fmt.Errorf("Volume %v not found!", i)
}
func (s *Store) GetVolume(i VolumeId) *Volume {
	return s.findVolume(i)
}

func (s *Store) HasVolume(i VolumeId) bool {
	v := s.findVolume(i)
	return v != nil
}
