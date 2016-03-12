package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"sync"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/operation"
	"github.com/chrislusf/seaweedfs/go/security"
	"github.com/chrislusf/seaweedfs/go/util"
	"github.com/golang/protobuf/proto"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

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
	glog.V(4).Infof("Resetting master nodes: %v", mn)
	if len(mn.nodes) > 1 && mn.lastNode >= 0 {
		glog.V(0).Infof("Reset master %s from: %v", mn.nodes[mn.lastNode], mn.nodes)
		mn.lastNode = -mn.lastNode - 1
	}
}
func (mn *MasterNodes) findMaster() (string, error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.lastNode < 0 {
		for _, m := range mn.nodes {
			glog.V(4).Infof("Listing masters on %s", m)
			if masters, e := operation.ListMasters(m); e == nil {
				if len(masters) == 0 {
					continue
				}
				mn.nodes = append(masters, m)
				mn.lastNode = rand.Intn(len(mn.nodes))
				glog.V(2).Infof("current master nodes is %v", mn)
				break
			} else {
				glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
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
	needleMapKind   NeedleMapType
	TaskManager     *TaskManager
	mutex           sync.RWMutex
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, PublicUrl:%s, dataCenter:%s, rack:%s, connected:%v, volumeSizeLimit:%d, masterNodes:%s",
		s.Ip, s.Port, s.PublicUrl, s.dataCenter, s.rack, s.IsConnected(), s.GetVolumeSizeLimit(), s.masterNodes)
	return
}

func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int, needleMapKind NeedleMapType) (s *Store) {
	s = &Store{
		Port:          port,
		Ip:            ip,
		PublicUrl:     publicUrl,
		TaskManager:   NewTaskManager(),
		needleMapKind: needleMapKind,
	}
	s.Locations = make([]*DiskLocation, 0)
	for i := 0; i < len(dirnames); i++ {
		location := NewDiskLocation(dirnames[i], maxVolumeCounts[i])
		location.LoadExistingVolumes(needleMapKind)
		s.Locations = append(s.Locations, location)
	}
	return
}
func (s *Store) AddVolume(volumeListString string, collection string, ttlString string) error {
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
			e = s.addVolume(VolumeId(id), collection, ttl)
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
				if err := s.addVolume(VolumeId(id), collection, ttl); err != nil {
					e = err
				}
			}
		}
	}
	return e
}
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		location.DeleteCollection(collection)
	}
	return
}

func (s *Store) findVolume(vid VolumeId) *Volume {
	for _, location := range s.Locations {
		if v, found := location.GetVolume(vid); found {
			return v
		}
	}
	return nil
}
func (s *Store) findFreeLocation() (ret *DiskLocation) {
	max := 0
	for _, location := range s.Locations {
		currentFreeCount := location.MaxVolumeCount - location.VolumeCount()
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}
func (s *Store) addVolume(vid VolumeId, collection string, ttl *TTL) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}
	if location := s.findFreeLocation(); location != nil {
		glog.V(0).Infof("In dir %s adds volume:%v collection:%s ttl:%v",
			location.Directory, vid, collection, ttl)
		if volume, err := NewVolume(location.Directory, collection, vid, s.needleMapKind, ttl); err == nil {
			location.AddVolume(vid, volume)
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) Status() []*VolumeInfo {
	var stats []*VolumeInfo
	for _, location := range s.Locations {
		location.WalkVolume(func(v *Volume) (e error) {
			s := &VolumeInfo{
				Id:               VolumeId(v.Id),
				Size:             v.ContentSize(),
				Collection:       v.Collection,
				Version:          v.Version(),
				FileCount:        v.nm.FileCount(),
				DeleteCount:      v.nm.DeletedCount(),
				DeletedByteCount: v.nm.DeletedSize(),
				ReadOnly:         v.IsReadOnly(),
				Ttl:              v.Ttl}
			stats = append(stats, s)
			return nil
		})
	}
	sortVolumeInfos(stats)
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
func (s *Store) SendHeartbeatToMaster() (masterNode string, secretKey security.Secret, e error) {
	masterNode, e = s.masterNodes.findMaster()
	if e != nil {
		return
	}
	var volumeMessages []*operation.VolumeInformationMessage
	maxVolumeCount := 0
	var maxFileKey uint64
	for _, location := range s.Locations {
		maxVolumeCount = maxVolumeCount + location.MaxVolumeCount
		volumeToDelete := []VolumeId{}
		location.WalkVolume(func(v *Volume) (e error) {
			if maxFileKey < v.nm.MaxFileKey() {
				maxFileKey = v.nm.MaxFileKey()
			}
			if !v.expired(s.GetVolumeSizeLimit()) {
				volumeMessage := &operation.VolumeInformationMessage{
					Id:               proto.Uint32(uint32(v.Id)),
					Size:             proto.Uint64(uint64(v.Size())),
					Collection:       proto.String(v.Collection),
					FileCount:        proto.Uint64(uint64(v.nm.FileCount())),
					DeleteCount:      proto.Uint64(uint64(v.nm.DeletedCount())),
					DeletedByteCount: proto.Uint64(v.nm.DeletedSize()),
					ReadOnly:         proto.Bool(v.IsReadOnly()),
					Version:          proto.Uint32(uint32(v.Version())),
					Ttl:              proto.Uint32(v.Ttl.ToUint32()),
				}
				volumeMessages = append(volumeMessages, volumeMessage)
			} else {
				if v.expiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
					volumeToDelete = append(volumeToDelete, v.Id)
					glog.V(0).Infoln("volume", v.Id, "is deleted.")
				} else {
					glog.V(0).Infoln("volume", v.Id, "is expired.")
				}
			}
			return nil
		})
		for _, vid := range volumeToDelete {
			location.DeleteVolume(vid)
		}
	}

	joinMessage := &operation.JoinMessage{
		IsInit:         proto.Bool(!s.IsConnected()),
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

	joinUrl := util.MkUrl(masterNode, "/dir/join", nil)
	glog.V(4).Infof("Connecting to %s ...", joinUrl)

	jsonBlob, err := util.PostBytes(joinUrl, data)
	if err != nil {
		s.masterNodes.reset()
		return "", "", err
	}
	var ret operation.JoinResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		glog.V(0).Infof("Failed to join %s with response: %s", joinUrl, string(jsonBlob))
		s.masterNodes.reset()
		return masterNode, "", err
	}
	if ret.Error != "" {
		s.masterNodes.reset()
		return masterNode, "", errors.New(ret.Error)
	}
	s.SetVolumeSizeLimit(ret.VolumeSizeLimit)
	secretKey = security.Secret(ret.SecretKey)
	s.SetConnected(true)
	return
}
func (s *Store) Close() {
	for _, location := range s.Locations {
		location.CloseAllVolume()
	}
}
func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) {
	if v := s.findVolume(i); v != nil {
		if v.IsReadOnly() {
			err = fmt.Errorf("Volume %d is read only", i)
			return
		}
		if MaxPossibleVolumeSize >= v.ContentSize()+uint64(size) {
			size, err = v.write(n)
		} else {
			err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", s.GetVolumeSizeLimit(), v.ContentSize())
		}
		if s.GetVolumeSizeLimit() < v.ContentSize()+3*uint64(size) {
			glog.V(0).Infoln("volume", i, "size", v.ContentSize(), "will exceed limit", s.GetVolumeSizeLimit())
			if _, _, e := s.SendHeartbeatToMaster(); e != nil {
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
	if v := s.findVolume(i); v != nil && !v.IsReadOnly() {
		return v.delete(n)
	}
	return 0, nil
}
func (s *Store) ReadVolumeNeedle(i VolumeId, n *Needle) (int, error) {
	if v := s.findVolume(i); v != nil {
		return v.readNeedle(n)
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

func (s *Store) WalkVolume(walker VolumeWalker) error {
	for _, location := range s.Locations {
		if e := location.WalkVolume(walker); e != nil {
			return e
		}
	}
	return nil
}

func (s *Store) GetVolumeSizeLimit() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.volumeSizeLimit
}

func (s *Store) SetVolumeSizeLimit(sz uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.volumeSizeLimit = sz
}

func (s *Store) IsConnected() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.connected
}

func (s *Store) SetConnected(b bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.connected = b
}
