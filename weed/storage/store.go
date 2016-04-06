package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"sync"

	"encoding/json"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/weedpb"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type MasterNodes struct {
	nodes  []string
	master string
	mutex  sync.RWMutex
}

func NewMasterNodes(bootstrapNode string) (mn *MasterNodes) {
	mn = &MasterNodes{nodes: []string{bootstrapNode}}
	return
}

func (mn *MasterNodes) String() string {
	mn.mutex.RLock()
	defer mn.mutex.RUnlock()
	return fmt.Sprintf("nodes:%v, master:%s", mn.nodes, mn.master)
}

func (mn *MasterNodes) Reset() {
	glog.V(4).Infof("Resetting master nodes: %v", mn)
	mn.mutex.Lock()
	defer mn.mutex.Unlock()
	if len(mn.nodes) > 1 && mn.master != "" {
		glog.V(0).Infof("Reset master %s from: %v", mn.master, mn.nodes)
		mn.master = ""
	}
}

func (mn *MasterNodes) findMaster() (string, error) {
	master := mn.GetMaster()
	if master != "" {
		return master, nil
	}
	mn.mutex.Lock()
	defer mn.mutex.Unlock()
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.master == "" {
		for _, m := range mn.nodes {
			glog.V(4).Infof("Listing masters on %s", m)
			if masters, e := operation.ListMasters(m); e == nil {
				if len(masters) == 0 {
					continue
				}
				mn.nodes = append(masters, m)
				mn.master = mn.nodes[rand.Intn(len(mn.nodes))]
				glog.V(2).Infof("current master nodes is (nodes:%v, master:%s)", mn.nodes, mn.master)
				break
			} else {
				glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
			}
		}
	}
	if mn.master == "" {
		return "", errors.New("No master node available!")
	}
	return mn.master, nil
}

func (mn *MasterNodes) GetMaster() string {
	mn.mutex.RLock()
	defer mn.mutex.RUnlock()
	return mn.master
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	joinKey         string
	ip              string
	Port            int
	PublicUrl       string
	Locations       []*DiskLocation
	colSettings     *CollectionSettings
	dataCenter      string //optional informaton, overwriting master setting if exists
	rack            string //optional information, overwriting master setting if exists
	volumeSizeLimit uint64 //read from the master
	masterNodes     *MasterNodes
	needleMapKind   NeedleMapType
	TaskManager     *TaskManager
	mutex           sync.RWMutex
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, PublicUrl:%s, dataCenter:%s, rack:%s, joinKey:%v, volumeSizeLimit:%d, masterNodes:%s",
		s.GetIP(), s.Port, s.PublicUrl, s.dataCenter, s.rack, s.GetJoinKey(), s.GetVolumeSizeLimit(), s.masterNodes)
	return
}

func NewStore(port int, ip, publicUrl string, dirnames []string, maxVolumeCounts []int, needleMapKind NeedleMapType) (s *Store) {
	s = &Store{
		Port:          port,
		ip:            ip,
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.masterNodes = NewMasterNodes(bootstrapMaster)
}

type SettingChanged func(s *weedpb.JoinResponse)

func (s *Store) SendHeartbeatToMaster(callback SettingChanged) error {
	masterNode, err := s.masterNodes.findMaster()
	if err != nil {
		return err
	}
	var volumeMessages []*weedpb.VolumeInformationMessage
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
				volumeMessage := &weedpb.VolumeInformationMessage{
					Id:               uint32(v.Id),
					Size:             uint64(v.Size()),
					Collection:       v.Collection,
					FileCount:        uint64(v.nm.FileCount()),
					DeleteCount:      uint64(v.nm.DeletedCount()),
					DeletedByteCount: v.nm.DeletedSize(),
					ReadOnly:         v.IsReadOnly(),
					Version:          uint32(v.Version()),
					Ttl:              v.Ttl.ToUint32(),
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

	joinMsgV2 := &weedpb.JoinMessageV2{
		JoinKey:        s.GetJoinKey(),
		Ip:             s.GetIP(),
		Port:           uint32(s.Port),
		PublicUrl:      s.PublicUrl,
		MaxVolumeCount: uint32(maxVolumeCount),
		MaxFileKey:     maxFileKey,
		DataCenter:     s.dataCenter,
		Rack:           s.rack,
		Volumes:        volumeMessages,
	}
	ret := &weedpb.JoinResponse{}
	joinUrl := util.MkUrl(masterNode, "/dir/join2", nil)
	glog.V(4).Infof("Sending heartbeat to %s ...", joinUrl)
	if err = util.PostPbMsg(joinUrl, joinMsgV2, ret); err != nil {
		s.masterNodes.Reset()
		return err
	}

	if ret.Error != "" {
		s.masterNodes.Reset()
		return errors.New(ret.Error)
	}
	if ret.JoinKey != s.GetJoinKey() {
		if glog.V(4) {
			jsonData, _ := json.Marshal(ret)
			glog.V(4).Infof("dir join sync settings: %v", string(jsonData))
		}
		s.SetJoinKey(ret.JoinKey)
		if ret.JoinIp != "" {
			s.SetIP(ret.JoinIp)
		}
		if ret.VolumeSizeLimit != 0 {
			s.SetVolumeSizeLimit(ret.VolumeSizeLimit)
		}
		if callback != nil {
			callback(ret)
		}
		if len(ret.CollectionSettings) > 0 {
			cs := NewCollectionSettingsFromPbMessage(ret.CollectionSettings)
			s.SetCollectionSettings(cs)
		}
	}
	return nil
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
			if e := s.SendHeartbeatToMaster(nil); e != nil {
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

func (s *Store) GetIP() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.ip
}

func (s *Store) SetIP(ip string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.ip = ip
}

func (s *Store) GetJoinKey() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.joinKey
}

func (s *Store) SetJoinKey(k string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.joinKey = k
}

func (s *Store) GetMaster() string {
	return s.masterNodes.GetMaster()
}

func (s *Store) GetCollectionSettings() *CollectionSettings {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.colSettings
}

func (s *Store) SetCollectionSettings(cs *CollectionSettings) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.colSettings = cs
}

func (s *Store) GetVolumeReplicaPlacement(volumeId VolumeId) *ReplicaPlacement {
	cs := s.GetCollectionSettings()
	if cs == nil {
		return nil
	}
	collection := ""
	if v := s.GetVolume(volumeId); v != nil {
		collection = v.Collection
	}
	return cs.GetReplicaPlacement(collection)
}
