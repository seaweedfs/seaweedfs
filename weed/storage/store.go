package storage

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type ReadOption struct {
	// request
	ReadDeleted     bool
	AttemptMetaOnly bool
	MustMetaOnly    bool
	// response
	IsMetaOnly     bool // read status
	VolumeRevision uint16
	IsOutOfRange   bool // whether read over MaxPossibleVolumeSize

	// If HasSlowRead is set to true:
	//  * read requests and write requests compete for the lock.
	//  * large file read P99 latency on busy sites will go up, due to the need to get locks multiple times.
	//  * write requests will see lower latency.
	// If HasSlowRead is set to false:
	//  * read requests should complete asap, not blocking other requests.
	//  * write requests may see high latency when downloading large files.
	HasSlowRead bool

	// increasing ReadBufferSize can reduce the number of get locks times and shorten read P99 latency.
	// but will increase memory usage a bit. Use with hasSlowRead normally.
	ReadBufferSize int
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	MasterAddress       pb.ServerAddress
	grpcDialOption      grpc.DialOption
	volumeSizeLimit     uint64      // read from the master
	preallocate         atomic.Bool // read from the master
	Ip                  string
	Port                int
	GrpcPort            int
	PublicUrl           string
	Locations           []*DiskLocation
	dataCenter          string // optional information, overwriting master setting if exists
	rack                string // optional information, overwriting master setting if exists
	connected           bool
	NeedleMapKind       NeedleMapKind
	NewVolumesChan      chan master_pb.VolumeShortInformationMessage
	DeletedVolumesChan  chan master_pb.VolumeShortInformationMessage
	NewEcShardsChan     chan master_pb.VolumeEcShardInformationMessage
	DeletedEcShardsChan chan master_pb.VolumeEcShardInformationMessage
	isStopping          bool
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, GrpcPort:%d PublicUrl:%s, dataCenter:%s, rack:%s, connected:%v, volumeSizeLimit:%d", s.Ip, s.Port, s.GrpcPort, s.PublicUrl, s.dataCenter, s.rack, s.connected, s.GetVolumeSizeLimit())
	return
}

func NewStore(grpcDialOption grpc.DialOption, ip string, port int, grpcPort int, publicUrl string, dirnames []string, maxVolumeCounts []int32,
	minFreeSpaces []util.MinFreeSpace, idxFolder string, needleMapKind NeedleMapKind, diskTypes []DiskType, ldbTimeout int64) (s *Store) {
	s = &Store{grpcDialOption: grpcDialOption, Port: port, Ip: ip, GrpcPort: grpcPort, PublicUrl: publicUrl, NeedleMapKind: needleMapKind}
	s.Locations = make([]*DiskLocation, 0)

	var wg sync.WaitGroup
	for i := 0; i < len(dirnames); i++ {
		location := NewDiskLocation(dirnames[i], int32(maxVolumeCounts[i]), minFreeSpaces[i], idxFolder, diskTypes[i])
		s.Locations = append(s.Locations, location)
		stats.VolumeServerMaxVolumeCounter.Add(float64(maxVolumeCounts[i]))

		wg.Add(1)
		go func() {
			defer wg.Done()
			location.loadExistingVolumes(needleMapKind, ldbTimeout)
		}()
	}
	wg.Wait()

	s.NewVolumesChan = make(chan master_pb.VolumeShortInformationMessage, 3)
	s.DeletedVolumesChan = make(chan master_pb.VolumeShortInformationMessage, 3)

	s.NewEcShardsChan = make(chan master_pb.VolumeEcShardInformationMessage, 3)
	s.DeletedEcShardsChan = make(chan master_pb.VolumeEcShardInformationMessage, 3)

	return
}
func (s *Store) AddVolume(volumeId needle.VolumeId, collection string, needleMapKind NeedleMapKind, replicaPlacement string, ttlString string, preallocate int64, MemoryMapMaxSizeMb uint32, diskType DiskType, ldbTimeout int64) error {
	rt, e := super_block.NewReplicaPlacementFromString(replicaPlacement)
	if e != nil {
		return e
	}
	ttl, e := needle.ReadTTL(ttlString)
	if e != nil {
		return e
	}
	e = s.addVolume(volumeId, collection, needleMapKind, rt, ttl, preallocate, MemoryMapMaxSizeMb, diskType, ldbTimeout)
	return e
}
func (s *Store) DeleteCollection(collection string) (e error) {
	for _, location := range s.Locations {
		e = location.DeleteCollectionFromDiskLocation(collection)
		if e != nil {
			return
		}
		stats.DeleteCollectionMetrics(collection)
		// let the heartbeat send the list of volumes, instead of sending the deleted volume ids to DeletedVolumesChan
	}
	return
}

func (s *Store) findVolume(vid needle.VolumeId) *Volume {
	for _, location := range s.Locations {
		if v, found := location.FindVolume(vid); found {
			return v
		}
	}
	return nil
}
func (s *Store) FindFreeLocation(filterFn func(location *DiskLocation) bool) (ret *DiskLocation) {
	max := int32(0)
	for _, location := range s.Locations {
		if filterFn != nil && !filterFn(location) {
			continue
		}
		if location.isDiskSpaceLow {
			continue
		}
		currentFreeCount := location.MaxVolumeCount - int32(location.VolumesLen())
		currentFreeCount *= erasure_coding.DataShardsCount
		currentFreeCount -= int32(location.EcShardCount())
		currentFreeCount /= erasure_coding.DataShardsCount
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}
func (s *Store) addVolume(vid needle.VolumeId, collection string, needleMapKind NeedleMapKind, replicaPlacement *super_block.ReplicaPlacement, ttl *needle.TTL, preallocate int64, memoryMapMaxSizeMb uint32, diskType DiskType, ldbTimeout int64) error {
	if s.findVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}
	if location := s.FindFreeLocation(func(location *DiskLocation) bool {
		return location.DiskType == diskType
	}); location != nil {
		glog.V(0).Infof("In dir %s adds volume:%v collection:%s replicaPlacement:%v ttl:%v",
			location.Directory, vid, collection, replicaPlacement, ttl)
		if volume, err := NewVolume(location.Directory, location.IdxDirectory, collection, vid, needleMapKind, replicaPlacement, ttl, preallocate, memoryMapMaxSizeMb, ldbTimeout); err == nil {
			location.SetVolume(vid, volume)
			glog.V(0).Infof("add volume %d", vid)
			s.NewVolumesChan <- master_pb.VolumeShortInformationMessage{
				Id:               uint32(vid),
				Collection:       collection,
				ReplicaPlacement: uint32(replicaPlacement.Byte()),
				Version:          uint32(volume.Version()),
				Ttl:              ttl.ToUint32(),
				DiskType:         string(diskType),
			}
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Store) VolumeInfos() (allStats []*VolumeInfo) {
	for _, location := range s.Locations {
		stats := collectStatsForOneLocation(location)
		allStats = append(allStats, stats...)
	}
	sortVolumeInfos(allStats)
	return allStats
}

func collectStatsForOneLocation(location *DiskLocation) (stats []*VolumeInfo) {
	location.volumesLock.RLock()
	defer location.volumesLock.RUnlock()

	for k, v := range location.volumes {
		s := collectStatForOneVolume(k, v)
		stats = append(stats, s)
	}
	return stats
}

func collectStatForOneVolume(vid needle.VolumeId, v *Volume) (s *VolumeInfo) {

	s = &VolumeInfo{
		Id:               vid,
		Collection:       v.Collection,
		ReplicaPlacement: v.ReplicaPlacement,
		Version:          v.Version(),
		ReadOnly:         v.IsReadOnly(),
		Ttl:              v.Ttl,
		CompactRevision:  uint32(v.CompactionRevision),
		DiskType:         v.DiskType().String(),
	}
	s.RemoteStorageName, s.RemoteStorageKey = v.RemoteStorageNameKey()

	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	if v.nm == nil {
		return
	}

	s.FileCount = v.nm.FileCount()
	s.DeleteCount = v.nm.DeletedCount()
	s.DeletedByteCount = v.nm.DeletedSize()
	s.Size = v.nm.ContentSize()

	return
}

func (s *Store) SetDataCenter(dataCenter string) {
	s.dataCenter = dataCenter
}
func (s *Store) SetRack(rack string) {
	s.rack = rack
}
func (s *Store) GetDataCenter() string {
	return s.dataCenter
}
func (s *Store) GetRack() string {
	return s.rack
}

func (s *Store) CollectHeartbeat() *master_pb.Heartbeat {
	var volumeMessages []*master_pb.VolumeInformationMessage
	maxVolumeCounts := make(map[string]uint32)
	var maxFileKey NeedleId
	collectionVolumeSize := make(map[string]int64)
	collectionVolumeDeletedBytes := make(map[string]int64)
	collectionVolumeReadOnlyCount := make(map[string]map[string]uint8)
	for _, location := range s.Locations {
		var deleteVids []needle.VolumeId
		maxVolumeCounts[string(location.DiskType)] += uint32(location.MaxVolumeCount)
		location.volumesLock.RLock()
		for _, v := range location.volumes {
			curMaxFileKey, volumeMessage := v.ToVolumeInformationMessage()
			if volumeMessage == nil {
				continue
			}
			if maxFileKey < curMaxFileKey {
				maxFileKey = curMaxFileKey
			}
			shouldDeleteVolume := false

			if v.lastIoError != nil {
				deleteVids = append(deleteVids, v.Id)
				shouldDeleteVolume = true
				glog.Warningf("volume %d has IO error: %v", v.Id, v.lastIoError)
			} else {
				if !v.expired(volumeMessage.Size, s.GetVolumeSizeLimit()) {
					volumeMessages = append(volumeMessages, volumeMessage)
				} else {
					if v.expiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
						if !shouldDeleteVolume {
							deleteVids = append(deleteVids, v.Id)
							shouldDeleteVolume = true
						}
					} else {
						glog.V(0).Infof("volume %d is expired", v.Id)
					}
				}
			}

			if _, exist := collectionVolumeSize[v.Collection]; !exist {
				collectionVolumeSize[v.Collection] = 0
				collectionVolumeDeletedBytes[v.Collection] = 0
			}
			if !shouldDeleteVolume {
				collectionVolumeSize[v.Collection] += int64(volumeMessage.Size)
				collectionVolumeDeletedBytes[v.Collection] += int64(volumeMessage.DeletedByteCount)
			} else {
				collectionVolumeSize[v.Collection] -= int64(volumeMessage.Size)
				if collectionVolumeSize[v.Collection] <= 0 {
					delete(collectionVolumeSize, v.Collection)
				}
			}

			if _, exist := collectionVolumeReadOnlyCount[v.Collection]; !exist {
				collectionVolumeReadOnlyCount[v.Collection] = map[string]uint8{
					stats.IsReadOnly:       0,
					stats.NoWriteOrDelete:  0,
					stats.NoWriteCanDelete: 0,
					stats.IsDiskSpaceLow:   0,
				}
			}
			if !shouldDeleteVolume && v.IsReadOnly() {
				collectionVolumeReadOnlyCount[v.Collection][stats.IsReadOnly] += 1
				if v.noWriteOrDelete {
					collectionVolumeReadOnlyCount[v.Collection][stats.NoWriteOrDelete] += 1
				}
				if v.noWriteCanDelete {
					collectionVolumeReadOnlyCount[v.Collection][stats.NoWriteCanDelete] += 1
				}
				if v.location.isDiskSpaceLow {
					collectionVolumeReadOnlyCount[v.Collection][stats.IsDiskSpaceLow] += 1
				}
			}
		}
		location.volumesLock.RUnlock()

		if len(deleteVids) > 0 {
			// delete expired volumes.
			location.volumesLock.Lock()
			for _, vid := range deleteVids {
				found, err := location.deleteVolumeById(vid, false)
				if err == nil {
					if found {
						glog.V(0).Infof("volume %d is deleted", vid)
					}
				} else {
					glog.Warningf("delete volume %d: %v", vid, err)
				}
			}
			location.volumesLock.Unlock()
		}
	}

	// delete expired ec volumes
	ecVolumeMessages, deletedEcVolumes := s.deleteExpiredEcVolumes()

	var uuidList []string
	for _, loc := range s.Locations {
		uuidList = append(uuidList, loc.DirectoryUuid)
	}

	for col, size := range collectionVolumeSize {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "normal").Set(float64(size))
	}

	for col, deletedBytes := range collectionVolumeDeletedBytes {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "deleted_bytes").Set(float64(deletedBytes))
	}

	for col, types := range collectionVolumeReadOnlyCount {
		for t, count := range types {
			stats.VolumeServerReadOnlyVolumeGauge.WithLabelValues(col, t).Set(float64(count))
		}
	}

	return &master_pb.Heartbeat{
		Ip:              s.Ip,
		Port:            uint32(s.Port),
		GrpcPort:        uint32(s.GrpcPort),
		PublicUrl:       s.PublicUrl,
		MaxVolumeCounts: maxVolumeCounts,
		MaxFileKey:      NeedleIdToUint64(maxFileKey),
		DataCenter:      s.dataCenter,
		Rack:            s.rack,
		Volumes:         volumeMessages,
		DeletedEcShards: deletedEcVolumes,
		HasNoVolumes:    len(volumeMessages) == 0,
		HasNoEcShards:   len(ecVolumeMessages) == 0,
		LocationUuids:   uuidList,
	}

}

func (s *Store) deleteExpiredEcVolumes() (ecShards, deleted []*master_pb.VolumeEcShardInformationMessage) {
	for _, location := range s.Locations {
		// Collect ecVolume to be deleted
		var toDeleteEvs []*erasure_coding.EcVolume
		location.ecVolumesLock.RLock()
		for _, ev := range location.ecVolumes {
			if ev.IsTimeToDestroy() {
				toDeleteEvs = append(toDeleteEvs, ev)
			} else {
				messages := ev.ToVolumeEcShardInformationMessage()
				ecShards = append(ecShards, messages...)
			}
		}
		location.ecVolumesLock.RUnlock()

		// Delete expired volumes
		for _, ev := range toDeleteEvs {
			messages := ev.ToVolumeEcShardInformationMessage()
			// deleteEcVolumeById has its own lock
			err := location.deleteEcVolumeById(ev.VolumeId)
			if err != nil {
				ecShards = append(ecShards, messages...)
				glog.Errorf("delete EcVolume err %d: %v", ev.VolumeId, err)
				continue
			}
			// No need for additional lock here since we only need the messages
			// from volumes that were already collected
			deleted = append(deleted, messages...)
		}
	}
	return
}

func (s *Store) SetStopping() {
	s.isStopping = true
	for _, location := range s.Locations {
		location.SetStopping()
	}
}

func (s *Store) LoadNewVolumes() {
	for _, location := range s.Locations {
		location.loadExistingVolumes(s.NeedleMapKind, 0)
	}
}

func (s *Store) Close() {
	for _, location := range s.Locations {
		location.Close()
	}
}

func (s *Store) WriteVolumeNeedle(i needle.VolumeId, n *needle.Needle, checkCookie bool, fsync bool) (isUnchanged bool, err error) {
	if v := s.findVolume(i); v != nil {
		if v.IsReadOnly() {
			err = fmt.Errorf("volume %d is read only", i)
			return
		}
		_, _, isUnchanged, err = v.writeNeedle2(n, checkCookie, fsync || s.isStopping)
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("volume %d not found on %s:%d", i, s.Ip, s.Port)
	return
}

func (s *Store) DeleteVolumeNeedle(i needle.VolumeId, n *needle.Needle) (Size, error) {
	if v := s.findVolume(i); v != nil {
		if v.noWriteOrDelete {
			return 0, fmt.Errorf("volume %d is read only", i)
		}
		return v.deleteNeedle2(n)
	}
	return 0, fmt.Errorf("volume %d not found on %s:%d", i, s.Ip, s.Port)
}

func (s *Store) ReadVolumeNeedle(i needle.VolumeId, n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size Size)) (int, error) {
	if v := s.findVolume(i); v != nil {
		return v.readNeedle(n, readOption, onReadSizeFn)
	}
	return 0, fmt.Errorf("volume %d not found", i)
}

func (s *Store) ReadVolumeNeedleMetaAt(i needle.VolumeId, n *needle.Needle, offset int64, size int32) error {
	if v := s.findVolume(i); v != nil {
		return v.readNeedleMetaAt(n, offset, size)
	}
	return fmt.Errorf("volume %d not found", i)
}

func (s *Store) ReadVolumeNeedleDataInto(i needle.VolumeId, n *needle.Needle, readOption *ReadOption, writer io.Writer, offset int64, size int64) error {
	if v := s.findVolume(i); v != nil {
		return v.readNeedleDataInto(n, readOption, writer, offset, size)
	}
	return fmt.Errorf("volume %d not found", i)
}
func (s *Store) GetVolume(i needle.VolumeId) *Volume {
	return s.findVolume(i)
}

func (s *Store) HasVolume(i needle.VolumeId) bool {
	v := s.findVolume(i)
	return v != nil
}

func (s *Store) MarkVolumeReadonly(i needle.VolumeId, persist bool) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("volume %d not found", i)
	}
	v.noWriteLock.Lock()
	v.noWriteOrDelete = true
	if persist {
		v.PersistReadOnly(true)
	}
	v.noWriteLock.Unlock()
	return nil
}

func (s *Store) MarkVolumeWritable(i needle.VolumeId) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("volume %d not found", i)
	}
	v.noWriteLock.Lock()
	v.noWriteOrDelete = false
	v.PersistReadOnly(false)
	v.noWriteLock.Unlock()
	return nil
}

func (s *Store) MountVolume(i needle.VolumeId) error {
	for _, location := range s.Locations {
		if found := location.LoadVolume(i, s.NeedleMapKind); found == true {
			glog.V(0).Infof("mount volume %d", i)
			v := s.findVolume(i)
			s.NewVolumesChan <- master_pb.VolumeShortInformationMessage{
				Id:               uint32(v.Id),
				Collection:       v.Collection,
				ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
				Version:          uint32(v.Version()),
				Ttl:              v.Ttl.ToUint32(),
				DiskType:         string(v.location.DiskType),
			}
			return nil
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

func (s *Store) UnmountVolume(i needle.VolumeId) error {
	v := s.findVolume(i)
	if v == nil {
		return nil
	}
	message := master_pb.VolumeShortInformationMessage{
		Id:               uint32(v.Id),
		Collection:       v.Collection,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		DiskType:         string(v.location.DiskType),
	}

	for _, location := range s.Locations {
		err := location.UnloadVolume(i)
		if err == nil {
			glog.V(0).Infof("UnmountVolume %d", i)
			s.DeletedVolumesChan <- message
			return nil
		} else if err == ErrVolumeNotFound {
			continue
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

func (s *Store) DeleteVolume(i needle.VolumeId, onlyEmpty bool) error {
	v := s.findVolume(i)
	if v == nil {
		return fmt.Errorf("delete volume %d not found on disk", i)
	}
	message := master_pb.VolumeShortInformationMessage{
		Id:               uint32(v.Id),
		Collection:       v.Collection,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		DiskType:         string(v.location.DiskType),
	}
	for _, location := range s.Locations {
		err := location.DeleteVolume(i, onlyEmpty)
		if err == nil {
			glog.V(0).Infof("DeleteVolume %d", i)
			s.DeletedVolumesChan <- message
			return nil
		} else if err == ErrVolumeNotFound {
			continue
		} else if err == ErrVolumeNotEmpty {
			return fmt.Errorf("DeleteVolume %d: %v", i, err)
		} else {
			glog.Errorf("DeleteVolume %d: %v", i, err)
		}
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

func (s *Store) ConfigureVolume(i needle.VolumeId, replication string) error {

	for _, location := range s.Locations {
		fileInfo, found := location.LocateVolume(i)
		if !found {
			continue
		}
		// load, modify, save
		baseFileName := strings.TrimSuffix(fileInfo.Name(), filepath.Ext(fileInfo.Name()))
		vifFile := filepath.Join(location.Directory, baseFileName+".vif")
		volumeInfo, _, _, err := volume_info.MaybeLoadVolumeInfo(vifFile)
		if err != nil {
			return fmt.Errorf("volume %d failed to load vif: %v", i, err)
		}
		volumeInfo.Replication = replication
		err = volume_info.SaveVolumeInfo(vifFile, volumeInfo)
		if err != nil {
			return fmt.Errorf("volume %d failed to save vif: %v", i, err)
		}
		return nil
	}

	return fmt.Errorf("volume %d not found on disk", i)
}

func (s *Store) SetVolumeSizeLimit(x uint64) {
	atomic.StoreUint64(&s.volumeSizeLimit, x)
}

func (s *Store) GetVolumeSizeLimit() uint64 {
	return atomic.LoadUint64(&s.volumeSizeLimit)
}

func (s *Store) SetPreallocate(x bool) {
	s.preallocate.Store(x)
}

func (s *Store) GetPreallocate() bool {
	return s.preallocate.Load()
}

func (s *Store) MaybeAdjustVolumeMax() (hasChanges bool) {
	volumeSizeLimit := s.GetVolumeSizeLimit()
	if volumeSizeLimit == 0 {
		return
	}
	var newMaxVolumeCount int32
	for _, diskLocation := range s.Locations {
		if diskLocation.OriginalMaxVolumeCount == 0 {
			currentMaxVolumeCount := atomic.LoadInt32(&diskLocation.MaxVolumeCount)
			diskStatus := stats.NewDiskStatus(diskLocation.Directory)
			var unusedSpace uint64 = 0
			unclaimedSpaces := int64(diskStatus.Free)
			if !s.GetPreallocate() {
				unusedSpace = diskLocation.UnUsedSpace(volumeSizeLimit)
				unclaimedSpaces -= int64(unusedSpace)
			}
			volCount := diskLocation.VolumesLen()
			ecShardCount := diskLocation.EcShardCount()
			maxVolumeCount := int32(volCount) + int32((ecShardCount+erasure_coding.DataShardsCount)/erasure_coding.DataShardsCount)
			if unclaimedSpaces > int64(volumeSizeLimit) {
				maxVolumeCount += int32(uint64(unclaimedSpaces)/volumeSizeLimit) - 1
			}
			newMaxVolumeCount = newMaxVolumeCount + maxVolumeCount
			atomic.StoreInt32(&diskLocation.MaxVolumeCount, maxVolumeCount)
			glog.V(4).Infof("disk %s max %d unclaimedSpace:%dMB, unused:%dMB volumeSizeLimit:%dMB",
				diskLocation.Directory, maxVolumeCount, unclaimedSpaces/1024/1024, unusedSpace/1024/1024, volumeSizeLimit/1024/1024)
			hasChanges = hasChanges || currentMaxVolumeCount != atomic.LoadInt32(&diskLocation.MaxVolumeCount)
		} else {
			newMaxVolumeCount = newMaxVolumeCount + diskLocation.OriginalMaxVolumeCount
		}
	}
	stats.VolumeServerMaxVolumeCounter.Set(float64(newMaxVolumeCount))
	return
}
