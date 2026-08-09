package storage

import (
	"fmt"
	"sort"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type VolumeInfo struct {
	Id                needle.VolumeId
	Size              uint64
	ReplicaPlacement  *super_block.ReplicaPlacement
	Ttl               *needle.TTL
	DiskType          string
	DiskId            uint32
	Collection        string
	Version           needle.Version
	FileCount         int
	DeleteCount       int
	DeletedByteCount  uint64
	ReadOnly          bool
	CompactRevision   uint32
	ModifiedAtSecond  int64
	RemoteStorageName string
	RemoteStorageKey  string
}

func NewVolumeInfo(m *master_pb.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:                needle.VolumeId(m.Id),
		Size:              m.Size,
		Collection:        internVolumeString(m.Collection),
		FileCount:         int(m.FileCount),
		DeleteCount:       int(m.DeleteCount),
		DeletedByteCount:  m.DeletedByteCount,
		ReadOnly:          m.ReadOnly,
		Version:           needle.Version(m.Version),
		CompactRevision:   m.CompactRevision,
		ModifiedAtSecond:  m.ModifiedAtSecond,
		RemoteStorageName: internVolumeString(m.RemoteStorageName),
		RemoteStorageKey:  m.RemoteStorageKey,
		DiskType:          internVolumeString(m.DiskType),
		DiskId:            m.DiskId,
	}
	rp, e := super_block.NewReplicaPlacementFromByte(byte(m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	vi.Ttl = needle.LoadTTLFromUint32(m.Ttl)
	return vi, nil
}

func NewVolumeInfoFromShort(m *master_pb.VolumeShortInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:         needle.VolumeId(m.Id),
		Collection: internVolumeString(m.Collection),
		Version:    needle.Version(m.Version),
	}
	rp, e := super_block.NewReplicaPlacementFromByte(byte(m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	vi.Ttl = needle.LoadTTLFromUint32(m.Ttl)
	vi.DiskType = internVolumeString(m.DiskType)
	return vi, nil
}

// internedVolumeStrings holds one copy of each value a cluster repeats across
// its volumes. It only ever grows, which is why it must stay restricted to
// values drawn from a small set: collection, disk type, remote backend. A
// cluster with ten thousand collections keeps a few hundred kilobytes here.
//
// unique.Make would clear entries by weak reference, but its canonical value
// does not survive a collection even while a caller still holds the string it
// returned, so a later volume would get a second copy. Holding them is the
// point.
var (
	internedVolumeStringsLock sync.RWMutex
	internedVolumeStrings     = make(map[string]string)
)

// internVolumeString shares one copy of a repeated value. Decoding a heartbeat
// allocates a fresh string for each, so a master holding a million volumes
// otherwise holds a million copies of the same handful of names.
//
// Never for something unique per volume, such as a remote storage key: that
// would fill the table rather than share anything.
func internVolumeString(s string) string {
	if s == "" {
		return ""
	}
	internedVolumeStringsLock.RLock()
	shared, found := internedVolumeStrings[s]
	internedVolumeStringsLock.RUnlock()
	if found {
		return shared
	}

	internedVolumeStringsLock.Lock()
	defer internedVolumeStringsLock.Unlock()
	if shared, found = internedVolumeStrings[s]; found {
		return shared
	}
	internedVolumeStrings[s] = s
	return s
}

func (vi VolumeInfo) IsRemote() bool {
	return vi.RemoteStorageName != ""
}

func (vi VolumeInfo) String() string {
	s := fmt.Sprintf("Id:%d, Size:%d, ReplicaPlacement:%s, Collection:%s, Version:%v, Ttl:%s, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v, ModifiedAtSecond:%d",
		vi.Id, vi.Size, vi.ReplicaPlacement, vi.Collection, vi.Version, vi.Ttl.String(), vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly, vi.ModifiedAtSecond)
	if vi.IsRemote() {
		s += fmt.Sprintf(", RemoteStorageName:%s, RemoteStorageKey:%s", vi.RemoteStorageName, vi.RemoteStorageKey)
	}
	return s
}

func (vi VolumeInfo) ToVolumeInformationMessage() *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id:                uint32(vi.Id),
		Size:              uint64(vi.Size),
		Collection:        vi.Collection,
		FileCount:         uint64(vi.FileCount),
		DeleteCount:       uint64(vi.DeleteCount),
		DeletedByteCount:  vi.DeletedByteCount,
		ReadOnly:          vi.ReadOnly,
		ReplicaPlacement:  uint32(vi.ReplicaPlacement.Byte()),
		Version:           uint32(vi.Version),
		Ttl:               vi.Ttl.ToUint32(),
		CompactRevision:   vi.CompactRevision,
		ModifiedAtSecond:  vi.ModifiedAtSecond,
		RemoteStorageName: vi.RemoteStorageName,
		RemoteStorageKey:  vi.RemoteStorageKey,
		DiskType:          vi.DiskType,
		DiskId:            vi.DiskId,
	}
}

/*VolumesInfo sorting*/

type volumeInfos []*VolumeInfo

func (vis volumeInfos) Len() int {
	return len(vis)
}

func (vis volumeInfos) Less(i, j int) bool {
	return vis[i].Id < vis[j].Id
}

func (vis volumeInfos) Swap(i, j int) {
	vis[i], vis[j] = vis[j], vis[i]
}

func sortVolumeInfos(vis volumeInfos) {
	sort.Sort(vis)
}
