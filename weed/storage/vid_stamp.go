package storage

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const vidStampFileName = ".vid_stamp"

type vidStamp struct {
	MaxVolumeId uint32 `json:"maxVolumeId"`
	TopologyId  string `json:"topologyId,omitempty"`
}

func (s *Store) MaxVolumeIdSeen() needle.VolumeId {
	return needle.VolumeId(s.maxVolumeIdSeen.Load())
}

func (s *Store) LastSeenTopologyId() string {
	if v := s.lastSeenTopologyId.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// UpdateMaxVolumeIdSeen raises the server's persisted bound and stamps every
// volume dir when it grows. Feeds: heartbeat echoes, AllocateVolume, local
// volume and EC shard creation.
func (s *Store) UpdateMaxVolumeIdSeen(vid needle.VolumeId) {
	grew := false
	for {
		current := s.maxVolumeIdSeen.Load()
		if current >= uint32(vid) {
			break
		}
		if s.maxVolumeIdSeen.CompareAndSwap(current, uint32(vid)) {
			grew = true
			break
		}
	}
	if s.vidStampReady.Load() && (grew || s.vidStampDirty.Load()) {
		s.saveVidStamps()
	}
}

func (s *Store) UpdateLastSeenTopologyId(id string) {
	if id == "" || (s.LastSeenTopologyId() == id && !s.vidStampDirty.Load()) {
		return
	}
	s.lastSeenTopologyId.Store(id)
	if s.vidStampReady.Load() {
		s.saveVidStamps()
	}
}

// bootstrapMaxVolumeIdSeen seeds the bound from persisted stamps plus the max
// of every volume and EC shard id on disk, so a server upgraded mid-incident
// still reports a bound covering its own holdings.
func (s *Store) bootstrapMaxVolumeIdSeen() {
	var maxVid needle.VolumeId
	for _, location := range s.Locations {
		if stamp, err := readVidStamp(location.Directory); err == nil {
			if needle.VolumeId(stamp.MaxVolumeId) > maxVid {
				maxVid = needle.VolumeId(stamp.MaxVolumeId)
			}
			if stamp.TopologyId != "" && s.LastSeenTopologyId() == "" {
				s.lastSeenTopologyId.Store(stamp.TopologyId)
			}
		}
		location.volumesLock.RLock()
		for vid := range location.volumes {
			if vid > maxVid {
				maxVid = vid
			}
		}
		location.volumesLock.RUnlock()
		location.ecVolumesLock.RLock()
		for vid := range location.ecVolumes {
			if vid > maxVid {
				maxVid = vid
			}
		}
		location.ecVolumesLock.RUnlock()
	}
	s.maxVolumeIdSeen.Store(uint32(maxVid))
	s.vidStampReady.Store(true)
	s.saveVidStamps()
}

func (s *Store) saveVidStamps() {
	s.vidStampLock.Lock()
	defer s.vidStampLock.Unlock()
	stamp, err := json.Marshal(vidStamp{
		MaxVolumeId: s.maxVolumeIdSeen.Load(),
		TopologyId:  s.LastSeenTopologyId(),
	})
	if err != nil {
		return
	}
	failed := false
	for _, location := range s.Locations {
		// The stamp lives on the data directory, which stays writable when
		// only IdxDirectory is read-only.
		if util.IsReadOnlyMount(location.Directory) {
			continue
		}
		if err := writeVidStamp(location.Directory, stamp); err != nil {
			glog.Warningf("write vid stamp in %s: %v", location.Directory, err)
			failed = true
		}
	}
	s.vidStampDirty.Store(failed)
}

func readVidStamp(dir string) (*vidStamp, error) {
	data, err := os.ReadFile(filepath.Join(dir, vidStampFileName))
	if err != nil {
		return nil, err
	}
	stamp := &vidStamp{}
	if err := json.Unmarshal(data, stamp); err != nil {
		return nil, err
	}
	return stamp, nil
}

func writeVidStamp(dir string, stamp []byte) error {
	tmp := filepath.Join(dir, vidStampFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(stamp); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, vidStampFileName)); err != nil {
		return err
	}
	return util.FsyncDir(dir)
}
