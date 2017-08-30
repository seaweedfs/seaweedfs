package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// The volume sync with a master volume via 2 steps:
// 1. The slave checks master side to find subscription checkpoint
//	  to setup the replication.
// 2. The slave receives the updates from master

/*
Assume the slave volume needs to follow the master volume.

The master volume could be compacted, and could be many files ahead of
slave volume.

Step 1:
The slave volume will ask the master volume for a snapshot
of (existing file entries, last offset, number of compacted times).

For each entry x in master existing file entries:
  if x does not exist locally:
    add x locally

For each entry y in local slave existing file entries:
  if y does not exist on master:
    delete y locally

Step 2:
After this, use the last offset and number of compacted times to request
the master volume to send a new file, and keep looping. If the number of
compacted times is changed, go back to step 1 (very likely this can be
optimized more later).

*/

func (v *Volume) Synchronize(volumeServer string) (err error) {
	var lastCompactRevision uint16 = 0
	var compactRevision uint16 = 0
	var masterMap *needle.CompactMap
	for i := 0; i < 3; i++ {
		if masterMap, _, compactRevision, err = fetchVolumeFileEntries(volumeServer, v.Id); err != nil {
			return fmt.Errorf("Failed to sync volume %d entries with %s: %v", v.Id, volumeServer, err)
		}
		if lastCompactRevision != compactRevision && lastCompactRevision != 0 {
			if err = v.Compact(0); err != nil {
				return fmt.Errorf("Compact Volume before synchronizing %v", err)
			}
			if err = v.commitCompact(); err != nil {
				return fmt.Errorf("Commit Compact before synchronizing %v", err)
			}
		}
		lastCompactRevision = compactRevision
		if err = v.trySynchronizing(volumeServer, masterMap, compactRevision); err == nil {
			return
		}
	}
	return
}

type ByOffset []needle.NeedleValue

func (a ByOffset) Len() int           { return len(a) }
func (a ByOffset) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByOffset) Less(i, j int) bool { return a[i].Offset < a[j].Offset }

// trySynchronizing sync with remote volume server incrementally by
// make up the local and remote delta.
func (v *Volume) trySynchronizing(volumeServer string, masterMap *needle.CompactMap, compactRevision uint16) error {
	slaveIdxFile, err := os.Open(v.nm.IndexFileName())
	if err != nil {
		return fmt.Errorf("Open volume %d index file: %v", v.Id, err)
	}
	defer slaveIdxFile.Close()
	slaveMap, err := LoadBtreeNeedleMap(slaveIdxFile)
	if err != nil {
		return fmt.Errorf("Load volume %d index file: %v", v.Id, err)
	}
	var delta []needle.NeedleValue
	if err := masterMap.Visit(func(needleValue needle.NeedleValue) error {
		if needleValue.Key == 0 {
			return nil
		}
		if _, ok := slaveMap.Get(uint64(needleValue.Key)); ok {
			return nil // skip intersection
		}
		delta = append(delta, needleValue)
		return nil
	}); err != nil {
		return fmt.Errorf("Add master entry: %v", err)
	}
	if err := slaveMap.m.Visit(func(needleValue needle.NeedleValue) error {
		if needleValue.Key == 0 {
			return nil
		}
		if _, ok := masterMap.Get(needleValue.Key); ok {
			return nil // skip intersection
		}
		needleValue.Size = 0
		delta = append(delta, needleValue)
		return nil
	}); err != nil {
		return fmt.Errorf("Remove local entry: %v", err)
	}

	// simulate to same ordering of remote .dat file needle entries
	sort.Sort(ByOffset(delta))

	// make up the delta
	fetchCount := 0
	volumeDataContentHandlerUrl := "http://" + volumeServer + "/admin/sync/data"
	for _, needleValue := range delta {
		if needleValue.Size == 0 {
			// remove file entry from local
			v.removeNeedle(needleValue.Key)
			continue
		}
		// add master file entry to local data file
		if err := v.fetchNeedle(volumeDataContentHandlerUrl, needleValue, compactRevision); err != nil {
			glog.V(0).Infof("Fetch needle %v from %s: %v", needleValue, volumeServer, err)
			return err
		}
		fetchCount++
	}
	glog.V(1).Infof("Fetched %d needles from %s", fetchCount, volumeServer)
	return nil
}

func fetchVolumeFileEntries(volumeServer string, vid VolumeId) (m *needle.CompactMap, lastOffset uint64, compactRevision uint16, err error) {
	m = needle.NewCompactMap()

	syncStatus, err := operation.GetVolumeSyncStatus(volumeServer, vid.String())
	if err != nil {
		return m, 0, 0, err
	}

	total := 0
	err = operation.GetVolumeIdxEntries(volumeServer, vid.String(), func(key uint64, offset, size uint32) {
		// println("remote key", key, "offset", offset*NeedlePaddingSize, "size", size)
		if offset > 0 && size != TombstoneFileSize {
			m.Set(needle.Key(key), offset, size)
		} else {
			m.Delete(needle.Key(key))
		}
		total++
	})

	glog.V(2).Infof("server %s volume %d, entries %d, last offset %d, revision %d", volumeServer, vid, total, syncStatus.TailOffset, syncStatus.CompactRevision)
	return m, syncStatus.TailOffset, syncStatus.CompactRevision, err

}

func (v *Volume) GetVolumeSyncStatus() operation.SyncVolumeResponse {
	var syncStatus = operation.SyncVolumeResponse{}
	if stat, err := v.dataFile.Stat(); err == nil {
		syncStatus.TailOffset = uint64(stat.Size())
	}
	syncStatus.IdxFileSize = v.nm.IndexFileSize()
	syncStatus.CompactRevision = v.SuperBlock.CompactRevision
	syncStatus.Ttl = v.SuperBlock.Ttl.String()
	syncStatus.Replication = v.SuperBlock.ReplicaPlacement.String()
	return syncStatus
}

func (v *Volume) IndexFileContent() ([]byte, error) {
	return v.nm.IndexFileContent()
}

// removeNeedle removes one needle by needle key
func (v *Volume) removeNeedle(key needle.Key) {
	n := new(Needle)
	n.Id = uint64(key)
	v.deleteNeedle(n)
}

// fetchNeedle fetches a remote volume needle by vid, id, offset
// The compact revision is checked first in case the remote volume
// is compacted and the offset is invalid any more.
func (v *Volume) fetchNeedle(volumeDataContentHandlerUrl string,
	needleValue needle.NeedleValue, compactRevision uint16) error {
	// add master file entry to local data file
	values := make(url.Values)
	values.Add("revision", strconv.Itoa(int(compactRevision)))
	values.Add("volume", v.Id.String())
	values.Add("id", needleValue.Key.String())
	values.Add("offset", strconv.FormatUint(uint64(needleValue.Offset), 10))
	values.Add("size", strconv.FormatUint(uint64(needleValue.Size), 10))
	glog.V(4).Infof("Fetch %+v", needleValue)
	return util.GetUrlStream(volumeDataContentHandlerUrl, values, func(r io.Reader) error {
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf("Reading from %s error: %v", volumeDataContentHandlerUrl, err)
		}
		offset, err := v.AppendBlob(b)
		if err != nil {
			return fmt.Errorf("Appending volume %d error: %v", v.Id, err)
		}
		// println("add key", needleValue.Key, "offset", offset, "size", needleValue.Size)
		v.nm.Put(uint64(needleValue.Key), uint32(offset/NeedlePaddingSize), needleValue.Size)
		return nil
	})
}
