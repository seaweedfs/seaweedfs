package storage

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func (v *Volume) maybeWriteSuperBlock(ver needle.Version) error {

	datSize, _, e := v.DataBackend.GetStat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %v", v.DataBackend.Name(), e)
		return e
	}
	if datSize == 0 {
		if !needle.IsSupportedVersion(ver) {
			return fmt.Errorf("volume super block version %d is not supported", ver)
		}
		v.SuperBlock.Version = ver
		_, e = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0)
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			var dataFile *os.File
			if dataFile, e = backend.OpenVolumeFile(v.DataBackend.Name(), os.O_RDWR|os.O_CREATE|os.O_TRUNC); e == nil {
				v.DataBackend = backend.NewDiskFile(dataFile)
				if _, e = v.DataBackend.WriteAt(v.SuperBlock.Bytes(), 0); e == nil {
					v.noWriteLock.Lock()
					v.noWriteOrDelete = false
					v.noWriteCanDelete = false
					v.noWriteLock.Unlock()
				}
			}
		}
	}
	return e
}

func (v *Volume) readSuperBlock() (err error) {
	if v.HasRemoteFile() && len(v.volumeInfo.GetFiles()) > 0 {
		v.SuperBlock, err = readRemoteSuperBlock(v.volumeInfo.GetFiles()[0].BackendName(), v.DataBackend)
	} else {
		v.SuperBlock, err = super_block.ReadSuperBlock(v.DataBackend)
	}
	if v.volumeInfo != nil && v.volumeInfo.Replication != "" {
		if replication, err := super_block.NewReplicaPlacementFromString(v.volumeInfo.Replication); err != nil {
			return fmt.Errorf("Error parse volume %d replication %s : %v", v.Id, v.volumeInfo.Replication, err)
		} else {
			v.SuperBlock.ReplicaPlacement = replication
		}
	}
	return err
}

// A tiered volume loads without its superblock when the backend cannot serve
// it; once a read stalls, the backend's other volumes skip it for a while.
var (
	remoteSuperBlockTimeout     = 10 * time.Second
	remoteSuperBlockStallPeriod = time.Minute
	remoteSuperBlockStalls      sync.Map
)

func readRemoteSuperBlock(backendName string, datBackend backend.BackendStorageFile) (super_block.SuperBlock, error) {
	if until, ok := remoteSuperBlockStalls.Load(backendName); ok && time.Now().Before(until.(time.Time)) {
		return super_block.SuperBlock{}, fmt.Errorf("backend %s stalled on a super block read, skipped", backendName)
	}
	type result struct {
		superBlock super_block.SuperBlock
		err        error
	}
	done := make(chan result, 1)
	go func() {
		sb, err := super_block.ReadSuperBlock(datBackend)
		done <- result{sb, err}
	}()
	timer := time.NewTimer(remoteSuperBlockTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		if r.err == nil {
			remoteSuperBlockStalls.Delete(backendName)
		}
		return r.superBlock, r.err
	case <-timer.C:
		remoteSuperBlockStalls.Store(backendName, time.Now().Add(remoteSuperBlockStallPeriod))
		return super_block.SuperBlock{}, fmt.Errorf("backend %s did not return the super block within %v", backendName, remoteSuperBlockTimeout)
	}
}
