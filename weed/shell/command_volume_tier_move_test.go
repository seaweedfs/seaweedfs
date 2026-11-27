package shell

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/assert"
)

func TestVolumeSelectionByDataCenter(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	vids, err := collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "dc2", "", 20.0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 83, len(vids))

	vids, err = collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "dc1", "", 20.0, 0)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(vids))
}

func TestFilterLocationsByDataCenter(t *testing.T) {
	_, allLocations := collectVolumeReplicaLocations(parseOutput(topoData))

	assert.Equal(t, 5, len(allLocations))
	assert.ElementsMatch(t, []string{"192.168.1.4:8080", "192.168.1.2:8080"}, locationNodeIds(filterLocationsByDataCenter(allLocations, "dc2")))
	assert.ElementsMatch(t, []string{"192.168.1.6:8080"}, locationNodeIds(filterLocationsByDataCenter(allLocations, "dc3")))
	assert.Empty(t, filterLocationsByDataCenter(allLocations, "dc9"))
}

func locationNodeIds(locations []location) (ids []string) {
	for _, loc := range locations {
		ids = append(ids, loc.dataNode.Id)
	}
	return
}

func TestDoMoveOneVolumeWithOps_KeepSource(t *testing.T) {
	locations := []wdclient.Location{
		{Url: "10.0.0.1:8080"},
		{Url: "10.0.0.2:8080"},
	}
	dst := location{
		dataNode: &master_pb.DataNodeInfo{Id: "10.0.0.3:8080"},
	}

	var liveMoveCalled, replicateCalled bool
	var deleted []pb.ServerAddress
	var restoredWritable []pb.ServerAddress

	ops := tierMoveOps{
		markReplicasWritable: func(vid needle.VolumeId, locs []wdclient.Location, writable bool) error {
			return nil
		},
		liveMove: func(writer io.Writer, vid needle.VolumeId, src, dst pb.ServerAddress, diskType string, ioBytePerSecond int64) error {
			liveMoveCalled = true
			return nil
		},
		replicate: func(writer io.Writer, vid needle.VolumeId, src, dst pb.ServerAddress, diskType string, ioBytePerSecond int64) error {
			replicateCalled = true
			assert.Equal(t, pb.ServerAddress("10.0.0.1:8080"), src)
			assert.Equal(t, pb.ServerAddress("10.0.0.3:8080"), dst)
			return nil
		},
		configureReplication: func(vid needle.VolumeId, addr pb.ServerAddress, replication string) error {
			return nil
		},
		ensureReplication: func(writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, toDataCenter string, dst location, replicationString string, ioBytePerSecond int64) (map[string]bool, error) {
			return map[string]bool{dst.dataNode.Id: true}, nil
		},
		markWritable: func(vid needle.VolumeId, addr pb.ServerAddress, writable bool) error {
			if writable {
				restoredWritable = append(restoredWritable, addr)
			}
			return nil
		},
		deleteReplica: func(vid needle.VolumeId, addr pb.ServerAddress) error {
			deleted = append(deleted, addr)
			return nil
		},
	}

	repl := ""
	var out bytes.Buffer
	err := doMoveOneVolumeWithOps(ops, &out, needle.VolumeId(42), types.SsdType, "", locations, pb.ServerAddress("10.0.0.1:8080"), dst, 0, &repl, false, true)
	assert.NoError(t, err)
	assert.False(t, liveMoveCalled, "liveMove must not be called when keepSource is true")
	assert.True(t, replicateCalled, "replicate must be called when keepSource is true")
	assert.Empty(t, deleted, "source replicas must not be deleted when keepSource is true")
	assert.ElementsMatch(t, []pb.ServerAddress{"10.0.0.1:8080", "10.0.0.2:8080"}, restoredWritable)
}

func TestDoMoveOneVolumeWithOps_DefaultDeletesSource(t *testing.T) {
	locations := []wdclient.Location{
		{Url: "10.0.0.1:8080"},
		{Url: "10.0.0.2:8080"},
	}
	dst := location{
		dataNode: &master_pb.DataNodeInfo{Id: "10.0.0.3:8080"},
	}

	var liveMoveCalled, replicateCalled bool
	var deleted []pb.ServerAddress

	ops := tierMoveOps{
		markReplicasWritable: func(vid needle.VolumeId, locs []wdclient.Location, writable bool) error {
			return nil
		},
		liveMove: func(writer io.Writer, vid needle.VolumeId, src, dst pb.ServerAddress, diskType string, ioBytePerSecond int64) error {
			liveMoveCalled = true
			return nil
		},
		replicate: func(writer io.Writer, vid needle.VolumeId, src, dst pb.ServerAddress, diskType string, ioBytePerSecond int64) error {
			replicateCalled = true
			return nil
		},
		configureReplication: func(vid needle.VolumeId, addr pb.ServerAddress, replication string) error {
			return nil
		},
		ensureReplication: func(writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, toDataCenter string, dst location, replicationString string, ioBytePerSecond int64) (map[string]bool, error) {
			return map[string]bool{dst.dataNode.Id: true}, nil
		},
		markWritable: func(vid needle.VolumeId, addr pb.ServerAddress, writable bool) error {
			return nil
		},
		deleteReplica: func(vid needle.VolumeId, addr pb.ServerAddress) error {
			deleted = append(deleted, addr)
			return nil
		},
	}

	repl := ""
	var out bytes.Buffer
	err := doMoveOneVolumeWithOps(ops, &out, needle.VolumeId(42), types.SsdType, "", locations, pb.ServerAddress("10.0.0.1:8080"), dst, 0, &repl, false, false)
	assert.NoError(t, err)
	assert.True(t, liveMoveCalled, "liveMove must be called when keepSource is false")
	assert.False(t, replicateCalled, "replicate must not be called when keepSource is false")
	assert.Equal(t, []pb.ServerAddress{"10.0.0.2:8080"}, deleted)
}

func TestDoMoveOneVolumeWithOps_KeepSourceCopyFailureRestoresWritable(t *testing.T) {
	locations := []wdclient.Location{
		{Url: "10.0.0.1:8080"},
		{Url: "10.0.0.2:8080"},
	}
	dst := location{
		dataNode: &master_pb.DataNodeInfo{Id: "10.0.0.3:8080"},
	}

	var writableStates []bool
	ops := tierMoveOps{
		markReplicasWritable: func(vid needle.VolumeId, locs []wdclient.Location, writable bool) error {
			writableStates = append(writableStates, writable)
			return nil
		},
		replicate: func(writer io.Writer, vid needle.VolumeId, src, dst pb.ServerAddress, diskType string, ioBytePerSecond int64) error {
			return errors.New("copy failed")
		},
	}

	repl := ""
	var out bytes.Buffer
	err := doMoveOneVolumeWithOps(ops, &out, needle.VolumeId(42), types.SsdType, "", locations, pb.ServerAddress("10.0.0.1:8080"), dst, 0, &repl, false, true)
	assert.ErrorContains(t, err, "copy volume 42")
	assert.Equal(t, []bool{false, true}, writableStates)
}
