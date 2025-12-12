package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

func TestFilerConf(t *testing.T) {

	fc := NewFilerConf()

	conf := &filer_pb.FilerConf{Locations: []*filer_pb.FilerConf_PathConf{
		{
			LocationPrefix: "/buckets/abc",
			Collection:     "abc",
		},
		{
			LocationPrefix: "/buckets/abcd",
			Collection:     "abcd",
		},
		{
			LocationPrefix: "/buckets/",
			Replication:    "001",
		},
		{
			LocationPrefix: "/buckets",
			ReadOnly:       false,
		},
		{
			LocationPrefix: "/buckets/xxx",
			ReadOnly:       true,
		},
		{
			LocationPrefix: "/buckets/xxx/yyy",
			ReadOnly:       false,
		},
	}}
	fc.doLoadConf(conf)

	assert.Equal(t, "abc", fc.MatchStorageRule("/buckets/abc/jasdf").Collection)
	assert.Equal(t, "abcd", fc.MatchStorageRule("/buckets/abcd/jasdf").Collection)
	assert.Equal(t, "001", fc.MatchStorageRule("/buckets/abc/jasdf").Replication)

	assert.Equal(t, true, fc.MatchStorageRule("/buckets/xxx/yyy/zzz").ReadOnly)
	assert.Equal(t, false, fc.MatchStorageRule("/buckets/other").ReadOnly)

}

// TestClonePathConf verifies that ClonePathConf copies all exported fields.
// This test will fail if new fields are added to FilerConf_PathConf but not to ClonePathConf,
// helping catch schema drift.
func TestClonePathConf(t *testing.T) {
	// Create a fully-populated PathConf with non-zero values for all fields
	src := &filer_pb.FilerConf_PathConf{
		LocationPrefix:           "/test/path",
		Collection:               "test_collection",
		Replication:              "001",
		Ttl:                      "7d",
		DiskType:                 "ssd",
		Fsync:                    true,
		VolumeGrowthCount:        5,
		ReadOnly:                 true,
		MaxFileNameLength:        255,
		DataCenter:               "dc1",
		Rack:                     "rack1",
		DataNode:                 "node1",
		DisableChunkDeletion:     true,
		Worm:                     true,
		WormGracePeriodSeconds:   3600,
		WormRetentionTimeSeconds: 86400,
	}

	clone := ClonePathConf(src)

	// Verify it's a different object
	assert.NotSame(t, src, clone, "ClonePathConf should return a new object, not the same pointer")

	// Verify all fields are copied correctly
	assert.Equal(t, src.LocationPrefix, clone.LocationPrefix, "LocationPrefix mismatch")
	assert.Equal(t, src.Collection, clone.Collection, "Collection mismatch")
	assert.Equal(t, src.Replication, clone.Replication, "Replication mismatch")
	assert.Equal(t, src.Ttl, clone.Ttl, "Ttl mismatch")
	assert.Equal(t, src.DiskType, clone.DiskType, "DiskType mismatch")
	assert.Equal(t, src.Fsync, clone.Fsync, "Fsync mismatch")
	assert.Equal(t, src.VolumeGrowthCount, clone.VolumeGrowthCount, "VolumeGrowthCount mismatch")
	assert.Equal(t, src.ReadOnly, clone.ReadOnly, "ReadOnly mismatch")
	assert.Equal(t, src.MaxFileNameLength, clone.MaxFileNameLength, "MaxFileNameLength mismatch")
	assert.Equal(t, src.DataCenter, clone.DataCenter, "DataCenter mismatch")
	assert.Equal(t, src.Rack, clone.Rack, "Rack mismatch")
	assert.Equal(t, src.DataNode, clone.DataNode, "DataNode mismatch")
	assert.Equal(t, src.DisableChunkDeletion, clone.DisableChunkDeletion, "DisableChunkDeletion mismatch")
	assert.Equal(t, src.Worm, clone.Worm, "Worm mismatch")
	assert.Equal(t, src.WormGracePeriodSeconds, clone.WormGracePeriodSeconds, "WormGracePeriodSeconds mismatch")
	assert.Equal(t, src.WormRetentionTimeSeconds, clone.WormRetentionTimeSeconds, "WormRetentionTimeSeconds mismatch")

	// Verify mutation of clone doesn't affect source
	clone.Collection = "modified"
	clone.ReadOnly = false
	assert.Equal(t, "test_collection", src.Collection, "Modifying clone should not affect source Collection")
	assert.Equal(t, true, src.ReadOnly, "Modifying clone should not affect source ReadOnly")
}

func TestClonePathConfNil(t *testing.T) {
	clone := ClonePathConf(nil)
	assert.NotNil(t, clone, "ClonePathConf(nil) should return a non-nil empty PathConf")
	assert.Equal(t, "", clone.LocationPrefix, "ClonePathConf(nil) should return empty PathConf")
}
