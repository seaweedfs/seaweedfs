package filer

import (
	"reflect"
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
// Uses reflection to automatically detect new fields added to the protobuf,
// ensuring the test fails if ClonePathConf is not updated for new fields.
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

	// Use reflection to compare all exported fields
	// This will automatically catch any new fields added to the protobuf
	srcVal := reflect.ValueOf(src).Elem()
	cloneVal := reflect.ValueOf(clone).Elem()
	srcType := srcVal.Type()

	for i := 0; i < srcType.NumField(); i++ {
		field := srcType.Field(i)

		// Skip unexported fields (protobuf internal fields like sizeCache, unknownFields)
		if !field.IsExported() {
			continue
		}

		srcField := srcVal.Field(i)
		cloneField := cloneVal.Field(i)

		// Compare field values
		if !reflect.DeepEqual(srcField.Interface(), cloneField.Interface()) {
			t.Errorf("Field %s not copied correctly: src=%v, clone=%v",
				field.Name, srcField.Interface(), cloneField.Interface())
		}
	}

	// Additionally verify that all exported fields in src are non-zero
	// This ensures we're testing with fully populated data
	for i := 0; i < srcType.NumField(); i++ {
		field := srcType.Field(i)
		if !field.IsExported() {
			continue
		}

		srcField := srcVal.Field(i)
		if srcField.IsZero() {
			t.Errorf("Test setup error: field %s has zero value, update test to set a non-zero value", field.Name)
		}
	}

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
