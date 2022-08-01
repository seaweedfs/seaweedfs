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
