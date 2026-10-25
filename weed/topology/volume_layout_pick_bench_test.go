package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var benchLayoutSmall = `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1, "size":2000, "replication":"000"},
          {"id":2, "size":5000, "replication":"000"},
          {"id":3, "size":8000, "replication":"000"}
        ],
        "limit":10
      }
    }
  }
}
`

var benchLayoutMedium = `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1,  "size":1000, "replication":"000"},
          {"id":2,  "size":2000, "replication":"000"},
          {"id":3,  "size":3000, "replication":"000"},
          {"id":4,  "size":4000, "replication":"000"},
          {"id":5,  "size":5000, "replication":"000"},
          {"id":6,  "size":6000, "replication":"000"},
          {"id":7,  "size":7000, "replication":"000"},
          {"id":8,  "size":8000, "replication":"000"},
          {"id":9,  "size":9000, "replication":"000"},
          {"id":10, "size":9500, "replication":"000"}
        ],
        "limit":20
      }
    }
  }
}
`

var benchLayoutLarge = `
{
  "dc1":{
    "rack1":{
      "server1":{
        "volumes":[
          {"id":1,  "size":500,  "replication":"000"},
          {"id":2,  "size":1000, "replication":"000"},
          {"id":3,  "size":1500, "replication":"000"},
          {"id":4,  "size":2000, "replication":"000"},
          {"id":5,  "size":2500, "replication":"000"},
          {"id":6,  "size":3000, "replication":"000"},
          {"id":7,  "size":3500, "replication":"000"},
          {"id":8,  "size":4000, "replication":"000"},
          {"id":9,  "size":4500, "replication":"000"},
          {"id":10, "size":5000, "replication":"000"},
          {"id":11, "size":5500, "replication":"000"},
          {"id":12, "size":6000, "replication":"000"},
          {"id":13, "size":6500, "replication":"000"},
          {"id":14, "size":7000, "replication":"000"},
          {"id":15, "size":7500, "replication":"000"},
          {"id":16, "size":8000, "replication":"000"},
          {"id":17, "size":8500, "replication":"000"},
          {"id":18, "size":9000, "replication":"000"},
          {"id":19, "size":9200, "replication":"000"},
          {"id":20, "size":9500, "replication":"000"}
        ],
        "limit":30
      }
    }
  }
}
`

func benchPickForWrite(b *testing.B, layout string, volumeSizeLimit uint64) {
	topo := setupWithLimit(b, layout, volumeSizeLimit)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	option := &VolumeGrowOption{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vl.PickForWrite(1, option)
	}
}

func BenchmarkPickForWrite_3Volumes(b *testing.B) {
	benchPickForWrite(b, benchLayoutSmall, 10000)
}

func BenchmarkPickForWrite_10Volumes(b *testing.B) {
	benchPickForWrite(b, benchLayoutMedium, 10000)
}

func BenchmarkPickForWrite_20Volumes(b *testing.B) {
	benchPickForWrite(b, benchLayoutLarge, 10000)
}

func benchPickForWriteConstrained(b *testing.B, layout string, volumeSizeLimit uint64) {
	topo := setupWithLimit(b, layout, volumeSizeLimit)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	option := &VolumeGrowOption{DataCenter: "dc1"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vl.PickForWrite(1, option)
	}
}

func BenchmarkPickForWriteConstrained_3Volumes(b *testing.B) {
	benchPickForWriteConstrained(b, benchLayoutSmall, 10000)
}

func BenchmarkPickForWriteConstrained_10Volumes(b *testing.B) {
	benchPickForWriteConstrained(b, benchLayoutMedium, 10000)
}

func BenchmarkPickForWriteConstrained_20Volumes(b *testing.B) {
	benchPickForWriteConstrained(b, benchLayoutLarge, 10000)
}
