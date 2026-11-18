package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// benchSpreadTopology builds 4 data centers of 120 single-disk servers and a
// layout with `writables` writable rp=100 volumes spread over them.
func benchSpreadTopology(b *testing.B, writables int) (*Topology, *VolumeGrowOption) {
	b.Helper()
	topo, racks := newSpreadTopology(4, 120, 1000)
	rp, _ := super_block.NewReplicaPlacementFromString("100")
	option := &VolumeGrowOption{Collection: "bench", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	var nodes [][]*DataNode
	for _, rack := range racks {
		var dcNodes []*DataNode
		for _, c := range rack.Children() {
			dcNodes = append(dcNodes, c.(*DataNode))
		}
		nodes = append(nodes, dcNodes)
	}
	for i := 0; i < writables; i++ {
		vi := storage.VolumeInfo{Id: needle.VolumeId(i + 1), Collection: "bench", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Version: needle.GetCurrentVersion()}
		for c := 0; c < 2; c++ {
			dn := nodes[(i+c)%4][i%120]
			dn.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, dn)
		}
	}
	return topo, option
}

func BenchmarkFindEmptySlotsForOneVolume(b *testing.B) {
	for _, writables := range []int{0, 100, 2000, 10000} {
		b.Run(fmt.Sprintf("writables=%d", writables), func(b *testing.B) {
			topo, option := benchSpreadTopology(b, writables)
			vg := NewDefaultVolumeGrowth()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := vg.findEmptySlotsForOneVolume(topo, option, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFindEmptySlotsWithRequestLoad is the per-volume cost inside a grow
// request, which scans the layout once and reuses the load.
func BenchmarkFindEmptySlotsWithRequestLoad(b *testing.B) {
	for _, writables := range []int{0, 2000, 10000} {
		b.Run(fmt.Sprintf("writables=%d", writables), func(b *testing.B) {
			topo, option := benchSpreadTopology(b, writables)
			option = option.withLayoutLoad(topo)
			vg := NewDefaultVolumeGrowth()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := vg.findEmptySlotsForOneVolume(topo, option, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWritableCountByNode(b *testing.B) {
	for _, writables := range []int{100, 2000, 10000} {
		b.Run(fmt.Sprintf("writables=%d", writables), func(b *testing.B) {
			topo, option := benchSpreadTopology(b, writables)
			vl := topo.FindVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vl.writableCountByNode()
			}
		})
	}
}
