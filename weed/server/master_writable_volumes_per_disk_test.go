package weed_server

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func writableVolumesPerDiskSeries(t *testing.T, collection string) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() {
		stats.MasterWritableVolumesPerDisk.Collect(ch)
		close(ch)
	}()
	got := make(map[string]float64)
	for m := range ch {
		var metric dto.Metric
		if err := m.Write(&metric); err != nil {
			t.Fatal(err)
		}
		labels := make(map[string]string)
		for _, l := range metric.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["collection"] == collection {
			got[labels["node"]+"/"+labels["disk_id"]+"/"+labels["state"]] = metric.GetGauge().GetValue()
		}
	}
	return got
}

func TestWritableDiskSeriesDropsDisksWithoutWritableVolumes(t *testing.T) {
	const collection = "perdisk"
	topo := topology.NewTopology("perdisk", nil, 10000, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3, DiskId: 0},
		{Id: 2, Collection: collection, Version: 3, DiskId: 0},
		{Id: 3, Collection: collection, Version: 3, DiskId: 1},
	}, dn)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout(collection, rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeCrowded(needle.VolumeId(3))
	node := string(dn.Id())

	series := make(writableDiskSeries)
	series.update(vl, collection, "hdd", "000", "")
	assertSeries(t, writableVolumesPerDiskSeries(t, collection), map[string]float64{
		node + "/0/writable": 2, node + "/0/crowded": 0,
		node + "/1/writable": 1, node + "/1/crowded": 1,
	})

	vl.SetVolumeReadOnly(dn, needle.VolumeId(3))
	series.update(vl, collection, "hdd", "000", "")
	assertSeries(t, writableVolumesPerDiskSeries(t, collection), map[string]float64{
		node + "/0/writable": 2, node + "/0/crowded": 0,
	})

	series.retain(nil)
	assertSeries(t, writableVolumesPerDiskSeries(t, collection), map[string]float64{})
	if len(series) != 0 {
		t.Errorf("series still tracks %d layouts", len(series))
	}
}

func assertSeries(t *testing.T, got, want map[string]float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("series = %v, want %v", got, want)
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Errorf("series[%s] = %v (present %v), want %v", k, g, ok, v)
		}
	}
}

func TestResetLeaderLayoutGaugesClearsFollowerValues(t *testing.T) {
	const collection = "follower"
	topo := topology.NewTopology("follower", nil, 10000, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: collection, Version: 3},
	}, dn)
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout(collection, rp, needle.EMPTY_TTL, types.HardDriveType)

	series := make(writableDiskSeries)
	series.update(vl, collection, "hdd", "000", "")
	stats.MasterVolumeLayoutWritable.WithLabelValues(collection, "hdd", "000", "").Set(1)
	stats.MasterVolumeLayoutCrowded.WithLabelValues(collection, "hdd", "000", "").Set(1)
	stats.MasterUnderReplicatedVolumes.WithLabelValues(collection, "hdd", "000", "").Set(1)

	resetLeaderLayoutGauges(series)

	for name, gauge := range map[string]*prometheus.GaugeVec{
		"volume_layout_writable":    stats.MasterVolumeLayoutWritable,
		"volume_layout_crowded":     stats.MasterVolumeLayoutCrowded,
		"under_replicated_volumes":  stats.MasterUnderReplicatedVolumes,
		"writable_volumes_per_disk": stats.MasterWritableVolumesPerDisk,
	} {
		if n := testutil.CollectAndCount(gauge); n != 0 {
			t.Errorf("%s still exports %d series after the reset", name, n)
		}
	}
	if len(series) != 0 {
		t.Errorf("series still tracks %d layouts", len(series))
	}
}
