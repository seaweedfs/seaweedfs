package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

const assignTestVolumeSizeLimit = 1 << 30

// assignTestCluster is a leader master over one data center with one volume
// server per rack, like a production cluster spreading "010" replicas.
type assignTestCluster struct {
	ms      *MasterServer
	nodes   []*topology.DataNode
	rp      *super_block.ReplicaPlacement
	nextVid uint32
}

func newAssignTestCluster(t *testing.T, racks int, replication string) *assignTestCluster {
	t.Helper()
	rp, err := super_block.NewReplicaPlacementFromString(replication)
	require.NoError(t, err)
	c := &assignTestCluster{ms: newLeaderMaster(), rp: rp}
	// Room for the per-assign size estimate, which would fill newLeaderMaster's
	// tiny volumes on the first assign.
	c.ms.Topo = topology.NewTopology("test", sequence.NewMemorySequencer(), assignTestVolumeSizeLimit, 5, false)
	c.ms.Topo.RaftServer = leaderRaftServer{}
	dc := c.ms.Topo.GetOrCreateDataCenter("dc1")
	for i := 0; i < racks; i++ {
		c.nodes = append(c.nodes, dc.GetOrCreateRack(fmt.Sprintf("rack%d", i)).
			GetOrCreateDataNode("127.0.0.1", 8080+i, 18080+i, "127.0.0.1", fmt.Sprintf("dn%d", i), map[string]uint32{"": 1000}))
	}
	return c
}

// grow registers n new empty volumes, each with its copies on consecutive
// racks, the way a finished volume grow lands in the topology.
func (c *assignTestCluster) grow(collection string, n uint32) {
	for i := uint32(0); i < n; i++ {
		c.nextVid++
		v := storage.VolumeInfo{
			Id:               needle.VolumeId(c.nextVid),
			Collection:       collection,
			ReplicaPlacement: c.rp,
			Ttl:              needle.EMPTY_TTL,
			Version:          needle.GetCurrentVersion(),
		}
		for r := 0; r < c.rp.GetCopyCount(); r++ {
			dn := c.nodes[(int(c.nextVid)+r)%len(c.nodes)]
			dn.AddOrUpdateVolume(v)
			c.ms.Topo.RegisterVolumeLayout(v, dn)
		}
	}
}

func (c *assignTestCluster) layout(collection string) *topology.VolumeLayout {
	return c.ms.Topo.GetVolumeLayout(collection, c.rp, needle.EMPTY_TTL, "")
}

func (c *assignTestCluster) httpAssign(t *testing.T, query string) {
	t.Helper()
	w := httptest.NewRecorder()
	c.ms.dirAssignHandler(w, httptest.NewRequest(http.MethodGet, "/dir/assign?"+query, nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// A client's writableVolumeCount is what the periodic growth scan keeps a
// layout topped up to. gRPC Assign has recorded it since the scan was added;
// HTTP /dir/assign parsed it but dropped it, so HTTP-only writers never got
// proactive growth. Both entry points must leave the layout in the same state.
func TestDirAssignRecordsWritableVolumeCountLikeGrpcAssign(t *testing.T) {
	tests := []struct {
		name   string
		counts []uint32 // writableVolumeCount per assign, 0 = not sent
		want   uint32
	}{
		{name: "sent", counts: []uint32{30}, want: 30},
		{name: "not sent", counts: []uint32{0}, want: 0},
		{name: "later assign without it keeps the last value", counts: []uint32{30, 0}, want: 30},
		{name: "latest value wins", counts: []uint32{30, 12}, want: 12},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpCluster := newAssignTestCluster(t, 1, "000")
			httpCluster.grow("c", 1)
			for _, n := range tt.counts {
				query := "collection=c&replication=000"
				if n > 0 {
					query += fmt.Sprintf("&writableVolumeCount=%d", n)
				}
				httpCluster.httpAssign(t, query)
			}

			grpcCluster := newAssignTestCluster(t, 1, "000")
			grpcCluster.grow("c", 1)
			for _, n := range tt.counts {
				_, err := grpcCluster.ms.Assign(context.Background(), &master_pb.AssignRequest{
					Count: 1, Collection: "c", Replication: "000", WritableVolumeCount: n,
				})
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, grpcCluster.layout("c").GetLastGrowCount(), "gRPC Assign")
			assert.Equal(t, tt.want, httpCluster.layout("c").GetLastGrowCount(), "HTTP /dir/assign")
		})
	}
}

// An assign naming a placement the topology cannot serve must fail before it
// records a growth target: the periodic scan drops the requested placement, so
// the volumes it would keep growing could never serve the requester.
func TestDirAssignRejectsUnknownPlacementBeforeGrowCount(t *testing.T) {
	c := newAssignTestCluster(t, 1, "000")
	c.grow("c", 1)
	vl := c.layout("c")

	// A rack and data node that exist, but in a different data center: the
	// requester named dc1, so these placements can never be served either.
	c.ms.Topo.GetOrCreateDataCenter("dc2").GetOrCreateRack("rackX").
		GetOrCreateDataNode("127.0.0.2", 8090, 18090, "127.0.0.2", "dnX", map[string]uint32{"": 1000})

	for _, query := range []string{
		"collection=c&replication=000&writableVolumeCount=30&rack=missing",
		"collection=c&replication=000&writableVolumeCount=30&dataNode=missing",
		"collection=c&replication=000&writableVolumeCount=30&dataCenter=dc1&rack=rackX",
		"collection=c&replication=000&writableVolumeCount=30&dataCenter=dc1&dataNode=dnX",
		"collection=c&replication=000&writableVolumeCount=30&dataCenter=dc1&rack=rack0&dataNode=dnX",
	} {
		w := httptest.NewRecorder()
		c.ms.dirAssignHandler(w, httptest.NewRequest(http.MethodGet, "/dir/assign?"+query, nil))
		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	}
	assert.Zero(t, vl.GetLastGrowCount())

	for _, req := range []*master_pb.AssignRequest{
		{Count: 1, Collection: "c", Replication: "000", WritableVolumeCount: 30, Rack: "missing"},
		{Count: 1, Collection: "c", Replication: "000", WritableVolumeCount: 30, DataCenter: "dc1", Rack: "rackX"},
		{Count: 1, Collection: "c", Replication: "000", WritableVolumeCount: 30, DataCenter: "dc1", Rack: "rack0", DataNode: "dnX"},
	} {
		_, err := c.ms.Assign(context.Background(), req)
		assert.Error(t, err)
	}
	assert.Zero(t, vl.GetLastGrowCount())
}

// Replays the outage of an HTTP-only writer with writableVolumeCount=30 on a
// "010" collection across 5 racks. Writes are spread by remaining capacity, so
// the 30 volumes fill in lockstep. Before the fix the periodic scan only ever
// saw lastGrowCount 0: it added the rack-aware step of 2 when everything was
// crowded and nothing more while those 2 stayed uncrowded. When the old batch
// went full, the 2 volumes took every write, filled before the next scan, and
// assigns failed until an emergency grow landed.
func TestPeriodicGrowthKeepsHTTPAssignLayoutWritable(t *testing.T) {
	const n = 30
	c := newAssignTestCluster(t, 5, "010")
	c.grow("photo", n)
	vl := c.layout("photo")
	c.httpAssign(t, fmt.Sprintf("collection=photo&replication=010&writableVolumeCount=%d", n))

	const limit = int64(assignTestVolumeSizeLimit)
	batch := vl.CloneWritableVolumes()
	writeTo := func(vids []needle.VolumeId, size int64) {
		for _, vid := range vids {
			vl.RecordAssign(vid, size)
		}
	}
	// scan runs the per-layout steps of ProcessGrowRequest, with a grow that
	// lands at once.
	scan := func() {
		writable, crowded := vl.GetWritableVolumeCount()
		count, _ := periodicGrowCount(vl.GetLastGrowCount(), writable, crowded)
		c.grow("photo", count)
		for _, plan := range vl.PlanRackAwareGrowth(c.ms.Topo.ListDCAndRacks(), vl.GetLastGrowCount(), volumeGrowStepCount) {
			c.grow("photo", plan.WritableVolumeCount)
		}
		writable, _ = vl.GetWritableVolumeCount()
		require.LessOrEqual(t, writable, 2*n, "growth must stay bounded by twice the requested writable count")
	}

	// The batch crosses the crowded threshold together, and stays crowded
	// over two scans.
	writeTo(batch, limit*95/100)
	_, crowded := vl.GetWritableVolumeCount()
	require.Equal(t, n, crowded)
	scan()
	scan()

	// The batch goes full; whatever the scans added is all that is left.
	writeTo(batch, limit)
	scan()

	writable, crowded := vl.GetWritableVolumeCount()
	assert.Zero(t, crowded)
	assert.GreaterOrEqual(t, writable, n,
		"after the batch went full the scan must restore the %d writable volumes the client asked for, not leave %d to take every write", n, writable)
}
