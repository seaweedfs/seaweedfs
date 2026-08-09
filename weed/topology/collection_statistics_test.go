package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// referenceCollectionStatistics is what callers computed for themselves from a
// full topology listing, kept here so the summary can be held to producing the
// same numbers. Quota enforcement reads these.
func referenceCollectionStatistics(t *master_pb.TopologyInfo) map[string]*CollectionStatistics {
	type volumeKey struct {
		collection string
		volumeId   uint32
	}
	out := map[string]*CollectionStatistics{}
	seen := map[volumeKey]bool{}
	ecCounts := map[volumeKey]*ecFileCounts{}
	statsFor := func(c string) *CollectionStatistics {
		s, ok := out[c]
		if !ok {
			s = &CollectionStatistics{Collection: c}
			out[c] = s
		}
		return s
	}

	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						s := statsFor(vi.Collection)
						s.PhysicalSize += vi.Size
						key := volumeKey{vi.Collection, vi.Id}
						if seen[key] {
							continue
						}
						seen[key] = true
						s.Size += vi.Size
						s.FileCount += vi.FileCount
						s.DeleteCount += vi.DeleteCount
						s.DeletedByteCount += vi.DeletedByteCount
						s.VolumeCount++
					}
					for _, esi := range diskInfo.EcShardInfos {
						s := statsFor(esi.Collection)
						s.PhysicalSize += uint64(erasure_coding.EcShardsTotalSize(esi))
						s.Size += uint64(erasure_coding.EcShardsDataSize(esi, 0))
						key := volumeKey{esi.Collection, esi.Id}
						agg, ok := ecCounts[key]
						if !ok {
							agg = &ecFileCounts{collection: esi.Collection}
							ecCounts[key] = agg
							s.VolumeCount++
						}
						if esi.FileCount > agg.fileCount {
							agg.fileCount = esi.FileCount
						}
						agg.deleteCount += esi.DeleteCount
					}
				}
			}
		}
	}
	for _, agg := range ecCounts {
		if s := out[agg.collection]; s != nil {
			s.FileCount += agg.fileCount
			s.DeleteCount += agg.deleteCount
		}
	}
	return out
}

func statsTopology(t *testing.T) *Topology {
	t.Helper()
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	nodes := make([]*DataNode, 3)
	for i := range nodes {
		nodes[i] = rack.GetOrCreateDataNode(
			"10.0.0."+string(rune('1'+i)), 8080, 18080, "", "", map[string]uint32{"": 1000, "ssd": 1000})
	}

	// Two collections, replicated volumes, one volume on a second disk type.
	// Replicas agree here; disagreement is covered on its own, where the
	// listing this is compared against is itself order dependent.
	report := func(dn *DataNode, msgs ...*master_pb.VolumeInformationMessage) {
		topo.SyncDataNodeRegistration(msgs, dn)
	}
	vol := func(id uint32, collection string, size uint64, diskType string) *master_pb.VolumeInformationMessage {
		return &master_pb.VolumeInformationMessage{
			Id: id, Collection: collection, Size: size, FileCount: uint64(id) * 10,
			DeleteCount: uint64(id), DeletedByteCount: uint64(id) * 100,
			Version: 3, ReplicaPlacement: 1, DiskType: diskType,
		}
	}
	report(nodes[0], vol(1, "bucket-a", 1000, ""), vol(2, "bucket-a", 2000, ""), vol(5, "bucket-b", 700, "ssd"))
	report(nodes[1], vol(1, "bucket-a", 1000, ""), vol(3, "bucket-b", 3000, ""))
	report(nodes[2], vol(2, "bucket-a", 2000, ""), vol(3, "bucket-b", 3000, ""))

	// Ec shards for a third collection, spread over two nodes, with the file
	// count reported differently by each holder.
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: "bucket-c", EcIndexBits: 0x1f, ShardSizes: []int64{10, 20, 30, 40, 50}, FileCount: 40, DeleteCount: 2},
	}, nodes[0])
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 9, Collection: "bucket-c", EcIndexBits: 0x3e0, ShardSizes: []int64{60, 70, 80, 90, 100}, FileCount: 44, DeleteCount: 3},
	}, nodes[1])
	return topo
}

// The summary replaces callers adding up a full topology listing, so it has to
// produce what that produced: these numbers enforce bucket quotas.
func TestCollectionStatisticsMatchesAFullListing(t *testing.T) {
	topo := statsTopology(t)

	want := referenceCollectionStatistics(topo.ToTopologyInfo())
	got := map[string]*CollectionStatistics{}
	for _, s := range topo.CollectionStatistics() {
		got[s.Collection] = s
	}

	if len(got) != len(want) {
		t.Fatalf("summarised %d collections, the listing had %d: %v vs %v", len(got), len(want), got, want)
	}
	for name, expected := range want {
		actual, found := got[name]
		if !found {
			t.Errorf("collection %s is missing from the summary", name)
			continue
		}
		if *actual != *expected {
			t.Errorf("collection %s:\n  summary %+v\n  listing %+v", name, *actual, *expected)
		}
	}
}

// Replicas disagree while a write is landing or a heartbeat is late. The full
// listing walked the topology in map order and took whichever replica it
// reached first, so its answer was not stable; the summary takes the largest.
func TestCollectionStatisticsPicksTheLargestReplica(t *testing.T) {
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	small := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})
	large := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "b", map[string]uint32{"": 100})

	behind := &master_pb.VolumeInformationMessage{
		Id: 1, Collection: "bucket-a", Size: 1000, FileCount: 10, Version: 3, ReplicaPlacement: 1,
	}
	ahead := &master_pb.VolumeInformationMessage{
		Id: 1, Collection: "bucket-a", Size: 4000, FileCount: 40, Version: 3, ReplicaPlacement: 1,
	}
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{behind}, small)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{ahead}, large)

	for i := 0; i < 8; i++ {
		stats := topo.CollectionStatistics()
		if len(stats) != 1 {
			t.Fatalf("expected one collection, got %d", len(stats))
		}
		if stats[0].Size != 4000 || stats[0].FileCount != 40 {
			t.Fatalf("run %d reported size %d files %d, want the larger replica's 4000 and 40",
				i, stats[0].Size, stats[0].FileCount)
		}
		if stats[0].PhysicalSize != 5000 {
			t.Fatalf("run %d reported physical size %d, want both replicas summed", i, stats[0].PhysicalSize)
		}
	}
}

func statsByCollection(topo *Topology) map[string]*CollectionStatistics {
	out := map[string]*CollectionStatistics{}
	for _, s := range topo.CollectionStatistics() {
		out[s.Collection] = s
	}
	return out
}

// The .ecx and .ecj travel with the shards, so a holder still loading them
// reports zero and must not pin the volume's count down.
func TestCollectionStatisticsTakesTheLargestEcFileCount(t *testing.T) {
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	loading := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})
	loaded := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "b", map[string]uint32{"": 100})

	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 11, Collection: "bucket-b", EcIndexBits: 0x7f, ShardSizes: []int64{1, 1, 1, 1, 1, 1, 1}, FileCount: 0},
	}, loading)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 11, Collection: "bucket-b", EcIndexBits: 0x3f80, ShardSizes: []int64{1, 1, 1, 1, 1, 1, 1}, FileCount: 6},
	}, loaded)

	stats := statsByCollection(topo)["bucket-b"]
	if stats == nil {
		t.Fatal("expected bucket-b to be reported")
	}
	if stats.FileCount != 6 {
		t.Errorf("file count %d, want the largest any holder reported (6)", stats.FileCount)
	}
	if stats.VolumeCount != 1 {
		t.Errorf("volume count %d, want one volume however many holders report shards", stats.VolumeCount)
	}
	// 14 shards of 1 byte each; only the 10 data shards count as logical.
	if stats.PhysicalSize != 14 {
		t.Errorf("physical size %d, want every shard counted (14)", stats.PhysicalSize)
	}
	if stats.Size != 10 {
		t.Errorf("size %d, want the data shards only (10)", stats.Size)
	}
}

// A collection with both kinds has to have them added together.
func TestCollectionStatisticsCountsRegularAndEcTogether(t *testing.T) {
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "mixed", Size: 500, FileCount: 5, DeleteCount: 1, DeletedByteCount: 50, Version: 3},
	}, dn)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 2, Collection: "mixed", EcIndexBits: 0x3fff,
			ShardSizes: []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, FileCount: 7, DeleteCount: 2},
	}, dn)

	stats := statsByCollection(topo)["mixed"]
	if stats == nil {
		t.Fatal("expected the mixed collection to be reported")
	}
	if stats.VolumeCount != 2 {
		t.Errorf("volume count %d, want the regular and the ec volume (2)", stats.VolumeCount)
	}
	if stats.FileCount != 12 {
		t.Errorf("file count %d, want 5 regular plus 7 ec", stats.FileCount)
	}
	if stats.DeleteCount != 3 {
		t.Errorf("delete count %d, want 1 regular plus 2 ec", stats.DeleteCount)
	}
	if stats.Size != 510 {
		t.Errorf("size %d, want 500 regular plus 10 data shards", stats.Size)
	}
	if stats.PhysicalSize != 514 {
		t.Errorf("physical size %d, want 500 regular plus all 14 shards", stats.PhysicalSize)
	}
}

// Quotas are enforced on size less deletions, so the replica holding the most
// live data is the one to count. The replica with the biggest raw size can be
// the one that has deleted the most, and counting that one would report a
// bucket smaller than it is and leave it writable over its quota.
func TestCollectionStatisticsPicksTheReplicaHoldingTheMostLiveData(t *testing.T) {
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	bigMostlyDeleted := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})
	smallerButLive := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "b", map[string]uint32{"": 100})

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id: 1, Collection: "bucket-a", Size: 1000, DeletedByteCount: 900,
		Version: 3, ReplicaPlacement: 1,
	}}, bigMostlyDeleted)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{
		Id: 1, Collection: "bucket-a", Size: 900, DeletedByteCount: 0,
		Version: 3, ReplicaPlacement: 1,
	}}, smallerButLive)

	stats := statsByCollection(topo)["bucket-a"]
	if stats == nil {
		t.Fatal("expected bucket-a to be reported")
	}
	live := stats.Size - stats.DeletedByteCount
	if live != 900 {
		t.Errorf("reported %d bytes live (size %d less %d deleted), want the 900 one replica holds",
			live, stats.Size, stats.DeletedByteCount)
	}
	if stats.PhysicalSize != 1900 {
		t.Errorf("physical size %d, want both replicas summed", stats.PhysicalSize)
	}
}

// Live usage is read as the collection's size less its deletions, so a volume
// reporting more deleted bytes than it holds must not cancel live bytes
// belonging to other volumes in the same bucket.
func TestCollectionStatisticsDeletionsNeverExceedTheVolume(t *testing.T) {
	topo := NewTopology("stats", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "a", map[string]uint32{"": 100})

	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		// More deleted bytes than the volume holds, which a compaction can
		// leave behind, and a healthy volume beside it.
		{Id: 1, Collection: "bucket-a", Size: 100, DeletedByteCount: 500, Version: 3},
		{Id: 2, Collection: "bucket-a", Size: 1000, DeletedByteCount: 0, Version: 3},
	}, dn)

	stats := statsByCollection(topo)["bucket-a"]
	if stats == nil {
		t.Fatal("expected bucket-a to be reported")
	}
	if stats.DeletedByteCount > stats.Size {
		t.Errorf("deletions %d exceed size %d, so live usage reads as zero", stats.DeletedByteCount, stats.Size)
	}
	if live := stats.Size - stats.DeletedByteCount; live != 1000 {
		t.Errorf("reported %d bytes live, want the 1000 the second volume holds", live)
	}
}
