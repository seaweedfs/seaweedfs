// Package ecbalancer holds the EC-shard rebalancing policy shared by the shell
// ec.balance command and the admin EC balance worker. It is pure: callers build
// a Topology snapshot from their own structures, call Plan to get the list of
// shard Moves, and execute them their own way (inline RPCs in the shell, task
// proposals in the worker). Keeping the policy here stops the two callers from
// drifting apart.
package ecbalancer

import (
	"math"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Topology is a snapshot of EC shard placement to plan against. Build it with
// NewTopology + AddNode + (*Node).AddDisk/AddShards.
type Topology struct {
	nodes map[string]*Node
}

// Node is a volume server in the snapshot. Fields are set through the builder
// methods; only its identity is read back (via Move).
type Node struct {
	id        string
	dc        string
	rack      string // composite rack key (e.g. "dc1:rack1")
	freeSlots int
	disks     map[uint32]*disk
	shards    map[uint32]*volumeShards
}

type disk struct {
	diskID     uint32
	diskType   string
	freeSlots  int
	shardCount int // total EC shards on this disk across all volumes
}

type volumeShards struct {
	collection    string
	shardBits     erasure_coding.ShardBits            // union across disks
	diskShardBits map[uint32]erasure_coding.ShardBits // disk_id -> shards of this volume on that disk
}

type rack struct {
	nodes     map[string]*Node
	freeSlots int
}

// Move is a planned shard relocation. For a dedup deletion SourceNode==TargetNode
// and SourceDisk==TargetDisk (unmount+delete only, no copy).
type Move struct {
	VolumeID   uint32
	ShardID    int
	Collection string
	SourceNode string
	SourceDisk uint32
	SourceRack string
	TargetNode string
	TargetDisk uint32
	TargetRack string
	Phase      string // "dedup", "cross_rack", "within_rack", "global"
}

// Options tunes a Plan run.
type Options struct {
	DiskType           string                        // "" matches any disk type
	ImbalanceThreshold float64                       // skip rack/node balancing below this skew
	ReplicaPlacement   *super_block.ReplicaPlacement // nil = even spread, no per-rack/node cap
	// Ratio returns a collection's (dataShards, parityShards); nil defaults to the
	// standard scheme. This is where a caller plugs in custom-ratio resolution.
	Ratio func(collection string) (dataShards, parityShards int)
	// GlobalMaxMovesPerRack caps how many shards the global (cross-volume) phase
	// moves out of one rack in a single Plan. 0 means unlimited (drain to balance
	// in one pass), which the shell uses; the worker sets a small value to make
	// incremental progress across repeated detection cycles.
	GlobalMaxMovesPerRack int
	// GlobalUtilizationBased selects the global phase's balance metric: when true,
	// nodes are balanced by fractional fullness (shards/capacity), which suits
	// heterogeneous-capacity racks; when false, by raw shard count. The worker
	// uses utilization; the shell uses raw count.
	GlobalUtilizationBased bool
}

// move is the internal form carrying node pointers; converted to Move at the end.
type move struct {
	volumeID   uint32
	shardID    int
	collection string
	source     *Node
	sourceDisk uint32
	target     *Node
	targetDisk uint32
	phase      string
}

// NewTopology returns an empty topology to populate.
func NewTopology() *Topology {
	return &Topology{nodes: make(map[string]*Node)}
}

// AddNode registers a volume server. freeSlots is the node's total free EC shard
// slots; per-disk free slots are supplied via AddDisk.
func (t *Topology) AddNode(id, dc, rackKey string, freeSlots int) *Node {
	n := &Node{
		id:        id,
		dc:        dc,
		rack:      rackKey,
		freeSlots: freeSlots,
		disks:     make(map[uint32]*disk),
		shards:    make(map[uint32]*volumeShards),
	}
	t.nodes[id] = n
	return n
}

// AddDisk registers a physical disk. shardCount is the disk's total EC shard
// count across all volumes (used for disk scoring); freeSlots is the per-disk
// free EC shard slots.
func (n *Node) AddDisk(diskID uint32, diskType string, freeSlots, shardCount int) {
	n.disks[diskID] = &disk{diskID: diskID, diskType: diskType, freeSlots: freeSlots, shardCount: shardCount}
}

// AddShards records that the volume's shards in bits live on diskID. Call it
// only for the volumes that should be balanced; the disk's overall occupancy is
// reported separately via AddDisk.
func (n *Node) AddShards(vid uint32, collection string, diskID uint32, bits erasure_coding.ShardBits) {
	vs, ok := n.shards[vid]
	if !ok {
		vs = &volumeShards{collection: collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
		n.shards[vid] = vs
	}
	vs.shardBits |= bits
	vs.diskShardBits[diskID] |= bits
}

// Plan runs the full multi-phase EC balance policy and returns the proposed
// moves: per collection it deduplicates, then spreads data and parity shards
// across racks and within racks (two-pass, with anti-affinity), and finally
// balances total shard load across nodes in each rack.
func Plan(topo *Topology, opts Options) []Move {
	if topo == nil || len(topo.nodes) == 0 {
		return nil
	}
	ratio := opts.Ratio
	if ratio == nil {
		ratio = func(string) (int, int) {
			return erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
		}
	}

	nodes := topo.nodes
	racks := buildRacks(nodes)

	// Group volume IDs by collection (deterministic order), and resolve each
	// collection's data-shard count once for the global phase's disk scoring.
	byCollection := make(map[string][]uint32)
	seen := make(map[string]map[uint32]bool)
	for _, n := range nodes {
		for vid, vs := range n.shards {
			if seen[vs.collection] == nil {
				seen[vs.collection] = make(map[uint32]bool)
			}
			if !seen[vs.collection][vid] {
				seen[vs.collection][vid] = true
				byCollection[vs.collection] = append(byCollection[vs.collection], vid)
			}
		}
	}
	collections := make([]string, 0, len(byCollection))
	dataShardsByCollection := make(map[string]int)
	for c := range byCollection {
		collections = append(collections, c)
		sort.Slice(byCollection[c], func(i, j int) bool { return byCollection[c][i] < byCollection[c][j] })
		d, _ := ratio(c)
		dataShardsByCollection[c] = d
	}
	sort.Strings(collections)

	var all []*move
	for _, collection := range collections {
		dataShards, parityShards := ratio(collection)

		for _, vid := range byCollection[collection] {
			m := detectDuplicateShards(vid, collection, nodes)
			applyMovesToTopology(m)
			all = append(all, m...)
		}
		for _, vid := range byCollection[collection] {
			m := detectCrossRackImbalance(vid, collection, nodes, racks, opts.DiskType, opts.ImbalanceThreshold, dataShards, parityShards, opts.ReplicaPlacement)
			applyMovesToTopology(m)
			all = append(all, m...)
		}
		for _, vid := range byCollection[collection] {
			m := detectWithinRackImbalance(vid, collection, nodes, racks, opts.DiskType, opts.ImbalanceThreshold, dataShards, parityShards, opts.ReplicaPlacement)
			applyMovesToTopology(m)
			all = append(all, m...)
		}
	}

	all = append(all, detectGlobalImbalance(nodes, racks, opts.DiskType, opts.ImbalanceThreshold, dataShardsByCollection, opts.GlobalMaxMovesPerRack, opts.GlobalUtilizationBased)...)

	out := make([]Move, 0, len(all))
	for _, m := range all {
		out = append(out, Move{
			VolumeID:   m.volumeID,
			ShardID:    m.shardID,
			Collection: m.collection,
			SourceNode: m.source.id,
			SourceDisk: m.sourceDisk,
			SourceRack: m.source.rack,
			TargetNode: m.target.id,
			TargetDisk: m.targetDisk,
			TargetRack: m.target.rack,
			Phase:      m.phase,
		})
	}
	return out
}

func buildRacks(nodes map[string]*Node) map[string]*rack {
	racks := make(map[string]*rack)
	for _, n := range nodes {
		r, ok := racks[n.rack]
		if !ok {
			r = &rack{nodes: make(map[string]*Node)}
			racks[n.rack] = r
		}
		r.nodes[n.id] = n
		r.freeSlots += n.freeSlots
	}
	return racks
}

// detectDuplicateShards finds shards present on more than one node and proposes
// deleting all copies but the one on the node with the most free slots.
func detectDuplicateShards(vid uint32, collection string, nodes map[string]*Node) []*move {
	shardLocations := make(map[int][]*Node)
	for _, node := range nodes {
		info, ok := node.shards[vid]
		if !ok {
			continue
		}
		for shardID := 0; shardID < erasure_coding.MaxShardCount; shardID++ {
			if info.shardBits.Has(erasure_coding.ShardId(shardID)) {
				shardLocations[shardID] = append(shardLocations[shardID], node)
			}
		}
	}

	var moves []*move
	for shardID, locs := range shardLocations {
		if len(locs) <= 1 {
			continue
		}
		sort.Slice(locs, func(i, j int) bool { return locs[i].freeSlots < locs[j].freeSlots })
		for _, node := range locs[:len(locs)-1] {
			moves = append(moves, &move{
				volumeID:   vid,
				shardID:    shardID,
				collection: collection,
				source:     node,
				sourceDisk: shardDiskID(node, vid, shardID),
				target:     node,
				targetDisk: shardDiskID(node, vid, shardID),
				phase:      "dedup",
			})
		}
	}
	return moves
}

// detectCrossRackImbalance spreads a volume's shards across racks in two passes
// (data, then parity with anti-affinity to data-bearing racks). Returns nil if
// the overall distribution is below the imbalance threshold.
func detectCrossRackImbalance(vid uint32, collection string, nodes map[string]*Node, racks map[string]*rack, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*move {
	numRacks := len(racks)
	if numRacks <= 1 {
		return nil
	}

	rackShardCount := countShardsByRack(vid, nodes)
	total := 0
	for _, c := range rackShardCount {
		total += c
	}
	if total == 0 || !exceedsImbalanceThreshold(rackShardCount, total, numRacks, threshold) {
		return nil
	}

	var moves []*move

	dataPerRack, _ := shardsByGroup(vid, nodes, dataShards, func(n *Node) string { return n.rack })
	moves = append(moves, balanceShardTypeAcrossRacks(vid, collection, nodes, racks, diskType, dataShards,
		dataPerRack, rackShardCount, ceilDivide(dataShards, numRacks), nil, rp)...)

	dataPerRack, parityPerRack := shardsByGroup(vid, nodes, dataShards, func(n *Node) string { return n.rack })
	antiAffinity := make(map[string]bool)
	for rackID, shards := range dataPerRack {
		if len(shards) > 0 {
			antiAffinity[rackID] = true
		}
	}
	moves = append(moves, balanceShardTypeAcrossRacks(vid, collection, nodes, racks, diskType, dataShards,
		parityPerRack, rackShardCount, ceilDivide(parityShards, numRacks), antiAffinity, rp)...)

	return moves
}

func balanceShardTypeAcrossRacks(vid uint32, collection string, nodes map[string]*Node, racks map[string]*rack, diskType string, dataShards int, shardsPerRack map[string][]int, rackShardCount map[string]int, maxPerRack int, antiAffinity map[string]bool, rp *super_block.ReplicaPlacement) []*move {
	if maxPerRack < 1 {
		maxPerRack = 1
	}
	rackKeys := sortedKeys(racks)

	type pending struct {
		shardID int
		src     *Node
	}
	var toMove []pending
	for _, rackID := range rackKeys {
		shards := append([]int(nil), shardsPerRack[rackID]...)
		if len(shards) <= maxPerRack {
			continue
		}
		sort.Ints(shards)
		for i := 0; i < len(shards)-maxPerRack; i++ {
			if src := nodeInRackHoldingShard(nodes, rackID, vid, shards[i]); src != nil {
				toMove = append(toMove, pending{shards[i], src})
			}
		}
	}

	var moves []*move
	for _, pm := range toMove {
		destRack, ok := pickTarget(rackKeys, shardsPerRack, maxPerRack, antiAffinity,
			func(r string) bool { return racks[r].freeSlots > 0 },
			func(r string) bool {
				if rp != nil && rp.DiffRackCount > 0 {
					return rackShardCount[r] < rp.DiffRackCount
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := pickNodeInRack(racks[destRack], vid, rp)
		if destNode == nil {
			continue
		}
		destDisk := pickBestDiskOnNode(destNode, vid, diskType, pm.shardID, dataShards)
		moves = append(moves, &move{
			volumeID:   vid,
			shardID:    pm.shardID,
			collection: collection,
			source:     pm.src,
			sourceDisk: shardDiskID(pm.src, vid, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "cross_rack",
		})
		releaseShard(pm.src, vid, pm.shardID)
		reserveShard(destNode, vid, collection, pm.shardID, destDisk)
		srcRack := pm.src.rack
		shardsPerRack[destRack] = append(shardsPerRack[destRack], pm.shardID)
		shardsPerRack[srcRack] = removeInt(shardsPerRack[srcRack], pm.shardID)
		rackShardCount[destRack]++
		rackShardCount[srcRack]--
		racks[destRack].freeSlots--
		racks[srcRack].freeSlots++
	}
	return moves
}

func pickNodeInRack(r *rack, vid uint32, rp *super_block.ReplicaPlacement) *Node {
	var best *Node
	bestCount := -1
	for _, id := range sortedNodeKeys(r.nodes) {
		node := r.nodes[id]
		if node.freeSlots <= 0 {
			continue
		}
		count := volumeShardCount(node, vid)
		if rp != nil && rp.SameRackCount > 0 && count >= rp.SameRackCount+1 {
			continue
		}
		if best == nil || count < bestCount {
			best, bestCount = node, count
		}
	}
	return best
}

// detectWithinRackImbalance spreads a volume's shards across the nodes of each
// rack, again data then parity with anti-affinity.
func detectWithinRackImbalance(vid uint32, collection string, nodes map[string]*Node, racks map[string]*rack, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*move {
	var moves []*move

	for _, rackID := range sortedKeys(racks) {
		r := racks[rackID]
		if len(r.nodes) <= 1 {
			continue
		}

		nodeShardCount := countShardsByNode(vid, r.nodes)
		total := 0
		for _, c := range nodeShardCount {
			total += c
		}
		if total == 0 || !exceedsImbalanceThreshold(nodeShardCount, total, len(r.nodes), threshold) {
			continue
		}
		numNodes := len(r.nodes)

		dataPerNode, _ := shardsByGroup(vid, r.nodes, dataShards, func(n *Node) string { return n.id })
		moves = append(moves, balanceShardTypeAcrossNodes(vid, collection, r, diskType, dataShards,
			dataPerNode, nodeShardCount, ceilDivide(sumLens(dataPerNode), numNodes), nil, rp)...)

		dataPerNode, parityPerNode := shardsByGroup(vid, r.nodes, dataShards, func(n *Node) string { return n.id })
		antiAffinity := make(map[string]bool)
		for nodeID, shards := range dataPerNode {
			if len(shards) > 0 {
				antiAffinity[nodeID] = true
			}
		}
		moves = append(moves, balanceShardTypeAcrossNodes(vid, collection, r, diskType, dataShards,
			parityPerNode, nodeShardCount, ceilDivide(sumLens(parityPerNode), numNodes), antiAffinity, rp)...)
	}

	return moves
}

func balanceShardTypeAcrossNodes(vid uint32, collection string, r *rack, diskType string, dataShards int, shardsPerNode map[string][]int, nodeShardCount map[string]int, maxPerNode int, antiAffinity map[string]bool, rp *super_block.ReplicaPlacement) []*move {
	if maxPerNode < 1 {
		maxPerNode = 1
	}
	nodeKeys := sortedNodeKeys(r.nodes)

	type pending struct {
		shardID int
		src     *Node
	}
	var toMove []pending
	for _, nodeID := range nodeKeys {
		shards := append([]int(nil), shardsPerNode[nodeID]...)
		if len(shards) <= maxPerNode {
			continue
		}
		sort.Ints(shards)
		src := r.nodes[nodeID]
		for i := 0; i < len(shards)-maxPerNode; i++ {
			toMove = append(toMove, pending{shards[i], src})
		}
	}

	var moves []*move
	for _, pm := range toMove {
		destID, ok := pickTarget(nodeKeys, shardsPerNode, maxPerNode, antiAffinity,
			func(n string) bool { return n != pm.src.id && r.nodes[n].freeSlots > 0 },
			func(n string) bool {
				if rp != nil && rp.SameRackCount > 0 {
					return nodeShardCount[n] < rp.SameRackCount+1
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := r.nodes[destID]
		destDisk := pickBestDiskOnNode(destNode, vid, diskType, pm.shardID, dataShards)
		moves = append(moves, &move{
			volumeID:   vid,
			shardID:    pm.shardID,
			collection: collection,
			source:     pm.src,
			sourceDisk: shardDiskID(pm.src, vid, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "within_rack",
		})
		releaseShard(pm.src, vid, pm.shardID)
		reserveShard(destNode, vid, collection, pm.shardID, destDisk)
		shardsPerNode[destID] = append(shardsPerNode[destID], pm.shardID)
		shardsPerNode[pm.src.id] = removeInt(shardsPerNode[pm.src.id], pm.shardID)
		nodeShardCount[destID]++
		nodeShardCount[pm.src.id]--
		pm.src.freeSlots++
		destNode.freeSlots--
	}
	return moves
}

// detectGlobalImbalance balances total EC shard load across the nodes of each
// rack (across all volumes), using utilization ratios so heterogeneous-capacity
// nodes are compared fairly.
func detectGlobalImbalance(nodes map[string]*Node, racks map[string]*rack, diskType string, threshold float64, dataShardsByCollection map[string]int, maxMovesPerRack int, byUtilization bool) []*move {
	var moves []*move

	for _, rackID := range sortedKeys(racks) {
		r := racks[rackID]
		if len(r.nodes) <= 1 {
			continue
		}

		nodeShardCounts := make(map[string]int)
		totalShards := 0
		for nodeID, node := range r.nodes {
			count := 0
			for _, info := range node.shards {
				count += info.shardBits.Count()
			}
			nodeShardCounts[nodeID] = count
			totalShards += count
		}
		if totalShards == 0 {
			continue
		}

		// The balance metric is shards/capacity. For utilization balancing the
		// capacity is the node's real shard-slot capacity; for raw-count balancing
		// it is a constant 1, so the metric reduces to the raw shard count. Either
		// way a node can only receive while its real freeSlots remain.
		nodeCapacity := make(map[string]int, len(r.nodes))
		for nodeID, count := range nodeShardCounts {
			if byUtilization {
				nodeCapacity[nodeID] = count + r.nodes[nodeID].freeSlots
			} else {
				nodeCapacity[nodeID] = 1
			}
		}
		if !exceedsUtilImbalanceThreshold(nodeShardCounts, nodeCapacity, threshold) {
			continue
		}

		// Each iteration moves one shard. 0 means unlimited (drain to balance in
		// one pass) — bounded by totalShards since the convergence guard stops
		// once no beneficial move remains.
		iterations := maxMovesPerRack
		if iterations <= 0 {
			iterations = totalShards
		}
		for i := 0; i < iterations; i++ {
			var minNode, maxNode *Node
			minUtil := math.Inf(1)
			maxUtil := -1.0
			var minCount, maxCount int
			for _, nodeID := range sortedNodeKeys(r.nodes) {
				count := nodeShardCounts[nodeID]
				node := r.nodes[nodeID]
				capacity := nodeCapacity[nodeID]
				if capacity <= 0 {
					continue
				}
				util := float64(count) / float64(capacity)
				if util < minUtil && node.freeSlots > 0 {
					minUtil, minCount, minNode = util, count, node
				}
				if util > maxUtil {
					maxUtil, maxCount, maxNode = util, count, node
				}
			}
			if maxNode == nil || minNode == nil || maxNode.id == minNode.id {
				break
			}

			maxCap := nodeCapacity[maxNode.id]
			minCap := nodeCapacity[minNode.id]
			if maxCap <= 0 || minCap <= 0 {
				break
			}
			if float64(minCount+1)/float64(minCap) > float64(maxCount-1)/float64(maxCap) {
				break
			}

			moved := false
			for _, vid := range sortedVolumeKeys(maxNode.shards) {
				if moved {
					break
				}
				info := maxNode.shards[vid]
				minInfo := minNode.shards[vid]
				for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
					sid := erasure_coding.ShardId(shardID)
					if !info.shardBits.Has(sid) {
						continue
					}
					if minInfo != nil && minInfo.shardBits.Has(sid) {
						continue
					}
					dataShards := dataShardsByCollection[info.collection]
					if dataShards <= 0 {
						dataShards = erasure_coding.DataShardsCount
					}
					destDisk := pickBestDiskOnNode(minNode, vid, diskType, shardID, dataShards)
					moves = append(moves, &move{
						volumeID:   vid,
						shardID:    shardID,
						collection: info.collection,
						source:     maxNode,
						sourceDisk: shardDiskID(maxNode, vid, shardID),
						target:     minNode,
						targetDisk: destDisk,
						phase:      "global",
					})
					info.shardBits = info.shardBits.Clear(sid)
					for diskID := range info.diskShardBits {
						info.diskShardBits[diskID] = info.diskShardBits[diskID].Clear(sid)
					}
					reserveShard(minNode, vid, info.collection, shardID, destDisk)
					nodeShardCounts[maxNode.id]--
					nodeShardCounts[minNode.id]++
					maxNode.freeSlots++
					minNode.freeSlots--
					moved = true
					break
				}
			}
			if !moved {
				break
			}
		}
	}

	return moves
}

// shardsByGroup classifies a volume's shards into data (id < dataShards) and
// parity buckets, grouped by key(node).
func shardsByGroup(vid uint32, nodes map[string]*Node, dataShards int, key func(*Node) string) (dataPer, parityPer map[string][]int) {
	dataPer = make(map[string][]int)
	parityPer = make(map[string][]int)
	for _, node := range nodes {
		info, ok := node.shards[vid]
		if !ok {
			continue
		}
		k := key(node)
		for s := 0; s < erasure_coding.MaxShardCount; s++ {
			if !info.shardBits.Has(erasure_coding.ShardId(s)) {
				continue
			}
			if s < dataShards {
				dataPer[k] = append(dataPer[k], s)
			} else {
				parityPer[k] = append(parityPer[k], s)
			}
		}
	}
	return
}

// pickTarget selects a destination key with room for another shard of a type, in
// two passes: first excluding anti-affinity targets, then any valid target. Among
// valid targets it prefers the fewest shards of this type; ties break on sorted
// key order, so selection is deterministic.
func pickTarget(candidates []string, shardsPerTarget map[string][]int, maxPerTarget int, antiAffinity map[string]bool, hasFreeSlots, withinLimit func(string) bool) (string, bool) {
	try := func(skipAnti bool) (string, bool) {
		best := ""
		bestCount := maxPerTarget + 1
		for _, c := range candidates {
			if skipAnti && antiAffinity[c] {
				continue
			}
			if !hasFreeSlots(c) {
				continue
			}
			if len(shardsPerTarget[c]) >= maxPerTarget {
				continue
			}
			if !withinLimit(c) {
				continue
			}
			if cnt := len(shardsPerTarget[c]); cnt < bestCount {
				best, bestCount = c, cnt
			}
		}
		return best, best != ""
	}
	if len(antiAffinity) > 0 {
		if t, ok := try(true); ok {
			return t, true
		}
	}
	return try(false)
}

// pickBestDiskOnNode chooses the physical disk to place a new shard of the
// volume: matching disk type with free capacity, preferring fewer total shards,
// far fewer shards of the same volume, and data/parity anti-affinity. Returns 0
// ("server picks") when no disk info or no disk with capacity.
func pickBestDiskOnNode(node *Node, vid uint32, diskType string, shardID, dataShardCount int) uint32 {
	if len(node.disks) == 0 {
		return 0
	}
	isDataShard := dataShardCount > 0 && shardID < dataShardCount
	info := node.shards[vid]

	var bestDiskID uint32
	bestScore := -1
	for _, diskID := range sortedDiskKeys(node.disks) {
		d := node.disks[diskID]
		if diskType != "" && d.diskType != diskType {
			continue
		}
		if d.freeSlots <= 0 {
			continue
		}

		existingShards := 0
		hasData := false
		hasParity := false
		if info != nil {
			bits := info.diskShardBits[diskID]
			existingShards = bits.Count()
			if dataShardCount > 0 {
				for s := 0; s < erasure_coding.MaxShardCount; s++ {
					if !bits.Has(erasure_coding.ShardId(s)) {
						continue
					}
					if s < dataShardCount {
						hasData = true
					} else {
						hasParity = true
					}
				}
			}
		}

		score := d.shardCount*10 + existingShards*100
		if dataShardCount > 0 {
			if isDataShard && hasParity {
				score += 1000
			} else if !isDataShard && hasData {
				score += 1000
			}
		}
		if bestScore == -1 || score < bestScore {
			bestScore = score
			bestDiskID = diskID
		}
	}
	return bestDiskID
}

// shardDiskID returns the disk holding the given shard of the volume, or 0.
func shardDiskID(node *Node, vid uint32, shardID int) uint32 {
	info, ok := node.shards[vid]
	if !ok {
		return 0
	}
	sid := erasure_coding.ShardId(shardID)
	for _, diskID := range sortedDiskKeys(info.diskShardBits) {
		if info.diskShardBits[diskID].Has(sid) {
			return diskID
		}
	}
	return 0
}

// reserveShard records a just-planned placement on the destination so later picks
// in the same run spread across disks/nodes.
func reserveShard(node *Node, vid uint32, collection string, shardID int, diskID uint32) {
	info, ok := node.shards[vid]
	if !ok {
		info = &volumeShards{collection: collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
		node.shards[vid] = info
	}
	if info.diskShardBits == nil {
		info.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
	}
	sid := erasure_coding.ShardId(shardID)
	info.shardBits = info.shardBits.Set(sid)
	info.diskShardBits[diskID] = info.diskShardBits[diskID].Set(sid)
	if d, ok := node.disks[diskID]; ok {
		d.shardCount++
		if d.freeSlots > 0 {
			d.freeSlots--
		}
	}
}

// releaseShard removes a shard of the volume from a node's model.
func releaseShard(node *Node, vid uint32, shardID int) {
	info, ok := node.shards[vid]
	if !ok {
		return
	}
	sid := erasure_coding.ShardId(shardID)
	for diskID, bits := range info.diskShardBits {
		if bits.Has(sid) {
			info.diskShardBits[diskID] = bits.Clear(sid)
			if d, ok := node.disks[diskID]; ok {
				d.shardCount--
				d.freeSlots++
			}
		}
	}
	info.shardBits = info.shardBits.Clear(sid)
}

// applyMovesToTopology simulates moves so later phases see updated placement.
func applyMovesToTopology(moves []*move) {
	for _, m := range moves {
		sid := erasure_coding.ShardId(m.shardID)
		if srcInfo, ok := m.source.shards[m.volumeID]; ok {
			srcInfo.shardBits = srcInfo.shardBits.Clear(sid)
			for diskID := range srcInfo.diskShardBits {
				srcInfo.diskShardBits[diskID] = srcInfo.diskShardBits[diskID].Clear(sid)
			}
		}
		if m.source.id != m.target.id {
			dstInfo, ok := m.target.shards[m.volumeID]
			if !ok {
				dstInfo = &volumeShards{collection: m.collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
				m.target.shards[m.volumeID] = dstInfo
			}
			if dstInfo.diskShardBits == nil {
				dstInfo.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
			}
			dstInfo.shardBits = dstInfo.shardBits.Set(sid)
			dstInfo.diskShardBits[m.targetDisk] = dstInfo.diskShardBits[m.targetDisk].Set(sid)
		}
	}
}

func volumeShardCount(node *Node, vid uint32) int {
	if info, ok := node.shards[vid]; ok {
		return info.shardBits.Count()
	}
	return 0
}

func nodeInRackHoldingShard(nodes map[string]*Node, rackID string, vid uint32, shardID int) *Node {
	sid := erasure_coding.ShardId(shardID)
	for _, id := range sortedNodeKeys(nodes) {
		node := nodes[id]
		if node.rack != rackID {
			continue
		}
		if info, ok := node.shards[vid]; ok && info.shardBits.Has(sid) {
			return node
		}
	}
	return nil
}

func countShardsByRack(vid uint32, nodes map[string]*Node) map[string]int {
	m := make(map[string]int)
	for _, node := range nodes {
		if info, ok := node.shards[vid]; ok {
			m[node.rack] += info.shardBits.Count()
		}
	}
	return m
}

func countShardsByNode(vid uint32, nodes map[string]*Node) map[string]int {
	m := make(map[string]int)
	for id, node := range nodes {
		if info, ok := node.shards[vid]; ok {
			m[id] = info.shardBits.Count()
		}
	}
	return m
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedNodeKeys(nodes map[string]*Node) []string {
	return sortedKeys(nodes)
}

func sortedDiskKeys[T any](m map[uint32]T) []uint32 {
	keys := make([]uint32, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sortedVolumeKeys(m map[uint32]*volumeShards) []uint32 {
	keys := make([]uint32, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sumLens(m map[string][]int) int {
	total := 0
	for _, v := range m {
		total += len(v)
	}
	return total
}

func removeInt(s []int, v int) []int {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func ceilDivide(a, b int) int {
	if b == 0 {
		return 0
	}
	return (a + b - 1) / b
}

// exceedsImbalanceThreshold reports whether (max-min)/avg over numGroups exceeds
// the threshold. Groups missing from counts are treated as zero.
func exceedsImbalanceThreshold(counts map[string]int, total, numGroups int, threshold float64) bool {
	if numGroups <= 1 || total == 0 {
		return false
	}
	minCount := 0
	if len(counts) >= numGroups {
		minCount = total + 1
		for _, count := range counts {
			if count < minCount {
				minCount = count
			}
		}
	}
	maxCount := -1
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	avg := float64(total) / float64(numGroups)
	if avg == 0 {
		return false
	}
	return float64(maxCount-minCount)/avg > threshold
}

// exceedsUtilImbalanceThreshold compares fractional fullness (count/capacity) so
// heterogeneous-capacity nodes are evaluated fairly.
func exceedsUtilImbalanceThreshold(counts, capacities map[string]int, threshold float64) bool {
	minUtil := math.Inf(1)
	maxUtil := -1.0
	seen := 0
	for nodeID, count := range counts {
		capacity := capacities[nodeID]
		if capacity <= 0 {
			continue
		}
		util := float64(count) / float64(capacity)
		if util < minUtil {
			minUtil = util
		}
		if util > maxUtil {
			maxUtil = util
		}
		seen++
	}
	if seen < 2 || maxUtil <= 0 {
		return false
	}
	avg := (maxUtil + minUtil) / 2
	if avg == 0 {
		return false
	}
	return (maxUtil-minUtil)/avg > threshold
}
