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

// volKey identifies an EC volume by (collection, id). A numeric volume id can be
// reused across collections, so the collection is part of the identity (see
// weed/storage/store_ec_attach_reservation.go); keying shards by id alone would
// merge unrelated volumes and could dedup/move shards across collections.
type volKey struct {
	collection string
	vid        uint32
}

// Node is a volume server in the snapshot. Fields are set through the builder
// methods; only its identity is read back (via Move).
type Node struct {
	id   string
	host string // physical machine (host/IP); nodes sharing a host are one fault domain
	dc   string
	rack string // composite rack key (e.g. "dc1:rack1")

	freeSlots int
	disks     map[uint32]*disk
	shards    map[volKey]*volumeShards
}

type disk struct {
	diskID     uint32
	diskType   string
	tags       []string // placement tags, for preferred-tag tiering
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
	// heterogeneous-capacity racks; when false, by raw shard count. Both the worker
	// and the shell enable it; the two metrics agree when capacities are uniform.
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
		host:      id, // default: each node is its own machine until SetHost overrides
		dc:        dc,
		rack:      rackKey,
		freeSlots: freeSlots,
		disks:     make(map[uint32]*disk),
		shards:    make(map[volKey]*volumeShards),
	}
	t.nodes[id] = n
	return n
}

// SetHost records the physical machine (host/IP) a node runs on so volume servers
// sharing a host are treated as one fault domain when spreading a volume's shards.
// Left at the node id (one machine per node) when never called, in which case all
// machine-level grouping reduces to node-level grouping.
func (n *Node) SetHost(host string) {
	if host != "" {
		n.host = host
	}
}

// AddDisk registers a physical disk. shardCount is the disk's total EC shard
// count across all volumes (used for disk scoring); freeSlots is the per-disk
// free EC shard slots.
func (n *Node) AddDisk(diskID uint32, diskType string, freeSlots, shardCount int) {
	n.disks[diskID] = &disk{diskID: diskID, diskType: diskType, freeSlots: freeSlots, shardCount: shardCount}
}

// AddDiskTags records placement tags (e.g. "ssd","fast") for a disk, used by
// preferred-tag tiering in Place. Call after AddDisk; a no-op if the disk is unknown.
func (n *Node) AddDiskTags(diskID uint32, tags []string) {
	if d, ok := n.disks[diskID]; ok {
		d.tags = append([]string(nil), tags...)
	}
}

// AddShards records that the volume's shards in bits live on diskID. Call it
// only for the volumes that should be balanced; the disk's overall occupancy is
// reported separately via AddDisk.
func (n *Node) AddShards(vid uint32, collection string, diskID uint32, bits erasure_coding.ShardBits) {
	key := volKey{collection: collection, vid: vid}
	vs, ok := n.shards[key]
	if !ok {
		vs = &volumeShards{collection: collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
		n.shards[key] = vs
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

	// Group volumes by collection (deterministic order), keyed by (collection, id)
	// so volumes that reuse a numeric id across collections stay distinct. Resolve
	// each collection's data-shard count once for the global phase's disk scoring.
	byCollection := make(map[string][]volKey)
	seen := make(map[volKey]bool)
	for _, n := range nodes {
		for vk := range n.shards {
			if !seen[vk] {
				seen[vk] = true
				byCollection[vk.collection] = append(byCollection[vk.collection], vk)
			}
		}
	}
	collections := make([]string, 0, len(byCollection))
	dataShardsByCollection := make(map[string]int)
	for c := range byCollection {
		collections = append(collections, c)
		sort.Slice(byCollection[c], func(i, j int) bool { return byCollection[c][i].vid < byCollection[c][j].vid })
		d, _ := ratio(c)
		dataShardsByCollection[c] = d
	}
	sort.Strings(collections)

	var all []*move
	for _, collection := range collections {
		dataShards, parityShards := ratio(collection)

		for _, vk := range byCollection[collection] {
			m := detectDuplicateShards(vk, nodes)
			applyMovesToTopology(m, racks)
			all = append(all, m...)
		}
		for _, vk := range byCollection[collection] {
			m := detectCrossRackImbalance(vk, nodes, racks, opts.DiskType, opts.ImbalanceThreshold, dataShards, parityShards, opts.ReplicaPlacement)
			applyMovesToTopology(m, racks)
			all = append(all, m...)
		}
		for _, vk := range byCollection[collection] {
			m := detectWithinRackImbalance(vk, nodes, racks, opts.DiskType, opts.ImbalanceThreshold, dataShards, parityShards, opts.ReplicaPlacement)
			applyMovesToTopology(m, racks)
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
func detectDuplicateShards(vk volKey, nodes map[string]*Node) []*move {
	shardLocations := make(map[int][]*Node)
	for _, node := range nodes {
		info, ok := node.shards[vk]
		if !ok {
			continue
		}
		for sid := range info.shardBits.All() {
			shardLocations[int(sid)] = append(shardLocations[int(sid)], node)
		}
	}

	var moves []*move
	for shardID, locs := range shardLocations {
		if len(locs) <= 1 {
			continue
		}
		// Keep the copy on the node with the most free slots and delete the
		// duplicates from the more-constrained nodes, relieving capacity pressure
		// where it is tightest. Sort ascending by free slots (tie-break on node id
		// for determinism) and keep the last entry.
		sort.Slice(locs, func(i, j int) bool {
			if locs[i].freeSlots != locs[j].freeSlots {
				return locs[i].freeSlots < locs[j].freeSlots
			}
			return locs[i].id < locs[j].id
		})
		for _, node := range locs[:len(locs)-1] {
			moves = append(moves, &move{
				volumeID:   vk.vid,
				shardID:    shardID,
				collection: vk.collection,
				source:     node,
				sourceDisk: shardDiskID(node, vk, shardID),
				target:     node,
				targetDisk: shardDiskID(node, vk, shardID),
				phase:      "dedup",
			})
		}
	}
	return moves
}

// detectCrossRackImbalance spreads a volume's shards across racks in two passes
// (data, then parity with anti-affinity to data-bearing racks). Returns nil if
// the overall distribution is below the imbalance threshold.
func detectCrossRackImbalance(vk volKey, nodes map[string]*Node, racks map[string]*rack, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*move {
	numRacks := len(racks)
	if numRacks <= 1 {
		return nil
	}

	// Gate on per-type spread: act when data OR parity shards are unevenly
	// distributed across racks, even if the per-rack totals happen to be even.
	gateData, gateParity := shardsByGroup(vk, nodes, dataShards, func(n *Node) string { return n.rack })
	if !typeImbalanced(gateData, numRacks, threshold) && !typeImbalanced(gateParity, numRacks, threshold) {
		return nil
	}

	rackShardCount := countShardsByRack(vk, nodes)
	var moves []*move

	dataPerRack, _ := shardsByGroup(vk, nodes, dataShards, func(n *Node) string { return n.rack })
	moves = append(moves, balanceShardTypeAcrossRacks(vk, nodes, racks, diskType, dataShards,
		dataPerRack, rackShardCount, ceilDivide(dataShards, numRacks), nil, rp)...)

	dataPerRack, parityPerRack := shardsByGroup(vk, nodes, dataShards, func(n *Node) string { return n.rack })
	antiAffinity := make(map[string]bool)
	for rackID, shards := range dataPerRack {
		if len(shards) > 0 {
			antiAffinity[rackID] = true
		}
	}
	moves = append(moves, balanceShardTypeAcrossRacks(vk, nodes, racks, diskType, dataShards,
		parityPerRack, rackShardCount, ceilDivide(parityShards, numRacks), antiAffinity, rp)...)

	return moves
}

func balanceShardTypeAcrossRacks(vk volKey, nodes map[string]*Node, racks map[string]*rack, diskType string, dataShards int, shardsPerRack map[string][]int, rackShardCount map[string]int, maxPerRack int, antiAffinity map[string]bool, rp *super_block.ReplicaPlacement) []*move {
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
			if src := nodeInRackHoldingShard(nodes, rackID, vk, shards[i]); src != nil {
				toMove = append(toMove, pending{shards[i], src})
			}
		}
	}

	var moves []*move
	for _, pm := range toMove {
		destRack, ok := pickTarget(rackKeys, shardsPerRack, maxPerRack, antiAffinity,
			func(r string) bool { return racks[r].freeSlots > 0 },
			func(r string) bool {
				if rp == nil {
					return true
				}
				if rp.DiffRackCount > 0 && rackShardCount[r] >= rp.DiffRackCount {
					return false
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := pickNodeInRack(racks[destRack], vk, rp)
		if destNode == nil {
			continue
		}
		destDisk := pickBestDiskOnNode(destNode, vk, diskType, pm.shardID, dataShards)
		moves = append(moves, &move{
			volumeID:   vk.vid,
			shardID:    pm.shardID,
			collection: vk.collection,
			source:     pm.src,
			sourceDisk: shardDiskID(pm.src, vk, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "cross_rack",
		})
		releaseShard(pm.src, vk, pm.shardID)
		reserveShard(destNode, vk, pm.shardID, destDisk)
		srcRack := pm.src.rack
		shardsPerRack[destRack] = append(shardsPerRack[destRack], pm.shardID)
		shardsPerRack[srcRack] = removeInt(shardsPerRack[srcRack], pm.shardID)
		rackShardCount[destRack]++
		rackShardCount[srcRack]--
		racks[destRack].freeSlots--
		racks[srcRack].freeSlots++
		// Account at the node level too, so pickNodeInRack does not over-plan a
		// limited-capacity destination across successive moves.
		destNode.freeSlots--
		pm.src.freeSlots++
	}
	return moves
}

func pickNodeInRack(r *rack, vk volKey, rp *super_block.ReplicaPlacement) *Node {
	return pickBestNodeForVolume(sortedNodeSlice(r.nodes), vk, rp)
}

// pickBestNodeForVolume returns the node (from a sorted slice, e.g. a rack's or a
// machine's nodes) with the fewest shards of the volume that still has a free slot
// and is under the per-node SameRackCount cap, or nil if none qualifies.
func pickBestNodeForVolume(nodes []*Node, vk volKey, rp *super_block.ReplicaPlacement) *Node {
	var best *Node
	bestCount := -1
	for _, node := range nodes {
		if node.freeSlots <= 0 {
			continue
		}
		count := volumeShardCount(node, vk)
		if rp != nil && rp.SameRackCount > 0 && count >= rp.SameRackCount {
			continue
		}
		if best == nil || count < bestCount {
			best, bestCount = node, count
		}
	}
	return best
}

// detectWithinRackImbalance spreads a volume's shards across the machines of each
// rack, again data then parity with anti-affinity. The fault domain is the machine
// (volume servers sharing a host), not the individual node: piling a volume's
// shards onto several servers of one box would lose them all if the box dies, even
// though they look spread across nodes. When every node is its own machine (the
// default), machine grouping is identical to node grouping.
func detectWithinRackImbalance(vk volKey, nodes map[string]*Node, racks map[string]*rack, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*move {
	var moves []*move

	for _, rackID := range sortedKeys(racks) {
		r := racks[rackID]
		machines := buildMachines(r)
		if len(machines) <= 1 {
			continue
		}

		numMachines := len(machines)
		// Gate on per-type spread across the rack's machines (see cross-rack phase).
		gateData, gateParity := shardsByGroup(vk, r.nodes, dataShards, func(n *Node) string { return n.host })
		if !typeImbalanced(gateData, numMachines, threshold) && !typeImbalanced(gateParity, numMachines, threshold) {
			continue
		}
		dataPerMachine, _ := shardsByGroup(vk, r.nodes, dataShards, func(n *Node) string { return n.host })
		moves = append(moves, balanceShardTypeAcrossMachines(vk, machines, diskType, dataShards,
			dataPerMachine, ceilDivide(sumLens(dataPerMachine), numMachines), nil, rp)...)

		dataPerMachine, parityPerMachine := shardsByGroup(vk, r.nodes, dataShards, func(n *Node) string { return n.host })
		antiAffinity := make(map[string]bool)
		for host, shards := range dataPerMachine {
			if len(shards) > 0 {
				antiAffinity[host] = true
			}
		}
		moves = append(moves, balanceShardTypeAcrossMachines(vk, machines, diskType, dataShards,
			parityPerMachine, ceilDivide(sumLens(parityPerMachine), numMachines), antiAffinity, rp)...)
	}

	return moves
}

// balanceShardTypeAcrossMachines spreads one shard type of a volume across the
// machines of a rack, moving from machines over maxPerMachine to under-loaded
// machines. The destination node is the least-loaded node within the chosen
// machine. Mirrors balanceShardTypeAcrossRacks one fault tier down.
func balanceShardTypeAcrossMachines(vk volKey, machines map[string][]*Node, diskType string, dataShards int, shardsPerMachine map[string][]int, maxPerMachine int, antiAffinity map[string]bool, rp *super_block.ReplicaPlacement) []*move {
	if maxPerMachine < 1 {
		maxPerMachine = 1
	}
	machineKeys := sortedKeys(machines)

	type pending struct {
		shardID int
		src     *Node
	}
	var toMove []pending
	for _, host := range machineKeys {
		shards := append([]int(nil), shardsPerMachine[host]...)
		if len(shards) <= maxPerMachine {
			continue
		}
		sort.Ints(shards)
		for i := 0; i < len(shards)-maxPerMachine; i++ {
			if src := nodeHoldingShard(machines[host], vk, shards[i]); src != nil {
				toMove = append(toMove, pending{shards[i], src})
			}
		}
	}

	var moves []*move
	for _, pm := range toMove {
		destHost, ok := pickTarget(machineKeys, shardsPerMachine, maxPerMachine, antiAffinity,
			func(h string) bool { return h != pm.src.host && machineHasFreeSlots(machines[h]) },
			func(h string) bool {
				// SameRackCount caps shards per node; a machine can hold up to that on
				// each of its nodes, so the per-machine cap is the even spread alone.
				return true
			})
		if !ok {
			continue
		}
		destNode := pickBestNodeForVolume(machines[destHost], vk, rp)
		if destNode == nil {
			continue
		}
		destDisk := pickBestDiskOnNode(destNode, vk, diskType, pm.shardID, dataShards)
		moves = append(moves, &move{
			volumeID:   vk.vid,
			shardID:    pm.shardID,
			collection: vk.collection,
			source:     pm.src,
			sourceDisk: shardDiskID(pm.src, vk, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "within_rack",
		})
		releaseShard(pm.src, vk, pm.shardID)
		reserveShard(destNode, vk, pm.shardID, destDisk)
		shardsPerMachine[destHost] = append(shardsPerMachine[destHost], pm.shardID)
		shardsPerMachine[pm.src.host] = removeInt(shardsPerMachine[pm.src.host], pm.shardID)
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

			// Prefer moving a shard of a volume the destination's machine does not
			// hold at all (pass 0) before adding another shard of an already-present
			// volume (pass 1), so load balancing does not re-concentrate a volume's
			// shards onto one machine. With one node per machine this is the
			// node-level spread it has always done.
			moved := false
			for pass := 0; pass < 2 && !moved; pass++ {
				for _, vk := range sortedVolumeKeys(maxNode.shards) {
					if moved {
						break
					}
					info := maxNode.shards[vk]
					minInfo := minNode.shards[vk]
					volumeOnMinMachine := machineHoldsVolume(r, minNode.host, vk)
					if pass == 0 && volumeOnMinMachine {
						continue // pass 0: only volumes absent from the destination machine
					}
					if pass == 1 && !volumeOnMinMachine {
						continue // pass 1: only volumes already on the destination machine
					}
					// Walk the volume's actual shard bitmap so custom ratios with more
					// than the standard total (ids 14..MaxShardCount-1) are candidates too.
					for sid := range info.shardBits.All() {
						shardID := int(sid)
						if minInfo != nil && minInfo.shardBits.Has(sid) {
							continue
						}
						dataShards := dataShardsByCollection[vk.collection]
						if dataShards <= 0 {
							dataShards = erasure_coding.DataShardsCount
						}
						destDisk := pickBestDiskOnNode(minNode, vk, diskType, shardID, dataShards)
						moves = append(moves, &move{
							volumeID:   vk.vid,
							shardID:    shardID,
							collection: vk.collection,
							source:     maxNode,
							sourceDisk: shardDiskID(maxNode, vk, shardID),
							target:     minNode,
							targetDisk: destDisk,
							phase:      "global",
						})
						info.shardBits = info.shardBits.Clear(sid)
						for diskID := range info.diskShardBits {
							info.diskShardBits[diskID] = info.diskShardBits[diskID].Clear(sid)
						}
						reserveShard(minNode, vk, shardID, destDisk)
						nodeShardCounts[maxNode.id]--
						nodeShardCounts[minNode.id]++
						maxNode.freeSlots++
						minNode.freeSlots--
						moved = true
						break
					}
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
func shardsByGroup(vk volKey, nodes map[string]*Node, dataShards int, key func(*Node) string) (dataPer, parityPer map[string][]int) {
	dataPer = make(map[string][]int)
	parityPer = make(map[string][]int)
	for _, node := range nodes {
		info, ok := node.shards[vk]
		if !ok {
			continue
		}
		k := key(node)
		for sid := range info.shardBits.All() {
			s := int(sid)
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
func pickBestDiskOnNode(node *Node, vk volKey, diskType string, shardID, dataShardCount int) uint32 {
	if len(node.disks) == 0 {
		return 0
	}
	isDataShard := dataShardCount > 0 && shardID < dataShardCount
	info := node.shards[vk]

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
				for sid := range bits.All() {
					if int(sid) < dataShardCount {
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
func shardDiskID(node *Node, vk volKey, shardID int) uint32 {
	info, ok := node.shards[vk]
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
func reserveShard(node *Node, vk volKey, shardID int, diskID uint32) {
	info, ok := node.shards[vk]
	if !ok {
		info = &volumeShards{collection: vk.collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
		node.shards[vk] = info
	}
	if info.diskShardBits == nil {
		info.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
	}
	sid := erasure_coding.ShardId(shardID)
	info.shardBits = info.shardBits.Set(sid)
	info.diskShardBits[diskID] = info.diskShardBits[diskID].Set(sid)
	if d, ok := node.disks[diskID]; ok {
		d.shardCount++
		// Decrement unconditionally so reserve/release stay symmetric (releaseShard
		// credits a slot unconditionally). Callers only reserve onto disks
		// pickBestDisk* already vetted as having free slots, so this won't go
		// negative; if it ever did, freeSlots<=0 correctly reads as full.
		d.freeSlots--
	}
}

// releaseShard removes a shard of the volume from a node's model.
func releaseShard(node *Node, vk volKey, shardID int) {
	info, ok := node.shards[vk]
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
// Dedup moves (source==target) are deletions that this helper alone applies, so
// it also credits the freed disk/node/rack capacity — otherwise a slot opened by
// dedup could not be used by the cross-rack/within-rack/global phases in the same
// run. Non-dedup moves already had their slots accounted inline by the phase that
// produced them, so only their shard bits are (idempotently) re-asserted here.
func applyMovesToTopology(moves []*move, racks map[string]*rack) {
	for _, m := range moves {
		sid := erasure_coding.ShardId(m.shardID)
		vk := volKey{collection: m.collection, vid: m.volumeID}
		dedup := m.source.id == m.target.id

		if srcInfo, ok := m.source.shards[vk]; ok {
			srcInfo.shardBits = srcInfo.shardBits.Clear(sid)
			for diskID := range srcInfo.diskShardBits {
				if !srcInfo.diskShardBits[diskID].Has(sid) {
					continue
				}
				srcInfo.diskShardBits[diskID] = srcInfo.diskShardBits[diskID].Clear(sid)
				if dedup {
					if d, ok := m.source.disks[diskID]; ok {
						d.shardCount--
						d.freeSlots++
					}
				}
			}
		}

		if dedup {
			m.source.freeSlots++
			if r, ok := racks[m.source.rack]; ok {
				r.freeSlots++
			}
			continue
		}

		dstInfo, ok := m.target.shards[vk]
		if !ok {
			dstInfo = &volumeShards{collection: m.collection, diskShardBits: make(map[uint32]erasure_coding.ShardBits)}
			m.target.shards[vk] = dstInfo
		}
		if dstInfo.diskShardBits == nil {
			dstInfo.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
		}
		dstInfo.shardBits = dstInfo.shardBits.Set(sid)
		dstInfo.diskShardBits[m.targetDisk] = dstInfo.diskShardBits[m.targetDisk].Set(sid)
	}
}

func volumeShardCount(node *Node, vk volKey) int {
	if info, ok := node.shards[vk]; ok {
		return info.shardBits.Count()
	}
	return 0
}

func nodeInRackHoldingShard(nodes map[string]*Node, rackID string, vk volKey, shardID int) *Node {
	var inRack []*Node
	for _, id := range sortedNodeKeys(nodes) {
		if nodes[id].rack == rackID {
			inRack = append(inRack, nodes[id])
		}
	}
	return nodeHoldingShard(inRack, vk, shardID)
}

// nodeHoldingShard returns the first node (from a sorted slice) holding the given
// shard of the volume, or nil.
func nodeHoldingShard(nodes []*Node, vk volKey, shardID int) *Node {
	sid := erasure_coding.ShardId(shardID)
	for _, node := range nodes {
		if info, ok := node.shards[vk]; ok && info.shardBits.Has(sid) {
			return node
		}
	}
	return nil
}

// buildMachines groups a rack's nodes by host (physical machine). Each machine's
// node slice is sorted by node id for deterministic selection.
func buildMachines(r *rack) map[string][]*Node {
	machines := make(map[string][]*Node)
	for _, n := range sortedNodeSlice(r.nodes) {
		machines[n.host] = append(machines[n.host], n)
	}
	return machines
}

func machineHasFreeSlots(nodes []*Node) bool {
	for _, n := range nodes {
		if n.freeSlots > 0 {
			return true
		}
	}
	return false
}

// machineHoldsVolume reports whether any node on the given machine (host) within
// the rack holds a shard of the volume.
func machineHoldsVolume(r *rack, host string, vk volKey) bool {
	for _, n := range r.nodes {
		if n.host != host {
			continue
		}
		if info, ok := n.shards[vk]; ok && info.shardBits != 0 {
			return true
		}
	}
	return false
}

func sortedNodeSlice(nodes map[string]*Node) []*Node {
	ids := sortedNodeKeys(nodes)
	out := make([]*Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, nodes[id])
	}
	return out
}

func countShardsByRack(vk volKey, nodes map[string]*Node) map[string]int {
	m := make(map[string]int)
	for _, node := range nodes {
		if info, ok := node.shards[vk]; ok {
			m[node.rack] += info.shardBits.Count()
		}
	}
	return m
}

func countShardsByHost(vk volKey, nodes map[string]*Node) map[string]int {
	m := make(map[string]int)
	for _, node := range nodes {
		if info, ok := node.shards[vk]; ok {
			m[node.host] += info.shardBits.Count()
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

func sortedVolumeKeys(m map[volKey]*volumeShards) []volKey {
	keys := make([]volKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].vid != keys[j].vid {
			return keys[i].vid < keys[j].vid
		}
		return keys[i].collection < keys[j].collection
	})
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

// typeImbalanced reports whether the shards of one type (data or parity),
// grouped by rack or node, are spread unevenly enough across numGroups to exceed
// the threshold. Gating per type (rather than on combined totals) ensures a
// data/parity skew is acted on even when the per-group totals are even.
func typeImbalanced(perGroup map[string][]int, numGroups int, threshold float64) bool {
	counts := make(map[string]int, len(perGroup))
	total := 0
	for k, v := range perGroup {
		counts[k] = len(v)
		total += len(v)
	}
	if total == 0 {
		return false
	}
	return exceedsImbalanceThreshold(counts, total, numGroups, threshold)
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
