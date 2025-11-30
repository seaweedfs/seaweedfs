// Package placement provides consolidated EC shard placement logic used by
// both shell commands and worker tasks.
//
// This package encapsulates the algorithms for:
// - Selecting destination nodes/disks for EC shards
// - Ensuring proper spread across racks, servers, and disks
// - Balancing shards across the cluster
package placement

import (
	"fmt"
	"sort"
)

// DiskCandidate represents a disk that can receive EC shards
type DiskCandidate struct {
	NodeID     string
	DiskID     uint32
	DataCenter string
	Rack       string

	// Capacity information
	VolumeCount    int64
	MaxVolumeCount int64
	ShardCount     int // Current number of EC shards on this disk
	FreeSlots      int // Available slots for new shards

	// Load information
	LoadCount int // Number of active tasks on this disk
}

// NodeCandidate represents a server node that can receive EC shards
type NodeCandidate struct {
	NodeID     string
	DataCenter string
	Rack       string
	FreeSlots  int
	ShardCount int              // Total shards across all disks
	Disks      []*DiskCandidate // All disks on this node
}

// PlacementRequest configures EC shard placement behavior
type PlacementRequest struct {
	// ShardsNeeded is the total number of shards to place
	ShardsNeeded int

	// MaxShardsPerServer limits how many shards can be placed on a single server
	// 0 means no limit (but prefer spreading when possible)
	MaxShardsPerServer int

	// MaxShardsPerRack limits how many shards can be placed in a single rack
	// 0 means no limit
	MaxShardsPerRack int

	// MaxTaskLoad is the maximum task load count for a disk to be considered
	MaxTaskLoad int

	// PreferDifferentServers when true, spreads shards across different servers
	// before using multiple disks on the same server
	PreferDifferentServers bool

	// PreferDifferentRacks when true, spreads shards across different racks
	// before using multiple servers in the same rack
	PreferDifferentRacks bool
}

// DefaultConfig returns the default placement configuration
func DefaultConfig() PlacementRequest {
	return PlacementRequest{
		ShardsNeeded:           14,
		MaxShardsPerServer:     0,
		MaxShardsPerRack:       0,
		MaxTaskLoad:            5,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}
}

// PlacementResult contains the selected destinations for EC shards
type PlacementResult struct {
	SelectedDisks []*DiskCandidate

	// Statistics
	ServersUsed int
	RacksUsed   int
	DCsUsed     int

	// Distribution maps
	ShardsPerServer map[string]int
	ShardsPerRack   map[string]int
	ShardsPerDC     map[string]int
}

// SelectDestinations selects the best disks for EC shard placement.
// This is the main entry point for EC placement logic.
//
// The algorithm works in multiple passes:
// 1. First pass: Select one disk from each rack (maximize rack diversity)
// 2. Second pass: Select one disk from each unused server in used racks (maximize server diversity)
// 3. Third pass: Select additional disks from servers already used (maximize disk diversity)
func SelectDestinations(disks []*DiskCandidate, config PlacementRequest) (*PlacementResult, error) {
	if len(disks) == 0 {
		return nil, fmt.Errorf("no disk candidates provided")
	}
	if config.ShardsNeeded <= 0 {
		return nil, fmt.Errorf("shardsNeeded must be positive, got %d", config.ShardsNeeded)
	}

	// Filter suitable disks
	suitable := filterSuitableDisks(disks, config)
	if len(suitable) == 0 {
		return nil, fmt.Errorf("no suitable disks found after filtering")
	}

	// Build indexes for efficient lookup
	rackToDisks := groupDisksByRack(suitable)
	serverToDisks := groupDisksByServer(suitable)
	_ = serverToDisks // Used for reference

	result := &PlacementResult{
		SelectedDisks:   make([]*DiskCandidate, 0, config.ShardsNeeded),
		ShardsPerServer: make(map[string]int),
		ShardsPerRack:   make(map[string]int),
		ShardsPerDC:     make(map[string]int),
	}

	usedDisks := make(map[string]bool)   // "nodeID:diskID" -> bool
	usedServers := make(map[string]bool) // nodeID -> bool
	usedRacks := make(map[string]bool)   // "dc:rack" -> bool

	// Pass 1: Select one disk from each rack (maximize rack diversity)
	if config.PreferDifferentRacks {
		// Sort racks by number of available servers (ascending) to prioritize underutilized racks
		sortedRacks := sortRacksByServerCount(rackToDisks)
		for _, rackKey := range sortedRacks {
			if len(result.SelectedDisks) >= config.ShardsNeeded {
				break
			}
			rackDisks := rackToDisks[rackKey]
			// Select best disk from this rack, preferring a new server
			disk := selectBestDiskFromRack(rackDisks, usedServers, usedDisks, config)
			if disk != nil {
				addDiskToResult(result, disk, usedDisks, usedServers, usedRacks)
			}
		}
	}

	// Pass 2: Select disks from unused servers in already-used racks
	if config.PreferDifferentServers && len(result.SelectedDisks) < config.ShardsNeeded {
		for _, rackKey := range getSortedRackKeys(rackToDisks) {
			if len(result.SelectedDisks) >= config.ShardsNeeded {
				break
			}
			rackDisks := rackToDisks[rackKey]
			for _, disk := range sortDisksByScore(rackDisks) {
				if len(result.SelectedDisks) >= config.ShardsNeeded {
					break
				}
				diskKey := getDiskKey(disk)
				if usedDisks[diskKey] {
					continue
				}
				// Skip if server already used (we want different servers in this pass)
				if usedServers[disk.NodeID] {
					continue
				}
				// Check server limit
				if config.MaxShardsPerServer > 0 && result.ShardsPerServer[disk.NodeID] >= config.MaxShardsPerServer {
					continue
				}
				// Check rack limit
				if config.MaxShardsPerRack > 0 && result.ShardsPerRack[getRackKey(disk)] >= config.MaxShardsPerRack {
					continue
				}
				addDiskToResult(result, disk, usedDisks, usedServers, usedRacks)
			}
		}
	}

	// Pass 3: Fill remaining slots from already-used servers (different disks)
	// Use round-robin across servers to balance shards evenly
	if len(result.SelectedDisks) < config.ShardsNeeded {
		// Group remaining disks by server
		serverToRemainingDisks := make(map[string][]*DiskCandidate)
		for _, disk := range suitable {
			if !usedDisks[getDiskKey(disk)] {
				serverToRemainingDisks[disk.NodeID] = append(serverToRemainingDisks[disk.NodeID], disk)
			}
		}

		// Sort each server's disks by score
		for serverID := range serverToRemainingDisks {
			serverToRemainingDisks[serverID] = sortDisksByScore(serverToRemainingDisks[serverID])
		}

		// Round-robin: repeatedly select from the server with the fewest shards
		for len(result.SelectedDisks) < config.ShardsNeeded {
			// Find server with fewest shards that still has available disks
			var bestServer string
			minShards := -1
			for serverID, disks := range serverToRemainingDisks {
				if len(disks) == 0 {
					continue
				}
				// Check server limit
				if config.MaxShardsPerServer > 0 && result.ShardsPerServer[serverID] >= config.MaxShardsPerServer {
					continue
				}
				shardCount := result.ShardsPerServer[serverID]
				if minShards == -1 || shardCount < minShards {
					minShards = shardCount
					bestServer = serverID
				} else if shardCount == minShards && serverID < bestServer {
					// Tie-break by server name for determinism
					bestServer = serverID
				}
			}

			if bestServer == "" {
				// No more servers with available disks
				break
			}

			// Pop the best disk from this server
			disks := serverToRemainingDisks[bestServer]
			disk := disks[0]
			serverToRemainingDisks[bestServer] = disks[1:]

			// Check rack limit
			if config.MaxShardsPerRack > 0 && result.ShardsPerRack[getRackKey(disk)] >= config.MaxShardsPerRack {
				continue
			}

			addDiskToResult(result, disk, usedDisks, usedServers, usedRacks)
		}
	}

	// Calculate final statistics
	result.ServersUsed = len(usedServers)
	result.RacksUsed = len(usedRacks)
	dcSet := make(map[string]bool)
	for _, disk := range result.SelectedDisks {
		dcSet[disk.DataCenter] = true
	}
	result.DCsUsed = len(dcSet)

	return result, nil
}

// filterSuitableDisks filters disks that are suitable for EC placement
func filterSuitableDisks(disks []*DiskCandidate, config PlacementRequest) []*DiskCandidate {
	var suitable []*DiskCandidate
	for _, disk := range disks {
		if disk.FreeSlots <= 0 {
			continue
		}
		if config.MaxTaskLoad > 0 && disk.LoadCount > config.MaxTaskLoad {
			continue
		}
		suitable = append(suitable, disk)
	}
	return suitable
}

// groupDisksByRack groups disks by their rack (dc:rack key)
func groupDisksByRack(disks []*DiskCandidate) map[string][]*DiskCandidate {
	result := make(map[string][]*DiskCandidate)
	for _, disk := range disks {
		key := getRackKey(disk)
		result[key] = append(result[key], disk)
	}
	return result
}

// groupDisksByServer groups disks by their server
func groupDisksByServer(disks []*DiskCandidate) map[string][]*DiskCandidate {
	result := make(map[string][]*DiskCandidate)
	for _, disk := range disks {
		result[disk.NodeID] = append(result[disk.NodeID], disk)
	}
	return result
}

// getRackKey returns the unique key for a rack (dc:rack)
func getRackKey(disk *DiskCandidate) string {
	return fmt.Sprintf("%s:%s", disk.DataCenter, disk.Rack)
}

// getDiskKey returns the unique key for a disk (nodeID:diskID)
func getDiskKey(disk *DiskCandidate) string {
	return fmt.Sprintf("%s:%d", disk.NodeID, disk.DiskID)
}

// sortRacksByServerCount returns rack keys sorted by number of servers (ascending)
func sortRacksByServerCount(rackToDisks map[string][]*DiskCandidate) []string {
	// Count unique servers per rack
	rackServerCount := make(map[string]int)
	for rackKey, disks := range rackToDisks {
		servers := make(map[string]bool)
		for _, disk := range disks {
			servers[disk.NodeID] = true
		}
		rackServerCount[rackKey] = len(servers)
	}

	keys := getSortedRackKeys(rackToDisks)
	sort.Slice(keys, func(i, j int) bool {
		// Sort by server count (descending) to pick from racks with more options first
		return rackServerCount[keys[i]] > rackServerCount[keys[j]]
	})
	return keys
}

// getSortedRackKeys returns rack keys in a deterministic order
func getSortedRackKeys(rackToDisks map[string][]*DiskCandidate) []string {
	keys := make([]string, 0, len(rackToDisks))
	for k := range rackToDisks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// selectBestDiskFromRack selects the best disk from a rack for EC placement
// It prefers servers that haven't been used yet
func selectBestDiskFromRack(disks []*DiskCandidate, usedServers, usedDisks map[string]bool, config PlacementRequest) *DiskCandidate {
	var bestDisk *DiskCandidate
	bestScore := -1.0
	bestIsFromUnusedServer := false

	for _, disk := range disks {
		if usedDisks[getDiskKey(disk)] {
			continue
		}
		isFromUnusedServer := !usedServers[disk.NodeID]
		score := calculateDiskScore(disk)

		// Prefer unused servers
		if isFromUnusedServer && !bestIsFromUnusedServer {
			bestDisk = disk
			bestScore = score
			bestIsFromUnusedServer = true
		} else if isFromUnusedServer == bestIsFromUnusedServer && score > bestScore {
			bestDisk = disk
			bestScore = score
		}
	}

	return bestDisk
}

// sortDisksByScore returns disks sorted by score (best first)
func sortDisksByScore(disks []*DiskCandidate) []*DiskCandidate {
	sorted := make([]*DiskCandidate, len(disks))
	copy(sorted, disks)
	sort.Slice(sorted, func(i, j int) bool {
		return calculateDiskScore(sorted[i]) > calculateDiskScore(sorted[j])
	})
	return sorted
}

// calculateDiskScore calculates a score for a disk candidate
// Higher score is better
func calculateDiskScore(disk *DiskCandidate) float64 {
	score := 0.0

	// Primary factor: available capacity (lower utilization is better)
	if disk.MaxVolumeCount > 0 {
		utilization := float64(disk.VolumeCount) / float64(disk.MaxVolumeCount)
		score += (1.0 - utilization) * 60.0 // Up to 60 points
	} else {
		score += 30.0 // Default if no max count
	}

	// Secondary factor: fewer shards already on this disk is better
	score += float64(10-disk.ShardCount) * 2.0 // Up to 20 points

	// Tertiary factor: lower load is better
	score += float64(10 - disk.LoadCount) // Up to 10 points

	return score
}

// addDiskToResult adds a disk to the result and updates tracking maps
func addDiskToResult(result *PlacementResult, disk *DiskCandidate,
	usedDisks, usedServers, usedRacks map[string]bool) {
	diskKey := getDiskKey(disk)
	rackKey := getRackKey(disk)

	result.SelectedDisks = append(result.SelectedDisks, disk)
	usedDisks[diskKey] = true
	usedServers[disk.NodeID] = true
	usedRacks[rackKey] = true
	result.ShardsPerServer[disk.NodeID]++
	result.ShardsPerRack[rackKey]++
	result.ShardsPerDC[disk.DataCenter]++
}

// VerifySpread checks if the placement result meets diversity requirements
func VerifySpread(result *PlacementResult, minServers, minRacks int) error {
	if result.ServersUsed < minServers {
		return fmt.Errorf("only %d servers used, need at least %d", result.ServersUsed, minServers)
	}
	if result.RacksUsed < minRacks {
		return fmt.Errorf("only %d racks used, need at least %d", result.RacksUsed, minRacks)
	}
	return nil
}

// CalculateIdealDistribution returns the ideal number of shards per server
// when we have a certain number of shards and servers
func CalculateIdealDistribution(totalShards, numServers int) (min, max int) {
	if numServers <= 0 {
		return 0, totalShards
	}
	min = totalShards / numServers
	max = min
	if totalShards%numServers != 0 {
		max = min + 1
	}
	return
}
