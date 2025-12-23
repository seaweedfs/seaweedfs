# EC Distribution Package

This package provides erasure coding (EC) shard distribution algorithms that are:

- **Configurable**: Works with any EC ratio (e.g., 10+4, 8+4, 6+3)
- **Reusable**: Used by shell commands, worker tasks, and seaweed-enterprise
- **Topology-aware**: Distributes shards across data centers, racks, and nodes proportionally

## Usage

### Basic Usage with Default 10+4 EC

```go
import (
    "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/distribution"
)

// Parse replication policy
rep, _ := distribution.NewReplicationConfigFromString("110")

// Use default 10+4 EC configuration
ec := distribution.DefaultECConfig()

// Calculate distribution plan
dist := distribution.CalculateDistribution(ec, rep)

fmt.Println(dist.Summary())
// Output:
// EC Configuration: 10+4 (total: 14, can lose: 4)
// Replication: replication=110 (DCs:2, Racks/DC:2, Nodes/Rack:1)
// Distribution Plan:
//   Data Centers: 2 (target 7 shards each, max 9)
//   Racks per DC: 2 (target 4 shards each, max 6)
//   Nodes per Rack: 1 (target 4 shards each, max 6)
```

### Custom EC Ratios (seaweed-enterprise)

```go
// Create custom 8+4 EC configuration
ec, err := distribution.NewECConfig(8, 4)
if err != nil {
    log.Fatal(err)
}

rep, _ := distribution.NewReplicationConfigFromString("200")
dist := distribution.CalculateDistribution(ec, rep)

// Check fault tolerance
fmt.Println(dist.FaultToleranceAnalysis())
// Output:
// Fault Tolerance Analysis for 8+4:
//   DC Failure: SURVIVABLE ✓
//     - Losing one DC loses ~4 shards
//     - Remaining: 8 shards (need 8)
```

### Planning Shard Moves

```go
// Build topology analysis
analysis := distribution.NewTopologyAnalysis()

// Add nodes and their shard locations
for _, node := range nodes {
    analysis.AddNode(&distribution.TopologyNode{
        NodeID:     node.ID,
        DataCenter: node.DC,
        Rack:       node.Rack,
        FreeSlots:  node.FreeSlots,
    })
    for _, shardID := range node.ShardIDs {
        analysis.AddShardLocation(distribution.ShardLocation{
            ShardID:    shardID,
            NodeID:     node.ID,
            DataCenter: node.DC,
            Rack:       node.Rack,
        })
    }
}
analysis.Finalize()

// Create rebalancer and plan moves
rebalancer := distribution.NewRebalancer(ec, rep)
plan, err := rebalancer.PlanRebalance(analysis)

for _, move := range plan.Moves {
    fmt.Printf("Move shard %d from %s to %s\n", 
        move.ShardID, move.SourceNode.NodeID, move.DestNode.NodeID)
}
```

## Algorithm

### Proportional Distribution

The replication policy `XYZ` is interpreted as a ratio:

| Replication | DCs | Racks/DC | Nodes/Rack | 14 Shards Distribution |
|-------------|-----|----------|------------|------------------------|
| `000` | 1 | 1 | 1 | All in one place |
| `001` | 1 | 1 | 2 | 7 per node |
| `010` | 1 | 2 | 1 | 7 per rack |
| `100` | 2 | 1 | 1 | 7 per DC |
| `110` | 2 | 2 | 1 | 7/DC, 4/rack |
| `200` | 3 | 1 | 1 | 5 per DC |

### Rebalancing Process

1. **DC-level balancing**: Move shards to achieve target shards per DC
2. **Rack-level balancing**: Within each DC, balance across racks
3. **Node-level balancing**: Within each rack, balance across nodes

### Shard Priority: Data First, Parity Moves First

When rebalancing, the algorithm prioritizes keeping data shards spread out:

- **Data shards (0 to DataShards-1)**: Serve read requests directly
- **Parity shards (DataShards to TotalShards-1)**: Only used for reconstruction

**Rebalancing Strategy**:
- When moving shards FROM an overloaded node, **parity shards are moved first**
- This keeps data shards in place on well-distributed nodes
- Result: Data shards remain spread out for optimal read performance

```go
// Check shard type
if ec.IsDataShard(shardID) {
    // Shard serves read requests
}
if ec.IsParityShard(shardID) {
    // Shard only used for reconstruction
}

// Sort shards for placement (data first for initial distribution)
sorted := ec.SortShardsDataFirst(shards)

// Sort shards for rebalancing (parity first to move them away)
sorted := ec.SortShardsParityFirst(shards)
```

### Fault Tolerance

The package provides fault tolerance analysis:

- **DC Failure**: Can the data survive complete DC loss?
- **Rack Failure**: Can the data survive complete rack loss?
- **Node Failure**: Can the data survive single node loss?

For example, with 10+4 EC (can lose 4 shards):
- Need 4+ DCs for DC-level fault tolerance
- Need 4+ racks for rack-level fault tolerance
- Usually survivable at node level

## API Reference

### Types

- `ECConfig`: EC configuration (data shards, parity shards)
- `ReplicationConfig`: Parsed replication policy
- `ECDistribution`: Calculated distribution plan
- `TopologyAnalysis`: Current shard distribution analysis
- `Rebalancer`: Plans shard moves
- `RebalancePlan`: List of planned moves
- `ShardMove`: Single shard move operation

### Key Functions

- `NewECConfig(data, parity int)`: Create EC configuration
- `DefaultECConfig()`: Returns 10+4 configuration
- `CalculateDistribution(ec, rep)`: Calculate distribution plan
- `NewRebalancer(ec, rep)`: Create rebalancer
- `PlanRebalance(analysis)`: Generate rebalancing plan

## Integration

### Shell Commands

The shell package wraps this distribution package for `ec.balance`:

```go
import "github.com/seaweedfs/seaweedfs/weed/shell"

rebalancer := shell.NewProportionalECRebalancer(nodes, rp, diskType)
moves, _ := rebalancer.PlanMoves(volumeId, locations)
```

### Worker Tasks

Worker tasks can use the distribution package directly:

```go
import "github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/distribution"

ec := distribution.ECConfig{DataShards: 8, ParityShards: 4}
rep := distribution.NewReplicationConfig(rp)
dist := distribution.CalculateDistribution(ec, rep)
```

### seaweed-enterprise

Enterprise features can provide custom EC configurations:

```go
// Custom EC ratio from license/config
ec, _ := distribution.NewECConfig(customData, customParity)
rebalancer := distribution.NewRebalancer(ec, rep)
```

