package distsim

import (
	"fmt"
	"math/rand"
	"sort"
)

type RandomEvent string

const (
	RandomCommitWrite   RandomEvent = "commit_write"
	RandomTick          RandomEvent = "tick"
	RandomDisconnect    RandomEvent = "disconnect"
	RandomReconnect     RandomEvent = "reconnect"
	RandomStopNode      RandomEvent = "stop_node"
	RandomStartNode     RandomEvent = "start_node"
	RandomPromote       RandomEvent = "promote"
	RandomTakeSnapshot  RandomEvent = "take_snapshot"
	RandomCatchup       RandomEvent = "catchup"
	RandomRebuild       RandomEvent = "rebuild"
)

type RandomStep struct {
	Step   int
	Event  RandomEvent
	Detail string
}

type RandomResult struct {
	Seed      int64
	Steps     []RandomStep
	Cluster   *Cluster
	Snapshots []string
}

func RunRandomScenario(seed int64, steps int) (*RandomResult, error) {
	rng := rand.New(rand.NewSource(seed))
	cluster := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	result := &RandomResult{
		Seed:    seed,
		Cluster: cluster,
	}

	for i := 0; i < steps; i++ {
		step, err := runRandomStep(cluster, rng, i)
		if err != nil {
			result.Steps = append(result.Steps, step)
			return result, err
		}
		result.Steps = append(result.Steps, step)
		if err := assertClusterInvariants(cluster); err != nil {
			return result, fmt.Errorf("seed=%d step=%d event=%s detail=%s: %w", seed, i, step.Event, step.Detail, err)
		}
	}
	return result, assertClusterInvariants(cluster)
}

func runRandomStep(c *Cluster, rng *rand.Rand, step int) (RandomStep, error) {
	events := []RandomEvent{
		RandomCommitWrite,
		RandomTick,
		RandomDisconnect,
		RandomReconnect,
		RandomStopNode,
		RandomStartNode,
		RandomPromote,
		RandomTakeSnapshot,
		RandomCatchup,
		RandomRebuild,
	}
	ev := events[rng.Intn(len(events))]
	rs := RandomStep{Step: step, Event: ev}

	switch ev {
	case RandomCommitWrite:
		block := uint64(rng.Intn(8) + 1)
		lsn := c.CommitWrite(block)
		rs.Detail = fmt.Sprintf("block=%d lsn=%d", block, lsn)
	case RandomTick:
		n := rng.Intn(3) + 1
		c.TickN(n)
		rs.Detail = fmt.Sprintf("ticks=%d", n)
	case RandomDisconnect:
		from, to := randomPair(c, rng)
		c.Disconnect(from, to)
		rs.Detail = fmt.Sprintf("%s->%s", from, to)
	case RandomReconnect:
		from, to := randomPair(c, rng)
		c.Connect(from, to)
		rs.Detail = fmt.Sprintf("%s->%s", from, to)
	case RandomStopNode:
		id := randomNodeID(c, rng)
		c.StopNode(id)
		rs.Detail = id
	case RandomStartNode:
		id := randomNodeID(c, rng)
		c.StartNode(id)
		rs.Detail = id
	case RandomPromote:
		if primary := c.Primary(); primary != nil && primary.Running {
			rs.Detail = "primary_still_running"
			return rs, nil
		}
		candidates := promotableNodes(c)
		if len(candidates) == 0 {
			rs.Detail = "no_candidate"
			return rs, nil
		}
		id := candidates[rng.Intn(len(candidates))]
		rs.Detail = id
		if err := c.Promote(id); err != nil {
			return rs, err
		}
	case RandomTakeSnapshot:
		primary := c.Primary()
		if primary == nil || !primary.Running {
			rs.Detail = "no_primary"
			return rs, nil
		}
		lsn := c.Coordinator.CommittedLSN
		id := fmt.Sprintf("snap-%s-%d", primary.ID, lsn)
		primary.Storage.TakeSnapshot(id, lsn)
		rs.Detail = fmt.Sprintf("%s@%d", id, lsn)
	case RandomCatchup:
		id := randomReplicaID(c, rng)
		if id == "" {
			rs.Detail = "no_replica"
			return rs, nil
		}
		node := c.Nodes[id]
		if node == nil || !node.Running {
			rs.Detail = id + ":down"
			return rs, nil
		}
		start := node.Storage.FlushedLSN
		end := c.Coordinator.CommittedLSN
		if end <= start {
			rs.Detail = fmt.Sprintf("%s:no_gap", id)
			return rs, nil
		}
		rs.Detail = fmt.Sprintf("%s:%d..%d", id, start+1, end)
		if err := c.RecoverReplicaFromPrimary(id, start, end); err != nil {
			return rs, err
		}
	case RandomRebuild:
		id := randomReplicaID(c, rng)
		if id == "" {
			rs.Detail = "no_replica"
			return rs, nil
		}
		primary := c.Primary()
		node := c.Nodes[id]
		if primary == nil || node == nil || !primary.Running || !node.Running {
			rs.Detail = id + ":unavailable"
			return rs, nil
		}
		snapshotIDs := make([]string, 0, len(primary.Storage.Snapshots))
		for snapID := range primary.Storage.Snapshots {
			snapshotIDs = append(snapshotIDs, snapID)
		}
		if len(snapshotIDs) == 0 {
			rs.Detail = id + ":no_snapshot"
			return rs, nil
		}
		sort.Strings(snapshotIDs)
		snapID := snapshotIDs[rng.Intn(len(snapshotIDs))]
		rs.Detail = fmt.Sprintf("%s:%s->%d", id, snapID, c.Coordinator.CommittedLSN)
		if err := c.RebuildReplicaFromSnapshot(id, snapID, c.Coordinator.CommittedLSN); err != nil {
			return rs, err
		}
	default:
		return rs, fmt.Errorf("unknown random event %s", ev)
	}

	return rs, nil
}

func randomNodeID(c *Cluster, rng *rand.Rand) string {
	ids := append([]string(nil), c.Coordinator.Members...)
	sort.Strings(ids)
	if len(ids) == 0 {
		return ""
	}
	return ids[rng.Intn(len(ids))]
}

func randomReplicaID(c *Cluster, rng *rand.Rand) string {
	ids := c.replicaIDs()
	if len(ids) == 0 {
		return ""
	}
	return ids[rng.Intn(len(ids))]
}

func randomPair(c *Cluster, rng *rand.Rand) (string, string) {
	from := randomNodeID(c, rng)
	to := randomNodeID(c, rng)
	if from == to {
		ids := append([]string(nil), c.Coordinator.Members...)
		sort.Strings(ids)
		for _, id := range ids {
			if id != from {
				to = id
				break
			}
		}
	}
	return from, to
}

func promotableNodes(c *Cluster) []string {
	out := make([]string, 0)
	want := c.Reference.StateAt(c.Coordinator.CommittedLSN)
	for _, id := range c.Coordinator.Members {
		n := c.Nodes[id]
		if n == nil || !n.Running || n.Storage.FlushedLSN < c.Coordinator.CommittedLSN {
			continue
		}
		if !EqualState(n.Storage.StateAt(c.Coordinator.CommittedLSN), want) {
			continue
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func assertClusterInvariants(c *Cluster) error {
	committed := c.Coordinator.CommittedLSN
	want := c.Reference.StateAt(committed)

	for lsn, p := range c.Pending {
		if p.Committed && lsn > committed {
			return fmt.Errorf("pending lsn %d marked committed above coordinator committed lsn %d", lsn, committed)
		}
	}

	for _, id := range promotableNodes(c) {
		n := c.Nodes[id]
		got := n.Storage.StateAt(committed)
		if !EqualState(got, want) {
			return fmt.Errorf("promotable node %s mismatch at committed lsn %d: got=%v want=%v", id, committed, got, want)
		}
	}

	primary := c.Primary()
	if primary != nil && primary.Running && primary.Epoch == c.Coordinator.Epoch {
		got := primary.Storage.StateAt(committed)
		if !EqualState(got, want) {
			return fmt.Errorf("primary %s mismatch at committed lsn %d: got=%v want=%v", primary.ID, committed, got, want)
		}
	}

	return nil
}
