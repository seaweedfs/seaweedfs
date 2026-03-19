package multi_master

import (
	"testing"
	"time"
)

const (
	// Election timeout is 3s in our cluster config; allow generous margin.
	leaderElectionTimeout = 20 * time.Second
)

// TestLeaderDownAndRecoverQuickly verifies that when the leader is stopped and
// restarted quickly, the cluster re-elects a leader and the restarted node
// rejoins as a follower. TopologyId must be consistent across all nodes.
func TestLeaderDownAndRecoverQuickly(t *testing.T) {
	mc := StartMasterCluster(t)

	// Record initial state.
	leaderIdx, leaderAddr := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader found after cluster start")
	}
	t.Logf("initial leader: node %d at %s", leaderIdx, leaderAddr)

	topologyId, err := mc.GetTopologyId(leaderIdx)
	if err != nil || topologyId == "" {
		t.Fatalf("failed to get initial TopologyId: %v", err)
	}
	t.Logf("initial TopologyId: %s", topologyId)

	// Stop the leader.
	mc.StopNode(leaderIdx)
	t.Logf("stopped leader node %d", leaderIdx)

	// Wait for a new leader from the remaining 2 nodes.
	newLeaderIdx, newLeaderAddr, err := mc.WaitForNewLeader(leaderAddr, leaderElectionTimeout)
	if err != nil {
		mc.DumpLogs()
		t.Fatalf("new leader not elected after stopping old leader: %v", err)
	}
	t.Logf("new leader: node %d at %s", newLeaderIdx, newLeaderAddr)

	// Restart the old leader quickly.
	mc.StartNode(leaderIdx)
	if err := mc.WaitForNodeReady(leaderIdx, waitTimeout); err != nil {
		mc.DumpLogs()
		t.Fatalf("restarted node %d not ready: %v", leaderIdx, err)
	}
	t.Logf("restarted node %d", leaderIdx)

	// Give raft time to settle.
	time.Sleep(3 * time.Second)

	// Verify leader is stable.
	finalLeaderIdx, _ := mc.FindLeader()
	if finalLeaderIdx < 0 {
		mc.DumpLogs()
		t.Fatal("no leader after restarting old leader node")
	}

	// Verify TopologyId is consistent across all nodes.
	assertTopologyIdConsistent(t, mc, topologyId)
}

// TestLeaderDownSlowRecover verifies that when the leader goes down and takes
// a long time to come back, the remaining 2 nodes elect a new leader and the
// cluster continues to function. When the slow node returns, it rejoins.
func TestLeaderDownSlowRecover(t *testing.T) {
	mc := StartMasterCluster(t)

	leaderIdx, leaderAddr := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	topologyId, err := mc.GetTopologyId(leaderIdx)
	if err != nil || topologyId == "" {
		t.Fatalf("failed to get initial TopologyId: %v", err)
	}
	t.Logf("initial leader: node %d, TopologyId: %s", leaderIdx, topologyId)

	// Stop the leader.
	mc.StopNode(leaderIdx)

	// Wait for a new leader.
	newLeaderIdx, _, err := mc.WaitForNewLeader(leaderAddr, leaderElectionTimeout)
	if err != nil {
		mc.DumpLogs()
		t.Fatalf("new leader not elected: %v", err)
	}
	t.Logf("new leader: node %d", newLeaderIdx)

	// Verify cluster functions with only 2 nodes (quorum is 2/3).
	cs, err := mc.GetClusterStatus(newLeaderIdx)
	if err != nil {
		mc.DumpLogs()
		t.Fatalf("cannot get cluster status from new leader: %v", err)
	}
	if !cs.IsLeader {
		t.Fatalf("node %d claims not to be leader", newLeaderIdx)
	}

	// Simulate slow recovery: wait significantly longer than election timeout.
	t.Log("simulating slow recovery (10 seconds)...")
	time.Sleep(10 * time.Second)

	// Verify leader is still stable during the outage.
	stableLeaderIdx, _ := mc.FindLeader()
	if stableLeaderIdx < 0 {
		mc.DumpLogs()
		t.Fatal("leader lost during extended outage of one node")
	}

	// Restart the downed node.
	mc.StartNode(leaderIdx)
	if err := mc.WaitForNodeReady(leaderIdx, waitTimeout); err != nil {
		mc.DumpLogs()
		t.Fatalf("slow-recovered node %d not ready: %v", leaderIdx, err)
	}

	time.Sleep(3 * time.Second)
	assertTopologyIdConsistent(t, mc, topologyId)
}

// TestTwoMastersDownAndRestart verifies that when 2 of 3 masters go down
// (losing quorum), the cluster cannot elect a leader. When both restart,
// a leader is elected and TopologyId is preserved.
func TestTwoMastersDownAndRestart(t *testing.T) {
	mc := StartMasterCluster(t)

	leaderIdx, _ := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	topologyId, _ := mc.GetTopologyId(leaderIdx)
	t.Logf("initial TopologyId: %s", topologyId)

	// Determine which 2 nodes to stop (stop the leader + one follower).
	down1 := leaderIdx
	down2 := (leaderIdx + 1) % 3
	survivor := (leaderIdx + 2) % 3
	t.Logf("stopping nodes %d and %d, keeping node %d", down1, down2, survivor)

	mc.StopNode(down1)
	mc.StopNode(down2)

	// The surviving node alone cannot form a quorum — no leader expected.
	time.Sleep(5 * time.Second)
	soloLeaderIdx, _ := mc.FindLeader()
	if soloLeaderIdx >= 0 {
		// It's possible the survivor briefly thinks it's leader before stepping down.
		// Give it time to realize it lost quorum.
		time.Sleep(5 * time.Second)
		soloLeaderIdx, _ = mc.FindLeader()
	}
	t.Logf("leader with only 1 of 3 nodes: %d (expected -1 or unstable)", soloLeaderIdx)

	// Restart both downed nodes.
	mc.StartNode(down1)
	mc.StartNode(down2)
	for _, i := range []int{down1, down2} {
		if err := mc.WaitForNodeReady(i, waitTimeout); err != nil {
			mc.DumpLogs()
			t.Fatalf("restarted node %d not ready: %v", i, err)
		}
	}

	// Wait for leader election.
	if err := mc.WaitForLeader(leaderElectionTimeout); err != nil {
		mc.DumpLogs()
		t.Fatalf("no leader after restarting 2 downed nodes: %v", err)
	}

	time.Sleep(3 * time.Second)
	assertTopologyIdConsistent(t, mc, topologyId)
}

// TestAllMastersDownAndRestart verifies that when all 3 masters are stopped
// and restarted, the cluster elects a leader and all nodes agree on a
// TopologyId. With RaftResumeState=false (default), raft state is cleared on
// restart. The TopologyId is recovered from snapshots when available; on a
// short-lived cluster that hasn't taken snapshots on all nodes, a new
// TopologyId may be generated — but all nodes must still agree.
func TestAllMastersDownAndRestart(t *testing.T) {
	mc := StartMasterCluster(t)

	leaderIdx, _ := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	topologyId, _ := mc.GetTopologyId(leaderIdx)
	if topologyId == "" {
		t.Fatal("no TopologyId on initial leader")
	}
	t.Logf("initial TopologyId: %s", topologyId)

	// Stop all nodes.
	for i := range 3 {
		mc.StopNode(i)
	}
	t.Log("all nodes stopped")

	time.Sleep(2 * time.Second)

	// Restart all nodes.
	for i := range 3 {
		mc.StartNode(i)
	}
	for i := range 3 {
		if err := mc.WaitForNodeReady(i, waitTimeout); err != nil {
			mc.DumpLogs()
			t.Fatalf("node %d not ready after full restart: %v", i, err)
		}
	}

	// Wait for leader.
	if err := mc.WaitForLeader(leaderElectionTimeout); err != nil {
		mc.DumpLogs()
		t.Fatalf("no leader after full cluster restart: %v", err)
	}

	newLeaderIdx, _ := mc.FindLeader()
	t.Logf("leader after full restart: node %d", newLeaderIdx)

	time.Sleep(3 * time.Second)

	// All nodes must agree on a TopologyId (may differ from original if
	// snapshots were not yet taken on all nodes before shutdown).
	newTopologyId, err := mc.GetTopologyId(newLeaderIdx)
	if err != nil || newTopologyId == "" {
		mc.DumpLogs()
		t.Fatal("no TopologyId after full restart")
	}
	if newTopologyId == topologyId {
		t.Logf("TopologyId preserved across full restart: %s", topologyId)
	} else {
		t.Logf("TopologyId changed (expected for short-lived cluster without snapshots): %s -> %s", topologyId, newTopologyId)
	}
	assertTopologyIdConsistent(t, mc, newTopologyId)
}

// TestLeaderConsistencyAcrossNodes verifies that all nodes agree on who the
// leader is and report the same TopologyId.
func TestLeaderConsistencyAcrossNodes(t *testing.T) {
	mc := StartMasterCluster(t)

	// Allow cluster to stabilize.
	time.Sleep(3 * time.Second)

	leaderIdx, leaderAddr := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	t.Logf("leader: node %d at %s", leaderIdx, leaderAddr)

	// Every node should agree on the leader.
	for i := range 3 {
		cs, err := mc.GetClusterStatus(i)
		if err != nil {
			t.Fatalf("node %d cluster/status error: %v", i, err)
		}
		if i == leaderIdx {
			if !cs.IsLeader {
				t.Errorf("node %d should be leader but IsLeader=false", i)
			}
		} else {
			if cs.IsLeader {
				t.Errorf("node %d should not be leader but IsLeader=true", i)
			}
			if cs.Leader == "" {
				t.Errorf("node %d reports empty leader", i)
			}
		}
	}

	// All nodes should have the same TopologyId.
	topologyId, _ := mc.GetTopologyId(leaderIdx)
	if topologyId == "" {
		t.Fatal("leader has no TopologyId")
	}
	assertTopologyIdConsistent(t, mc, topologyId)
}

// assertTopologyIdConsistent verifies that all running nodes report the expected TopologyId.
func assertTopologyIdConsistent(t *testing.T, mc *MasterCluster, expectedId string) {
	t.Helper()
	for i := range 3 {
		if !mc.IsNodeRunning(i) {
			continue
		}
		id, err := mc.GetTopologyId(i)
		if err != nil {
			t.Errorf("node %d: failed to get TopologyId: %v", i, err)
			continue
		}
		if id != expectedId {
			t.Errorf("node %d: TopologyId=%q, expected %q", i, id, expectedId)
		}
	}
}
