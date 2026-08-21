package multi_master

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

var raftImplementations = []struct {
	name          string
	raftHashicorp bool
}{
	{"goraft", false},
	{"hashicorp", true},
}

// TestFreshClusterFormsWithoutALeader covers the other half of the bootstrap
// decision: with no leader anywhere, three masters starting together still have
// to mint a cluster between them.
func TestFreshClusterFormsWithoutALeader(t *testing.T) {
	for _, impl := range raftImplementations {
		t.Run(impl.name, func(t *testing.T) {
			mc := NewMasterCluster(t, impl.raftHashicorp)
			for i := range 3 {
				mc.StartNode(i)
			}

			if _, err := waitForCommonLeader(mc, waitTimeout); err != nil {
				mc.DumpLogs()
				t.Fatalf("fresh cluster did not converge: %v", err)
			}
			for i := range 3 {
				if err := waitForPeerCount(mc, i, 2, waitTimeout); err != nil {
					mc.DumpLogs()
					t.Fatalf("master %d does not see the full cluster: %v", i, err)
				}
			}
		})
	}
}

// TestScaleUpOntoExistingLeader mirrors a Kubernetes master StatefulSet whose
// replica count goes back from one to three: master 0 keeps running as the
// leader of a single-peer cluster while two fresh masters come up pointing at
// all three. The newcomers start with an empty raft log, so neither raft
// implementation lets them campaign — the sitting leader has to admit them.
func TestScaleUpOntoExistingLeader(t *testing.T) {
	for _, impl := range raftImplementations {
		t.Run(impl.name, func(t *testing.T) {
			mc := NewMasterCluster(t, impl.raftHashicorp)

			// Master 0 is alone in its peer list, the way the operator renders
			// -peers when spec.master.replicas is 1.
			mc.SetNodePeers(0, mc.NodeAddress(0))
			mc.StartNode(0)
			if err := mc.WaitForLeader(waitTimeout); err != nil {
				mc.DumpLogs()
				t.Fatalf("single master did not become leader: %v", err)
			}

			// Scale up. These two carry the full peer list; master 0 still
			// runs with the old one and has never heard of them.
			mc.StartNode(1)
			mc.StartNode(2)

			leader, err := waitForCommonLeader(mc, waitTimeout)
			if err != nil {
				mc.DumpLogs()
				t.Fatalf("masters did not converge after scaling up: %v", err)
			}
			if leader != mc.NodeAddress(0) {
				t.Fatalf("leader moved to %s, want the sitting leader %s", leader, mc.NodeAddress(0))
			}

			if err := waitForPeerCount(mc, 0, 2, waitTimeout); err != nil {
				mc.DumpLogs()
				t.Fatalf("leader did not admit both new masters: %v", err)
			}
		})
	}
}

// waitForCommonLeader waits until every running master names the same leader,
// and returns it.
func waitForCommonLeader(mc *MasterCluster, timeout time.Duration) (string, error) {
	var lastErr error
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, err := commonLeader(mc)
		if err == nil {
			return leader, nil
		}
		lastErr = err
		time.Sleep(waitTick)
	}
	return "", lastErr
}

func commonLeader(mc *MasterCluster) (string, error) {
	agreed := ""
	for i := range 3 {
		if !mc.IsNodeRunning(i) {
			continue
		}
		cs, err := mc.GetClusterStatus(i)
		if err != nil {
			return "", err
		}
		leader := pb.ServerAddress(cs.Leader).ToHttpAddress()
		if leader == "" {
			return "", fmt.Errorf("master %d has no leader", i)
		}
		if agreed == "" {
			agreed = leader
		} else if agreed != leader {
			return "", fmt.Errorf("masters disagree on the leader: %s and %s", agreed, leader)
		}
	}
	if agreed == "" {
		return "", fmt.Errorf("no master is running")
	}
	return agreed, nil
}

// waitForPeerCount waits until node i reports the given number of raft peers.
// The count excludes the node itself.
func waitForPeerCount(mc *MasterCluster, i, want int, timeout time.Duration) error {
	got := -1
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cs, err := mc.GetClusterStatus(i)
		if err == nil {
			got = peerCountExcludingSelf(cs.Peers, mc.NodeAddress(i))
			if got == want {
				return nil
			}
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("master %d reports %d peers, want %d", i, got, want)
}

func peerCountExcludingSelf(peers []string, self string) int {
	count := 0
	for _, peer := range peers {
		if pb.ServerAddress(peer).ToHttpAddress() != self {
			count++
		}
	}
	return count
}
