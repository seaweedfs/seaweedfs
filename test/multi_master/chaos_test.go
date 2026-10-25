package multi_master

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"
)

// chaosRounds is one stop or start per round, enough to walk in and out of
// quorum several times without turning this into a soak test.
const chaosRounds = 12

// TestRandomStartStopElection bounces masters at random and holds the election
// to the two things it must never get wrong: two masters claiming leadership at
// once, and a quorum that comes back without agreeing on one. The cluster's
// identity has to survive the whole walk — a master that re-mints a TopologyId
// here is the split brain SetTopologyId kills its peers over.
func TestRandomStartStopElection(t *testing.T) {
	for _, impl := range raftImplementations {
		t.Run(impl.name, func(t *testing.T) {
			seed := chaosSeed(t)
			t.Logf("seed %d — replay this walk with MULTI_MASTER_IT_SEED=%d", seed, seed)
			rng := rand.New(rand.NewPCG(seed, seed))

			mc := NewMasterCluster(t, impl.raftHashicorp)
			for i := range 3 {
				mc.StartNode(i)
			}
			if err := mc.WaitForLeader(waitTimeout); err != nil {
				mc.DumpLogs()
				t.Fatalf("cluster did not elect a leader: %v", err)
			}
			topologyId, err := mc.WaitForTopologyId(waitTimeout)
			if err != nil {
				mc.DumpLogs()
				t.Fatalf("no initial TopologyId: %v", err)
			}

			for round := 1; round <= chaosRounds; round++ {
				target := rng.IntN(3)
				if mc.IsNodeRunning(target) {
					mc.StopNode(target)
					t.Logf("round %d: stopped master %d, %d left running", round, target, runningMasters(mc))
				} else {
					mc.StartNode(target)
					t.Logf("round %d: started master %d, %d now running", round, target, runningMasters(mc))
				}

				if err := waitForSettledElection(mc, leaderElectionTimeout); err != nil {
					mc.DumpLogs()
					t.Fatalf("round %d (seed %d): %v", round, seed, err)
				}

				// /dir/status proxies to the leader, so this reads the value the
				// cluster as a whole is carrying, not any one master's copy.
				if runningMasters(mc) >= 2 {
					id, err := mc.WaitForTopologyId(leaderElectionTimeout)
					if err != nil {
						mc.DumpLogs()
						t.Fatalf("round %d (seed %d): %v", round, seed, err)
					}
					if id != topologyId {
						mc.DumpLogs()
						t.Fatalf("round %d (seed %d): TopologyId changed from %s to %s", round, seed, topologyId, id)
					}
				}
			}

			// Everything back up, so the walk ends on a full cluster.
			for i := range 3 {
				mc.StartNode(i)
			}
			if _, err := waitForCommonLeader(mc, leaderElectionTimeout); err != nil {
				mc.DumpLogs()
				t.Fatalf("cluster did not recover after the walk (seed %d): %v", seed, err)
			}
			for i := range 3 {
				if err := waitForPeerCount(mc, i, 2, leaderElectionTimeout); err != nil {
					mc.DumpLogs()
					t.Fatalf("master %d does not see the full cluster (seed %d): %v", i, seed, err)
				}
			}
			id, err := mc.WaitForTopologyId(leaderElectionTimeout)
			if err != nil {
				mc.DumpLogs()
				t.Fatalf("no TopologyId after the walk (seed %d): %v", seed, err)
			}
			if id != topologyId {
				mc.DumpLogs()
				t.Fatalf("TopologyId changed from %s to %s over the walk (seed %d)", topologyId, id, seed)
			}
		})
	}
}

// waitForSettledElection waits for a quorum to agree on one leader, and fails
// if two masters claim leadership across consecutive polls — anything shorter
// than that is a master on its way down.
//
// Below a quorum there is nothing to wait for: the walk moves on. A master that
// has lost its quorum can keep claiming leadership for tens of seconds under
// goraft, and it cannot commit anything in that window, so stepping down is a
// liveness question rather than a safety one. TestTwoMastersDownAndRestart
// holds that direction to account.
func waitForSettledElection(mc *MasterCluster, timeout time.Duration) error {
	if runningMasters(mc) < 2 {
		return nil
	}

	var lastErr error
	splitPolls := 0
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		claims, err := leaderClaims(mc)
		if err != nil {
			lastErr = err
		}
		if len(claims) > 1 {
			splitPolls++
			if splitPolls > 1 {
				return fmt.Errorf("masters %v all claim leadership", claims)
			}
			time.Sleep(waitTick)
			continue
		}
		splitPolls = 0

		if _, err := commonLeader(mc); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(waitTick)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("cluster did not settle within %v", timeout)
	}
	return lastErr
}

// leaderClaims returns the running masters that call themselves leader. The
// error reports masters that did not answer at all, which is a reason to keep
// waiting rather than a verdict.
func leaderClaims(mc *MasterCluster) (claims []int, err error) {
	for i := range 3 {
		if !mc.IsNodeRunning(i) {
			continue
		}
		cs, statusErr := mc.GetClusterStatus(i)
		if statusErr != nil {
			err = fmt.Errorf("master %d: %w", i, statusErr)
			continue
		}
		if cs.IsLeader {
			claims = append(claims, i)
		}
	}
	return claims, err
}

func runningMasters(mc *MasterCluster) int {
	count := 0
	for i := range 3 {
		if mc.IsNodeRunning(i) {
			count++
		}
	}
	return count
}

// chaosSeed picks the walk. It is random by default and always logged, so a
// failure names the seed that reproduces it.
func chaosSeed(t *testing.T) uint64 {
	t.Helper()
	if v := os.Getenv("MULTI_MASTER_IT_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("MULTI_MASTER_IT_SEED %q: %v", v, err)
		}
		return seed
	}
	return uint64(time.Now().UnixNano())
}
