package distsim

import (
	"fmt"
	"strings"
	"testing"
)

// --- Fixed scenarios ---

func TestSim_BasicWriteAndCommit(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	sim := NewSimulator(c, 42)

	sim.ScheduleWrites(3, 1, 5)
	sim.Run()

	if c.Coordinator.CommittedLSN < 1 {
		t.Fatalf("expected at least 1 committed write, got %d", c.Coordinator.CommittedLSN)
	}
	if err := sim.AssertCommittedDataCorrect(); err != nil {
		t.Fatal(err)
	}
	if len(sim.Errors) > 0 {
		t.Fatalf("invariant violations:\n%s", sim.ErrorString())
	}
}

func TestSim_CrashAfterCommit_DataSurvives(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	sim := NewSimulator(c, 99)

	// Write, let it commit, then crash primary, promote r1.
	sim.ScheduleWrites(5, 1, 3)
	sim.ScheduleCrashAndPromote(20, "r1", 22)

	sim.Run()

	if len(sim.Errors) > 0 {
		t.Fatalf("invariant violations:\n%s\nTrace:\n%s", sim.ErrorString(), sim.TraceString())
	}
	if err := sim.AssertCommittedDataCorrect(); err != nil {
		t.Fatal(err)
	}
}

func TestSim_PartitionThenHeal(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	sim := NewSimulator(c, 777)

	// Write some data.
	sim.ScheduleWrites(3, 1, 3)
	// Partition r2 at time 5.
	sim.EnqueueAt(5, EvLinkDown, "p", EventPayload{FromNode: "p", ToNode: "r2"})
	// Write more during partition.
	sim.ScheduleWrites(3, 8, 3)
	// Heal at time 15.
	sim.EnqueueAt(15, EvLinkUp, "p", EventPayload{FromNode: "p", ToNode: "r2"})
	// Write after heal.
	sim.ScheduleWrites(2, 18, 3)

	sim.Run()

	if len(sim.Errors) > 0 {
		t.Fatalf("invariant violations:\n%s", sim.ErrorString())
	}
	if err := sim.AssertCommittedDataCorrect(); err != nil {
		t.Fatal(err)
	}
}

func TestSim_SyncAll_UncommittedNotVisible(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1")
	sim := NewSimulator(c, 123)

	// Partition r1 so nothing can commit under sync_all.
	sim.EnqueueAt(0, EvLinkDown, "p", EventPayload{FromNode: "p", ToNode: "r1"})
	sim.ScheduleWrites(3, 1, 3)

	sim.Run()

	// Nothing should be committed.
	if c.Coordinator.CommittedLSN != 0 {
		t.Fatalf("sync_all with partitioned replica should not commit, got %d", c.Coordinator.CommittedLSN)
	}
	if len(sim.Errors) > 0 {
		t.Fatalf("invariant violations:\n%s", sim.ErrorString())
	}
}

func TestSim_MessageReorderingDoesNotBreakSafety(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	sim := NewSimulator(c, 555)
	sim.jitterMax = 8 // high jitter to force reordering

	sim.ScheduleWrites(10, 1, 5)
	sim.Run()

	if len(sim.Errors) > 0 {
		t.Fatalf("invariant violations with high jitter:\n%s", sim.ErrorString())
	}
	if err := sim.AssertCommittedDataCorrect(); err != nil {
		t.Fatal(err)
	}
}

// --- Randomized property-based testing ---

func TestSim_Randomized_CommitSafety(t *testing.T) {
	const numSeeds = 500
	const numWrites = 20
	failures := 0

	for seed := int64(0); seed < numSeeds; seed++ {
		c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
		sim := NewSimulator(c, seed)
		sim.MaxEvents = 2000

		// Random writes.
		sim.ScheduleWrites(numWrites, 1, 30)

		// Random crash + promote somewhere in the middle.
		crashTime := uint64(sim.rng.Intn(25) + 5)
		sim.ScheduleCrashAndPromote(crashTime, "r1", crashTime+3)

		sim.Run()

		if len(sim.Errors) > 0 {
			t.Errorf("seed %d: invariant violation:\n%s\nTrace (last 20):\n%s",
				seed, sim.ErrorString(), lastN(sim.trace, 20))
			failures++
			if failures >= 3 {
				t.Fatal("too many failures, stopping")
			}
		}
	}
	t.Logf("randomized: %d/%d seeds passed", numSeeds-failures, numSeeds)
}

func TestSim_Randomized_WithFaults(t *testing.T) {
	const numSeeds = 300
	failures := 0

	for seed := int64(0); seed < numSeeds; seed++ {
		c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
		sim := NewSimulator(c, seed)
		sim.FaultRate = 0.08
		sim.MaxEvents = 1500
		sim.jitterMax = 5

		// Interleave writes and random faults.
		for i := 0; i < 15; i++ {
			t := uint64(i*3 + 1)
			sim.EnqueueAt(t, EvWriteStart, "p", EventPayload{
				Write: Write{Block: uint64(sim.rng.Intn(8))},
			})
			sim.InjectRandomFault()
		}

		sim.Run()

		if len(sim.Errors) > 0 {
			t.Errorf("seed %d: invariant violation:\n%s", seed, sim.ErrorString())
			failures++
			if failures >= 3 {
				t.Fatal("too many failures, stopping")
			}
		}
	}
	t.Logf("randomized+faults: %d/%d seeds passed", numSeeds-failures, numSeeds)
}

func TestSim_Randomized_SyncAll(t *testing.T) {
	const numSeeds = 200
	failures := 0

	for seed := int64(0); seed < numSeeds; seed++ {
		c := NewCluster(CommitSyncAll, "p", "r1")
		sim := NewSimulator(c, seed)
		sim.MaxEvents = 1000

		sim.ScheduleWrites(10, 1, 20)

		// Random partition/heal.
		if sim.rng.Float64() < 0.5 {
			pTime := uint64(sim.rng.Intn(15) + 1)
			sim.EnqueueAt(pTime, EvLinkDown, "p", EventPayload{FromNode: "p", ToNode: "r1"})
			sim.EnqueueAt(pTime+uint64(sim.rng.Intn(10)+3), EvLinkUp, "p", EventPayload{FromNode: "p", ToNode: "r1"})
		}

		sim.Run()

		if len(sim.Errors) > 0 {
			t.Errorf("seed %d: invariant violation:\n%s", seed, sim.ErrorString())
			failures++
			if failures >= 3 {
				t.Fatal("too many failures, stopping")
			}
		}
	}
	t.Logf("sync_all randomized: %d/%d seeds passed", numSeeds-failures, numSeeds)
}

// --- Lock contention tests ---

func TestSim_LockContention_NoDoubleHold(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	sim := NewSimulator(c, 42)

	// Two threads try to acquire the same lock at the same time.
	sim.EnqueueAt(5, EvLockAcquire, "p", EventPayload{LockName: "shipMu", ThreadID: "writer-1"})
	sim.EnqueueAt(5, EvLockAcquire, "p", EventPayload{LockName: "shipMu", ThreadID: "writer-2"})

	// First release.
	sim.EnqueueAt(8, EvLockRelease, "p", EventPayload{LockName: "shipMu", ThreadID: "writer-1"})
	// Second release (whoever got granted after writer-1 releases).
	sim.EnqueueAt(11, EvLockRelease, "p", EventPayload{LockName: "shipMu", ThreadID: "writer-2"})

	sim.Run()

	if len(sim.Errors) > 0 {
		t.Fatalf("lock invariant violated:\n%s\nTrace:\n%s", sim.ErrorString(), sim.TraceString())
	}

	// Verify the trace shows one blocked, one granted.
	trace := sim.TraceString()
	if !containsStr(trace, "BLOCKED") {
		t.Fatal("expected one thread to be BLOCKED on lock contention")
	}
	if !containsStr(trace, "granted to") {
		t.Fatal("expected blocked thread to be granted after release")
	}
}

func TestSim_LockContention_Randomized(t *testing.T) {
	// Run many seeds with concurrent lock acquires at the same time.
	// The simulator should pick a random winner each time (seed-dependent).
	winners := map[string]int{}
	for seed := int64(0); seed < 100; seed++ {
		c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
		sim := NewSimulator(c, seed)

		sim.EnqueueAt(1, EvLockAcquire, "p", EventPayload{LockName: "mu", ThreadID: "A"})
		sim.EnqueueAt(1, EvLockAcquire, "p", EventPayload{LockName: "mu", ThreadID: "B"})
		sim.EnqueueAt(3, EvLockRelease, "p", EventPayload{LockName: "mu", ThreadID: "A"})
		sim.EnqueueAt(3, EvLockRelease, "p", EventPayload{LockName: "mu", ThreadID: "B"})

		sim.Run()

		if len(sim.Errors) > 0 {
			t.Fatalf("seed %d: %s", seed, sim.ErrorString())
		}

		// Check who got the lock first by looking at the trace.
		for _, te := range sim.trace {
			if te.Event.Kind == EvLockAcquire && containsStr(te.Note, "acquired") {
				winners[te.Event.Payload.ThreadID]++
				break
			}
		}
	}
	// Both threads should win at least some seeds (randomization works).
	if winners["A"] == 0 || winners["B"] == 0 {
		t.Fatalf("lock winner not randomized: A=%d B=%d", winners["A"], winners["B"])
	}
	t.Logf("lock winner distribution: A=%d B=%d", winners["A"], winners["B"])
}

func containsStr(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && strings.Contains(s, substr)
}

// --- Helpers ---

func lastN(trace []TraceEntry, n int) string {
	start := len(trace) - n
	if start < 0 {
		start = 0
	}
	s := ""
	for _, te := range trace[start:] {
		s += fmt.Sprintf("[t=%d] %s on %s: %s\n", te.Time, te.Event.Kind, te.Event.NodeID, te.Note)
	}
	return s
}
