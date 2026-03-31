package distsim

import (
	"math/rand"
	"testing"
)

// Phase 4.5: Adversarial predicate search.
// These tests run randomized/semi-randomized scenarios and check danger
// predicates after each step. The goal is to find protocol violations
// that handwritten scenarios might miss.

// TestAdversarial_RandomWritesAndCrashes runs random write + crash + restart
// sequences and checks all danger predicates after each step.
func TestAdversarial_RandomWritesAndCrashes(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 50; trial++ {
		c := NewCluster(CommitSyncAll, "p", "r1", "r2")

		// Random sequence of operations.
		for step := 0; step < 30; step++ {
			op := rng.Intn(10)
			switch {
			case op < 5:
				// Write.
				block := uint64(rng.Intn(10) + 1)
				c.CommitWrite(block)
			case op < 7:
				// Tick (advance time, deliver messages).
				c.Tick()
			case op < 8:
				// Crash a random node.
				nodes := []string{"p", "r1", "r2"}
				target := nodes[rng.Intn(3)]
				node := c.Nodes[target]
				if node.Running {
					node.Storage.Crash()
					node.Running = false
					node.ReplicaState = NodeStateLagging
				}
			case op < 9:
				// Restart a crashed node.
				nodes := []string{"p", "r1", "r2"}
				target := nodes[rng.Intn(3)]
				node := c.Nodes[target]
				if !node.Running {
					node.Storage.Restart()
					node.Running = true
					node.ReplicaState = NodeStateLagging // needs catch-up
				}
			default:
				// Flusher tick on all running nodes.
				for _, node := range c.Nodes {
					if node.Running {
						node.Storage.ApplyToExtent(node.Storage.WALDurableLSN)
						node.Storage.AdvanceCheckpoint(node.Storage.WALDurableLSN)
					}
				}
			}

			// Check predicates after every step.
			violations := CheckAllPredicates(c)
			if len(violations) > 0 {
				for name, detail := range violations {
					t.Errorf("trial %d step %d: PREDICATE VIOLATED [%s]: %s", trial, step, name, detail)
				}
				t.FailNow()
			}
		}
	}
}

// TestAdversarial_FailoverChainWithPredicates runs a sequence of
// failovers (promote, crash, promote) and checks predicates.
func TestAdversarial_FailoverChainWithPredicates(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1", "r2")

	// Write some data and commit.
	for i := 0; i < 5; i++ {
		c.CommitWrite(uint64(i + 1))
	}
	c.TickN(5)

	check := func(label string) {
		violations := CheckAllPredicates(c)
		for name, detail := range violations {
			t.Fatalf("%s: PREDICATE VIOLATED [%s]: %s", label, name, detail)
		}
	}

	check("after initial writes")

	// Kill primary.
	c.Nodes["p"].Running = false
	c.Nodes["p"].Storage.Crash()

	// Promote r1.
	c.Promote("r1")
	c.TickN(3)
	check("after first promotion")

	// Write more under new primary.
	for i := 0; i < 3; i++ {
		c.CommitWrite(uint64(i + 10))
	}
	c.TickN(5)
	check("after writes on new primary")

	// Kill new primary.
	c.Nodes["r1"].Running = false
	c.Nodes["r1"].Storage.Crash()

	// Promote r2.
	c.Promote("r2")
	c.TickN(3)
	check("after second promotion")

	// Write more under third primary.
	c.CommitWrite(99)
	c.TickN(5)
	check("after writes on third primary")
}

// TestAdversarial_CatchUpUnderLoad runs catch-up while the primary keeps
// writing, then checks predicates for livelock.
func TestAdversarial_CatchUpUnderLoad(t *testing.T) {
	c := NewCluster(CommitSyncAll, "p", "r1")

	// Write initial data.
	for i := 0; i < 10; i++ {
		c.CommitWrite(uint64(i + 1))
	}
	c.TickN(5)

	// Disconnect r1.
	c.Nodes["r1"].Running = false

	// Write more while r1 is down.
	for i := 0; i < 20; i++ {
		c.CommitWrite(uint64(i + 100))
		c.Tick()
	}

	// Reconnect r1 — needs catch-up.
	c.Nodes["r1"].Running = true
	c.Nodes["r1"].ReplicaState = NodeStateLagging

	// Attempt catch-up while primary keeps writing.
	for step := 0; step < 20; step++ {
		// Primary writes more.
		c.CommitWrite(uint64(step + 200))
		c.Tick()

		// Attempt catch-up progress.
		c.CatchUpWithEscalation("r1", 5)

		// Check predicates.
		violations := CheckAllPredicates(c)
		for name, detail := range violations {
			t.Fatalf("step %d: PREDICATE VIOLATED [%s]: %s", step, name, detail)
		}
	}

	// After the loop, r1 should be either InSync or NeedsRebuild.
	state := c.Nodes["r1"].ReplicaState
	if state != NodeStateInSync && state != NodeStateNeedsRebuild {
		t.Fatalf("r1 should be InSync or NeedsRebuild after catch-up under load, got %s", state)
	}
}

// TestAdversarial_CheckpointGCThenCrash runs checkpoint + WAL GC + crash
// sequences and verifies acked data is never lost.
func TestAdversarial_CheckpointGCThenCrash(t *testing.T) {
	rng := rand.New(rand.NewSource(99))

	for trial := 0; trial < 30; trial++ {
		c := NewCluster(CommitSyncAll, "p", "r1")

		// Write and commit data.
		for i := 0; i < 15; i++ {
			c.CommitWrite(uint64(rng.Intn(20) + 1))
		}
		c.TickN(10)

		// Flusher + checkpoint at various points.
		for _, node := range c.Nodes {
			if node.Running {
				flushTo := node.Storage.WALDurableLSN
				node.Storage.ApplyToExtent(flushTo)
				// Checkpoint at a random point up to flush.
				cpLSN := uint64(rng.Int63n(int64(flushTo+1)))
				node.Storage.AdvanceCheckpoint(cpLSN)

				// GC WAL entries before checkpoint.
				retained := make([]Write, 0)
				for _, w := range node.Storage.WAL {
					if w.LSN > node.Storage.CheckpointLSN {
						retained = append(retained, w)
					}
				}
				node.Storage.WAL = retained
			}
		}

		// Crash primary.
		primary := c.Primary()
		if primary != nil {
			primary.Storage.Crash()
			primary.Storage.Restart()
		}

		// Check predicates — committed data must still be recoverable.
		violations := CheckAllPredicates(c)
		for name, detail := range violations {
			t.Errorf("trial %d: PREDICATE VIOLATED [%s]: %s", trial, name, detail)
		}
	}
}
