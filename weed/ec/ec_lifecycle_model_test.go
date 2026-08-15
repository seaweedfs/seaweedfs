package ec

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Bounded-exhaustive model check of the EC volume lifecycle.
//
// The randomized chaos harness samples the state space; this test enumerates
// it. The lifecycle of one volume is modeled as a state machine whose
// transitions mirror, step by step, the real pipelines in this package, and
// the checker explores EVERY schedule within the bound: every operation, a
// crash (kill, no cleanup) at every step boundary, an error return (which
// runs the rollback, itself crashable at every step) at every step boundary,
// a volume-server restart (running the startup reconciliation rules) in every
// quiescent state, and every recovery re-run from every crashed state.
//
// Two safety invariants are asserted in every reachable state, including
// mid-operation and post-crash:
//
//	I1 durability: the volume's data is always recoverable — the regular
//	   replica exists, or some single generation holds at least dataShards
//	   shard files.
//	I2 single serving generation: at most one generation is ever mounted.
//	I3 single generation on disk: the sweep-before-generate discipline keeps
//	   at most one generation's files on disk through every schedule. (An
//	   externally planted file breaks this by construction; that path is the
//	   chaos harness's, not the orchestration's.)
//
// And one convergence property: from every quiescent state, the prescribed
// recovery — re-running the interrupted operation, per the restart-not-resume
// model — terminates in a clean state (a writable regular volume with no EC
// leftovers, or exactly one complete mounted generation with no source
// replica and no surplus copies).
//
// The model's fidelity contract: each step below names the code it stands
// for. When the pipeline order changes, this file must change with it.
//
//	encode      = ProcessEcEncodeBatch/doEcEncode: markReadonly
//	              (markVolumeReplicaWritable false) → sweep
//	              (clearPreexistingEcShards, removes every prior generation) →
//	              generate (VolumeEcShardsGenerate, a new generation's files) →
//	              mount (VolumeEcShardsMount, registers it) → balance
//	              (EcBalance; a crash mid-move leaves a surplus copy, never a
//	              loss, because moves copy before they delete) → verify
//	              (verifyEcShardsBeforeDelete) → deleteSource
//	              (doDeleteVolumesWithLocations, the commit point).
//	rollback    = rollbackFailedEcEncode, run only on an error return before
//	              the commit: rollbackSweep (clearPreexistingEcShards again) →
//	              restoreWritable. A kill runs nothing.
//	decode      = DoEcDecode: collect (collectEcShards) → generateVolume
//	              (VolumeEcShardsToVolume) → mountVolume (VolumeMount) →
//	              verifyDecoded (verifyDecodedVolumeBeforeDelete) → three
//	              per-location shard deletions (unmountAndDeleteEcShards).
//	              Forward-only: no rollback.
//	reconcile   = the volume server startup rules (weed/storage
//	              disk_location_ec.go): with the source .dat present, a
//	              partial generation fails validation and is deleted
//	              (handleFoundEcxFile/validateEcVolume); surviving shard files
//	              with their sidecars re-register (the #9212 adoption).
//	dedup       = the balance's dedup phase removing surplus copies
//	              (ecbalancer "dedup" moves + verifyEcShardOnKeepNode).
//
// Deliberately outside the model: multi-replica sources (the pipeline syncs
// and reduces to one best replica before encoding), per-node shard placement
// (I1/I2 do not depend on it), and externally planted files (the chaos
// harness covers adoption of foreign shards).

const (
	modelDataShards  = 10
	modelTotalShards = 14
)

type modelGen struct {
	files   int  // shard files on disk, whether or not registered
	mounted bool // registered and serving
	surplus int  // duplicate copies left by an interrupted balance move
}

type modelState struct {
	replica         bool
	replicaReadonly bool
	gens            []modelGen // oldest first; a new encode appends
}

func (s modelState) clone() modelState {
	c := s
	c.gens = append([]modelGen(nil), s.gens...)
	return c
}

func (s modelState) key() string {
	var b strings.Builder
	fmt.Fprintf(&b, "r%v,ro%v", s.replica, s.replicaReadonly)
	for _, g := range s.gens {
		fmt.Fprintf(&b, "|f%d,m%v,s%d", g.files, g.mounted, g.surplus)
	}
	return b.String()
}

// dropEmptyGens removes generations with no files left.
func (s *modelState) dropEmptyGens() {
	kept := s.gens[:0]
	for _, g := range s.gens {
		if g.files > 0 {
			kept = append(kept, g)
		}
	}
	s.gens = kept
}

// ── invariants ──────────────────────────────────────────────────────────────

func checkInvariants(s modelState) error {
	// I1: durability.
	recoverable := s.replica
	for _, g := range s.gens {
		if g.files >= modelDataShards {
			recoverable = true
		}
	}
	if !recoverable {
		return fmt.Errorf("I1 violated: no replica and no generation with >= %d shard files", modelDataShards)
	}
	// I2: at most one mounted generation.
	mounted := 0
	for _, g := range s.gens {
		if g.mounted {
			mounted++
		}
	}
	if mounted > 1 {
		return fmt.Errorf("I2 violated: %d generations mounted", mounted)
	}
	// I3: at most one generation's files on disk.
	withFiles := 0
	for _, g := range s.gens {
		if g.files > 0 {
			withFiles++
		}
	}
	if withFiles > 1 {
		return fmt.Errorf("I3 violated: %d generations have files on disk", withFiles)
	}
	return nil
}

func isCleanEc(s modelState) bool {
	if s.replica || len(s.gens) != 1 {
		return false
	}
	g := s.gens[0]
	return g.files == modelTotalShards && g.mounted && g.surplus == 0
}

// ── operations as step lists ────────────────────────────────────────────────

// A step mutates the state; ok=false means the step's precondition failed
// (the real command would return an error at this point).
type modelStep struct {
	name  string
	apply func(*modelState) (ok bool)
}

func encodeSteps() []modelStep {
	return []modelStep{
		{"markReadonly", func(s *modelState) bool {
			if !s.replica {
				return false
			}
			s.replicaReadonly = true
			return true
		}},
		// The orphan sweep tears down per (node, volume); a crash can land
		// between nodes, leaving a partially removed generation.
		{"sweepLoc0", func(s *modelState) bool { sweepLocation(s, 0); return true }},
		{"sweepLoc1", func(s *modelState) bool { sweepLocation(s, 1); return true }},
		{"sweepLoc2", func(s *modelState) bool { sweepLocation(s, 2); return true }},
		// Generation writes the shard files on the generation host; a crash
		// mid-write leaves a partial, unregistered set beside the .dat, which
		// the startup validation removes and the next sweep also covers.
		{"generateHalf", func(s *modelState) bool {
			if !s.replica {
				return false
			}
			s.gens = append(s.gens, modelGen{files: modelTotalShards / 2})
			return true
		}},
		{"generateRest", func(s *modelState) bool {
			s.gens[len(s.gens)-1].files = modelTotalShards
			return true
		}},
		{"mount", func(s *modelState) bool {
			s.gens[len(s.gens)-1].mounted = true
			return true
		}},
		// balance: a completed run changes placement only. Its crash variant
		// is modeled by crashing between "balanceCopy" and "balanceDelete":
		// the copy landed, the source deletion did not.
		{"balanceCopy", func(s *modelState) bool {
			s.gens[len(s.gens)-1].surplus++
			return true
		}},
		{"balanceDelete", func(s *modelState) bool {
			s.gens[len(s.gens)-1].surplus--
			return true
		}},
		{"verify", func(s *modelState) bool {
			return s.gens[len(s.gens)-1].files >= modelDataShards
		}},
		{"deleteSource", func(s *modelState) bool {
			s.replica = false
			s.replicaReadonly = false
			return true
		}},
	}
}

// encodeCommitIndex is the index of deleteSource: an error return at or after
// it must not roll back (the shards are the only copy once it runs).
func encodeCommitIndex() int { return len(encodeSteps()) - 1 }

// sweepLocation removes one location's share of every generation's files —
// the per-(node, volume) teardown of clearPreexistingEcShards. The last
// location clears the remainder, sidecars included.
func sweepLocation(s *modelState, loc int) {
	for gi := range s.gens {
		g := &s.gens[gi]
		chunk := modelTotalShards / 3
		if loc == 2 {
			g.files, g.mounted, g.surplus = 0, false, 0
		} else if g.files > chunk {
			g.files -= chunk
		} else {
			g.files = 0
		}
	}
	s.dropEmptyGens()
}

func rollbackSteps() []modelStep {
	return []modelStep{
		{"rollbackSweep", func(s *modelState) bool {
			s.gens = nil
			return true
		}},
		{"restoreWritable", func(s *modelState) bool {
			if s.replica {
				s.replicaReadonly = false
			}
			return true
		}},
	}
}

func decodeSteps() []modelStep {
	steps := []modelStep{
		{"collect", func(s *modelState) bool {
			for _, g := range s.gens {
				if g.mounted && g.files >= modelDataShards {
					return true
				}
			}
			return false
		}},
		{"generateVolume", func(s *modelState) bool {
			s.replica = true
			return true
		}},
		{"mountVolume", func(s *modelState) bool { return true }},
		{"verifyDecoded", func(s *modelState) bool { return s.replica }},
	}
	// Shard deletion is per holder location; three locations model a spread
	// volume, so a crash can land between any two of them.
	for i := 0; i < 3; i++ {
		i := i
		steps = append(steps, modelStep{fmt.Sprintf("deleteShardsLoc%d", i), func(s *modelState) bool {
			for gi := range s.gens {
				g := &s.gens[gi]
				if !g.mounted && g.files == 0 {
					continue
				}
				// Each location holds roughly a third of the shards; the last
				// pass clears the remainder and the generation with it.
				chunk := modelTotalShards / 3
				if i == 2 {
					g.files, g.surplus, g.mounted = 0, 0, false
				} else if g.files > chunk {
					g.files -= chunk
				}
			}
			s.dropEmptyGens()
			return true
		}})
	}
	return steps
}

// reconcile applies the volume-server startup rules after a crash-restart.
func reconcile(s *modelState) {
	for gi := range s.gens {
		g := &s.gens[gi]
		if s.replica && g.files < modelTotalShards {
			// .dat present and the local generation is partial: validation
			// fails and the files are removed (disk_location_ec.go).
			g.files, g.mounted, g.surplus = 0, false, 0
			continue
		}
		if g.files > 0 {
			// Surviving files re-register through their sidecars (#9212).
			g.mounted = true
		}
	}
	s.dropEmptyGens()
}

// dedupSurplus is the balance dedup phase.
func dedupSurplus(s *modelState) {
	for gi := range s.gens {
		s.gens[gi].surplus = 0
	}
}

// ── recovery convergence ────────────────────────────────────────────────────

// runToCompletion applies steps without interruption; false when a
// precondition fails (the real re-run would error out).
func runToCompletion(s *modelState, steps []modelStep) bool {
	for _, st := range steps {
		if !st.apply(s) {
			return false
		}
	}
	return true
}

// recover applies the prescribed recovery for a quiescent state and reports
// whether it converges to a clean state. Per the restart-not-resume model:
// a surviving replica means the encode restarts from scratch; no replica
// means the encode had committed and only surplus cleanup can remain; a
// mounted generation beside a replica can also be decoded away.
func recoverModel(s modelState) (modelState, error) {
	// A crash-restart happens before any operator action; its reconciliation
	// must itself keep the invariants.
	reconcile(&s)
	if err := checkInvariants(s); err != nil {
		return s, fmt.Errorf("after reconcile: %w", err)
	}

	if !s.replica {
		// The encode committed: shards are the volume. Dedup cleans surplus.
		dedupSurplus(&s)
		if !isCleanEc(s) {
			return s, fmt.Errorf("no replica and not clean EC")
		}
		return s, nil
	}

	// Replica survives: re-run the encode from scratch (sweep + regenerate),
	// then the volume is EC; this is the restart path every interrupted
	// encode and every leftover-orphan state takes.
	if !runToCompletion(&s, encodeSteps()) {
		return s, fmt.Errorf("recovery encode did not complete")
	}
	dedupSurplus(&s)
	if err := checkInvariants(s); err != nil {
		return s, fmt.Errorf("after recovery encode: %w", err)
	}
	if !isCleanEc(s) {
		return s, fmt.Errorf("recovery encode did not converge to clean EC")
	}
	// And a decode must be able to bring it back to a regular volume.
	if !runToCompletion(&s, decodeSteps()) {
		return s, fmt.Errorf("decode after recovery did not complete")
	}
	if err := checkInvariants(s); err != nil {
		return s, fmt.Errorf("after decode: %w", err)
	}
	if !s.replica || len(s.gens) != 0 {
		return s, fmt.Errorf("decode after recovery did not converge to a bare regular volume")
	}
	return s, nil
}

// ── exhaustive exploration ──────────────────────────────────────────────────

type trace []string

// explore runs every schedule of one operation from state s: after every
// step, the schedule may continue, crash (kill: stop, nothing else runs), or
// error out (rollback runs, itself crashable at every step). Every resulting
// quiescent state is handed to onQuiescent together with the trace that
// produced it.
func exploreOp(t *testing.T, s modelState, opName string, steps []modelStep, commitIndex int, withRollback bool, tr trace, onQuiescent func(modelState, trace)) {
	t.Helper()
	// Crash before the first step is the same as never starting.
	for prefix := 1; prefix <= len(steps); prefix++ {
		run := s.clone()
		ok := true
		for i := 0; i < prefix; i++ {
			if !steps[i].apply(&run) {
				ok = false
				break
			}
			if err := checkInvariants(run); err != nil {
				t.Fatalf("%s after %s: %v\ntrace: %v", opName, steps[i].name, err, append(tr, opName+"/"+steps[i].name))
			}
		}
		if !ok {
			continue // precondition stopped the op; same as an early error
		}
		stepTrace := append(append(trace{}, tr...), fmt.Sprintf("%s..%s", opName, steps[prefix-1].name))

		// Kill here: nothing else runs.
		onQuiescent(run.clone(), append(append(trace{}, stepTrace...), "KILL"))

		// Error return here: the rollback runs, unless the op has committed;
		// the rollback itself can be killed at each of its step boundaries.
		if withRollback && prefix <= commitIndex {
			rb := rollbackSteps()
			for rbPrefix := 0; rbPrefix <= len(rb); rbPrefix++ {
				rbRun := run.clone()
				for i := 0; i < rbPrefix; i++ {
					rb[i].apply(&rbRun)
					if err := checkInvariants(rbRun); err != nil {
						t.Fatalf("%s rollback after %s: %v\ntrace: %v", opName, rb[i].name, err, stepTrace)
					}
				}
				suffix := "FAIL+rollback-killed"
				if rbPrefix == len(rb) {
					suffix = "FAIL+rollback-complete"
				}
				onQuiescent(rbRun.clone(), append(append(trace{}, stepTrace...), fmt.Sprintf("%s@%d", suffix, rbPrefix)))
			}
		}
	}
}

// TestECLifecycleModelExhaustive enumerates every crash and failure schedule
// of encode and decode up to two chained interrupted operations, checks the
// safety invariants in every intermediate state, and requires the prescribed
// recovery to converge from every quiescent state.
func TestECLifecycleModelExhaustive(t *testing.T) {
	start := modelState{replica: true}

	visited := map[string]trace{}
	quiescent := 0
	schedules := 0
	var enqueue func(s modelState, depth int, tr trace)

	checkRecovery := func(s modelState, tr trace) {
		schedules++
		if _, seen := visited["Q"+s.key()]; seen {
			return
		}
		visited["Q"+s.key()] = tr
		quiescent++
		if _, err := recoverModel(s.clone()); err != nil {
			t.Fatalf("recovery does not converge: %v\nstate: %s\ntrace: %v", err, s.key(), tr)
		}
	}

	enqueue = func(s modelState, depth int, tr trace) {
		checkRecovery(s, tr)
		if depth == 0 {
			return
		}
		// A crash-restart may happen in any quiescent state before the next
		// operation; explore both with and without it.
		restarted := s.clone()
		reconcile(&restarted)
		if err := checkInvariants(restarted); err != nil {
			t.Fatalf("reconcile violated invariants: %v\nstate: %s\ntrace: %v", err, s.key(), tr)
		}
		for _, variant := range []struct {
			st  modelState
			tag string
		}{{s, ""}, {restarted, "+restart"}} {
			st, tag := variant.st, variant.tag
			if st.replica {
				exploreOp(t, st, "encode"+tag, encodeSteps(), encodeCommitIndex(), true, tr, func(q modelState, qtr trace) {
					enqueue(q, depth-1, qtr)
				})
			}
			canDecode := false
			for _, g := range st.gens {
				if g.mounted && g.files >= modelDataShards {
					canDecode = true
				}
			}
			if canDecode {
				exploreOp(t, st, "decode"+tag, decodeSteps(), len(decodeSteps()), false, tr, func(q modelState, qtr trace) {
					enqueue(q, depth-1, qtr)
				})
			}
		}
	}

	// Depth 2: an interrupted operation followed by another interrupted
	// operation, recovery checked at every quiescent point in between and
	// after. Deeper chains only revisit already-explored states (the state
	// space is finite and memoized), which the visited count makes apparent.
	enqueue(start, 2, trace{"start"})

	// Not spec constants — collapse guards: if a refactor of the model or the
	// explorer accidentally prunes schedules, these counts crater and this
	// catches it. Update deliberately when the model changes fidelity.
	if quiescent < 15 || schedules < 500 {
		t.Fatalf("suspiciously small exploration: %d distinct quiescent states from %d schedules", quiescent, schedules)
	}
	keys := make([]string, 0, len(visited))
	for k := range visited {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("explored %d schedules reaching %d distinct quiescent states, all recoverable", schedules, quiescent)
}
