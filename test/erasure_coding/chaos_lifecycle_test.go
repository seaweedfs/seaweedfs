package erasure_coding

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestECChaosLifecycle drives randomized sequences of the EC lifecycle —
// encode (hdd and ssd targets), balance, shard damage + rebuild, decode,
// re-encode (a new generation), deletes, scrub, tier moves, crash-restarts,
// and sidecar fault injections — against a live cluster running the
// production-shaped layout: multiple data disks per server with a separate
// -dir.idx directory, so the .ecx/.ecj sidecars are shared across disks.
//
// One invariant is checked after every step: every byte a client stored comes
// back identical, and every deleted needle stays deleted. Shard counting alone
// cannot tell a healthy volume from one serving a stale generation or a
// mis-rebuilt shard; reading the payloads back can.
//
// The sequence is seeded (EC_CHAOS_SEED) and reproducible; EC_CHAOS_STEPS
// scales the random portion. The fault scenarios that motivated the test —
// losing a data-dir .vif (forcing the shared idx-dir fallback), and planting a
// stale-generation shard file next to a newer encode — are always exercised
// once, regardless of what the random schedule picks.
const (
	chaosMasterAddr   = "127.0.0.1:9338"
	chaosMasterPort   = "9338"
	chaosCollection   = "chaos"
	chaosServerCount  = 3
	chaosDisksPerNode = 3 // disk0, disk1 default type; disk2 tagged ssd
)

func chaosVolumePort(i int) string { return fmt.Sprintf("811%d", i) }

func TestECChaosLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EC chaos lifecycle test in short mode")
	}

	seedStr := os.Getenv("EC_CHAOS_SEED")
	if seedStr == "" {
		t.Skip("randomized exploration is opt-in: set EC_CHAOS_SEED to run it; " +
			"systematic coverage lives in TestECInterruptionMatrix and weed/ec's TestECLifecycleModelExhaustive")
	}
	seed, err := strconv.ParseInt(seedStr, 10, 64)
	require.NoError(t, err, "EC_CHAOS_SEED must be an integer")
	steps := 8
	if s := os.Getenv("EC_CHAOS_STEPS"); s != "" {
		v, err := strconv.Atoi(s)
		require.NoError(t, err, "EC_CHAOS_STEPS must be an integer")
		steps = v
	}
	t.Logf("chaos seed=%d steps=%d (override with EC_CHAOS_SEED / EC_CHAOS_STEPS)", seed, steps)

	testDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	cluster, clusterErr := startChaosCluster(ctx, testDir)
	require.NoError(t, clusterErr)
	defer cluster.Stop()

	require.NoError(t, waitForServer(chaosMasterAddr, 30*time.Second))
	for i := 0; i < chaosServerCount; i++ {
		require.NoError(t, waitForServer("127.0.0.1:"+chaosVolumePort(i), 30*time.Second))
	}
	time.Sleep(8 * time.Second)

	commandEnv := shell.NewCommandEnv(&shell.ShellOptions{
		Masters:        stringPtr(chaosMasterAddr),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	})
	connectToMasterAndSync(ctx, t, commandEnv)

	r := newChaosRun(t, ctx, cluster, commandEnv, testDir, seed)
	r.relock()
	defer r.unlockIfHeld()
	r.seedAndSpread()
	r.verify("seeding")

	// Random schedule. Every op re-verifies the full payload set.
	ops := []struct {
		name   string
		weight int
		run    func() bool
	}{
		{"encode", 4, r.opEncode},
		{"decode", 2, r.opDecode},
		{"balance", 2, r.opBalance},
		{"damage+rebuild", 2, r.opDamageAndRebuild},
		{"delete", 2, r.opDelete},
		{"upload", 2, r.opUpload},
		{"scrub", 2, r.opScrub},
		{"crash-restart", 1, r.opCrashRestart},
		{"tier-move", 1, r.opTierMove},
		{"vif-fallback", 1, r.opVifFallback},
		{"stale-generation", 1, r.opPlantStaleGeneration},
		{"interrupted-encode", 2, r.opInterruptedEncode},
		{"interrupted-decode", 1, r.opInterruptedDecode},
		{"interrupted-balance", 1, r.opInterruptedBalance},
	}
	total := 0
	for _, op := range ops {
		total += op.weight
	}
	ran := map[string]bool{}
	for step := 1; step <= steps; step++ {
		n := r.rng.Intn(total)
		for _, op := range ops {
			if n -= op.weight; n < 0 {
				t.Logf("── chaos step %d/%d: %s ──", step, steps, op.name)
				if op.run() {
					ran[op.name] = true
					r.verify(op.name)
				} else {
					t.Logf("step %d: %s not applicable, skipped", step, op.name)
				}
				break
			}
		}
	}

	// Deterministic tail: the scenarios this test exists for always run once.
	for _, must := range []struct {
		name string
		run  func() bool
	}{
		{"encode", r.opEncode},
		{"damage+rebuild", r.opDamageAndRebuild},
		{"vif-fallback", r.opVifFallback},
		{"stale-generation", r.opPlantStaleGeneration},
		{"interrupted-encode", r.opInterruptedEncode},
		{"interrupted-decode", r.opInterruptedDecode},
		{"interrupted-balance", r.opInterruptedBalance},
		{"decode", r.opDecode},
	} {
		if ran[must.name] {
			continue
		}
		t.Logf("── chaos tail: %s ──", must.name)
		if must.run() {
			r.verify(must.name)
		} else {
			t.Logf("tail: %s not applicable, skipped", must.name)
		}
	}

	r.verify("final")
	t.Logf("chaos done: %d payloads live, %d deleted, %d volumes tracked", len(r.payloads), len(r.deleted), len(r.volumes))
}

type chaosVolumeState struct {
	encoded bool
}

type chaosRun struct {
	t       *testing.T
	ctx     context.Context
	cluster *chaosCluster
	env     *shell.CommandEnv
	rng     *mrand.Rand
	testDir string

	payloads map[string][]byte // live fid -> expected bytes
	deleted  map[string]bool   // fids that must stay deleted
	fidVol   map[string]uint32
	volumes  map[uint32]*chaosVolumeState

	unlock func()
}

// ── invariants ──────────────────────────────────────────────────────────────

// verify is the single invariant of the whole test: after any operation, every
// live payload reads back byte-identical from the current cluster state, and
// every deleted needle stays unreadable. Retries absorb heartbeat and mount
// propagation delays; content mismatches fail immediately — waiting cannot fix
// wrong bytes, and the first wrong read is the most useful state to stop in.
func (r *chaosRun) verify(afterStep string) {
	r.t.Helper()
	for fid, want := range r.payloads {
		fid, want := fid, want
		// The condition runs on Eventually's own goroutine, where t.Fatalf
		// would only kill that goroutine; record a corruption and fail on the
		// test goroutine instead. A wrong read still ends the polling at once —
		// waiting cannot fix wrong bytes, and the first wrong read is the most
		// useful state to stop in.
		var corrupted string
		require.Eventuallyf(r.t, func() bool {
			got, err := chaosReadFid(fid, r.fidVol[fid])
			if err != nil {
				return false
			}
			if !bytes.Equal(got, want) {
				corrupted = fmt.Sprintf("payload %s corrupted after %s: got %d bytes, want %d bytes", fid, afterStep, len(got), len(want))
			}
			return true
		}, 90*time.Second, time.Second, "payload %s unreadable after %s", fid, afterStep)
		require.Empty(r.t, corrupted, "%s", corrupted)
	}
	for fid := range r.deleted {
		fid := fid
		require.Eventuallyf(r.t, func() bool {
			got, err := chaosReadFid(fid, r.fidVol[fid])
			return err != nil || len(got) == 0
		}, 30*time.Second, time.Second, "deleted payload %s came back after %s", fid, afterStep)
	}
	r.t.Logf("verified %d live + %d deleted payloads after %s", len(r.payloads), len(r.deleted), afterStep)
}

// ── operations ──────────────────────────────────────────────────────────────

func (r *chaosRun) opEncode() bool {
	vid, ok := r.pickVolume(false)
	if !ok {
		return false
	}
	args := []string{"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-force"}
	if r.rng.Intn(2) == 0 {
		args = append(args, "-diskType", "ssd")
	}
	r.t.Logf("ec.encode args: %v", args)
	out, err := r.shellCommand("ec.encode", args...)
	r.t.Logf("ec.encode v%d output:\n%s", vid, out)
	if err != nil {
		vl, _ := r.shellCommand("volume.list")
		r.t.Logf("volume.list at encode failure:\n%s", vl)
	}
	require.NoError(r.t, err, "ec.encode volume %d", vid)
	r.volumes[vid].encoded = true
	r.requireSingleGeneration(vid, "ec.encode")
	return true
}

func (r *chaosRun) opDecode() bool {
	vid, ok := r.pickVolume(true)
	if !ok {
		return false
	}
	// -checkMinFreeSpace=false: this intentionally tiny cluster would otherwise
	// refuse the decode for lack of headroom, which is not what is under test.
	out, err := r.shellCommand("ec.decode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-checkMinFreeSpace=false")
	r.t.Logf("ec.decode v%d output:\n%s", vid, out)
	require.NoError(r.t, err, "ec.decode volume %d", vid)
	r.volumes[vid].encoded = false
	return true
}

func (r *chaosRun) opBalance() bool {
	out, err := r.shellCommand("ec.balance", "-collection", chaosCollection, "-apply")
	r.t.Logf("ec.balance output:\n%s", out)
	require.NoError(r.t, err, "ec.balance")
	return true
}

// opDamageAndRebuild removes up to two shard files of an encoded volume
// straight off the disks, restarts the servers so the master relearns disk
// truth, and repairs with ec.rebuild — the flow of a real shard-loss incident.
func (r *chaosRun) opDamageAndRebuild() bool {
	vid, ok := r.pickVolume(true)
	if !ok {
		return false
	}
	before := len(collectDistinctShardIDs(r.testDir, vid))
	if before < erasureShardCount {
		r.t.Logf("damage+rebuild: volume %d has %d/%d distinct shards on disk, skipping", vid, before, erasureShardCount)
		return false // a prior fault is still outstanding; skip rather than stack damage
	}
	removed := removeTwoShardFiles(r.t, r.testDir, vid)
	r.t.Logf("removed shard files for shards %v of volume %d", removed, vid)

	// After the restart the master must agree with disk truth before any repair
	// runs. The exact count is not fixed: a stale shard file planted by an
	// earlier fault can legitimately re-register from disk and cover one of the
	// removed ids, so the invariant is agreement, not a specific number.
	r.restartAllVolumeServers()
	require.Eventually(r.t, func() bool {
		registered := masterEcShardIds(r.env, vid)
		onDisk := collectDistinctShardIDs(r.testDir, vid)
		if len(registered) != len(onDisk) {
			return false
		}
		for id := range onDisk {
			if !registered[id] {
				return false
			}
		}
		return true
	}, 90*time.Second, 2*time.Second, "master never agreed with disk truth for volume %d (master=%v disk=%v)",
		vid, sortedKeysOf(masterEcShardIds(r.env, vid)), sortedKeysOf(collectDistinctShardIDs(r.testDir, vid)))

	out, err := r.shellCommand("ec.rebuild", "-collection", chaosCollection, "-apply")
	r.t.Logf("ec.rebuild output:\n%s", out)
	require.NoError(r.t, err, "ec.rebuild")
	require.Eventually(r.t, func() bool {
		return len(collectDistinctShardIDs(r.testDir, vid)) == erasureShardCount
	}, 90*time.Second, time.Second, "ec.rebuild did not restore all shards of volume %d", vid)
	return true
}

func (r *chaosRun) opDelete() bool {
	// Keep at least one live payload per volume so no volume ever empties out
	// completely (an all-deleted volume decodes into nothing, which is its own
	// test, not this one).
	liveByVol := map[uint32]int{}
	for fid := range r.payloads {
		liveByVol[r.fidVol[fid]]++
	}
	var candidates []string
	for fid := range r.payloads {
		if liveByVol[r.fidVol[fid]] > 1 {
			candidates = append(candidates, fid)
			liveByVol[r.fidVol[fid]]--
		}
		if len(candidates) == 2 {
			break
		}
	}
	if len(candidates) == 0 {
		return false
	}
	for _, fid := range candidates {
		require.NoError(r.t, chaosDeleteFid(fid, r.fidVol[fid]), "delete %s", fid)
		delete(r.payloads, fid)
		r.deleted[fid] = true
		r.t.Logf("deleted %s (volume %d)", fid, r.fidVol[fid])
	}
	return true
}

func (r *chaosRun) opUpload() bool {
	for i := 0; i < 3; i++ {
		r.uploadOne()
	}
	return true
}

func (r *chaosRun) opScrub() bool {
	out, err := r.shellCommand("ec.scrub", "-mode", "local")
	r.t.Logf("ec.scrub output:\n%s", out)
	require.NoError(r.t, err, "ec.scrub")
	require.NotContains(r.t, out, "scrub failures", "ec.scrub reported broken EC volumes")
	return true
}

func (r *chaosRun) opCrashRestart() bool {
	r.restartAllVolumeServers()
	return true
}

func (r *chaosRun) opTierMove() bool {
	// Best effort: with -fullPercent=0 every quiet regular volume qualifies.
	// Zero moved volumes is fine — the invariant read-back is the point.
	out, err := r.shellCommand("volume.tier.move",
		"-fromDiskType", "hdd", "-toDiskType", "ssd",
		"-collectionPattern", "^"+chaosCollection+"$",
		"-fullPercent", "0", "-quietFor", "1s", "-apply")
	r.t.Logf("volume.tier.move output:\n%s", out)
	require.NoError(r.t, err, "volume.tier.move")
	return true
}

// opVifFallback simulates the split-sidecar layout: the data-dir .vif of one
// disk's shards moves into the server's shared -dir.idx directory, and the
// server restarts. Loading must fall back to the idx-dir copy (issue #9212
// layout) and reads must stay correct.
func (r *chaosRun) opVifFallback() bool {
	vid, ok := r.pickVolume(true)
	if !ok {
		return false
	}
	moved := false
	for server := 0; server < chaosServerCount && !moved; server++ {
		for disk := 0; disk < chaosDisksPerNode; disk++ {
			dataVif := filepath.Join(r.testDir, fmt.Sprintf("server%d_disk%d", server, disk),
				fmt.Sprintf("%s_%d.vif", chaosCollection, vid))
			if _, err := os.Stat(dataVif); err != nil {
				continue
			}
			idxVif := filepath.Join(r.testDir, fmt.Sprintf("server%d_idx", server),
				fmt.Sprintf("%s_%d.vif", chaosCollection, vid))
			require.NoError(r.t, os.Rename(dataVif, idxVif), "move .vif to idx dir")
			r.t.Logf("moved %s -> %s", dataVif, idxVif)
			r.restartOneVolumeServer(server)
			moved = true
			break
		}
	}
	return moved
}

// opPlantStaleGeneration reproduces the orphaned-generation hazard: it stashes
// an encoded volume's shard files, decodes and re-encodes the volume (a new
// generation with a new .vif stamp), then plants one stale shard file from the
// old generation onto a disk of a server that holds new-generation shards, and
// restarts that server. Whatever the server decides to do with the orphan —
// delete it, quarantine it, or register it — reads must never serve its bytes.
func (r *chaosRun) opPlantStaleGeneration() bool {
	vid, ok := r.pickVolume(true)
	if !ok {
		return false
	}
	if n := len(collectDistinctShardIDs(r.testDir, vid)); n < erasureShardCount {
		r.t.Logf("stale-generation: volume %d has %d/%d distinct shards on disk, skipping", vid, n, erasureShardCount)
		return false
	}

	// Stash one old-generation shard file.
	stale := r.stashOneShardFile(vid)
	if stale == "" {
		return false
	}

	out, err := r.shellCommand("ec.decode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-checkMinFreeSpace=false")
	r.t.Logf("ec.decode v%d output:\n%s", vid, out)
	require.NoError(r.t, err, "ec.decode volume %d (stale-generation scenario)", vid)
	r.verify("decode before re-encode")

	out, err = r.shellCommand("ec.encode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-force")
	r.t.Logf("ec.encode v%d output:\n%s", vid, out)
	require.NoError(r.t, err, "re-encode volume %d (stale-generation scenario)", vid)
	r.volumes[vid].encoded = true

	// Plant the old-generation shard on a server that holds new shards, on a
	// disk of that server that does not currently hold this volume — the
	// cross-disk mixing case, where the shared idx-dir sidecars are the only
	// local generation authority for the planted file.
	planted := false
	for server := 0; server < chaosServerCount && !planted; server++ {
		serverHasNew, diskWithout := false, -1
		for disk := 0; disk < chaosDisksPerNode; disk++ {
			pattern := filepath.Join(r.testDir, fmt.Sprintf("server%d_disk%d", server, disk),
				fmt.Sprintf("%s_%d.ec*", chaosCollection, vid))
			if m, _ := filepath.Glob(pattern); len(m) > 0 {
				serverHasNew = true
			} else if diskWithout == -1 {
				diskWithout = disk
			}
		}
		if serverHasNew && diskWithout >= 0 {
			dst := filepath.Join(r.testDir, fmt.Sprintf("server%d_disk%d", server, diskWithout), filepath.Base(stale))
			require.NoError(r.t, copyFileContents(stale, dst), "plant stale shard")
			r.t.Logf("planted stale generation shard %s on server %d disk %d", filepath.Base(stale), server, diskWithout)
			r.restartOneVolumeServer(server)
			planted = true
		}
	}
	if !planted {
		r.t.Logf("no server had both new shards and a free disk; stale plant skipped")
	}
	return true
}

// ── interruption ops ────────────────────────────────────────────────────────
//
// These kill a real `weed shell` subprocess mid-operation — the operator's
// shell dying — and then prove the cluster recovers: whatever half-finished
// state the kill left (readonly sources, partial or unmounted shards, an
// undeleted original, a half-collected decode), the next run of the same
// command must converge to a clean state, and reads must stay correct
// throughout. This is the restart-not-resume recovery model: an interrupted
// run is never resumed, the retry starts clean via the pre-encode sweep.

// runInterruptedShell feeds "lock" plus the command to a weed shell
// subprocess and kills the process after killAfter. Stdin stays open so the
// shell never exits gracefully — the lock releases only through the master
// noticing the dead connection, which the follow-up relock must survive.
func (r *chaosRun) runInterruptedShell(command string, killAfter time.Duration) string {
	weedBinary := findWeedBinary()
	require.NotEmpty(r.t, weedBinary, "weed binary not found")
	cmd := exec.CommandContext(r.ctx, weedBinary, "shell", "-master="+chaosMasterAddr)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	stdin, err := cmd.StdinPipe()
	require.NoError(r.t, err)
	require.NoError(r.t, cmd.Start())
	fmt.Fprintf(stdin, "lock\n%s\n", command)
	time.Sleep(killAfter)
	cmd.Process.Kill()
	cmd.Wait()
	return out.String()
}

func (r *chaosRun) volumeHasRegularReplica(vid uint32) bool {
	found := false
	for _, dn := range chaosDataNodes(r.env) {
		for _, di := range dn.GetDiskInfos() {
			for _, vi := range di.GetVolumeInfos() {
				if vi.GetId() == vid {
					found = true
				}
			}
		}
	}
	return found
}

func (r *chaosRun) opInterruptedEncode() bool {
	vid, ok := r.pickVolume(false)
	if !ok {
		return false
	}
	killAfter := time.Duration(1+r.rng.Intn(8)) * time.Second
	r.unlockIfHeld()
	out := r.runInterruptedShell(
		fmt.Sprintf("ec.encode -volumeId %d -collection %s -force", vid, chaosCollection), killAfter)
	r.t.Logf("killed ec.encode v%d after %v; output so far:\n%s", vid, killAfter, out)
	r.relock()

	r.recoverInterruptedEncode(vid)
	return true
}

// recoverInterruptedEncode is the prescribed recovery after an encode was
// killed mid-flight: if the kill came after the originals were deleted, the
// encode had effectively completed and the volume is EC now; any earlier kill
// leaves the regular volume in place (possibly readonly, possibly beside
// partial shards), and a re-run must sweep the leftovers and finish.
func (r *chaosRun) recoverInterruptedEncode(vid uint32) {
	r.t.Helper()
	if !r.volumeHasRegularReplica(vid) {
		r.t.Logf("interrupted encode of volume %d had already completed", vid)
		r.volumes[vid].encoded = true
		r.requireSingleGeneration(vid, "interrupted ec.encode (completed)")
		return
	}
	out2, err := r.shellCommand("ec.encode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-force")
	r.t.Logf("recovery ec.encode v%d output:\n%s", vid, out2)
	if err != nil && !r.volumeHasRegularReplica(vid) {
		// The killed run's original-deletion outran the topology snapshot the
		// re-run planned from; the encode had in fact completed.
		r.t.Logf("interrupted encode of volume %d had already completed (original deletion outran the topology)", vid)
		err = nil
	}
	if err != nil {
		vl, _ := r.shellCommand("volume.list")
		r.t.Logf("volume.list at recovery-encode failure:\n%s", vl)
	}
	require.NoError(r.t, err, "recovery ec.encode volume %d after interruption", vid)
	r.volumes[vid].encoded = true
	r.requireSingleGeneration(vid, "recovery ec.encode")
}

func (r *chaosRun) opInterruptedDecode() bool {
	vid, ok := r.pickVolume(true)
	if !ok {
		return false
	}
	killAfter := time.Duration(1+r.rng.Intn(6)) * time.Second
	r.unlockIfHeld()
	out := r.runInterruptedShell(
		fmt.Sprintf("ec.decode -volumeId %d -collection %s -checkMinFreeSpace=false", vid, chaosCollection), killAfter)
	r.t.Logf("killed ec.decode v%d after %v; output so far:\n%s", vid, killAfter, out)
	r.relock()

	r.recoverInterruptedDecode(vid)
	return true
}

// recoverInterruptedDecode is the prescribed recovery after a decode was
// killed mid-flight: while any shards remain the decode is unfinished (the
// kill may have left a hybrid: regenerated volume plus undeleted shards) and
// a re-run must complete it. No shards left means the decode had finished —
// and the master's view can lag the killed run's final deletions, so a re-run
// that finds no shards is also completion, not a failure.
func (r *chaosRun) recoverInterruptedDecode(vid uint32) {
	r.t.Helper()
	if len(masterEcShardIds(r.env, vid)) > 0 {
		out2, err := r.shellCommand("ec.decode",
			"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-checkMinFreeSpace=false")
		r.t.Logf("recovery ec.decode v%d output:\n%s", vid, out2)
		if err != nil && strings.Contains(err.Error(), "no EC shards found") {
			r.t.Logf("interrupted decode of volume %d had already completed (shard deletions outran the topology)", vid)
		} else {
			require.NoError(r.t, err, "recovery ec.decode volume %d after interruption", vid)
		}
	} else {
		r.t.Logf("interrupted decode of volume %d had already completed", vid)
	}
	r.volumes[vid].encoded = false
}

func (r *chaosRun) opInterruptedBalance() bool {
	anyEncoded := false
	for _, st := range r.volumes {
		if st.encoded {
			anyEncoded = true
		}
	}
	if !anyEncoded {
		return false
	}
	killAfter := time.Duration(1+r.rng.Intn(5)) * time.Second
	r.unlockIfHeld()
	out := r.runInterruptedShell(
		fmt.Sprintf("ec.balance -collection %s -apply", chaosCollection), killAfter)
	r.t.Logf("killed ec.balance after %v; output so far:\n%s", killAfter, out)
	r.relock()

	r.recoverInterruptedBalance()
	return true
}

// recoverInterruptedBalance is the prescribed recovery after a balance was
// killed mid-flight: an interrupted move leaves a shard copied but not yet
// deleted at the source. Re-running the balance must converge — its dedup
// phase removes the extra copies — until the replication check is clean. A
// lock lost to the killed shell's reap is re-taken by shellCommand, so the
// loop only has to judge convergence.
func (r *chaosRun) recoverInterruptedBalance() {
	r.t.Helper()
	require.Eventually(r.t, func() bool {
		if _, err := r.shellCommand("ec.balance", "-collection", chaosCollection, "-apply"); err != nil {
			r.t.Logf("recovery ec.balance: %v", err)
			return false
		}
		report, err := r.shellCommand("ec.check.replication", "-details")
		if err != nil {
			r.t.Logf("ec.check.replication: %v", err)
			return false
		}
		if strings.Contains(report, "under-replicated") {
			r.t.Logf("replication not clean yet:\n%s", report)
			return false
		}
		if crossNode, sameNode := classifyOverReplication(report); crossNode {
			r.t.Logf("replication not clean yet:\n%s", report)
			return false
		} else if sameNode {
			// KNOWN GAP: a shard mounted on two disks of ONE node (e.g. an
			// orphan adopted after an interrupted copy) is invisible to
			// ec.balance's dedup, and ec.shard.unmount's shard@address form
			// cannot disambiguate two copies behind one address. Nothing can
			// clean this state today; reads stay correct, so tolerate it here
			// and keep it visible in the log.
			r.t.Logf("tolerating same-node duplicate shards (no cleanup path exists):\n%s", report)
		}
		return true
	}, 120*time.Second, 3*time.Second, "cluster never converged to clean replication after interrupted balance")
}

// classifyOverReplication parses an ec.check.replication -details report and
// says whether any shard is duplicated across distinct nodes (crossNode) or
// only within one node (sameNode, the two-disks-one-node adoption case).
func classifyOverReplication(report string) (crossNode, sameNode bool) {
	for _, line := range strings.Split(report, "\n") {
		open := strings.Index(line, "=> [")
		if open < 0 {
			continue
		}
		addrs := strings.Fields(strings.Trim(line[open+len("=> ["):], "[] \r"))
		if len(addrs) < 2 {
			continue
		}
		distinct := map[string]bool{}
		for _, a := range addrs {
			distinct[a] = true
		}
		if len(distinct) > 1 {
			crossNode = true
		} else {
			sameNode = true
		}
	}
	return crossNode, sameNode
}

// ── helpers ─────────────────────────────────────────────────────────────────

// newChaosRun wires a run driver over a started cluster. The rng only shapes
// randomized schedules and payload sizes; deterministic drivers (the
// interruption matrix) never draw from it beyond seeding uploads.
func newChaosRun(t *testing.T, ctx context.Context, cluster *chaosCluster, env *shell.CommandEnv, testDir string, seed int64) *chaosRun {
	return &chaosRun{
		t:        t,
		ctx:      ctx,
		cluster:  cluster,
		env:      env,
		rng:      mrand.New(mrand.NewSource(seed)),
		testDir:  testDir,
		payloads: map[string][]byte{},
		deleted:  map[string]bool{},
		fidVol:   map[string]uint32{},
		volumes:  map[uint32]*chaosVolumeState{},
	}
}

// seedAndSpread uploads the payload set and then spreads volumes onto every
// disk of every node: the master only enumerates disks that already hold
// data, and shards only spread across enumerated disks (see
// TestMultiDiskECBalanceNoShardLoss). Without this a balance can find no
// eligible targets and the encode's clump guard aborts.
func (r *chaosRun) seedAndSpread() {
	r.t.Helper()
	for i := 0; i < 24; i++ {
		r.uploadOne()
	}
	require.GreaterOrEqual(r.t, len(r.volumes), 2, "seeding should produce at least two volumes")
	time.Sleep(3 * time.Second)

	require.Eventually(r.t, func() bool {
		spread := nodeVolumeDiskCounts(r.t, r.env)
		if len(spread) == chaosServerCount && allAtLeast(spread, 2) {
			return true
		}
		for i := 0; i < chaosServerCount; i++ {
			server := "127.0.0.1:" + chaosVolumePort(i)
			if spread[server] < 2 {
				out, gerr := captureCommandOutput(r.t, shell.Commands[findCommandIndex("volume.grow")],
					[]string{"-collection", chaosCollection, "-dataNode", server, "-count", "4"}, r.env)
				r.t.Logf("volume.grow on %s: err=%v output:\n%s", server, gerr, out)
			}
		}
		return false
	}, 90*time.Second, 2*time.Second, "volumes never spread across >=2 disks on all %d nodes", chaosServerCount)
}

// ensureRegularVolume returns a tracked volume in the regular (not encoded)
// state, decoding one if every tracked volume is EC.
func (r *chaosRun) ensureRegularVolume() uint32 {
	r.t.Helper()
	if vid, ok := r.pickVolume(false); ok {
		return vid
	}
	vid, ok := r.pickVolume(true)
	require.True(r.t, ok, "no volumes tracked at all")
	out, err := r.shellCommand("ec.decode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-checkMinFreeSpace=false")
	r.t.Logf("ensureRegular ec.decode v%d output:\n%s", vid, out)
	require.NoError(r.t, err, "decode volume %d to restore a regular volume", vid)
	r.volumes[vid].encoded = false
	return vid
}

// ensureEncodedVolume returns a tracked volume in the encoded state, encoding
// one (hdd target, deterministic) if none is.
func (r *chaosRun) ensureEncodedVolume() uint32 {
	r.t.Helper()
	if vid, ok := r.pickVolume(true); ok {
		return vid
	}
	vid, ok := r.pickVolume(false)
	require.True(r.t, ok, "no volumes tracked at all")
	out, err := r.shellCommand("ec.encode",
		"-volumeId", fmt.Sprintf("%d", vid), "-collection", chaosCollection, "-force")
	r.t.Logf("ensureEncoded ec.encode v%d output:\n%s", vid, out)
	require.NoError(r.t, err, "encode volume %d", vid)
	r.volumes[vid].encoded = true
	r.requireSingleGeneration(vid, "ensureEncodedVolume")
	return vid
}

func (r *chaosRun) pickVolume(encoded bool) (uint32, bool) {
	var candidates []uint32
	for vid, st := range r.volumes {
		if st.encoded == encoded {
			candidates = append(candidates, vid)
		}
	}
	if len(candidates) == 0 {
		return 0, false
	}
	// Deterministic pick under one seed: order by volume id.
	min := candidates[0]
	for _, v := range candidates {
		if v < min {
			min = v
		}
	}
	return min, true
}

func (r *chaosRun) uploadOne() {
	data := make([]byte, 2048+r.rng.Intn(14*1024))
	_, err := rand.Read(data)
	require.NoError(r.t, err)
	var vid needle.VolumeId
	var fid string
	for retry := 0; retry < 5; retry++ {
		vid, fid, err = chaosUploadPayload(data)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	require.NoError(r.t, err, "upload payload")
	r.payloads[fid] = data
	r.fidVol[fid] = uint32(vid)
	if _, ok := r.volumes[uint32(vid)]; !ok {
		r.volumes[uint32(vid)] = &chaosVolumeState{}
	}
}

// lostShellLock reports whether a command failed because the shell lock this
// harness holds is no longer recognised. A killed shell's lock is released
// only when the master notices the dead connection, and that cleanup lands
// asynchronously -- after the harness has already re-acquired the lock -- so
// it can clear the lock this run holds and the next command then refuses with
// `need to run "lock" first to continue`.
func lostShellLock(err error) bool {
	return err != nil && strings.Contains(err.Error(), `need to run "lock" first`)
}

// shellCommand runs a shell command, answering a lost lock the way an operator
// would: run lock again and retry. Every recovery path needs this, not just the
// balance one -- the reap can land during any command that follows a kill.
func (r *chaosRun) shellCommand(name string, args ...string) (string, error) {
	const attempts = 3
	var out string
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		out, err = captureCommandOutput(r.t, shell.Commands[findCommandIndex(name)], args, r.env)
		if !lostShellLock(err) {
			return out, err
		}
		r.t.Logf("%s lost the shell lock (%v); re-locking and retrying", name, err)
		r.relock()
	}
	return out, err
}

func (r *chaosRun) relock() {
	locked, unlock := tryLockWithTimeout(r.t, r.env, 45*time.Second)
	require.True(r.t, locked, "could not acquire shell lock")
	r.unlock = unlock
}

func (r *chaosRun) unlockIfHeld() {
	if r.unlock != nil {
		r.unlock()
		r.unlock = nil
	}
}

func (r *chaosRun) restartAllVolumeServers() {
	require.NoError(r.t, r.cluster.RestartVolumeServers(r.ctx))
	for i := 0; i < chaosServerCount; i++ {
		require.NoError(r.t, waitForServer("127.0.0.1:"+chaosVolumePort(i), 30*time.Second))
	}
	time.Sleep(3 * time.Second)
	r.relock() // the restart's master disconnect drops the shell lock
}

func (r *chaosRun) restartOneVolumeServer(i int) {
	require.NoError(r.t, r.cluster.RestartVolumeServer(r.ctx, i))
	require.NoError(r.t, waitForServer("127.0.0.1:"+chaosVolumePort(i), 30*time.Second))
	time.Sleep(3 * time.Second)
	r.relock()
}

// requireSingleGeneration asserts that, at a quiescent point, every EC shard
// entry the master reports for the volume carries the same encode generation
// stamp — the state the encode pipeline is supposed to leave behind.
func (r *chaosRun) requireSingleGeneration(vid uint32, step string) {
	r.t.Helper()
	require.Eventually(r.t, func() bool {
		generations := masterEcGenerations(r.env, vid)
		return len(generations) == 1
	}, 60*time.Second, 2*time.Second,
		"volume %d reports mixed encode generations after %s: %v", vid, step, masterEcGenerations(r.env, vid))
}

// stashOneShardFile copies one current shard file of the volume into a stash
// dir and returns the stash path ("" when none found).
func (r *chaosRun) stashOneShardFile(vid uint32) string {
	stashDir := filepath.Join(r.testDir, "stale_stash")
	_ = os.MkdirAll(stashDir, 0o755)
	for server := 0; server < chaosServerCount; server++ {
		for disk := 0; disk < chaosDisksPerNode; disk++ {
			pattern := filepath.Join(r.testDir, fmt.Sprintf("server%d_disk%d", server, disk),
				fmt.Sprintf("%s_%d.ec*", chaosCollection, vid))
			matches, _ := filepath.Glob(pattern)
			for _, m := range matches {
				if strings.HasSuffix(m, ".ecx") || strings.HasSuffix(m, ".ecj") || strings.HasSuffix(m, ".ecsum") {
					continue
				}
				dst := filepath.Join(stashDir, filepath.Base(m))
				if err := copyFileContents(m, dst); err != nil {
					continue
				}
				return dst
			}
		}
	}
	return ""
}

func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// chaosDataNodes lists the data nodes from a fresh master topology snapshot.
func chaosDataNodes(commandEnv *shell.CommandEnv) []*master_pb.DataNodeInfo {
	var resp *master_pb.VolumeListResponse
	err := commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		var e error
		resp, e = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return e
	})
	if err != nil || resp.GetTopologyInfo() == nil {
		return nil
	}
	var nodes []*master_pb.DataNodeInfo
	for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			nodes = append(nodes, rack.GetDataNodeInfos()...)
		}
	}
	return nodes
}

// masterEcGenerations returns the distinct encode generation stamps the master
// currently reports for a volume's EC shards.
func masterEcGenerations(commandEnv *shell.CommandEnv, volumeId uint32) map[int64]bool {
	generations := map[int64]bool{}
	var resp *master_pb.VolumeListResponse
	err := commandEnv.MasterClient.WithClient(context.Background(), false, func(client master_pb.SeaweedClient) error {
		var e error
		resp, e = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return e
	})
	if err != nil || resp.GetTopologyInfo() == nil {
		return generations
	}
	for _, dc := range resp.GetTopologyInfo().GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, dn := range rack.GetDataNodeInfos() {
				for _, di := range dn.GetDiskInfos() {
					for _, eci := range di.GetEcShardInfos() {
						if eci.GetId() == volumeId {
							generations[eci.GetEncodeTsNs()] = true
						}
					}
				}
			}
		}
	}
	return generations
}

// ── payload plumbing against the chaos master ───────────────────────────────

func chaosUploadPayload(data []byte) (needle.VolumeId, string, error) {
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(chaosMasterAddr)
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:       1,
		Collection:  chaosCollection,
		Replication: "000",
	})
	if err != nil {
		return 0, "", err
	}
	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, "", err
	}
	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "chaos.bin",
		MimeType:  "application/octet-stream",
	})
	if err != nil {
		return 0, "", err
	}
	if uploadResult.Error != "" {
		return 0, "", fmt.Errorf("upload error: %s", uploadResult.Error)
	}
	fidObj, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return 0, "", err
	}
	return fidObj.VolumeId, assignResult.Fid, nil
}

func chaosLookupLocations(volumeId uint32) ([]string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/dir/lookup?volumeId=%d", chaosMasterAddr, volumeId))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var lookup struct {
		Locations []struct {
			Url string `json:"url"`
		} `json:"locations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lookup); err != nil {
		return nil, err
	}
	var urls []string
	for _, l := range lookup.Locations {
		urls = append(urls, l.Url)
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("no locations for volume %d", volumeId)
	}
	return urls, nil
}

func chaosReadFid(fid string, volumeId uint32) ([]byte, error) {
	urls, err := chaosLookupLocations(volumeId)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, url := range urls {
		get, err := http.Get(fmt.Sprintf("http://%s/%s", url, fid))
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if get.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("GET %s from %s: %d", fid, url, get.StatusCode)
			continue
		}
		return body, nil
	}
	return nil, lastErr
}

func chaosDeleteFid(fid string, volumeId uint32) error {
	urls, err := chaosLookupLocations(volumeId)
	if err != nil {
		return err
	}
	var lastErr error
	for _, url := range urls {
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/%s", url, fid), nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("DELETE %s from %s: %d", fid, url, resp.StatusCode)
	}
	return lastErr
}

// ── cluster ─────────────────────────────────────────────────────────────────

// chaosCluster is a master plus three volume servers, each with three data
// disks (the third tagged ssd) and a separate -dir.idx directory, so the
// .ecx/.ecj sidecars are shared across the server's disks — the layout whose
// edge cases this test exists to exercise. Individual servers can be killed
// and restarted over their existing directories.
type chaosCluster struct {
	masterCmd     *exec.Cmd
	volumeServers []*exec.Cmd
	testDir       string
	logFiles      []*os.File
}

func (c *chaosCluster) Stop() {
	for _, cmd := range c.volumeServers {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}
	for _, f := range c.logFiles {
		if f != nil {
			f.Close()
		}
	}
}

func (c *chaosCluster) RestartVolumeServers(ctx context.Context) error {
	for i := range c.volumeServers {
		if err := c.RestartVolumeServer(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (c *chaosCluster) RestartVolumeServer(ctx context.Context, i int) error {
	if cmd := c.volumeServers[i]; cmd != nil && cmd.Process != nil {
		cmd.Process.Kill()
		cmd.Wait()
	}
	time.Sleep(time.Second)
	cmd, err := c.startVolumeServer(ctx, i, "volume-restart.log")
	if err != nil {
		return err
	}
	c.volumeServers[i] = cmd
	time.Sleep(2 * time.Second)
	return nil
}

func (c *chaosCluster) startVolumeServer(ctx context.Context, i int, logName string) (*exec.Cmd, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}
	var diskDirs, maxVolumes, diskTypes []string
	for d := 0; d < chaosDisksPerNode; d++ {
		dir := filepath.Join(c.testDir, fmt.Sprintf("server%d_disk%d", i, d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		diskDirs = append(diskDirs, dir)
		maxVolumes = append(maxVolumes, "4")
		if d == chaosDisksPerNode-1 {
			diskTypes = append(diskTypes, "ssd")
		} else {
			diskTypes = append(diskTypes, "hdd")
		}
	}
	idxDir := filepath.Join(c.testDir, fmt.Sprintf("server%d_idx", i))
	if err := os.MkdirAll(idxDir, 0o755); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, weedBinary, "volume",
		"-port", chaosVolumePort(i),
		"-dir", strings.Join(diskDirs, ","),
		"-dir.idx", idxDir,
		"-disk", strings.Join(diskTypes, ","),
		"-max", strings.Join(maxVolumes, ","),
		"-master", chaosMasterAddr,
		"-ip", "127.0.0.1",
		"-dataCenter", "dc1",
		"-rack", fmt.Sprintf("rack%d", i),
	)
	logDir := filepath.Join(c.testDir, fmt.Sprintf("server%d_logs", i))
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(logDir, logName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	c.logFiles = append(c.logFiles, logFile)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func startChaosCluster(ctx context.Context, dataDir string) (*chaosCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}
	// A leaked cluster from an earlier run would silently absorb this run's
	// traffic (same fixed ports) and make every on-disk assertion meaningless.
	// Refuse to start over occupied ports.
	ports := []string{chaosMasterAddr}
	for i := 0; i < chaosServerCount; i++ {
		ports = append(ports, "127.0.0.1:"+chaosVolumePort(i))
	}
	for _, addr := range ports {
		if conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond); err == nil {
			conn.Close()
			return nil, fmt.Errorf("port %s is already in use (stale cluster from an earlier run?)", addr)
		}
	}
	cluster := &chaosCluster{testDir: dataDir}

	masterDir := filepath.Join(dataDir, "master")
	if err := os.MkdirAll(masterDir, 0o755); err != nil {
		return nil, err
	}
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", chaosMasterPort,
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
		"-peers", "none",
	)
	masterLog, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, err
	}
	cluster.logFiles = append(cluster.logFiles, masterLog)
	masterCmd.Stdout = masterLog
	masterCmd.Stderr = masterLog
	if err := masterCmd.Start(); err != nil {
		return nil, err
	}
	cluster.masterCmd = masterCmd
	time.Sleep(2 * time.Second)

	for i := 0; i < chaosServerCount; i++ {
		cmd, err := cluster.startVolumeServer(ctx, i, "volume.log")
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("start volume server %d: %w", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, cmd)
	}
	time.Sleep(8 * time.Second)
	return cluster, nil
}
