package shell

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestLiveDivergenceClassifiesOneSidedVsTwoSided verifies the divergence
// classifier used to turn check.disk's dead-end "cannot prove" no-op into an
// actionable verdict: one-sided (safe to re-copy) vs two-sided (split-brain).
func TestLiveDivergenceClassifiesOneSidedVsTwoSided(t *testing.T) {
	// one-sided: source has an extra live needle; target has none unique.
	src, tgt := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer src.Close()
	defer tgt.Close()
	if err := src.Set(types.NeedleId(1001), types.ToOffset(8), types.Size(123)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := tgt.Set(types.NeedleId(1002), types.ToOffset(8), types.Size(45)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// a shared live needle (should NOT count as divergence either way)
	if err := src.Set(types.NeedleId(2000), types.ToOffset(8), types.Size(9)); err != nil {
		t.Fatalf("seed shared: %v", err)
	}
	if err := tgt.Set(types.NeedleId(2000), types.ToOffset(8), types.Size(9)); err != nil {
		t.Fatalf("seed shared: %v", err)
	}
	vcd := &volumeCheckDisk{writer: &bytes.Buffer{}, now: time.Now()}
	sOnly, tOnly := vcd.liveDivergence(src, tgt)
	if sOnly != 1 || tOnly != 1 {
		t.Fatalf("one-sided: got sOnly=%d tOnly=%d, want 1/1 (1001 only on src, 1002 only on tgt)", sOnly, tOnly)
	}

	// strict subset: target has no unique needles -> tOnly must be 0.
	subset, full := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer subset.Close()
	defer full.Close()
	if err := subset.Set(types.NeedleId(2000), types.ToOffset(8), types.Size(9)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, id := range []types.NeedleId{2000, 2001, 2002} {
		if err := full.Set(id, types.ToOffset(8), types.Size(9)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	aOnly, bOnly := vcd.liveDivergence(full, subset) // full->subset
	if aOnly != 2 || bOnly != 0 {
		t.Fatalf("subset: full is superset; want aOnly=2 (2001,2002) bOnly=0, got %d/%d", aOnly, bOnly)
	}

	// tombstones must be excluded: a deleted-only difference is not divergence.
	delSrc, delTgt := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer delSrc.Close()
	defer delTgt.Close()
	if err := delSrc.Set(types.NeedleId(3000), types.ToOffset(8), types.Size(9)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := delTgt.Set(types.NeedleId(3000), types.ToOffset(8), types.Size(9)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// delete 3000 on the source as a real tombstone (negative size), leave it
	// live on target. liveDivergence must skip the tombstone on the source and
	// must not count 3000 on the target either, because Get still finds the
	// tombstone entry — so the expected answer is 0/0, not 0/1.
	if err := delSrc.Set(types.NeedleId(3000), types.ToOffset(8), types.Size(-9)); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	x, y := vcd.liveDivergence(delSrc, delTgt)
	if x != 0 || y != 0 {
		t.Fatalf("tombstone: deleted needle on src should not count; want src-only=0, tgt-only=0, got %d/%d", x, y)
	}
}

// TestReportDivergenceVerdictEmitsCopyCommand checks that a one-sided verdict
// prints the exact volume.copy command (complete -> lagging, dialable address),
// that the command never uses the logical node Id, that a vacuumed (or
// unproven) lagging replica gets the no-re-copy warning instead of a copy
// command, that a proven never-vacuumed lagging replica gets the safe copy
// command — including in unidirectional mode where only the target's revision
// is known — and that a two-sided verdict warns instead of auto-repairing.
func TestReportDivergenceVerdictEmitsCopyCommand(t *testing.T) {
	// Id is a logical node identifier (NOT dialable); Address is the real
	// ip:port the copy command must use.
	src := &VolumeReplica{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "node-1", Address: "10.0.0.1:8081"}}, info: &master_pb.VolumeInformationMessage{Id: 42}}
	tgt := &VolumeReplica{location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "node-2", Address: "10.0.0.2:8083"}}, info: &master_pb.VolumeInformationMessage{Id: 42}}

	// Case A: source has 5 unique live, target has none -> target is lagging.
	// Revisions were not read (both known flags false), so no copy command.
	var one bytes.Buffer
	vcd := &volumeCheckDisk{writer: &one, now: time.Now()}
	vcd.reportDivergenceVerdict(src, tgt, 5, 0, 0, 0, false, false, false)
	got := one.String()
	if !strings.Contains(got, "ONE-SIDED") {
		t.Fatalf("expected ONE-SIDED verdict, got: %s", got)
	}
	if strings.Contains(got, "volume.copy") {
		t.Fatalf("unproven lagging replica must not get a volume.copy command, got: %s", got)
	}
	if !strings.Contains(got, "Do NOT re-copy the whole volume") {
		t.Fatalf("expected no-re-copy warning for unproven lagging replica, got: %s", got)
	}
	if !strings.Contains(got, "10.0.0.1:8081") || !strings.Contains(got, "10.0.0.2:8083") {
		t.Fatalf("expected dialable addresses in verdict, got: %s", got)
	}
	if strings.Contains(got, "node-1") || strings.Contains(got, "node-2") {
		t.Fatalf("verdict must use dialable Address, not logical node Id, got: %s", got)
	}
	if !strings.Contains(got, "5") {
		t.Fatalf("expected missing-needle count 5, got: %s", got)
	}

	// Case B: source is lagging (target complete). The safe command must go
	// complete -> lagging, i.e. -source <target> -target <source>.
	var safeRev bytes.Buffer
	vcdSafeRev := &volumeCheckDisk{writer: &safeRev, now: time.Now()}
	vcdSafeRev.reportDivergenceVerdict(src, tgt, 0, 5, 0, 0, true, false, true)
	gotRev := safeRev.String()
	if !strings.Contains(gotRev, "volume.copy -source 10.0.0.2:8083 -target 10.0.0.1:8081 -volumeId 42") {
		t.Fatalf("expected copy complete(10.0.0.2:8083)->lagging(10.0.0.1:8081), got: %s", gotRev)
	}
	if strings.Contains(gotRev, "volume.copy -source 10.0.0.1:8081 -target 10.0.0.2:8083") {
		t.Fatalf("reversed direction: must not copy lagging source over complete target, got: %s", gotRev)
	}

	// Case C: target-lagging, unidirectional run (-resurrectMissingNeedles,
	// not -bidirectional): sourceRevision was never read (srcRevKnown=false)
	// but targetRevision was (tgtRevKnown=true) and is 0. The safe copy
	// command must still be emitted — the false-warning regression.
	var uni bytes.Buffer
	vcdUni := &volumeCheckDisk{writer: &uni, now: time.Now()}
	vcdUni.reportDivergenceVerdict(src, tgt, 5, 0, 0, 0, false, true, true)
	gotUni := uni.String()
	if !strings.Contains(gotUni, "ONE-SIDED") {
		t.Fatalf("expected ONE-SIDED verdict, got: %s", gotUni)
	}
	if !strings.Contains(gotUni, "volume.copy -source 10.0.0.1:8081 -target 10.0.0.2:8083 -volumeId 42") {
		t.Fatalf("unidirectional run with proven never-vacuumed target must emit the safe copy command, got: %s", gotUni)
	}
	if strings.Contains(gotUni, "Do NOT re-copy the whole volume") {
		t.Fatalf("false warning: proven never-vacuumed lagging target must not get the no-re-copy warning, got: %s", gotUni)
	}

	// Case D: target-lagging but the lagging side was vacuumed (rev 17) ->
	// no copy command, even though the proof was taken under the flag.
	var vac bytes.Buffer
	vcdVac := &volumeCheckDisk{writer: &vac, now: time.Now()}
	vcdVac.reportDivergenceVerdict(src, tgt, 5, 0, 17, 17, true, true, true)
	gotVac := vac.String()
	if strings.Contains(gotVac, "volume.copy") {
		t.Fatalf("vacuumed lagging replica must not get a volume.copy command, got: %s", gotVac)
	}
	if !strings.Contains(gotVac, "Do NOT re-copy the whole volume") {
		t.Fatalf("expected no-re-copy warning for vacuumed lagging replica, got: %s", gotVac)
	}

	// Case E: two-sided split-brain warns instead of auto-repairing.
	var two bytes.Buffer
	vcd2 := &volumeCheckDisk{writer: &two, now: time.Now()}
	vcd2.reportDivergenceVerdict(src, tgt, 3, 4, 0, 0, false, false, false)
	got2 := two.String()
	if !strings.Contains(got2, "TWO-SIDED") || !strings.Contains(got2, "split-brain") {
		t.Fatalf("expected TWO-SIDED split-brain warning, got: %s", got2)
	}
	if strings.Contains(got2, "volume.copy") {
		t.Fatalf("two-sided must not emit an auto volume.copy command, got: %s", got2)
	}
}

// TestDoVolumeCheckDiskDoesNotResurrectAbsentNeedle verifies that a needle
// present-and-live on the source but entirely absent on the target is NOT
// pushed back by default. Such an absence is indistinguishable from a needle
// that was deleted on the target and then vacuumed away, so resurrecting it
// would raise back data the operator deleted.
func TestDoVolumeCheckDiskDoesNotResurrectAbsentNeedle(t *testing.T) {
	sourceDB, targetDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer sourceDB.Close()
	defer targetDB.Close()

	// Source has a live needle; target lacks it (e.g. deleted+vacuumed there).
	if err := sourceDB.Set(types.NeedleId(1001), types.ToOffset(8), types.Size(123)); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	var buf bytes.Buffer
	vcd := &volumeCheckDisk{
		commandEnv: &CommandEnv{
			option: &ShellOptions{
				GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		},
		writer:             &buf,
		now:                time.Now(),
		applyChanges:       false, // simulation: should not even read the source blob
		nonRepairThreshold: 1,
		// resurrectMissingNeedles left false: the safe default.
	}

	// Source points at an unreachable address: with the fix no blob read is
	// attempted, so this is never contacted. The pre-fix code queues the
	// absent needle and tries to read it from here, surfacing as an error.
	source := &VolumeReplica{
		location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "127.0.0.1:1"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}
	target := &VolumeReplica{
		location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "127.0.0.1:2"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}

	// With the safe default, the absent needle is skipped before any network
	// read, so this returns cleanly with no changes. On the pre-fix code the
	// needle is queued and a source blob read is attempted, which has no server
	// to reach and surfaces as an error (or a resurrection) instead.
	hasChanges, err := vcd.doVolumeCheckDisk(sourceDB, targetDB, source, target, false, 0)
	if err != nil {
		t.Fatalf("doVolumeCheckDisk returned error: %v", err)
	}
	if hasChanges {
		t.Fatalf("absent-on-target needle was resurrected; expected no changes")
	}
}

// TestDoVolumeCheckDiskResurrectSkipsVacuumedTarget verifies that even with
// -resurrectMissingNeedles, an absent needle is not pushed into a replica that
// has been vacuumed (compaction revision > 0): the vacuum may have erased the
// tombstone of a legitimate delete, so the absence proves nothing.
func TestDoVolumeCheckDiskResurrectSkipsVacuumedTarget(t *testing.T) {
	sourceDB, targetDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer sourceDB.Close()
	defer targetDB.Close()

	if err := sourceDB.Set(types.NeedleId(1001), types.ToOffset(8), types.Size(123)); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	var buf bytes.Buffer
	vcd := &volumeCheckDisk{
		commandEnv: &CommandEnv{
			option: &ShellOptions{
				GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		},
		writer:                  &buf,
		now:                     time.Now(),
		applyChanges:            false,
		nonRepairThreshold:      1,
		resurrectMissingNeedles: true,
	}

	source := &VolumeReplica{
		location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "127.0.0.1:1"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}
	target := &VolumeReplica{
		location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "127.0.0.1:2"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}

	// resurrectAbsent=false with revision 2: the caller found the target vacuumed.
	hasChanges, err := vcd.doVolumeCheckDisk(sourceDB, targetDB, source, target, false, 2)
	if err != nil {
		t.Fatalf("doVolumeCheckDisk returned error: %v", err)
	}
	if hasChanges {
		t.Fatalf("absent needle was resurrected into a vacuumed replica; expected no changes")
	}
	if !bytes.Contains(buf.Bytes(), []byte("compaction revision 2")) {
		t.Fatalf("expected skip message to mention the compaction revision, got: %s", buf.String())
	}
}

// TestDoVolumeCheckDiskResurrectQueuesProvenMissingWrite verifies that with
// -resurrectMissingNeedles and a never-vacuumed target, the absent needle is
// treated as a missing write and queued for repair: the blob read against the
// unreachable source server is attempted, which is only possible if the needle
// passed the resurrection guard.
func TestDoVolumeCheckDiskResurrectQueuesProvenMissingWrite(t *testing.T) {
	sourceDB, targetDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer sourceDB.Close()
	defer targetDB.Close()

	if err := sourceDB.Set(types.NeedleId(1001), types.ToOffset(8), types.Size(123)); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	var buf bytes.Buffer
	vcd := &volumeCheckDisk{
		commandEnv: &CommandEnv{
			option: &ShellOptions{
				GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		},
		writer:                  &buf,
		now:                     time.Now(),
		applyChanges:            false,
		nonRepairThreshold:      1,
		resurrectMissingNeedles: true,
	}

	source := &VolumeReplica{
		location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "127.0.0.1:1"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}
	target := &VolumeReplica{
		location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "127.0.0.1:2"}},
		info:     &master_pb.VolumeInformationMessage{Id: 7},
	}

	_, err := vcd.doVolumeCheckDisk(sourceDB, targetDB, source, target, true, 0)
	if err == nil {
		t.Fatalf("expected blob read against unreachable source, got nil error (needle did not pass the guard?)")
	}
	if !bytes.Contains(buf.Bytes(), []byte("missed 1")) {
		t.Fatalf("expected the absent needle to be counted as missed, got: %s", buf.String())
	}
}

type testCommandVolumeCheckDisk struct {
	commandVolumeCheckDisk
}

type shouldSkipVolume struct {
	name              string
	a                 VolumeReplica
	b                 VolumeReplica
	pulseTimeAtSecond int64
	syncDeletions     bool
	shouldSkipVolume  bool
}

func TestShouldSkipVolume(t *testing.T) {
	var tests = []shouldSkipVolume{
		{
			name: "identical volumes should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true,
		},
		{
			name: "different file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1001,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		{
			name: "different delete counts with syncDeletions enabled should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      101,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		{
			name: "different delete counts with syncDeletions disabled should be skipped if file counts match",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      101,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     false,
			shouldSkipVolume:  true,
		},
		// Edge case: Zero file and delete counts
		{
			name: "volumes with zero file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true,
		},
		{
			name: "volumes with zero and non-zero file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        0,
				DeleteCount:      0,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false,
		},
		// Edge case: Recently modified volumes (after pulse time)
		// Note: VolumePulsePeriod is 10 seconds, so pulse cutoff is now - 20 seconds
		// When both volumes are recently modified, skip check to avoid false positives
		{
			name: "recently modified volumes with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583395}, // Modified 5 seconds ago
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583390}, // Modified 10 seconds ago
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts = skip
		},
		{
			name: "one volume modified before pulse cutoff with different file counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583370}, // Modified 30 seconds ago (before cutoff at -20s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        999,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583370}, // Same modification time
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // Different counts + old enough = needs sync
		},
		// Edge case: Different ModifiedAtSecond values, same file counts
		{
			name: "different modification times with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300}, // 100 seconds before pulse time
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583350}, // 50 seconds before pulse time
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts, both before cutoff
		},
		// Edge case: Very close to pulse time boundary
		{
			name: "volumes modified exactly at pulse cutoff boundary with different counts should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1001,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583379}, // Just before cutoff (pulseTime - 21s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583379}, // Just before cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // At boundary with different counts - needs sync
		},
		{
			name: "volumes modified just after pulse cutoff boundary with same counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583381}, // Just after cutoff (pulseTime - 19s)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583381}, // Just after cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts + recent = skip to avoid false positive
		},
		// Edge case: Large file count differences
		{
			name: "large file count difference with old modification time should not be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        10000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        5000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583300},
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  false, // Large difference requires sync
		},
		// Edge case: Both volumes modified AFTER pulse cutoff time
		// When ModifiedAtSecond >= pulseTimeAtSecond for both volumes with same counts,
		// the condition (a.info.FileCount != b.info.FileCount) is false, so we skip
		// without calling eqVolumeFileCount
		{
			name: "both volumes modified after pulse cutoff with same file counts should be skipped",
			a: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583405}, // After pulse cutoff (1696583380)
			},
			b: VolumeReplica{nil, &master_pb.VolumeInformationMessage{
				FileCount:        1000,
				DeleteCount:      100,
				ModifiedAtSecond: 1696583410}, // After pulse cutoff
			},
			pulseTimeAtSecond: 1696583400,
			syncDeletions:     true,
			shouldSkipVolume:  true, // Same counts = skip without calling eqVolumeFileCount
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			vcd := &volumeCheckDisk{
				writer:        &buf,
				now:           time.Unix(tt.pulseTimeAtSecond, 0),
				verbose:       false, // reduce noise in tests
				syncDeletions: tt.syncDeletions,
			}
			result, err := vcd.shouldSkipVolume(&tt.a, &tt.b)
			if err != nil {
				// In unit tests, we expect no errors from shouldSkipVolume
				// since we're using test data without actual network calls
				t.Errorf("shouldSkipVolume() returned unexpected error: %v", err)
				return
			}
			if result != tt.shouldSkipVolume {
				t.Errorf("shouldSkipVolume() = %v, want %v\nFileCount A=%d B=%d, DeleteCount A=%d B=%d",
					result, tt.shouldSkipVolume,
					tt.a.info.FileCount, tt.b.info.FileCount,
					tt.a.info.DeleteCount, tt.b.info.DeleteCount)
			}
		})
	}
}

// TestVolumeCheckDiskHelperMethods tests the helper methods on volumeCheckDisk
func TestVolumeCheckDiskHelperMethods(t *testing.T) {
	var buf bytes.Buffer
	vcd := &volumeCheckDisk{
		writer:  &buf,
		verbose: true,
	}

	// Test write method
	vcd.write("test %s", "message")
	if buf.String() != "test message\n" {
		t.Errorf("write() output = %q, want %q", buf.String(), "test message\n")
	}

	// Test writeVerbose with verbose=true
	buf.Reset()
	vcd.writeVerbose("verbose %d", 123)
	if buf.String() != "verbose 123\n" {
		t.Errorf("writeVerbose() with verbose=true output = %q, want %q", buf.String(), "verbose 123\n")
	}

	// Test writeVerbose with verbose=false
	buf.Reset()
	vcd.verbose = false
	vcd.writeVerbose("should not appear")
	if buf.String() != "" {
		t.Errorf("writeVerbose() with verbose=false output = %q, want empty", buf.String())
	}
}
