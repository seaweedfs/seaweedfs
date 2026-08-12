package volume_move

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	srcAddr = pb.ServerAddress("src:8080")
	dstAddr = pb.ServerAddress("dst:8080")
)

func volumeStatus(datSize, idxSize, fileCount uint64) *volume_server_pb.ReadVolumeFileStatusResponse {
	return &volume_server_pb.ReadVolumeFileStatusResponse{
		DatFileSize: datSize,
		IdxFileSize: idxSize,
		FileCount:   fileCount,
	}
}

func TestSameServer(t *testing.T) {
	if !SameServer("node:8080", "node:8080.18080") {
		t.Error("one server written with and without an explicit grpc port compared unequal")
	}
	// Test harnesses run several servers on HTTP port 0 with distinct grpc
	// ports; the grpc endpoint is the server's identity.
	if SameServer("127.0.0.1:0.36825", "127.0.0.1:0.38609") {
		t.Error("two servers sharing a degenerate HTTP address compared equal")
	}
	// Malformed addresses (an unvalidated -source/-target flag) must compare
	// without touching the fatal ToGrpcAddress parser.
	if !SameServer("node:abc", "node:abc") {
		t.Error("identical malformed addresses compared unequal")
	}
	if SameServer("node:abc", "node:8080") {
		t.Error("malformed and well-formed addresses compared equal")
	}
	if SameServer("", "node:8080") {
		t.Error("empty and well-formed addresses compared equal")
	}
}

func TestCheckDialable(t *testing.T) {
	// Rejected: the class the fatal parser aborts on, plus dotted forms with
	// a non-numeric component — those canonicalize differently across dial
	// paths ("node:8080.bad" dials as node:bad on one and node:18080 on the
	// other, silently aliasing another server).
	for _, addr := range []pb.ServerAddress{"node:abc", "host.domain.com:x8080", "[::1]:abc",
		"node:8080.bad", "node:bad.8080", "node:.bad", "node:8080."} {
		if err := checkDialable(addr); err == nil {
			t.Errorf("%q accepted although it is unsafe to dial", addr)
		}
	}
	// Accepted: normalized cleanly, or handed to the dialer untouched where
	// a bad address fails as an ordinary error.
	for _, addr := range []pb.ServerAddress{"node:8080", "node:8080.18080", "127.0.0.1:0.36825", "", "node", "node:"} {
		if err := checkDialable(addr); err != nil {
			t.Errorf("%q rejected: %v", addr, err)
		}
	}
}

func TestEmbeddedSourceAddressValidated(t *testing.T) {
	// The target dials the source embedded in copy/tail requests through the
	// same fatal parser; the client must reject a malformed source before
	// issuing any RPC.
	bad := pb.ServerAddress("node:abc")

	ops := map[string]func(m *Mover) error{
		"ReplicateVolume": func(m *Mover) error {
			return m.ReplicateVolume(context.Background(), 7, bad, dstAddr, "", nil)
		},
		"TailVolume": func(m *Mover) error {
			return m.TailVolume(context.Background(), 7, bad, dstAddr, 0, time.Second)
		},
		"CopyAndMountEcShards": func(m *Mover) error {
			return m.CopyAndMountEcShards(context.Background(), 7, "c1", []erasure_coding.ShardId{3}, bad, dstAddr, 0, nil)
		},
		"MoveEcShards": func(m *Mover) error {
			move := ecMove(3)
			move.Source = bad
			return m.MoveEcShards(context.Background(), move, EcMoveOptions{})
		},
	}
	for name, op := range ops {
		cluster := newFakeCluster()
		err := op(cluster.mover())
		if err == nil || !strings.Contains(err.Error(), "invalid volume server address") {
			t.Errorf("%s: expected invalid-address error, got: %v", name, err)
		}
		if len(cluster.callList()) != 0 {
			t.Errorf("%s: RPCs issued with a malformed embedded source: %v", name, cluster.callList())
		}
	}
}

func TestNewMoverRejectsMalformedAddressWithoutDialing(t *testing.T) {
	// The production dial path must fail the move, not the process, when a
	// caller passes an unvalidated malformed address.
	mover := NewMover(grpc.WithTransportCredentials(insecure.NewCredentials()))
	err := mover.LiveMoveVolume(context.Background(), 7, "node:abc", "node:8080", VolumeMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "invalid volume server address") {
		t.Fatalf("expected invalid-address error, got: %v", err)
	}
}

func assertCalls(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("RPC sequence mismatch:\n got:  %v\n want: %v", got, want)
	}
}

func TestLiveMoveVolumeSequence(t *testing.T) {
	cluster := newFakeCluster()
	cluster.lastAppendAtNs = 12345
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{
		DiskType:        "ssd",
		IoBytePerSecond: 42,
	})
	if err != nil {
		t.Fatalf("LiveMoveVolume: %v", err)
	}

	// The source status read must come after the tail: a write in flight when
	// the source was frozen can land after an earlier read, and verifying
	// against that stale snapshot would miss a missed tail. The first target
	// status read probes whether the target already held the volume, so a
	// failed copy never cleans up someone else's replica.
	assertCalls(t, cluster.callList(), []string{
		"src:8080 VolumeStatus",
		"src:8080 VolumeMarkReadonly",
		"dst:8080 ReadVolumeFileStatus",
		"dst:8080 VolumeCopy",
		"dst:8080 VolumeTailReceiver",
		"src:8080 ReadVolumeFileStatus",
		"dst:8080 ReadVolumeFileStatus",
		"src:8080 VolumeDelete",
	})

	copyReq := cluster.copyReqs[0]
	if copyReq.DiskType != "ssd" || copyReq.IoBytePerSecond != 42 || copyReq.SourceDataNode != string(srcAddr) {
		t.Errorf("copy request not propagated: %+v", copyReq)
	}
	deleteReq := cluster.deleteReqs[0]
	if deleteReq.OnlyEmpty || !deleteReq.KeepRemoteData {
		t.Errorf("source delete must keep remote data and not be only-empty: %+v", deleteReq)
	}
}

func TestLiveMoveVolumeVerifyMismatchKeepsSource(t *testing.T) {
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(900, 100, 10)

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "behind the source") {
		t.Fatalf("expected target-behind error, got: %v", err)
	}

	calls := cluster.callList()
	for _, call := range calls {
		if call == "src:8080 VolumeDelete" {
			t.Fatalf("source deleted despite verification failure: %v", calls)
		}
	}
	// The incomplete target copy is removed so the restored source stays the
	// only replica, then the source is made writable again.
	if fmt.Sprint(calls[len(calls)-2:]) != fmt.Sprint([]string{"dst:8080 VolumeDelete", "src:8080 VolumeMarkWritable"}) {
		t.Fatalf("expected target cleanup then source restore, got: %v", calls)
	}
}

func TestLiveMoveVolumeAbortKeepsSourceReadonlyWhenTargetCleanupFails(t *testing.T) {
	// Restoring the source while the stale target stays mounted risks
	// divergent replicas; the source stays readonly until a re-run recovers.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(900, 100, 10)
	cluster.errs["dst:8080 VolumeDelete"] = errors.New("target unreachable")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil {
		t.Fatal("expected verification failure")
	}
	if !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("error does not mark the source as deliberately kept readonly: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("source made writable with the stale target still mounted: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumeAlreadyReadonlyStaysReadonly(t *testing.T) {
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.errs["dst:8080 VolumeCopy"] = errors.New("copy failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil {
		t.Fatal("expected copy failure")
	}

	// The hard mark is issued even for a readonly-reporting volume: that
	// status also covers low-disk and readonly-but-can-delete states, which
	// still accept needle deletes.
	marked := false
	for _, call := range cluster.callList() {
		if call == "src:8080 VolumeMarkReadonly" {
			marked = true
		}
		// A volume that was readonly before the move (e.g. full) must stay so.
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatal("made an already-readonly volume writable after a failed move")
		}
	}
	if !marked {
		t.Fatalf("readonly-reporting source not hard-frozen: %v", cluster.callList())
	}
}

func TestLiveMoveVolumePreFrozenAbortCleansTarget(t *testing.T) {
	// tiering.run freezes replicas itself and thaws them after a failed move,
	// so even a move that did not do the freezing must remove its target copy
	// on abort — a stale mounted target plus a thawed source is divergence.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(900, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1 // no volume on the target before the copy

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil {
		t.Fatal("expected verification failure")
	}

	cleaned := false
	for _, call := range cluster.callList() {
		if call == "dst:8080 VolumeDelete" {
			cleaned = true
		}
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("thawed a source this move did not freeze: %v", cluster.callList())
		}
	}
	if !cleaned {
		t.Fatalf("incomplete target copy not cleaned up: %v", cluster.callList())
	}
}

func TestLiveMoveVolumeCopyErrorCleansMountedTarget(t *testing.T) {
	// The server can finish the copy and mount the target even when the client
	// loses the stream; the abort probes the target and cleans up the copy.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1 // no volume on the target before the copy
	cluster.errs["dst:8080 VolumeCopy"] = errors.New("stream lost")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil {
		t.Fatal("expected copy failure")
	}

	calls := cluster.callList()
	if fmt.Sprint(calls[len(calls)-2:]) != fmt.Sprint([]string{"dst:8080 VolumeDelete", "src:8080 VolumeMarkWritable"}) {
		t.Fatalf("expected target cleanup then source restore, got: %v", calls)
	}
}

func TestLiveMoveVolumeCopyErrorUnknownTargetKeepsSourceReadonly(t *testing.T) {
	// The pre-copy probe failed at the transport level, so whether the target
	// held a replica before the move is unknown; the failed copy may still
	// have mounted a complete copy there (the server can finish after the
	// client loses the stream). Deleting it risks a healthy pre-existing
	// replica, and reopening the source beside it risks two writable replicas
	// taking divergent writes — so the source stays readonly.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1
	cluster.statusFailureErr = status.Error(codes.Unavailable, "connection refused")
	cluster.errs["dst:8080 VolumeCopy"] = errors.New("stream lost")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("expected kept-readonly failure, got: %v", err)
	}

	calls := cluster.callList()
	for _, call := range calls {
		if call == "dst:8080 VolumeDelete" {
			t.Fatalf("deleted the target with its prior state unknown: %v", calls)
		}
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("reopened the source beside a possibly-mounted copy: %v", calls)
		}
	}
}

func TestLiveMoveVolumeCopyErrorLeavesPreexistingTarget(t *testing.T) {
	// A target that held the volume before the move is someone else's replica;
	// a failed copy must not delete it.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.errs["dst:8080 VolumeCopy"] = errors.New("copy failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("expected kept-readonly failure, got: %v", err)
	}

	calls := cluster.callList()
	for _, call := range calls {
		if call == "dst:8080 VolumeDelete" {
			t.Fatalf("deleted a pre-existing target replica after a failed copy: %v", calls)
		}
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("reopened the source with a copy of unprovable origin on the target: %v", calls)
		}
	}
}

func TestLiveMoveVolumeTailErrorAborts(t *testing.T) {
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.errs["dst:8080 VolumeTailReceiver"] = errors.New("tail failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "tail volume") {
		t.Fatalf("expected tail error, got: %v", err)
	}

	calls := cluster.callList()
	for _, call := range calls {
		if call == "src:8080 VolumeDelete" {
			t.Fatalf("source deleted despite tail failure: %v", calls)
		}
	}
	if fmt.Sprint(calls[len(calls)-2:]) != fmt.Sprint([]string{"dst:8080 VolumeDelete", "src:8080 VolumeMarkWritable"}) {
		t.Fatalf("expected target cleanup then source restore, got: %v", calls)
	}
}

func TestLiveMoveVolumeTailErrorToleratedForReadonlySource(t *testing.T) {
	// A volume that was already readonly before the move was frozen for the
	// whole copy, so a failed tail has nothing to deliver.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1 // no volume on the target before the copy
	cluster.errs["dst:8080 VolumeTailReceiver"] = errors.New("tail failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{IdleTimeout: time.Millisecond})
	if err != nil {
		t.Fatalf("LiveMoveVolume on readonly source with failed tail: %v", err)
	}

	calls := cluster.callList()
	if calls[len(calls)-1] != "src:8080 VolumeDelete" {
		t.Fatalf("move did not complete with source delete: %v", calls)
	}
}

func TestLiveMoveVolumeTailFailureAbortsWhenSourceStillChanging(t *testing.T) {
	// A tolerated tail failure substitutes a drain barrier: the source status
	// must hold still across the idle window, or the move aborts.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1 // no volume on the target before the copy
	cluster.statusSeq[string(srcAddr)] = []*volume_server_pb.ReadVolumeFileStatusResponse{
		volumeStatus(1000, 100, 10),
		volumeStatus(1024, 116, 11), // a straggler landed between the two reads
	}
	cluster.errs["dst:8080 VolumeTailReceiver"] = errors.New("tail failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{IdleTimeout: time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "still changing") {
		t.Fatalf("expected still-changing abort, got: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "src:8080 VolumeDelete" {
			t.Fatalf("source deleted while still changing: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumeSourceDeleteFailureKeepsBothReadonly(t *testing.T) {
	// Past verification the target is complete and may hold new writes, and
	// the failed delete may or may not have reached the source. Deleting the
	// target or reopening the source could fork the volume; keep everything
	// readonly for a re-run.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.errs["src:8080 VolumeDelete"] = errors.New("delete timed out")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil {
		t.Fatal("expected source delete failure")
	}
	if !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("error does not mark the source as deliberately kept readonly: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "dst:8080 VolumeDelete" {
			t.Fatalf("deleted the verified target copy after a failed source delete: %v", cluster.callList())
		}
		if call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("restored source writability next to a mounted verified copy: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumeRefusesRecopyOverCompleteTarget(t *testing.T) {
	// A readonly source next to a target already holding a copy that is not
	// behind it is the signature of a previous move whose source delete
	// failed; recopying would tear down the possibly-authoritative target.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "already has a copy") {
		t.Fatalf("expected recopy refusal, got: %v", err)
	}
	if !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("refusal does not mark the source as deliberately kept readonly: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "dst:8080 VolumeCopy" || call == "dst:8080 VolumeDelete" {
			t.Fatalf("touched the possibly-authoritative target: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumeReadonlySourceUnknownTargetRefused(t *testing.T) {
	// With a readonly source, an unreachable target probe must fail closed:
	// the target could hold the authoritative copy of a previous move.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1
	cluster.statusFailureErr = status.Error(codes.Unavailable, "connection refused")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("expected fail-closed refusal, got: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "dst:8080 VolumeCopy" {
			t.Fatalf("copied over a target in unknown state: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumePreFrozenSourceMissingRustTargetProceeds(t *testing.T) {
	// The Rust volume server answers a missing volume with codes.NotFound
	// (the Go server uses a plain error, code Unknown). Both are definitive
	// absence: with a pre-frozen source (tiering freezes before moving), a
	// NotFound misclassified as "unknown" would refuse the move with
	// ErrSourceKeptReadonly — and tiering callers then deliberately skip
	// thawing, stranding every replica readonly after an ordinary move.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1000, 100, 10)
	cluster.statusFailures[string(dstAddr)] = 1
	cluster.statusFailureErr = status.Error(codes.NotFound, "not found volume id 7")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err != nil {
		t.Fatalf("LiveMoveVolume to an empty Rust target: %v", err)
	}

	calls := cluster.callList()
	if calls[len(calls)-1] != "src:8080 VolumeDelete" {
		t.Fatalf("move did not complete with source delete: %v", calls)
	}
}

func TestLiveMoveVolumeReadonlySourceExistingTargetRefused(t *testing.T) {
	// No client-side observation can prove an existing copy is a stale
	// remnant rather than the authoritative copy of an unfinished move —
	// compaction shrinks the authoritative copy below the stale source, and
	// any snapshot can be invalidated by a write right after it. Even a
	// target that reads as behind is refused.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(900, 90, 9)

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !errors.Is(err, ErrSourceKeptReadonly) {
		t.Fatalf("expected fail-closed refusal, got: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "dst:8080 VolumeCopy" || call == "dst:8080 VolumeDelete" {
			t.Fatalf("touched a possibly-authoritative target: %v", cluster.callList())
		}
	}
}

func TestCopyVolumeLeavesReadonlySourceUntouched(t *testing.T) {
	// The copy is non-destructive, so a readonly-reporting source (possibly
	// only transiently, e.g. low disk) is not hard-marked.
	cluster := newFakeCluster()
	cluster.readonly[string(srcAddr)] = true

	_, err := cluster.mover().CopyVolume(context.Background(), 7, srcAddr, dstAddr, "", 0, true, nil)
	if err != nil {
		t.Fatalf("CopyVolume: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "src:8080 VolumeMarkReadonly" || call == "src:8080 VolumeMarkWritable" {
			t.Fatalf("readonly source touched by a non-destructive copy: %v", cluster.callList())
		}
	}
}

func TestLiveMoveVolumeTargetAheadCommits(t *testing.T) {
	// The target serves writes while the move tails; a target that got ahead
	// holds those acknowledged writes, and the move must commit so they stay
	// on the surviving copy.
	cluster := newFakeCluster()
	cluster.status[string(srcAddr)] = volumeStatus(1000, 100, 10)
	cluster.status[string(dstAddr)] = volumeStatus(1100, 110, 11)

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err != nil {
		t.Fatalf("LiveMoveVolume with target ahead: %v", err)
	}

	calls := cluster.callList()
	if calls[len(calls)-1] != "src:8080 VolumeDelete" {
		t.Fatalf("move did not commit with source delete: %v", calls)
	}
}

func TestLiveMoveVolumeRejectsSameServer(t *testing.T) {
	// The second target is the same server written with an explicit grpc port;
	// the guard must see through the representation difference.
	for _, target := range []pb.ServerAddress{srcAddr, pb.ServerAddress("src:8080.18080")} {
		cluster := newFakeCluster()

		err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, target, VolumeMoveOptions{})
		if err == nil || !strings.Contains(err.Error(), "its own server") {
			t.Fatalf("target %q: expected same-server rejection, got: %v", target, err)
		}
		if len(cluster.callList()) != 0 {
			t.Fatalf("target %q: RPCs issued for a rejected move: %v", target, cluster.callList())
		}
	}
}

func TestLiveMoveVolumeReadonlyMarkFailureRestores(t *testing.T) {
	// The marking RPC can fail after the server applied the mark (e.g. its
	// master notification failed), so a mark error must still restore.
	cluster := newFakeCluster()
	cluster.errs["src:8080 VolumeMarkReadonly"] = errors.New("master notification failed")

	err := cluster.mover().LiveMoveVolume(context.Background(), 7, srcAddr, dstAddr, VolumeMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "mark volume") {
		t.Fatalf("expected mark-readonly error, got: %v", err)
	}

	calls := cluster.callList()
	if calls[len(calls)-1] != "src:8080 VolumeMarkWritable" {
		t.Fatalf("source writability not restored after failed readonly mark: %v", calls)
	}
}

func TestCopyVolumeRestoreWritable(t *testing.T) {
	for _, restoreWritable := range []bool{true, false} {
		cluster := newFakeCluster()
		_, err := cluster.mover().CopyVolume(context.Background(), 7, srcAddr, dstAddr, "", 0, restoreWritable, nil)
		if err != nil {
			t.Fatalf("CopyVolume(restoreWritable=%v): %v", restoreWritable, err)
		}
		restored := false
		for _, call := range cluster.callList() {
			if call == "src:8080 VolumeMarkWritable" {
				restored = true
			}
		}
		if restored != restoreWritable {
			t.Errorf("restoreWritable=%v but writability restored=%v", restoreWritable, restored)
		}
	}
}
