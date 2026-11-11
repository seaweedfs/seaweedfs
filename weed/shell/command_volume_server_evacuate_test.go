package shell

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/ecbalancer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient/exclusive_locks"
)

func TestEvacuatePrefersLowerVolumeRatio(t *testing.T) {
	volumes := mkByteVolumes(1, 1, 1)
	source := makeByteNode("source", 10, 0, 0, volumes)
	fuller := makeByteNode("fuller", 10, 0, 0, mkByteVolumes(10, 4, 1))
	emptier := makeByteNode("emptier", 10, 0, 0, mkByteVolumes(20, 2, 1))

	// Put the 40%-full server first: truncating 0.2 - 0.4 to int makes the ratios tie.
	moved, err := moveAwayOneNormalVolume(nil, nil, volumes[0], source, []*Node{fuller, emptier}, false)
	if err != nil || !moved {
		t.Fatalf("evacuate: moved=%v, err=%v", moved, err)
	}
	if _, found := emptier.selectedVolumes[volumes[0].Id]; !found {
		t.Fatal("expected evacuation to choose the server with lower occupancy")
	}
}

func TestVolumeServerEvacuate(t *testing.T) {
	targetServer := ""
	volumeRack := "rack1"
	c := commandVolumeServerEvacuate{
		targetServer: &targetServer,
		volumeRack:   &volumeRack,
	}
	c.topologyInfo = parseOutput(topoData)

	volumeServer := "192.168.1.4:8080"
	if err := c.evacuateNormalVolumes(nil, volumeServer, true, false, os.Stdout); err != nil {
		t.Errorf("evacuate: %v", err)
	}

}

func TestVolumeServerEvacuateNoLockFlag(t *testing.T) {
	c := &commandVolumeServerEvacuate{}

	// Test without -noLock: when -apply is set and shell is unlocked, Do must fail with lock error
	unlockedEnv := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	err := c.Do([]string{"-apply"}, unlockedEnv, io.Discard)
	if err == nil {
		t.Fatalf("expected error when running with -apply without lock or -noLock")
	}
	if !strings.Contains(err.Error(), "need to run \"lock\" first to continue") {
		t.Fatalf("expected lock error message, got: %v", err)
	}

	// Test with -noLock: should set commandEnv.noLock to true and bypass the lock requirement
	noLockEnv := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	errNoLock := c.Do([]string{"-noLock", "-apply"}, noLockEnv, io.Discard)
	if !noLockEnv.noLock {
		t.Fatalf("expected commandEnv.noLock to be true when -noLock flag is provided")
	}
	if err := noLockEnv.confirmIsLocked([]string{"volumeServer.evacuate"}); err != nil {
		t.Fatalf("confirmIsLocked should succeed when noLock is true: %v", err)
	}
	// The lock error should not be returned because -noLock bypassed it; it should reach node validation
	if errNoLock == nil || !strings.Contains(errNoLock.Error(), "need to specify volume server") {
		t.Fatalf("expected node validation error after bypassing lock check, got: %v", errNoLock)
	}
}

func TestVolumeServerEvacuateReturnsFlagParseError(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	err := (&commandVolumeServerEvacuate{}).Do([]string{"-noLock=invalid", "-apply"}, env, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "invalid boolean value") {
		t.Fatalf("expected a flag parse error, got: %v", err)
	}
	if env.noLock {
		t.Fatalf("a rejected -noLock must not unlock the shell")
	}
}

func TestVolumeServerEvacuateHelpIsNotAnError(t *testing.T) {
	env := &CommandEnv{locker: exclusive_locks.NewExclusiveLocker(nil, cluster.AdminShellLockName)}
	if err := (&commandVolumeServerEvacuate{}).Do([]string{"-h"}, env, io.Discard); err != nil {
		t.Fatalf("-h should print usage without failing, got: %v", err)
	}
}

func newEvacuateCommandForTest(movesPerExec int) *commandVolumeServerEvacuate {
	targetServer := ""
	volumeRack := ""
	c := &commandVolumeServerEvacuate{
		targetServer:       &targetServer,
		volumeRack:         &volumeRack,
		maxParallelization: 1,
		movesPerExec:       movesPerExec,
	}
	c.topologyInfo = parseOutput(topoDataEc)
	return c
}

// ecShardCount counts the EC shards a volume server holds in the topology.
func ecShardCount(topo *master_pb.TopologyInfo, volumeServer string) (shards int) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				if dn.Id != volumeServer {
					continue
				}
				for _, disk := range dn.DiskInfos {
					for _, eci := range disk.EcShardInfos {
						shards += erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci).Count()
					}
				}
			}
		}
	}
	return shards
}

func TestVolumeServerEvacuateEcPlansEveryShard(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	volumeServer := "172.19.0.10:8702"
	shards := ecShardCount(c.topologyInfo, volumeServer)
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	planned := strings.Count(out.String(), "moving ec volume")
	if shards == 0 || planned != shards || c.movedCount != shards {
		t.Fatalf("%s holds %d ec shards, planned %d moves, counted %d", volumeServer, shards, planned, c.movedCount)
	}
	if left := ecShardCount(c.topologyInfo, volumeServer); left != 0 {
		t.Fatalf("the model still has %d ec shards on %s after the plan", left, volumeServer)
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.HasPrefix(line, "moving ec volume") && strings.Fields(line)[6] == volumeServer {
			t.Fatalf("a shard is planned onto the evacuated server: %s", line)
		}
	}
}

func TestVolumeServerEvacuateEcMovesPerExec(t *testing.T) {
	all := newEvacuateCommandForTest(0)
	volumeServer := "172.19.0.10:8702"
	if err := all.evacuateEcVolumes(nil, volumeServer, false, false, io.Discard); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if all.movedCount < 3 {
		t.Fatalf("the test needs a server with at least 3 shards, it has %d", all.movedCount)
	}

	c := newEvacuateCommandForTest(2)
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if planned := strings.Count(out.String(), "moving ec volume"); planned != 2 || c.movedCount != 2 {
		t.Fatalf("expected 2 planned shard moves with -movesPerExec=2, got %d (counted %d)", planned, c.movedCount)
	}
	if !strings.Contains(out.String(), "reached -movesPerExec=2") {
		t.Fatalf("expected the limit to be reported, got:\n%s", out.String())
	}
}

// withEcShards gives an EC volume on a volume server the listed shards. The
// volume.list parser keeps only the first shard of a "shards:[5 11 12]" entry.
func withEcShards(topo *master_pb.TopologyInfo, volumeServer string, vid uint32, shardIds ...erasure_coding.ShardId) bool {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				if dn.Id != volumeServer {
					continue
				}
				for _, disk := range dn.DiskInfos {
					for _, eci := range disk.EcShardInfos {
						if eci.Id != vid {
							continue
						}
						si := erasure_coding.NewShardsInfo()
						for _, id := range shardIds {
							si.Set(erasure_coding.NewShardInfo(id, 0))
						}
						eci.EcIndexBits = si.Bitmap()
						eci.ShardSizes = si.SizesInt64()
						return true
					}
				}
			}
		}
	}
	return false
}

// Every limit, including the ones that end in the middle of a volume's shards,
// plans exactly that many moves.
func TestVolumeServerEvacuateEcMovesPerExecCutsInsideVolume(t *testing.T) {
	volumeServer := "172.19.0.20:8706"
	newCommand := func(movesPerExec int) *commandVolumeServerEvacuate {
		c := newEvacuateCommandForTest(movesPerExec)
		if !withEcShards(c.topologyInfo, volumeServer, 14322, 5, 11, 12) {
			t.Fatalf("ec volume 14322 is not on %s", volumeServer)
		}
		return c
	}
	shards := ecShardCount(newCommand(0).topologyInfo, volumeServer)
	if shards != 6 {
		t.Fatalf("expected 6 ec shards on %s, found %d", volumeServer, shards)
	}
	for limit := 1; limit <= shards; limit++ {
		c := newCommand(limit)
		var out bytes.Buffer
		if err := c.evacuateEcVolumes(nil, volumeServer, false, false, &out); err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if planned := strings.Count(out.String(), "moving ec volume"); planned != limit || c.movedCount != limit {
			t.Fatalf("limit %d: planned %d moves, counted %d", limit, planned, c.movedCount)
		}
	}
	c := newCommand(0)
	if err := c.evacuateEcVolumes(nil, volumeServer, false, false, io.Discard); err != nil {
		t.Fatalf("no limit: %v", err)
	}
	if c.movedCount != shards || ecShardCount(c.topologyInfo, volumeServer) != 0 {
		t.Fatalf("no limit: planned %d of %d shards", c.movedCount, shards)
	}
}

// tightTargetTopology has a source holding shard 0 of volumes 1 and 2, and one
// target with room for a single planned shard: two free slots, and the plan
// takes two per shard.
func tightTargetTopology() *master_pb.TopologyInfo {
	ecShard := func(vid uint32, shardIds ...erasure_coding.ShardId) *master_pb.VolumeEcShardInformationMessage {
		si := erasure_coding.NewShardsInfo()
		for _, id := range shardIds {
			si.Set(erasure_coding.NewShardInfo(id, 0))
		}
		return &master_pb.VolumeEcShardInformationMessage{Id: vid, Collection: "c", EcIndexBits: si.Bitmap(), ShardSizes: si.SizesInt64()}
	}
	node := func(id string, maxVolumes int64, shards ...*master_pb.VolumeEcShardInformationMessage) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{Id: id, DiskInfos: map[string]*master_pb.DiskInfo{
			"": {MaxVolumeCount: maxVolumes, EcShardInfos: shards},
		}}
	}
	source := node("source:8080", 10, ecShard(1, 0), ecShard(2, 0))
	// 1 volume slot = 10 shard slots, 8 of them taken by volume 99.
	target := node("target:8080", 1, ecShard(99, 0, 1, 2, 3, 4, 5, 6, 7))
	return &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{
		Id: "dc1",
		RackInfos: []*master_pb.RackInfo{
			{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{source}},
			{Id: "rack2", DataNodeInfos: []*master_pb.DataNodeInfo{target}},
		},
	}}}
}

// A shard that fails to move must not keep the room it was planned on: the
// readable shard that found no destination is planned again onto it.
func TestVolumeServerEvacuateEcFailedMoveFreesItsTarget(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	var ran []ecbalancer.Move
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (ec.EcShardMovesResult, error) {
		var result ec.EcShardMovesResult
		for _, m := range moves {
			ran = append(ran, m)
			if m.VolumeID == 1 {
				result.Failed++
				result.FailedMoves = append(result.FailedMoves, m)
				continue
			}
			result.Moved++
		}
		if result.Failed > 0 {
			return result, errors.New("input/output error")
		}
		return result, nil
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "source:8080", true, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if len(ran) != 2 || ran[0].VolumeID != 1 || ran[1].VolumeID != 2 {
		t.Fatalf("expected volume 1 to fail and volume 2 to move after it, ran %+v\n%s", ran, out.String())
	}
	if c.movedCount != 1 {
		t.Fatalf("expected 1 moved shard, counted %d", c.movedCount)
	}
	if strings.Contains(out.String(), "failed to move away ec volume 2") {
		t.Fatalf("volume 2 should have found a destination:\n%s", out.String())
	}
}

// When every move fails, planning again ends once no shard is left to plan.
func TestVolumeServerEvacuateEcEndsWhenEveryMoveFails(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	rounds := 0
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (ec.EcShardMovesResult, error) {
		rounds++
		return ec.EcShardMovesResult{Failed: len(moves), FailedMoves: moves}, errors.New("input/output error")
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "source:8080", true, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	// Round 1 fails volume 1, round 2 fails volume 2; nothing is left to plan.
	if rounds != 2 {
		t.Fatalf("expected 2 rounds, got %d:\n%s", rounds, out.String())
	}
	if c.movedCount != 0 {
		t.Fatalf("expected nothing moved, counted %d", c.movedCount)
	}
}

// A lost lock fails the run even with -skipNonMoveable, so the caller does not
// take an unfinished evacuation for a finished one.
func TestVolumeServerEvacuateEcLostLockFailsTheRun(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (ec.EcShardMovesResult, error) {
		return ec.EcShardMovesResult{Failed: 1, NotStarted: len(moves) - 1, FailedMoves: moves[:1]}, errors.Join(ec.ErrLockLost)
	}
	err := c.evacuateEcVolumes(nil, "source:8080", true, true, io.Discard)
	if !errors.Is(err, ec.ErrLockLost) {
		t.Fatalf("expected the lost lock to fail the run, got %v", err)
	}
}

// An applied run that stops at -movesPerExec says so, so the operator runs it again.
func TestVolumeServerEvacuateEcReportsMovesPerExecWhenApplied(t *testing.T) {
	c := newEvacuateCommandForTest(2)
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (ec.EcShardMovesResult, error) {
		return ec.EcShardMovesResult{Moved: len(moves)}, nil
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "172.19.0.10:8702", false, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if c.movedCount != 2 || !strings.Contains(out.String(), "reached -movesPerExec=2") {
		t.Fatalf("expected 2 moves and the limit reported, counted %d:\n%s", c.movedCount, out.String())
	}
}

// Every round moves or drops each planned shard, so planning again goes on for as
// long as failed moves leave room: a readable shard behind many unreadable ones
// still moves.
func TestVolumeServerEvacuateEcPlansAgainUntilNoRoomIsLeft(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	const unreadable = 12
	source := c.topologyInfo.DataCenterInfos[0].RackInfos[0].DataNodeInfos[0]
	var shards []*master_pb.VolumeEcShardInformationMessage
	for vid := uint32(1); vid <= unreadable+1; vid++ {
		si := erasure_coding.NewShardsInfo()
		si.Set(erasure_coding.NewShardInfo(0, 0))
		shards = append(shards, &master_pb.VolumeEcShardInformationMessage{Id: vid, Collection: "c", EcIndexBits: si.Bitmap(), ShardSizes: si.SizesInt64()})
	}
	source.DiskInfos[""].EcShardInfos = shards
	var moved []uint32
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (result ec.EcShardMovesResult, err error) {
		for _, m := range moves {
			if m.VolumeID <= unreadable {
				result.Failed++
				result.FailedMoves = append(result.FailedMoves, m)
				err = errors.New("input/output error")
				continue
			}
			result.Moved++
			moved = append(moved, m.VolumeID)
		}
		return result, err
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "source:8080", true, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if len(moved) != 1 || moved[0] != unreadable+1 {
		t.Fatalf("expected volume %d to move after %d failed ones, moved %v:\n%s", unreadable+1, unreadable, moved, out.String())
	}
}

// A failed move gives back every slot the plan took from its target, also when
// the target holds the shard in another disk type, which the undo leaves alone.
func TestVolumeServerEvacuateEcFailedMoveGivesBackItsSlots(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	target := c.topologyInfo.DataCenterInfos[0].RackInfos[1].DataNodeInfos[0]
	// One free slot: volume 99 takes 9 of the 10.
	disk := target.DiskInfos[""]
	si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(disk.EcShardInfos[0])
	si.Set(erasure_coding.NewShardInfo(8, 0))
	disk.EcShardInfos[0].EcIndexBits, disk.EcShardInfos[0].ShardSizes = si.Bitmap(), si.SizesInt64()
	ssd := erasure_coding.NewShardsInfo()
	ssd.Set(erasure_coding.NewShardInfo(0, 0))
	target.DiskInfos["ssd"] = &master_pb.DiskInfo{Type: "ssd", EcShardInfos: []*master_pb.VolumeEcShardInformationMessage{
		{Id: 1, Collection: "c", DiskType: "ssd", EcIndexBits: ssd.Bitmap(), ShardSizes: ssd.SizesInt64()},
	}}
	var moved []uint32
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (result ec.EcShardMovesResult, err error) {
		for _, m := range moves {
			if m.VolumeID == 1 {
				result.Failed++
				result.FailedMoves = append(result.FailedMoves, m)
				err = errors.New("input/output error")
				continue
			}
			result.Moved++
			moved = append(moved, m.VolumeID)
		}
		return result, err
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "source:8080", true, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if len(moved) != 1 || moved[0] != 2 {
		t.Fatalf("expected volume 2 to take the slot volume 1 gave back, moved %v:\n%s", moved, out.String())
	}
	if erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(target.DiskInfos["ssd"].EcShardInfos[0]).Count() != 1 {
		t.Fatal("the undo removed the shard the target already had")
	}
}

// A failed move onto a target that already had the shard gives its slot back,
// although the target reports the shard after the failure.
func TestVolumeServerEvacuateEcFailedMoveOntoAnExistingShardGivesBackItsSlot(t *testing.T) {
	c := newEvacuateCommandForTest(0)
	c.topologyInfo = tightTargetTopology()
	target := c.topologyInfo.DataCenterInfos[0].RackInfos[1].DataNodeInfos[0]
	// One free slot: volume 99 takes 8 of the 10 and shard 1.0 the ninth.
	disk := target.DiskInfos[""]
	si := erasure_coding.NewShardsInfo()
	si.Set(erasure_coding.NewShardInfo(0, 0))
	disk.EcShardInfos = append(disk.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{Id: 1, Collection: "c", EcIndexBits: si.Bitmap(), ShardSizes: si.SizesInt64()})
	var moved []uint32
	c.moveEcShards = func(moves []ecbalancer.Move, stopOnError bool) (result ec.EcShardMovesResult, err error) {
		for _, m := range moves {
			if m.VolumeID == 1 {
				result.Failed++
				result.LandedMoves = append(result.LandedMoves, m)
				err = errors.New("input/output error")
				continue
			}
			result.Moved++
			moved = append(moved, m.VolumeID)
		}
		return result, err
	}
	var out bytes.Buffer
	if err := c.evacuateEcVolumes(nil, "source:8080", true, true, &out); err != nil {
		t.Fatalf("evacuate: %v", err)
	}
	if len(moved) != 1 || moved[0] != 2 {
		t.Fatalf("expected volume 2 to take the slot volume 1 gave back, moved %v:\n%s", moved, out.String())
	}
	if erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(disk.EcShardInfos[1]).Count() != 1 {
		t.Fatal("the undo removed the shard the target already had")
	}
}
