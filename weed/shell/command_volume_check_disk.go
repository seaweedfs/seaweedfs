package shell

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVolumeCheckDisk{})
}

type commandVolumeCheckDisk struct{}

type volumeCheckDisk struct {
	commandEnv *CommandEnv
	writer     io.Writer
	writerMu   sync.Mutex
	now        time.Time

	slowMode           bool
	verbose            bool
	applyChanges       bool
	syncDeletions      bool
	fixReadOnly        bool
	nonRepairThreshold float64
	// resurrectMissingNeedles controls whether a needle present on the source
	// but entirely absent on the target is pushed back. Default false: an
	// absent needle is indistinguishable from a vacuumed delete, so the safe
	// default never raises deleted data. Even when enabled, resurrection only
	// happens into a replica whose compaction revision is 0: a never-vacuumed
	// index still holds a tombstone for every delete it processed, so a needle
	// absent there is provably a missing write.
	resurrectMissingNeedles bool

	ewg *ErrorWaitGroup
}

func (c *commandVolumeCheckDisk) Name() string {
	return "volume.check.disk"
}

func (c *commandVolumeCheckDisk) Help() string {
	return `check all replicated volumes to find and fix inconsistencies. It is optional and resource intensive.

	How it works:

	find all volumes that are replicated

	for each writable volume ID, if there are more than 2 replicas, find one pair with the largest 2 in file count
		for the pair volume A and B
			append entries in A and not in B to B
			append entries in B and not in A to A

	optionally, for each non-writable volume replica A
		select a writable volume replica B
		if entries in A don't match B
			prune late volume entries not matching its index file
			append missing entries from B into A
			mark the volume as writable (healthy)

	Options:
	  -slow: check all replicas even if file counts are the same
	  -v: verbose mode with detailed progress output
	  -volumeId: check only a specific volume ID (0 for all)
	  -apply: actually apply the fixes (default is simulation mode)
	  -fixReadOnly: also check and repair read-only volumes using uni-directional sync
	  -syncDeleted: sync deletion records during repair
	  -nonRepairThreshold: maximum fraction of missing keys allowed for repair (default 0.3)
	  -resurrectMissingNeedles: copy needles absent on one replica back from the other, e.g. after replication failures.
	    Only repairs replicas that were never vacuumed (compaction revision 0), where an absent needle is provably
	    a missing write and not a vacuumed delete. Counts toward -nonRepairThreshold.

`
}

func (c *commandVolumeCheckDisk) HasTag(tag CommandTag) bool {
	return tag == ResourceHeavy
}

func (c *commandVolumeCheckDisk) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	slowMode := fsckCommand.Bool("slow", false, "slow mode checks all replicas even file counts are the same")
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	volumeId := fsckCommand.Uint("volumeId", 0, "the volume ID (0 for all)")
	applyChanges := fsckCommand.Bool("apply", false, "apply the fix")
	// TODO: remove this alias
	applyChangesAlias := fsckCommand.Bool("force", false, "apply the fix (alias for -apply)")
	fixReadOnly := fsckCommand.Bool("fixReadOnly", false, "apply the fix even on readonly volumes (EXPERIMENTAL!)")
	syncDeletions := fsckCommand.Bool("syncDeleted", false, "sync of deletions the fix")
	maxParallelization := fsckCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	nonRepairThreshold := fsckCommand.Float64("nonRepairThreshold", 0.3, "repair when missing keys is not more than this limit")
	resurrectMissingNeedles := fsckCommand.Bool("resurrectMissingNeedles", false, "copy needles absent on one replica back from the other, only into never-vacuumed replicas (compaction revision 0)")
	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, fsckCommand, applyChangesAlias, applyChanges)
	infoAboutSimulationMode(writer, *applyChanges, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vcd := &volumeCheckDisk{
		commandEnv: commandEnv,
		writer:     writer,
		now:        time.Now(),

		slowMode:           *slowMode,
		verbose:            *verbose,
		applyChanges:       *applyChanges,
		syncDeletions:      *syncDeletions,
		fixReadOnly:        *fixReadOnly,
		nonRepairThreshold: *nonRepairThreshold,

		resurrectMissingNeedles: *resurrectMissingNeedles,

		ewg: NewErrorWaitGroup(*maxParallelization),
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	// collect volume replicas, optionally filtered by volume ID
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	if vid := uint32(*volumeId); vid > 0 {
		if replicas, ok := volumeReplicas[vid]; ok {
			volumeReplicas = map[uint32][]*VolumeReplica{
				vid: replicas,
			}
		} else {
			return fmt.Errorf("volume %d not found", vid)
		}
	}

	if err := vcd.checkWritableVolumes(volumeReplicas); err != nil {
		return err
	}
	vcd.checkReadOnlyVolumes(volumeReplicas)

	return vcd.ewg.Wait()
}

// checkWritableVolumes fixes volume replicas which are not read-only.
func (vcd *volumeCheckDisk) checkWritableVolumes(volumeReplicas map[uint32][]*VolumeReplica) error {
	vcd.write("Pass #1 (writable volumes)")

	for _, replicas := range volumeReplicas {
		// filter readonly replica
		var writableReplicas []*VolumeReplica
		for _, replica := range replicas {
			if replica.info.ReadOnly {
				vcd.write("skipping readonly volume %d on %s", replica.info.Id, replica.location.dataNode.Id)
			} else {
				writableReplicas = append(writableReplicas, replica)
			}
		}

		slices.SortFunc(writableReplicas, func(a, b *VolumeReplica) int {
			return int(b.info.FileCount - a.info.FileCount)
		})
		for len(writableReplicas) >= 2 {
			a, b := writableReplicas[0], writableReplicas[1]
			shouldSkip, err := vcd.shouldSkipVolume(a, b)
			if err != nil {
				vcd.write("error checking if volume %d should be skipped: %v", a.info.Id, err)
				// Continue with sync despite error to be safe
			} else if shouldSkip {
				// always choose the larger volume to be the source
				writableReplicas = append(writableReplicas[:1], writableReplicas[2:]...)
				continue
			}

			modified, err := vcd.syncTwoReplicas(a, b, true)
			if err != nil {
				vcd.write("failed to sync volumes %d on %s and %s: %v", a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, err)
			} else {
				if modified {
					vcd.write("synced %s and %s for volume %d", a.location.dataNode.Id, b.location.dataNode.Id, a.info.Id)
				}
			}

			// always choose the larger volume to be the source
			if a.info.FileCount > b.info.FileCount {
				writableReplicas = append(writableReplicas[:1], writableReplicas[2:]...)
			} else {
				writableReplicas = writableReplicas[1:]
			}
		}
	}

	return nil
}

// makeVolumeWritable flags a volume as writable, by volume ID.
func (vcd *volumeCheckDisk) makeVolumeWritable(vid uint32, vr *VolumeReplica) error {
	if !vcd.applyChanges {
		return nil
	}

	err := operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(vr.location.dataNode), vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, vsErr := volumeServerClient.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{
			VolumeId: vid,
		})
		return vsErr
	})
	if err != nil {
		return err
	}

	vcd.write("volume %d on %s is now writable", vid, vr.location.dataNode.Id)
	return nil
}

// makeVolumeReadOnly flags a volume as read-only, by volume ID.
func (vcd *volumeCheckDisk) makeVolumeReadonly(vid uint32, vr *VolumeReplica) error {
	if !vcd.applyChanges {
		return nil
	}

	err := operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(vr.location.dataNode), vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, vsErr := volumeServerClient.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
			VolumeId: vid,
		})
		return vsErr
	})
	if err != nil {
		return err
	}

	vcd.write("volume %d on %s is now read-only", vid, vr.location.dataNode.Id)
	return nil
}

func (vcd *volumeCheckDisk) checkReadOnlyVolumes(volumeReplicas map[uint32][]*VolumeReplica) {
	if !vcd.fixReadOnly {
		return
	}
	vcd.write("Pass #2 (read-only volumes)")

	for vid, replicas := range volumeReplicas {
		roReplicas := []*VolumeReplica{}
		rwReplicas := []*VolumeReplica{}

		for _, r := range replicas {
			if r.info.ReadOnly {
				roReplicas = append(roReplicas, r)
			} else {
				rwReplicas = append(rwReplicas, r)
			}
		}
		if len(roReplicas) == 0 {
			vcd.write("no read-only replicas for volume %d", vid)
			continue
		}
		if len(rwReplicas) == 0 {
			vcd.write("got %d read-only replicas for volume %d and no writable replicas to fix from", len(roReplicas), vid)
			continue
		}

		// attempt to fix read-only replicas from known good sources
		for _, r := range roReplicas {
			// select a random writable source replica. we assume these are identical by this point, after the checkWritableVolumes() pass.
			source := rwReplicas[rand.IntN(len(rwReplicas))]

			skip, err := vcd.shouldSkipVolume(r, source)
			if err != nil {
				vcd.ewg.AddErrorf("failed to check if volume %d should be skipped: %v\n", r.info.Id, err)
				continue
			}
			if skip {
				continue
			}

			vcd.ewg.Add(func() error {
				// make volume writable...
				if err := vcd.makeVolumeWritable(vid, r); err != nil {
					return err
				}

				// ...try to fix it...
				// TODO: test whether syncTwoReplicas() is enough to prune garbage entries on broken volumes...
				modified, err := vcd.syncTwoReplicas(source, r, false)
				if err != nil {
					vcd.write("sync read-only volume %d on %s from %s: %v", vid, r.location.dataNode.Id, source.location.dataNode.Id, err)

					if roErr := vcd.makeVolumeReadonly(vid, r); roErr != nil {
						return fmt.Errorf("failed to revert volume %d on %s to readonly after: %v: %v", vid, r.location.dataNode.Id, err, roErr)
					}
					return err
				} else {
					if modified {
						vcd.write("volume %d on %s is now synced to %d and writable", vid, r.location.dataNode.Id, source.location.dataNode.Id)
					} else {
						// ...or restore back to read-only, if no changes were made.
						if err := vcd.makeVolumeReadonly(vid, r); err != nil {
							return fmt.Errorf("failed to revert volume %d on %s to readonly: %v", vid, r.location.dataNode.Id, err)
						}
					}
				}

				return nil
			})
		}
	}
}

func (vcd *volumeCheckDisk) grpcDialOption() grpc.DialOption {
	return vcd.commandEnv.option.GrpcDialOption
}

func (vcd *volumeCheckDisk) write(format string, a ...any) {
	vcd.writerMu.Lock()
	defer vcd.writerMu.Unlock()
	fmt.Fprintf(vcd.writer, strings.TrimRight(format, "\r\n "), a...)
	fmt.Fprint(vcd.writer, "\n")
}

func (vcd *volumeCheckDisk) writeVerbose(format string, a ...any) {
	if vcd.verbose {
		vcd.write(format, a...)
	}
}

// compactionRevision reads the live compaction revision of a volume replica.
// Zero means the volume has never been vacuumed.
func (vcd *volumeCheckDisk) compactionRevision(replica *VolumeReplica) (revision uint32, err error) {
	err = operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(replica.location.dataNode), vcd.grpcDialOption(), func(client volume_server_pb.VolumeServerClient) error {
		resp, reqErr := client.ReadVolumeFileStatus(context.Background(), &volume_server_pb.ReadVolumeFileStatusRequest{
			VolumeId: replica.info.Id,
		})
		if resp != nil {
			revision = resp.CompactionRevision
		}
		return reqErr
	})
	return
}

// getVolumeStatusFileCount retrieves the current file count and deleted file count
// from a volume server via gRPC.
func (vcd *volumeCheckDisk) getVolumeStatusFileCount(vid uint32, dn *master_pb.DataNodeInfo) (totalFileCount, deletedFileCount uint64, err error) {
	err = operation.WithVolumeServerClient(false, pb.NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort)), vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, reqErr := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: uint32(vid),
		})
		if resp != nil {
			totalFileCount = resp.FileCount
			deletedFileCount = resp.FileDeletedCount
		}
		return reqErr
	})
	return totalFileCount, deletedFileCount, err
}

// eqVolumeFileCount compares the real-time file counts of two volume replicas
// by making sequential gRPC calls to their volume servers.
//
// Returns:
//   - bool: true if file counts match
//   - bool: true if deleted file counts match
//   - error: any error from volume server communication
//
// Error Handling: Errors from getVolumeStatusFileCount are wrapped with context
// (volume ID and server) and propagated up. Uses fmt.Errorf with %w to maintain
// error chain for errors.Is() and errors.As().
func (vcd *volumeCheckDisk) eqVolumeFileCount(a, b *VolumeReplica) (bool, bool, error) {
	fileCountA, fileDeletedCountA, errA := vcd.getVolumeStatusFileCount(a.info.Id, a.location.dataNode)
	if errA != nil {
		return false, false, fmt.Errorf("getting volume %d status from %s: %w", a.info.Id, a.location.dataNode.Id, errA)
	}

	fileCountB, fileDeletedCountB, errB := vcd.getVolumeStatusFileCount(b.info.Id, b.location.dataNode)
	if errB != nil {
		return false, false, fmt.Errorf("getting volume %d status from %s: %w", b.info.Id, b.location.dataNode.Id, errB)
	}

	return fileCountA == fileCountB, fileDeletedCountA == fileDeletedCountB, nil
}

// shouldSkipVolume determines whether two volume replicas should skip synchronization.
//
// Logic:
//  1. If file counts and delete counts match (when syncDeletions enabled), skip sync
//  2. If counts differ AND both volumes were modified recently (>= pulseTimeAtSecond),
//     they may still be actively receiving writes, so we return true to skip sync and
//     avoid false positives
//  3. If counts differ AND at least one volume was modified before the pulse cutoff,
//     call eqVolumeFileCount to get real-time counts from volume servers
//
// Returns:
//   - bool: true if sync should be skipped
//   - error: any error from volume server communication (when eqVolumeFileCount is called)
//
// Error Handling: Errors from eqVolumeFileCount are wrapped with context and propagated.
// The Do method logs these errors and continues processing to ensure other volumes are checked.
func (vcd *volumeCheckDisk) shouldSkipVolume(a, b *VolumeReplica) (bool, error) {
	if vcd.slowMode {
		// never skip volumes on slow mode
		return false, nil
	}

	pulseTimeAtSecond := vcd.now.Add(-constants.VolumePulsePeriod * 2).Unix()
	doSyncDeletedCount := false
	if vcd.syncDeletions && a.info.DeleteCount != b.info.DeleteCount {
		doSyncDeletedCount = true
	}
	if (a.info.FileCount != b.info.FileCount) || doSyncDeletedCount {
		// Do synchronization of volumes, if the modification time was before the last pulsation time
		if a.info.ModifiedAtSecond < pulseTimeAtSecond || b.info.ModifiedAtSecond < pulseTimeAtSecond {
			return false, nil
		}
		eqFileCount, eqDeletedFileCount, err := vcd.eqVolumeFileCount(a, b)
		if err != nil {
			return false, fmt.Errorf("comparing volume %d file counts on %s and %s: %w",
				a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, err)
		}
		if eqFileCount {
			if doSyncDeletedCount && !eqDeletedFileCount {
				return false, nil
			}
			vcd.writeVerbose("skipping active volumes %d with the same file counts on %s and %s",
				a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id)
		} else {
			return false, nil
		}
	}
	return true, nil
}

// syncTwoReplicas attempts to sync all entries from a source volume replica into a target. If bi-directional mode
// is enabled, changes from target are also synced back into the source.
// Returns true if source and/or target were modified, false otherwise.
func (vcd *volumeCheckDisk) syncTwoReplicas(source, target *VolumeReplica, bidi bool) (modified bool, err error) {
	sourceHasChanges, targetHasChanges := true, true
	const maxIterations = 5
	iteration := 0

	modified = false

	for (sourceHasChanges || targetHasChanges) && iteration < maxIterations {
		iteration++
		vcd.writeVerbose("sync iteration %d/%d for volume %d", iteration, maxIterations, source.info.Id)

		prevSourceHasChanges, prevTargetHasChanges := sourceHasChanges, targetHasChanges
		if sourceHasChanges, targetHasChanges, err = vcd.checkBoth(source, target, bidi); err != nil {
			return modified, err
		}
		modified = modified || sourceHasChanges || targetHasChanges

		// Detect if we're stuck in a loop with no progress
		if iteration > 1 && prevSourceHasChanges == sourceHasChanges && prevTargetHasChanges == targetHasChanges && (sourceHasChanges || targetHasChanges) {
			vcd.write("volume %d sync is not making progress between %s and %s after iteration %d, stopping to prevent infinite loop",
				source.info.Id, source.location.dataNode.Id, target.location.dataNode.Id, iteration)
			return modified, fmt.Errorf("sync not making progress after %d iterations", iteration)
		}
	}

	if iteration >= maxIterations && (sourceHasChanges || targetHasChanges) {
		vcd.write("volume %d sync reached maximum iterations (%d) between %s and %s, may need manual intervention",
			source.info.Id, maxIterations, source.location.dataNode.Id, target.location.dataNode.Id)
		return modified, fmt.Errorf("reached maximum sync iterations (%d)", maxIterations)
	}

	return modified, nil
}

// checkBoth performs a sync between source and target volume replicas. If bi-directional mode is enabled, changes from target are also synced back into the source.
// Returns whether the source and/or target were modified.
func (vcd *volumeCheckDisk) checkBoth(source, target *VolumeReplica, bidi bool) (sourceHasChanges bool, targetHasChanges bool, err error) {
	sourceDB, targetDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	if sourceDB == nil || targetDB == nil {
		return false, false, fmt.Errorf("failed to allocate in-memory needle DBs")
	}
	defer func() {
		sourceDB.Close()
		targetDB.Close()
	}()

	// read index db
	if err = vcd.readIndexDatabase(sourceDB, source.info.Collection, source.info.Id, pb.NewServerAddressFromDataNode(source.location.dataNode)); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %w", source.location.dataNode.Id, source.info.Id, err)
	}
	if err := vcd.readIndexDatabase(targetDB, target.info.Collection, target.info.Id, pb.NewServerAddressFromDataNode(target.location.dataNode)); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %w", target.location.dataNode.Id, target.info.Id, err)
	}

	// Resurrection is gated per direction on the receiving replica's compaction
	// revision: only a never-vacuumed index (revision 0) proves an absent needle
	// is a missing write rather than a vacuumed delete. The revision is read
	// after the index snapshot above, so the proof covers the snapshot.
	resurrectIntoTarget, resurrectIntoSource := false, false
	var targetRevision, sourceRevision uint32
	if vcd.resurrectMissingNeedles {
		if targetRevision, err = vcd.compactionRevision(target); err != nil {
			return true, true, fmt.Errorf("compactionRevision %s volume %d: %w", target.location.dataNode.Id, target.info.Id, err)
		}
		resurrectIntoTarget = targetRevision == 0
		if bidi {
			if sourceRevision, err = vcd.compactionRevision(source); err != nil {
				return true, true, fmt.Errorf("compactionRevision %s volume %d: %w", source.location.dataNode.Id, source.info.Id, err)
			}
			resurrectIntoSource = sourceRevision == 0
		}
	}

	// find and make up the differences
	var errs []error
	targetHasChanges, errTarget := vcd.doVolumeCheckDisk(sourceDB, targetDB, source, target, resurrectIntoTarget, targetRevision)
	if errTarget != nil {
		errs = append(errs,
			fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %w",
				source.location.dataNode.Id, target.location.dataNode.Id, source.info.Id, errTarget))
	}
	sourceHasChanges = false
	if bidi {
		var errSource error
		sourceHasChanges, errSource = vcd.doVolumeCheckDisk(targetDB, sourceDB, target, source, resurrectIntoSource, sourceRevision)
		if errSource != nil {
			errs = append(errs,
				fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %w",
					target.location.dataNode.Id, source.location.dataNode.Id, target.info.Id, errSource))
		}
	}
	if len(errs) > 0 {
		return sourceHasChanges, targetHasChanges, errors.Join(errs...)
	}

	// When nothing was repaired — typically because resurrection is gated off
	// since both replicas have been vacuumed (compaction revision > 0), which is
	// the normal state of any production cluster — doVolumeCheckDisk only logged
	// "cannot prove they are missing writes vs vacuumed deletes" and stopped.
	// That dead-end leaves a diverged replica with no actionable path, so classify
	// the divergence and print the exact repair. This is report-only: it changes
	// no data and does not bypass the resurrection safety gate.
	if !targetHasChanges && !sourceHasChanges {
		sourceOnly, targetOnly := vcd.liveDivergence(sourceDB, targetDB)
		if sourceOnly > 0 || targetOnly > 0 {
			vcd.reportDivergenceVerdict(source, target, sourceOnly, targetOnly,
				sourceRevision, targetRevision,
				vcd.resurrectMissingNeedles && bidi, vcd.resurrectMissingNeedles, vcd.resurrectMissingNeedles)
		}
	}

	return sourceHasChanges, targetHasChanges, nil
}

// liveDivergence counts live (non-deleted) needles present on a's index but
// entirely absent from b's, and the reverse. It is used to classify a diverged
// replica pair: one-sided (the lagging replica holds no unique live data, so a
// re-copy of the complete side converges it — subject to the deletion caveat
// reportDivergenceVerdict states, since the absent needles may be valid
// deletions on a vacuumed replica) versus two-sided (true split-brain — do
// not auto-repair). Tombstones are excluded, so vacuum asymmetry (a compacted
// replica that has dropped deleted entries) does not create a false
// difference.
func (vcd *volumeCheckDisk) liveDivergence(a, b *needle_map.MemDb) (aOnly, bOnly int) {
	a.DescendingVisit(func(v needle_map.NeedleValue) error {
		if v.Size.IsDeleted() {
			return nil
		}
		if _, found := b.Get(v.Key); !found {
			aOnly++
		}
		return nil
	})
	b.DescendingVisit(func(v needle_map.NeedleValue) error {
		if v.Size.IsDeleted() {
			return nil
		}
		if _, found := a.Get(v.Key); !found {
			bOnly++
		}
		return nil
	})
	return
}

// reportDivergenceVerdict prints the actionable outcome for a diverged replica
// pair that check.disk could not repair in place. revSrc/revTgt are the
// replicas' compaction revisions (0 = never vacuumed; >0 = at least one
// vacuum dropped deleted entries from the index); srcRevKnown/tgtRevKnown
// report whether each revision was actually read (target is read even in
// unidirectional mode; source only under -bidirectional). resurrectFlag is
// the command line's -resurrectMissingNeedles.
//
// The verdict is tombstone-aware: when the lagging replica has been vacuumed,
// a needle that is live on the complete side but absent on the lagging side is
// ambiguous — either a missing write, or a valid deletion whose tombstone the
// lagging side's vacuum already dropped. A whole-volume re-copy converges the
// divergence either way, but in the latter case it resurrects deleted data, so
// the volume.copy command is only emitted when the lagging side is proven
// never-vacuumed; otherwise the verdict points at a non-destructive
// needle-level repair instead.
func (vcd *volumeCheckDisk) reportDivergenceVerdict(source, target *VolumeReplica, sourceOnly, targetOnly int, revSrc, revTgt uint32, srcRevKnown, tgtRevKnown, resurrectFlag bool) {
	// Raw string form, NOT String(): ServerAddress.String() drops the custom
	// gRPC port suffix, and volume.copy's dialer accepts the host:port.grpcPort
	// form (see checkDialable in weed/operation/volume_move).
	srcAddr := string(pb.NewServerAddressFromDataNode(source.location.dataNode))
	tgtAddr := string(pb.NewServerAddressFromDataNode(target.location.dataNode))
	vid := source.info.Id
	switch {
	case sourceOnly > 0 && targetOnly == 0:
		// target is lagging: it holds no unique live data.
		safe := deletionCaveat(resurrectFlag, tgtRevKnown, revTgt == 0)
		if safe {
			vcd.write("volume %d: ONE-SIDED divergence — %s is missing %d live needle(s) that exist on %s; %s holds no unique live data. Safe repair (whole-volume re-copy, complete -> lagging; verify-before-destroy is enforced): volume.copy -source %s -target %s -volumeId %d",
				vid, tgtAddr, sourceOnly, srcAddr, tgtAddr, srcAddr, tgtAddr, vid)
		} else {
			vcd.write("volume %d: ONE-SIDED divergence — %s is missing %d live needle(s) that exist on %s; %s holds no unique live data. Do NOT re-copy the whole volume: the absent needles may be valid deletions already vacuumed away on the lagging side, which a re-copy would resurrect. Restore only the confirmed-missing needles (volume.fsck -collection <c> -volumeId %d -findMissingChunksInFiler, then needle-level repair), or re-copy only after accepting that risk.",
				vid, tgtAddr, sourceOnly, srcAddr, tgtAddr, vid)
		}
	case targetOnly > 0 && sourceOnly == 0:
		// source is lagging: it holds no unique live data.
		safe := deletionCaveat(resurrectFlag, srcRevKnown, revSrc == 0)
		if safe {
			vcd.write("volume %d: ONE-SIDED divergence — %s is missing %d live needle(s) that exist on %s; %s holds no unique live data. Safe repair (whole-volume re-copy, complete -> lagging; verify-before-destroy is enforced): volume.copy -source %s -target %s -volumeId %d",
				vid, srcAddr, targetOnly, tgtAddr, srcAddr, tgtAddr, srcAddr, vid)
		} else {
			vcd.write("volume %d: ONE-SIDED divergence — %s is missing %d live needle(s) that exist on %s; %s holds no unique live data. Do NOT re-copy the whole volume: the absent needles may be valid deletions already vacuumed away on the lagging side, which a re-copy would resurrect. Restore only the confirmed-missing needles (volume.fsck -collection <c> -volumeId %d -findMissingChunksInFiler, then needle-level repair), or re-copy only after accepting that risk.",
				vid, srcAddr, targetOnly, tgtAddr, srcAddr, vid)
		}
	case sourceOnly > 0 && targetOnly > 0:
		if resurrectFlag && srcRevKnown && tgtRevKnown && revSrc == 0 && revTgt == 0 {
			// Both replicas proven never-vacuumed: the mutually missing
			// needles are provably missing writes on both sides (the same
			// proof the resurrection gate uses), not split-brain. The two
			// doVolumeCheckDisk passes above already queued them for
			// in-place resurrection but this was a simulation run, so
			// nothing was applied.
			vcd.write("volume %d: TWO-SIDED divergence, both replicas never vacuumed — %s is missing %d live needle(s) that exist on %s AND %s is missing %d that exist on %s. These are mutually missed writes, not split-brain: re-run this command with -apply to resurrect them in place (both directions).",
				vid, tgtAddr, sourceOnly, srcAddr, srcAddr, targetOnly, tgtAddr)
		} else {
			vcd.write("volume %d: TWO-SIDED (split-brain) divergence — %s has %d unique live needle(s) AND %s has %d. Do NOT auto-repair: each side may hold data the other lacks. Confirm orphans with volume.fsck -collection <c> -volumeId %d -findMissingChunksInFiler before re-copying the complete replica, or restore the missing needles manually.",
				vid, srcAddr, sourceOnly, tgtAddr, targetOnly, vid)
		}
	}
}

// deletionCaveat reports whether a one-sided divergence's complete -> lagging
// re-copy is safe: the lagging side is proven never-vacuumed (compaction
// revision 0) and that proof was actually taken under the
// -resurrectMissingNeedles flag, so its absent live needles are missing
// writes by the same proof the resurrection gate uses — not vacuumed
// deletions.
func deletionCaveat(resurrectFlag, laggingRevKnown, laggingNeverVacuumed bool) bool {
	return resurrectFlag && laggingRevKnown && laggingNeverVacuumed
}

func (vcd *volumeCheckDisk) doVolumeCheckDisk(minuend, subtrahend *needle_map.MemDb, source, target *VolumeReplica, resurrectAbsent bool, targetRevision uint32) (hasChanges bool, err error) {

	// find missing keys
	// hash join, can be more efficient
	var missingNeedles []needle_map.NeedleValue
	var partiallyDeletedNeedles []needle_map.NeedleValue
	var skippedAbsentNeedles int
	var counter int

	minuend.DescendingVisit(func(minuendValue needle_map.NeedleValue) error {
		counter++
		if subtrahendValue, found := subtrahend.Get(minuendValue.Key); !found {
			if minuendValue.Size.IsDeleted() {
				return nil
			}
			// A key present-and-live on the source but entirely absent on the
			// target is ambiguous: either a genuine missing write, or a needle
			// that was deleted on the target and then vacuumed away (its index
			// entry, including any tombstone, is gone after vacuum). An
			// individual needle's AppendAtNs has no monotonic relation to a
			// vacuum watermark, so it cannot distinguish the two. Without
			// positive proof the absence is a missing write (rather than a
			// vacuumed delete), the safe default is to NOT resurrect: a real
			// missing write may go unrepaired, but we never raise back data
			// the operator deleted. resurrectAbsent carries that proof: the
			// caller sets it only when the target was never vacuumed.
			if !resurrectAbsent {
				skippedAbsentNeedles++
				return nil
			}
			missingNeedles = append(missingNeedles, minuendValue)
		} else {
			if minuendValue.Size.IsDeleted() && !subtrahendValue.Size.IsDeleted() {
				partiallyDeletedNeedles = append(partiallyDeletedNeedles, minuendValue)
			}
		}
		return nil
	})

	if skippedAbsentNeedles > 0 {
		if vcd.resurrectMissingNeedles {
			vcd.write("volume %d %s: not resurrecting %d needle(s) absent on %s (compaction revision %d: a vacuum may have erased deleted needles there)",
				source.info.Id, source.location.dataNode.Id, skippedAbsentNeedles, target.location.dataNode.Id, targetRevision)
		} else {
			vcd.write("volume %d %s: not resurrecting %d needle(s) absent on %s (cannot prove they are missing writes vs vacuumed deletes)",
				source.info.Id, source.location.dataNode.Id, skippedAbsentNeedles, target.location.dataNode.Id)
		}
	}

	vcd.write("volume %d %s has %d entries, %s missed %d and partially deleted %d entries",
		source.info.Id, source.location.dataNode.Id, counter, target.location.dataNode.Id, len(missingNeedles), len(partiallyDeletedNeedles))

	if counter == 0 || (len(missingNeedles) == 0 && len(partiallyDeletedNeedles) == 0) {
		return false, nil
	}

	missingNeedlesFraction := float64(len(missingNeedles)) / float64(counter)
	if missingNeedlesFraction > vcd.nonRepairThreshold {
		return false, fmt.Errorf(
			"failed to start repair volume %d, percentage of missing keys is greater than the threshold: %.2f > %.2f",
			source.info.Id, missingNeedlesFraction, vcd.nonRepairThreshold)
	}

	for _, needleValue := range missingNeedles {
		needleBlob, err := vcd.readSourceNeedleBlob(pb.NewServerAddressFromDataNode(source.location.dataNode), source.info.Id, needleValue)
		if err != nil {
			return hasChanges, err
		}

		if !vcd.applyChanges {
			continue
		}

		vcd.writeVerbose("read %s %s => %s", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
		hasChanges = true

		if err = vcd.writeNeedleBlobToTarget(pb.NewServerAddressFromDataNode(target.location.dataNode), source.info.Id, needleValue, needleBlob); err != nil {
			return hasChanges, err
		}

	}

	if vcd.syncDeletions && vcd.applyChanges && len(partiallyDeletedNeedles) > 0 {
		var fidList []string
		for _, needleValue := range partiallyDeletedNeedles {
			fidList = append(fidList, needleValue.Key.FileId(source.info.Id))
			vcd.writeVerbose("delete %s %s => %s", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
		}
		deleteResults := operation.DeleteFileIdsAtOneVolumeServer(
			pb.NewServerAddressFromDataNode(target.location.dataNode),
			vcd.grpcDialOption(), fidList, false)

		// Check for errors in results
		for _, deleteResult := range deleteResults {
			if deleteResult.Error != "" && deleteResult.Error != "not found" {
				return hasChanges, fmt.Errorf("delete file %s: %v", deleteResult.FileId, deleteResult.Error)
			}
			if deleteResult.Status == http.StatusAccepted && deleteResult.Size > 0 {
				hasChanges = true
			}
		}
	}
	return hasChanges, nil
}

func (vcd *volumeCheckDisk) readSourceNeedleBlob(sourceVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (needleBlob []byte, err error) {

	err = operation.WithVolumeServerClient(false, sourceVolumeServer, vcd.grpcDialOption(), func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.ReadNeedleBlob(context.Background(), &volume_server_pb.ReadNeedleBlobRequest{
			VolumeId: volumeId,
			Offset:   needleValue.Offset.ToActualOffset(),
			Size:     int32(needleValue.Size),
		})
		if err != nil {
			return err
		}
		needleBlob = resp.NeedleBlob
		return nil
	})
	return
}

func (vcd *volumeCheckDisk) writeNeedleBlobToTarget(targetVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue, needleBlob []byte) error {

	return operation.WithVolumeServerClient(false, targetVolumeServer, vcd.grpcDialOption(), func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.WriteNeedleBlob(context.Background(), &volume_server_pb.WriteNeedleBlobRequest{
			VolumeId:   volumeId,
			NeedleId:   uint64(needleValue.Key),
			Size:       int32(needleValue.Size),
			NeedleBlob: needleBlob,
		})
		return err
	})
}

func (vcd *volumeCheckDisk) readIndexDatabase(db *needle_map.MemDb, collection string, volumeId uint32, volumeServer pb.ServerAddress) error {
	var buf bytes.Buffer
	if err := vcd.copyVolumeIndexFile(collection, volumeId, volumeServer, &buf); err != nil {
		return err
	}

	vcd.writeVerbose("load collection %s volume %d index size %d from %s ...", collection, volumeId, buf.Len(), volumeServer)
	return db.LoadFilterFromReaderAt(bytes.NewReader(buf.Bytes()), true, false)
}

func (vcd *volumeCheckDisk) copyVolumeIndexFile(collection string, volumeId uint32, volumeServer pb.ServerAddress, buf *bytes.Buffer) error {

	return operation.WithVolumeServerClient(true, volumeServer, vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		ext := ".idx"

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ext,
			CompactionRevision:       math.MaxUint32,
			StopOffset:               math.MaxInt64,
			Collection:               collection,
			IsEcVolume:               false,
			IgnoreSourceFileNotFound: false,
		})
		if err != nil {
			return fmt.Errorf("failed to start copying volume %d%s: %v", volumeId, ext, err)
		}

		err = vcd.writeToBuffer(copyFileClient, buf)
		if err != nil {
			return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, volumeServer, err)
		}

		return nil

	})
}

func (vcd *volumeCheckDisk) writeToBuffer(client volume_server_pb.VolumeServer_CopyFileClient, buf *bytes.Buffer) error {
	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving: %w", receiveErr)
		}
		buf.Write(resp.FileContent)
	}
	return nil
}
