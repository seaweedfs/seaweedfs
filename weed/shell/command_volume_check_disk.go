package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
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
	now        time.Time

	slowMode           bool
	verbose            bool
	applyChanges       bool
	syncDeletions      bool
	nonRepairThreshold float64
}

func (c *commandVolumeCheckDisk) Name() string {
	return "volume.check.disk"
}

func (c *commandVolumeCheckDisk) Help() string {
	return `check all replicated volumes to find and fix inconsistencies. It is optional and resource intensive.

	How it works:
	
	find all volumes that are replicated
	  for each volume id, if there are more than 2 replicas, find one pair with the largest 2 in file count.
      for the pair volume A and B
        append entries in A and not in B to B
        append entries in B and not in A to A

`
}

func (c *commandVolumeCheckDisk) HasTag(tag CommandTag) bool {
	return tag == ResourceHeavy
}

func (c *commandVolumeCheckDisk) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	slowMode := fsckCommand.Bool("slow", false, "slow mode checks all replicas even file counts are the same")
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	volumeId := fsckCommand.Uint("volumeId", 0, "the volume id")
	applyChanges := fsckCommand.Bool("apply", false, "apply the fix")
	// TODO: remove this alias
	applyChangesAlias := fsckCommand.Bool("force", false, "apply the fix (alias for -apply)")
	syncDeletions := fsckCommand.Bool("syncDeleted", false, "sync of deletions the fix")
	nonRepairThreshold := fsckCommand.Float64("nonRepairThreshold", 0.3, "repair when missing keys is not more than this limit")
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
		nonRepairThreshold: *nonRepairThreshold,
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)

	// pick 1 pairs of volume replica
	for _, replicas := range volumeReplicas {
		if *volumeId > 0 && replicas[0].info.Id != uint32(*volumeId) {
			continue
		}
		// filter readonly replica
		var writableReplicas []*VolumeReplica
		for _, replica := range replicas {
			if replica.info.ReadOnly {
				vcd.write("skipping readonly volume %d on %s\n", replica.info.Id, replica.location.dataNode.Id)
			} else {
				writableReplicas = append(writableReplicas, replica)
			}
		}

		slices.SortFunc(writableReplicas, func(a, b *VolumeReplica) int {
			return int(b.info.FileCount - a.info.FileCount)
		})
		for len(writableReplicas) >= 2 {
			a, b := writableReplicas[0], writableReplicas[1]
			if !vcd.slowMode && vcd.shouldSkipVolume(a, b) {
				// always choose the larger volume to be the source
				writableReplicas = append(writableReplicas[:1], writableReplicas[2:]...)
				continue
			}
			if err := vcd.syncTwoReplicas(a, b); err != nil {
				vcd.write("sync volume %d on %s and %s: %v\n", a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, err)
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

func (vcd *volumeCheckDisk) isLocked() bool {
	return vcd.commandEnv.isLocked()
}

func (vcd *volumeCheckDisk) grpcDialOption() grpc.DialOption {
	return vcd.commandEnv.option.GrpcDialOption
}

func (vcd *volumeCheckDisk) write(format string, a ...any) {
	fmt.Fprintf(vcd.writer, format, a...)
}

func (vcd *volumeCheckDisk) writeVerbose(format string, a ...any) {
	if vcd.verbose {
		fmt.Fprintf(vcd.writer, format, a...)
	}
}

func (vcd *volumeCheckDisk) getVolumeStatusFileCount(vid uint32, dn *master_pb.DataNodeInfo) (totalFileCount, deletedFileCount uint64) {
	err := operation.WithVolumeServerClient(false, pb.NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort)), vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, reqErr := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: uint32(vid),
		})
		if resp != nil {
			totalFileCount = resp.FileCount
			deletedFileCount = resp.FileDeletedCount
		}
		return reqErr
	})
	if err != nil {
		vcd.write("getting number of files for volume id %d from volumes status: %+v\n", vid, err)
	}
	return totalFileCount, deletedFileCount
}

func (vcd *volumeCheckDisk) eqVolumeFileCount(a, b *VolumeReplica) (bool, bool) {
	var fileCountA, fileCountB, fileDeletedCountA, fileDeletedCountB uint64

	ewg := NewErrorWaitGroup(DefaultMaxParallelization)
	ewg.Add(func() error {
		fileCountA, fileDeletedCountA = vcd.getVolumeStatusFileCount(a.info.Id, a.location.dataNode)
		return nil
	})
	ewg.Add(func() error {
		fileCountB, fileDeletedCountB = vcd.getVolumeStatusFileCount(b.info.Id, b.location.dataNode)
		return nil
	})
	// Trying to synchronize a remote call to two nodes
	// TODO: bubble errors up?
	ewg.Wait()

	return fileCountA == fileCountB, fileDeletedCountA == fileDeletedCountB
}

func (vcd *volumeCheckDisk) shouldSkipVolume(a, b *VolumeReplica) bool {
	pulseTimeAtSecond := vcd.now.Add(-constants.VolumePulsePeriod * 2).Unix()
	doSyncDeletedCount := false
	if vcd.syncDeletions && a.info.DeleteCount != b.info.DeleteCount {
		doSyncDeletedCount = true
	}
	if (a.info.FileCount != b.info.FileCount) || doSyncDeletedCount {
		// Do synchronization of volumes, if the modification time was before the last pulsation time
		if a.info.ModifiedAtSecond < pulseTimeAtSecond || b.info.ModifiedAtSecond < pulseTimeAtSecond {
			return false
		}
		if eqFileCount, eqDeletedFileCount := vcd.eqVolumeFileCount(a, b); eqFileCount {
			if doSyncDeletedCount && !eqDeletedFileCount {
				return false
			}
			vcd.writeVerbose("skipping active volumes %d with the same file counts on %s and %s\n",
				a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id)
		} else {
			return false
		}
	}
	return true
}

func (vcd *volumeCheckDisk) syncTwoReplicas(a *VolumeReplica, b *VolumeReplica) (err error) {
	aHasChanges, bHasChanges := true, true
	const maxIterations = 5
	iteration := 0

	for (aHasChanges || bHasChanges) && iteration < maxIterations {
		iteration++
		vcd.writeVerbose("sync iteration %d for volume %d\n", iteration, a.info.Id)

		prevAHasChanges, prevBHasChanges := aHasChanges, bHasChanges
		if aHasChanges, bHasChanges, err = vcd.checkBoth(a, b); err != nil {
			return err
		}

		// Detect if we're stuck in a loop with no progress
		if iteration > 1 && prevAHasChanges == aHasChanges && prevBHasChanges == bHasChanges && (aHasChanges || bHasChanges) {
			vcd.write("volume %d sync is not making progress between %s and %s after iteration %d, stopping to prevent infinite loop\n",
				a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, iteration)
			return fmt.Errorf("sync not making progress after %d iterations", iteration)
		}
	}

	if iteration >= maxIterations && (aHasChanges || bHasChanges) {
		vcd.write("volume %d sync reached maximum iterations (%d) between %s and %s, may need manual intervention\n",
			a.info.Id, maxIterations, a.location.dataNode.Id, b.location.dataNode.Id)
		return fmt.Errorf("reached maximum sync iterations (%d)", maxIterations)
	}

	return nil
}

func (vcd *volumeCheckDisk) checkBoth(a *VolumeReplica, b *VolumeReplica) (aHasChanges bool, bHasChanges bool, err error) {
	aDB, bDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer func() {
		aDB.Close()
		bDB.Close()
	}()

	// read index db
	if err = vcd.readIndexDatabase(aDB, a.info.Collection, a.info.Id, pb.NewServerAddressFromDataNode(a.location.dataNode)); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", a.location.dataNode, a.info.Id, err)
	}
	if err := vcd.readIndexDatabase(bDB, b.info.Collection, b.info.Id, pb.NewServerAddressFromDataNode(b.location.dataNode)); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", b.location.dataNode, b.info.Id, err)
	}

	// find and make up the differences
	aHasChanges, err1 := vcd.doVolumeCheckDisk(bDB, aDB, b, a)
	bHasChanges, err2 := vcd.doVolumeCheckDisk(aDB, bDB, a, b)
	if err1 != nil {
		return aHasChanges, bHasChanges, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", b.location.dataNode.Id, a.location.dataNode.Id, b.info.Id, err1)
	}
	if err2 != nil {
		return aHasChanges, bHasChanges, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", a.location.dataNode.Id, b.location.dataNode.Id, a.info.Id, err2)
	}
	return aHasChanges, bHasChanges, nil
}

func (vcd *volumeCheckDisk) doVolumeCheckDisk(minuend, subtrahend *needle_map.MemDb, source, target *VolumeReplica) (hasChanges bool, err error) {

	// find missing keys
	// hash join, can be more efficient
	var missingNeedles []needle_map.NeedleValue
	var partiallyDeletedNeedles []needle_map.NeedleValue
	var counter int
	doCutoffOfLastNeedle := true
	cutoffFromAtNs := uint64(vcd.now.UnixNano())

	minuend.DescendingVisit(func(minuendValue needle_map.NeedleValue) error {
		counter++
		if subtrahendValue, found := subtrahend.Get(minuendValue.Key); !found {
			if minuendValue.Size.IsDeleted() {
				return nil
			}
			if doCutoffOfLastNeedle {
				if needleMeta, err := readNeedleMeta(vcd.grpcDialOption(), pb.NewServerAddressFromDataNode(source.location.dataNode), source.info.Id, minuendValue); err == nil {
					// needles older than the cutoff time are not missing yet
					if needleMeta.AppendAtNs > cutoffFromAtNs {
						return nil
					}
					doCutoffOfLastNeedle = false
				}
			}
			missingNeedles = append(missingNeedles, minuendValue)
		} else {
			if minuendValue.Size.IsDeleted() && !subtrahendValue.Size.IsDeleted() {
				partiallyDeletedNeedles = append(partiallyDeletedNeedles, minuendValue)
			}
			if doCutoffOfLastNeedle {
				doCutoffOfLastNeedle = false
			}
		}
		return nil
	})

	vcd.write("volume %d %s has %d entries, %s missed %d and partially deleted %d entries\n",
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

		vcd.writeVerbose("read %s %s => %s\n", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
		hasChanges = true

		if err = vcd.writeNeedleBlobToTarget(pb.NewServerAddressFromDataNode(target.location.dataNode), source.info.Id, needleValue, needleBlob); err != nil {
			return hasChanges, err
		}

	}

	if vcd.syncDeletions && vcd.applyChanges && len(partiallyDeletedNeedles) > 0 {
		var fidList []string
		for _, needleValue := range partiallyDeletedNeedles {
			fidList = append(fidList, needleValue.Key.FileId(source.info.Id))
			vcd.writeVerbose("delete %s %s => %s\n", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
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

	vcd.writeVerbose("load collection %s volume %d index size %d from %s ...\n", collection, volumeId, buf.Len(), volumeServer)
	return db.LoadFilterFromReaderAt(bytes.NewReader(buf.Bytes()), true, false)
}

func (vcd *volumeCheckDisk) copyVolumeIndexFile(collection string, volumeId uint32, volumeServer pb.ServerAddress, buf *bytes.Buffer) error {

	return operation.WithVolumeServerClient(true, volumeServer, vcd.grpcDialOption(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		ext := ".idx"

		copyFileClient, err := volumeServerClient.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Ext:                      ".idx",
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
