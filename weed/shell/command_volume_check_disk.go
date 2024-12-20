package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"google.golang.org/grpc"
	"slices"
)

func init() {
	Commands = append(Commands, &commandVolumeCheckDisk{})
}

type commandVolumeCheckDisk struct {
	env    *CommandEnv
	writer io.Writer
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

func (c *commandVolumeCheckDisk) getVolumeStatusFileCount(vid uint32, dn *master_pb.DataNodeInfo) (totalFileCount, deletedFileCount uint64) {
	err := operation.WithVolumeServerClient(false, pb.NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort)), c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
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
		fmt.Fprintf(c.writer, "getting number of files for volume id %d from volumes status: %+v\n", vid, err)
	}
	return totalFileCount, deletedFileCount
}

func (c *commandVolumeCheckDisk) eqVolumeFileCount(a, b *VolumeReplica) (bool, bool) {
	var waitGroup sync.WaitGroup
	var fileCountA, fileCountB, fileDeletedCountA, fileDeletedCountB uint64
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		fileCountA, fileDeletedCountA = c.getVolumeStatusFileCount(a.info.Id, a.location.dataNode)
	}()
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		fileCountB, fileDeletedCountB = c.getVolumeStatusFileCount(b.info.Id, b.location.dataNode)
	}()
	// Trying to synchronize a remote call to two nodes
	waitGroup.Wait()
	return fileCountA == fileCountB, fileDeletedCountA == fileDeletedCountB
}

func (c *commandVolumeCheckDisk) shouldSkipVolume(a, b *VolumeReplica, pulseTimeAtSecond int64, syncDeletions, verbose bool) bool {
	doSyncDeletedCount := false
	if syncDeletions && a.info.DeleteCount != b.info.DeleteCount {
		doSyncDeletedCount = true
	}
	if (a.info.FileCount != b.info.FileCount) || doSyncDeletedCount {
		// Do synchronization of volumes, if the modification time was before the last pulsation time
		if a.info.ModifiedAtSecond < pulseTimeAtSecond || b.info.ModifiedAtSecond < pulseTimeAtSecond {
			return false
		}
		if eqFileCount, eqDeletedFileCount := c.eqVolumeFileCount(a, b); eqFileCount {
			if doSyncDeletedCount && !eqDeletedFileCount {
				return false
			}
			if verbose {
				fmt.Fprintf(c.writer, "skipping active volumes %d with the same file counts on %s and %s\n",
					a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id)
			}
		} else {
			return false
		}
	}
	return true
}

func (c *commandVolumeCheckDisk) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fsckCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	slowMode := fsckCommand.Bool("slow", false, "slow mode checks all replicas even file counts are the same")
	verbose := fsckCommand.Bool("v", false, "verbose mode")
	volumeId := fsckCommand.Uint("volumeId", 0, "the volume id")
	applyChanges := fsckCommand.Bool("force", false, "apply the fix")
	syncDeletions := fsckCommand.Bool("syncDeleted", false, "sync of deletions the fix")
	nonRepairThreshold := fsckCommand.Float64("nonRepairThreshold", 0.3, "repair when missing keys is not more than this limit")
	if err = fsckCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyChanges, "-force")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	c.env = commandEnv
	c.writer = writer

	// collect topology information
	pulseTimeAtSecond := time.Now().Unix() - constants.VolumePulseSeconds*2
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
				fmt.Fprintf(writer, "skipping readonly volume %d on %s\n", replica.info.Id, replica.location.dataNode.Id)
			} else {
				writableReplicas = append(writableReplicas, replica)
			}
		}

		slices.SortFunc(writableReplicas, func(a, b *VolumeReplica) int {
			return int(b.info.FileCount - a.info.FileCount)
		})
		for len(writableReplicas) >= 2 {
			a, b := writableReplicas[0], writableReplicas[1]
			if !*slowMode && c.shouldSkipVolume(a, b, pulseTimeAtSecond, *syncDeletions, *verbose) {
				// always choose the larger volume to be the source
				writableReplicas = append(replicas[:1], writableReplicas[2:]...)
				continue
			}
			if err := c.syncTwoReplicas(a, b, *applyChanges, *syncDeletions, *nonRepairThreshold, *verbose); err != nil {
				fmt.Fprintf(writer, "sync volume %d on %s and %s: %v\n", a.info.Id, a.location.dataNode.Id, b.location.dataNode.Id, err)
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

func (c *commandVolumeCheckDisk) syncTwoReplicas(a *VolumeReplica, b *VolumeReplica, applyChanges bool, doSyncDeletions bool, nonRepairThreshold float64, verbose bool) (err error) {
	aHasChanges, bHasChanges := true, true
	for aHasChanges || bHasChanges {
		if aHasChanges, bHasChanges, err = c.checkBoth(a, b, applyChanges, doSyncDeletions, nonRepairThreshold, verbose); err != nil {
			return err
		}
	}
	return nil
}

func (c *commandVolumeCheckDisk) checkBoth(a *VolumeReplica, b *VolumeReplica, applyChanges bool, doSyncDeletions bool, nonRepairThreshold float64, verbose bool) (aHasChanges bool, bHasChanges bool, err error) {
	aDB, bDB := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer func() {
		aDB.Close()
		bDB.Close()
	}()

	// read index db
	readIndexDbCutoffFrom := uint64(time.Now().UnixNano())
	if err = readIndexDatabase(aDB, a.info.Collection, a.info.Id, pb.NewServerAddressFromDataNode(a.location.dataNode), verbose, c.writer, c.env.option.GrpcDialOption); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", a.location.dataNode, a.info.Id, err)
	}
	if err := readIndexDatabase(bDB, b.info.Collection, b.info.Id, pb.NewServerAddressFromDataNode(b.location.dataNode), verbose, c.writer, c.env.option.GrpcDialOption); err != nil {
		return true, true, fmt.Errorf("readIndexDatabase %s volume %d: %v", b.location.dataNode, b.info.Id, err)
	}

	// find and make up the differences
	aHasChanges, err1 := doVolumeCheckDisk(bDB, aDB, b, a, verbose, c.writer, applyChanges, doSyncDeletions, nonRepairThreshold, readIndexDbCutoffFrom, c.env.option.GrpcDialOption)
	bHasChanges, err2 := doVolumeCheckDisk(aDB, bDB, a, b, verbose, c.writer, applyChanges, doSyncDeletions, nonRepairThreshold, readIndexDbCutoffFrom, c.env.option.GrpcDialOption)
	if err1 != nil {
		return aHasChanges, bHasChanges, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", b.location.dataNode.Id, a.location.dataNode.Id, b.info.Id, err1)
	}
	if err2 != nil {
		return aHasChanges, bHasChanges, fmt.Errorf("doVolumeCheckDisk source:%s target:%s volume %d: %v", a.location.dataNode.Id, b.location.dataNode.Id, a.info.Id, err2)
	}
	return aHasChanges, bHasChanges, nil
}

func doVolumeCheckDisk(minuend, subtrahend *needle_map.MemDb, source, target *VolumeReplica, verbose bool, writer io.Writer, applyChanges bool, doSyncDeletions bool, nonRepairThreshold float64, cutoffFromAtNs uint64, grpcDialOption grpc.DialOption) (hasChanges bool, err error) {

	// find missing keys
	// hash join, can be more efficient
	var missingNeedles []needle_map.NeedleValue
	var partiallyDeletedNeedles []needle_map.NeedleValue
	var counter int
	doCutoffOfLastNeedle := true
	minuend.DescendingVisit(func(minuendValue needle_map.NeedleValue) error {
		counter++
		if subtrahendValue, found := subtrahend.Get(minuendValue.Key); !found {
			if minuendValue.Size.IsDeleted() {
				return nil
			}
			if doCutoffOfLastNeedle {
				if needleMeta, err := readNeedleMeta(grpcDialOption, pb.NewServerAddressFromDataNode(source.location.dataNode), source.info.Id, minuendValue); err == nil {
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

	fmt.Fprintf(writer, "volume %d %s has %d entries, %s missed %d and partially deleted %d entries\n",
		source.info.Id, source.location.dataNode.Id, counter, target.location.dataNode.Id, len(missingNeedles), len(partiallyDeletedNeedles))

	if counter == 0 || (len(missingNeedles) == 0 && len(partiallyDeletedNeedles) == 0) {
		return false, nil
	}

	missingNeedlesFraction := float64(len(missingNeedles)) / float64(counter)
	if missingNeedlesFraction > nonRepairThreshold {
		return false, fmt.Errorf(
			"failed to start repair volume %d, percentage of missing keys is greater than the threshold: %.2f > %.2f",
			source.info.Id, missingNeedlesFraction, nonRepairThreshold)
	}

	for _, needleValue := range missingNeedles {
		needleBlob, err := readSourceNeedleBlob(grpcDialOption, pb.NewServerAddressFromDataNode(source.location.dataNode), source.info.Id, needleValue)
		if err != nil {
			return hasChanges, err
		}

		if !applyChanges {
			continue
		}

		if verbose {
			fmt.Fprintf(writer, "read %s %s => %s\n", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
		}

		hasChanges = true

		if err = writeNeedleBlobToTarget(grpcDialOption, pb.NewServerAddressFromDataNode(target.location.dataNode), source.info.Id, needleValue, needleBlob); err != nil {
			return hasChanges, err
		}

	}

	if doSyncDeletions && applyChanges && len(partiallyDeletedNeedles) > 0 {
		var fidList []string
		for _, needleValue := range partiallyDeletedNeedles {
			fidList = append(fidList, needleValue.Key.FileId(source.info.Id))
			if verbose {
				fmt.Fprintf(writer, "delete %s %s => %s\n", needleValue.Key.FileId(source.info.Id), source.location.dataNode.Id, target.location.dataNode.Id)
			}
		}
		deleteResults, deleteErr := operation.DeleteFileIdsAtOneVolumeServer(
			pb.NewServerAddressFromDataNode(target.location.dataNode),
			grpcDialOption, fidList, false)
		if deleteErr != nil {
			return hasChanges, deleteErr
		}
		for _, deleteResult := range deleteResults {
			if deleteResult.Status == http.StatusAccepted && deleteResult.Size > 0 {
				hasChanges = true
				return
			}
		}
	}
	return
}

func readSourceNeedleBlob(grpcDialOption grpc.DialOption, sourceVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue) (needleBlob []byte, err error) {

	err = operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
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

func writeNeedleBlobToTarget(grpcDialOption grpc.DialOption, targetVolumeServer pb.ServerAddress, volumeId uint32, needleValue needle_map.NeedleValue, needleBlob []byte) error {

	return operation.WithVolumeServerClient(false, targetVolumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.WriteNeedleBlob(context.Background(), &volume_server_pb.WriteNeedleBlobRequest{
			VolumeId:   volumeId,
			NeedleId:   uint64(needleValue.Key),
			Size:       int32(needleValue.Size),
			NeedleBlob: needleBlob,
		})
		return err
	})

}

func readIndexDatabase(db *needle_map.MemDb, collection string, volumeId uint32, volumeServer pb.ServerAddress, verbose bool, writer io.Writer, grpcDialOption grpc.DialOption) error {

	var buf bytes.Buffer
	if err := copyVolumeIndexFile(collection, volumeId, volumeServer, &buf, verbose, writer, grpcDialOption); err != nil {
		return err
	}

	if verbose {
		fmt.Fprintf(writer, "load collection %s volume %d index size %d from %s ...\n", collection, volumeId, buf.Len(), volumeServer)
	}
	return db.LoadFilterFromReaderAt(bytes.NewReader(buf.Bytes()), true, false)
}

func copyVolumeIndexFile(collection string, volumeId uint32, volumeServer pb.ServerAddress, buf *bytes.Buffer, verbose bool, writer io.Writer, grpcDialOption grpc.DialOption) error {

	return operation.WithVolumeServerClient(true, volumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

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

		err = writeToBuffer(copyFileClient, buf)
		if err != nil {
			return fmt.Errorf("failed to copy %d%s from %s: %v", volumeId, ext, volumeServer, err)
		}

		return nil

	})
}

func writeToBuffer(client volume_server_pb.VolumeServer_CopyFileClient, buf *bytes.Buffer) error {
	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving: %v", receiveErr)
		}
		buf.Write(resp.FileContent)
	}
	return nil
}
