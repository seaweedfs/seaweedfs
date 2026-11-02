package shell

import (
	"context"
	"flag"
	"io"
	"log"
	"maps"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

func init() {
	Commands = append(Commands, &commandVolumeDeleteEmpty{})
}

type commandVolumeDeleteEmpty struct {
}

func (c *commandVolumeDeleteEmpty) Name() string {
	return "volume.deleteEmpty"
}

func (c *commandVolumeDeleteEmpty) Help() string {
	return `delete empty volumes from all volume servers

	volume.deleteEmpty -quietFor=24h -apply
	volume.deleteEmpty -collectionPattern=important* -quietFor=24h -apply

	This command deletes all empty volumes from all volume servers.
	A volume with no live needles left is empty too, even when its
	.dat file is still large: compacting it first would only rewrite
	bytes that are all deleted already.
	A replicated volume is deleted only when all of its copies are
	listed and every copy is empty.

`
}

func (c *commandVolumeDeleteEmpty) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeDeleteEmpty) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	quietPeriod := volDeleteCommand.Duration("quietFor", 24*time.Hour, "select empty volumes with no recent writes, avoid newly created ones")
	collectionPattern := volDeleteCommand.String("collectionPattern", "", "comma-separated collection names, with '*' and '?' wildcards; empty matches all")
	applyBalancing := volDeleteCommand.Bool("apply", false, "apply to delete empty volumes")
	// TODO: remove this alias
	applyBalancingAlias := volDeleteCommand.Bool("force", false, "apply to delete empty volumes (alias for -apply)")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	handleDeprecatedForceFlag(writer, volDeleteCommand, applyBalancingAlias, applyBalancing)
	infoAboutSimulationMode(writer, *applyBalancing, "-apply")

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	collectionMatcher, err := wildcard.CompileCollectionMatcher(*collectionPattern)
	if err != nil {
		return err
	}

	quietSeconds := int64(*quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	for _, vid := range slices.Sorted(maps.Keys(volumeReplicas)) {
		replicas := volumeReplicas[vid]
		if !isEmptyVolumeCopiesDeleteCandidate(replicas, quietSeconds, nowUnixSeconds, collectionMatcher) {
			continue
		}
		for _, r := range replicas {
			dn := r.location.dataNode
			if !*applyBalancing {
				log.Printf("empty volume %d from %s", vid, dn.Id)
				continue
			}
			log.Printf("deleting empty volume %d from %s", vid, dn.Id)
			onlyGarbage := r.info.FileCount > 0 && r.info.FileCount <= r.info.DeleteCount
			// onlyEmpty stays set so a pre-upgrade server checks
			// emptiness and refuses instead of deleting unseen.
			if deleteErr := deleteVolume(context.Background(), commandEnv.option.GrpcDialOption, needle.VolumeId(vid),
				pb.NewServerAddressFromDataNode(dn), true, onlyGarbage, false); deleteErr != nil {
				err = deleteErr
			}
		}
	}

	return
}

// isEmptyVolumeCopiesDeleteCandidate requires every copy to be listed and
// empty: deleting only the empty ones would leave a copy that holds data, or
// one that is offline, as the volume's only copy.
func isEmptyVolumeCopiesDeleteCandidate(replicas []*VolumeReplica, quietSeconds, nowUnixSeconds int64, collectionMatcher *wildcard.CollectionMatcher) bool {
	for _, r := range replicas {
		rp, err := super_block.NewReplicaPlacementFromByte(byte(r.info.ReplicaPlacement))
		if err != nil || len(replicas) < rp.GetCopyCount() ||
			!isEmptyVolumeDeleteCandidate(r.info, quietSeconds, nowUnixSeconds, collectionMatcher) {
			return false
		}
	}
	return len(replicas) > 0
}

func isEmptyVolumeDeleteCandidate(v *master_pb.VolumeInformationMessage, quietSeconds, nowUnixSeconds int64, collectionMatcher *wildcard.CollectionMatcher) bool {
	return collectionMatcher.Matches(v.Collection) &&
		// A remote-backed replica shares its cloud object with the others, so
		// deleting one cannot drop it without hurting the survivors.
		v.RemoteStorageName == "" &&
		(!v.ReadOnly || v.ReadOnlyCanDelete) &&
		(v.Size <= super_block.SuperBlockSize || v.FileCount > 0 && v.FileCount <= v.DeleteCount) &&
		v.ModifiedAtSecond > 0 &&
		v.ModifiedAtSecond+quietSeconds < nowUnixSeconds
}
