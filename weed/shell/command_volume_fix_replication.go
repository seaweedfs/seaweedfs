package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"io"
	"path/filepath"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

func init() {
	Commands = append(Commands, &commandVolumeFixReplication{})
}

type commandVolumeFixReplication struct {
	collectionPattern *string
}

func (c *commandVolumeFixReplication) Name() string {
	return "volume.fix.replication"
}

func (c *commandVolumeFixReplication) Help() string {
	return `add replicas to volumes that are missing replicas

	This command finds all over-replicated volumes. If found, it will purge the oldest copies and stop.

	This command also finds all under-replicated volumes, and finds volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	volume.fix.replication -n                             # do not take action
	volume.fix.replication                                # actually deleting or copying the volume files and mount the volume
	volume.fix.replication -collectionPattern=important*  # fix any collections with prefix "important"

	Note:
		* each time this will only add back one replica for each volume id that is under replicated.
		  If there are multiple replicas are missing, e.g. replica count is > 2, you may need to run this multiple times.
		* do not run this too quickly within seconds, since the new volume replica may take a few seconds 
		  to register itself to the master.

`
}

func (c *commandVolumeFixReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	volFixReplicationCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.collectionPattern = volFixReplicationCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	skipChange := volFixReplicationCommand.Bool("n", false, "skip the changes")
	if err = volFixReplicationCommand.Parse(args); err != nil {
		return nil
	}

	takeAction := !*skipChange

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv)
	if err != nil {
		return err
	}

	// find all volumes that needs replication
	// collect all data nodes
	volumeReplicas, allLocations := collectVolumeReplicaLocations(topologyInfo)

	if len(allLocations) == 0 {
		return fmt.Errorf("no data nodes at all")
	}

	// find all under replicated volumes
	var underReplicatedVolumeIds, overReplicatedVolumeIds []uint32
	for vid, replicas := range volumeReplicas {
		replica := replicas[0]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
		if replicaPlacement.GetCopyCount() > len(replicas) {
			underReplicatedVolumeIds = append(underReplicatedVolumeIds, vid)
		} else if replicaPlacement.GetCopyCount() < len(replicas) {
			overReplicatedVolumeIds = append(overReplicatedVolumeIds, vid)
			fmt.Fprintf(writer, "volume %d replication %s, but over replicated %+d\n", replica.info.Id, replicaPlacement, len(replicas))
		}
	}

	if len(overReplicatedVolumeIds) > 0 {
		return c.fixOverReplicatedVolumes(commandEnv, writer, takeAction, overReplicatedVolumeIds, volumeReplicas, allLocations)
	}

	if len(underReplicatedVolumeIds) == 0 {
		return nil
	}

	// find the most under populated data nodes
	return c.fixUnderReplicatedVolumes(commandEnv, writer, takeAction, underReplicatedVolumeIds, volumeReplicas, allLocations)

}

func collectVolumeReplicaLocations(topologyInfo *master_pb.TopologyInfo) (map[uint32][]*VolumeReplica, []location) {
	volumeReplicas := make(map[uint32][]*VolumeReplica)
	var allLocations []location
	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(dc, string(rack), dn)
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				volumeReplicas[v.Id] = append(volumeReplicas[v.Id], &VolumeReplica{
					location: &loc,
					info:     v,
				})
			}
		}
		allLocations = append(allLocations, loc)
	})
	return volumeReplicas, allLocations
}

func (c *commandVolumeFixReplication) fixOverReplicatedVolumes(commandEnv *CommandEnv, writer io.Writer, takeAction bool, overReplicatedVolumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location) error {
	for _, vid := range overReplicatedVolumeIds {
		replicas := volumeReplicas[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replicas[0].info.ReplicaPlacement))

		replica := pickOneReplicaToDelete(replicas, replicaPlacement)

		// check collection name pattern
		if *c.collectionPattern != "" {
			matched, err := filepath.Match(*c.collectionPattern, replica.info.Collection)
			if err != nil {
				return fmt.Errorf("match pattern %s with collection %s: %v", *c.collectionPattern, replica.info.Collection, err)
			}
			if !matched {
				break
			}
		}

		fmt.Fprintf(writer, "deleting volume %d from %s ...\n", replica.info.Id, replica.location.dataNode.Id)

		if !takeAction {
			break
		}

		if err := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(replica.info.Id), replica.location.dataNode.Id); err != nil {
			return fmt.Errorf("deleting volume %d from %s : %v", replica.info.Id, replica.location.dataNode.Id, err)
		}

	}
	return nil
}

func (c *commandVolumeFixReplication) fixUnderReplicatedVolumes(commandEnv *CommandEnv, writer io.Writer, takeAction bool, underReplicatedVolumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location) error {

	for _, vid := range underReplicatedVolumeIds {
		replicas := volumeReplicas[vid]
		replica := pickOneReplicaToCopyFrom(replicas)
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
		foundNewLocation := false
		hasSkippedCollection := false
		keepDataNodesSorted(allLocations, types.ToDiskType(replica.info.DiskType))
		fn := capacityByFreeVolumeCount(types.ToDiskType(replica.info.DiskType))
		for _, dst := range allLocations {
			// check whether data nodes satisfy the constraints
			if fn(dst.dataNode) > 0 && satisfyReplicaPlacement(replicaPlacement, replicas, dst) {
				// check collection name pattern
				if *c.collectionPattern != "" {
					matched, err := filepath.Match(*c.collectionPattern, replica.info.Collection)
					if err != nil {
						return fmt.Errorf("match pattern %s with collection %s: %v", *c.collectionPattern, replica.info.Collection, err)
					}
					if !matched {
						hasSkippedCollection = true
						break
					}
				}

				// ask the volume server to replicate the volume
				foundNewLocation = true
				fmt.Fprintf(writer, "replicating volume %d %s from %s to dataNode %s ...\n", replica.info.Id, replicaPlacement, replica.location.dataNode.Id, dst.dataNode.Id)

				if !takeAction {
					break
				}

				err := operation.WithVolumeServerClient(dst.dataNode.Id, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
					_, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
						VolumeId:       replica.info.Id,
						SourceDataNode: replica.location.dataNode.Id,
					})
					if replicateErr != nil {
						return fmt.Errorf("copying from %s => %s : %v", replica.location.dataNode.Id, dst.dataNode.Id, replicateErr)
					}
					return nil
				})

				if err != nil {
					return err
				}

				// adjust free volume count
				dst.dataNode.DiskInfos[replica.info.DiskType].FreeVolumeCount--
				break
			}
		}

		if !foundNewLocation && !hasSkippedCollection {
			fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", replica.info.Id, replicaPlacement, len(replicas))
		}

	}
	return nil
}

func keepDataNodesSorted(dataNodes []location, diskType types.DiskType) {
	fn := capacityByFreeVolumeCount(diskType)
	sort.Slice(dataNodes, func(i, j int) bool {
		return fn(dataNodes[i].dataNode) > fn(dataNodes[j].dataNode)
	})
}

/*
  if on an existing data node {
    return false
  }
  if different from existing dcs {
    if lack on different dcs {
      return true
    }else{
      return false
    }
  }
  if not on primary dc {
    return false
  }
  if different from existing racks {
    if lack on different racks {
      return true
    }else{
      return false
    }
  }
  if not on primary rack {
    return false
  }
  if lacks on same rack {
    return true
  } else {
    return false
  }
*/
func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, replicas []*VolumeReplica, possibleLocation location) bool {

	existingDataCenters, _, existingDataNodes := countReplicas(replicas)

	if _, found := existingDataNodes[possibleLocation.String()]; found {
		// avoid duplicated volume on the same data node
		return false
	}

	primaryDataCenters, _ := findTopKeys(existingDataCenters)

	// ensure data center count is within limit
	if _, found := existingDataCenters[possibleLocation.DataCenter()]; !found {
		// different from existing dcs
		if len(existingDataCenters) < replicaPlacement.DiffDataCenterCount+1 {
			// lack on different dcs
			return true
		} else {
			// adding this would go over the different dcs limit
			return false
		}
	}
	// now this is same as one of the existing data center
	if !isAmong(possibleLocation.DataCenter(), primaryDataCenters) {
		// not on one of the primary dcs
		return false
	}

	// now this is one of the primary dcs
	primaryDcRacks := make(map[string]int)
	for _, replica := range replicas {
		if replica.location.DataCenter() != possibleLocation.DataCenter() {
			continue
		}
		primaryDcRacks[replica.location.Rack()] += 1
	}
	primaryRacks, _ := findTopKeys(primaryDcRacks)
	sameRackCount := primaryDcRacks[possibleLocation.Rack()]

	// ensure rack count is within limit
	if _, found := primaryDcRacks[possibleLocation.Rack()]; !found {
		// different from existing racks
		if len(primaryDcRacks) < replicaPlacement.DiffRackCount+1 {
			// lack on different racks
			return true
		} else {
			// adding this would go over the different racks limit
			return false
		}
	}
	// now this is same as one of the existing racks
	if !isAmong(possibleLocation.Rack(), primaryRacks) {
		// not on the primary rack
		return false
	}

	// now this is on the primary rack

	// different from existing data nodes
	if sameRackCount < replicaPlacement.SameRackCount+1 {
		// lack on same rack
		return true
	} else {
		// adding this would go over the same data node limit
		return false
	}

}

func findTopKeys(m map[string]int) (topKeys []string, max int) {
	for k, c := range m {
		if max < c {
			topKeys = topKeys[:0]
			topKeys = append(topKeys, k)
			max = c
		} else if max == c {
			topKeys = append(topKeys, k)
		}
	}
	return
}

func isAmong(key string, keys []string) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

type VolumeReplica struct {
	location *location
	info     *master_pb.VolumeInformationMessage
}

type location struct {
	dc       string
	rack     string
	dataNode *master_pb.DataNodeInfo
}

func newLocation(dc, rack string, dataNode *master_pb.DataNodeInfo) location {
	return location{
		dc:       dc,
		rack:     rack,
		dataNode: dataNode,
	}
}

func (l location) String() string {
	return fmt.Sprintf("%s %s %s", l.dc, l.rack, l.dataNode.Id)
}

func (l location) Rack() string {
	return fmt.Sprintf("%s %s", l.dc, l.rack)
}

func (l location) DataCenter() string {
	return l.dc
}

func pickOneReplicaToCopyFrom(replicas []*VolumeReplica) *VolumeReplica {
	mostRecent := replicas[0]
	for _, replica := range replicas {
		if replica.info.ModifiedAtSecond > mostRecent.info.ModifiedAtSecond {
			mostRecent = replica
		}
	}
	return mostRecent
}

func countReplicas(replicas []*VolumeReplica) (diffDc, diffRack, diffNode map[string]int) {
	diffDc = make(map[string]int)
	diffRack = make(map[string]int)
	diffNode = make(map[string]int)
	for _, replica := range replicas {
		diffDc[replica.location.DataCenter()] += 1
		diffRack[replica.location.Rack()] += 1
		diffNode[replica.location.String()] += 1
	}
	return
}

func pickOneReplicaToDelete(replicas []*VolumeReplica, replicaPlacement *super_block.ReplicaPlacement) *VolumeReplica {

	sort.Slice(replicas, func(i, j int) bool {
		a, b := replicas[i], replicas[j]
		if a.info.CompactRevision != b.info.CompactRevision {
			return a.info.CompactRevision < b.info.CompactRevision
		}
		if a.info.ModifiedAtSecond != b.info.ModifiedAtSecond {
			return a.info.ModifiedAtSecond < b.info.ModifiedAtSecond
		}
		if a.info.Size != b.info.Size {
			return a.info.Size < b.info.Size
		}
		return false
	})

	return replicas[0]

}
