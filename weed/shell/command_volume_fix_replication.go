package shell

import (
	"context"
	"fmt"
	"io"
	"math/rand"
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
}

func (c *commandVolumeFixReplication) Name() string {
	return "volume.fix.replication"
}

func (c *commandVolumeFixReplication) Help() string {
	return `add replicas to volumes that are missing replicas

	This command finds all under-replicated volumes, and finds volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	volume.fix.replication -n # do not take action
	volume.fix.replication    # actually copying the volume files and mount the volume

	Note:
		* each time this will only add back one replica for one volume id. If there are multiple replicas
		  are missing, e.g. multiple volume servers are new, you may need to run this multiple times.
		* do not run this too quickly within seconds, since the new volume replica may take a few seconds 
		  to register itself to the master.

`
}

func (c *commandVolumeFixReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	takeAction := true
	if len(args) > 0 && args[0] == "-n" {
		takeAction = false
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	// find all volumes that needs replication
	// collect all data nodes
	volumeReplicas := make(map[uint32][]*VolumeReplica)
	var allLocations []location
	eachDataNode(resp.TopologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(dc, string(rack), dn)
		for _, v := range dn.VolumeInfos {
			if v.ReplicaPlacement > 0 {
				volumeReplicas[v.Id] = append(volumeReplicas[v.Id], &VolumeReplica{
					location: &loc,
					info:     v,
				})
			}
		}
		allLocations = append(allLocations, loc)
	})

	// find all under replicated volumes
	var underReplicatedVolumeIds, overReplicatedVolumeIds []uint32
	for vid, replicas := range volumeReplicas {
		replica := replicas[rand.Intn(len(replicas))]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
		if replicaPlacement.GetCopyCount() > len(replicas) {
			underReplicatedVolumeIds = append(underReplicatedVolumeIds, vid)
		} else if replicaPlacement.GetCopyCount() < len(replicas) {
			overReplicatedVolumeIds = append(overReplicatedVolumeIds, vid)
			fmt.Fprintf(writer, "volume %d replication %s, but over replicated %+d\n", replica.info.Id, replicaPlacement, len(replicas))
		}
	}

	if len(underReplicatedVolumeIds) == 0 {
		return fmt.Errorf("no under replicated volumes")
	}

	if len(allLocations) == 0 {
		return fmt.Errorf("no data nodes at all")
	}

	// find the most under populated data nodes
	keepDataNodesSorted(allLocations)

	return c.fixUnderReplicatedVolumes(commandEnv, writer, takeAction, underReplicatedVolumeIds, volumeReplicas, allLocations)

}

func (c *commandVolumeFixReplication) fixUnderReplicatedVolumes(commandEnv *CommandEnv, writer io.Writer, takeAction bool, underReplicatedVolumeIds []uint32, volumeReplicas map[uint32][]*VolumeReplica, allLocations []location) error {
	for _, vid := range underReplicatedVolumeIds {
		replicas := volumeReplicas[vid]
		replica := replicas[rand.Intn(len(replicas))]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(replica.info.ReplicaPlacement))
		foundNewLocation := false
		for _, dst := range allLocations {
			// check whether data nodes satisfy the constraints
			if dst.dataNode.FreeVolumeCount > 0 && satisfyReplicaPlacement(replicaPlacement, replicas, dst) {
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
				dst.dataNode.FreeVolumeCount--
				keepDataNodesSorted(allLocations)
				break
			}
		}
		if !foundNewLocation {
			fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", replica.info.Id, replicaPlacement, len(replicas))
		}

	}
	return nil
}

func keepDataNodesSorted(dataNodes []location) {
	sort.Slice(dataNodes, func(i, j int) bool {
		return dataNodes[i].dataNode.FreeVolumeCount > dataNodes[j].dataNode.FreeVolumeCount
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

	existingDataNodes := make(map[string]int)
	for _, replica := range replicas {
		existingDataNodes[replica.location.String()] += 1
	}
	sameDataNodeCount := existingDataNodes[possibleLocation.String()]
	// avoid duplicated volume on the same data node
	if sameDataNodeCount > 0 {
		return false
	}

	existingDataCenters := make(map[string]int)
	for _, replica := range replicas {
		existingDataCenters[replica.location.DataCenter()] += 1
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
	existingRacks := make(map[string]int)
	for _, replica := range replicas {
		if replica.location.DataCenter() != possibleLocation.DataCenter() {
			continue
		}
		existingRacks[replica.location.Rack()] += 1
	}
	primaryRacks, _ := findTopKeys(existingRacks)
	sameRackCount := existingRacks[possibleLocation.Rack()]

	// ensure rack count is within limit
	if _, found := existingRacks[possibleLocation.Rack()]; !found {
		// different from existing racks
		if len(existingRacks) < replicaPlacement.DiffRackCount+1 {
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
