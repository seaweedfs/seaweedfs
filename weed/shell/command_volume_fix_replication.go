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

	This command file all under-replicated volumes, and find volume servers with free slots.
	If the free slots satisfy the replication requirement, the volume content is copied over and mounted.

	volume.fix.replication -n # do not take action
	volume.fix.replication    # actually copying the volume files and mount the volume

	Note:
		* each time this will only add back one replica for one volume id. If there are multiple replicas
		  are missing, e.g. multiple volume servers are new, you may need to run this multiple times.
		* do not run this too quick within seconds, since the new volume replica may take a few seconds 
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
	replicatedVolumeLocations := make(map[uint32][]location)
	replicatedVolumeInfo := make(map[uint32]*master_pb.VolumeInformationMessage)
	var allLocations []location
	eachDataNode(resp.TopologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		loc := newLocation(dc, string(rack), dn)
		for _, v := range dn.VolumeInfos {
			if v.ReplicaPlacement > 0 {
				replicatedVolumeLocations[v.Id] = append(replicatedVolumeLocations[v.Id], loc)
				replicatedVolumeInfo[v.Id] = v
			}
		}
		allLocations = append(allLocations, loc)
	})

	// find all under replicated volumes
	underReplicatedVolumeLocations := make(map[uint32][]location)
	for vid, locations := range replicatedVolumeLocations {
		volumeInfo := replicatedVolumeInfo[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(volumeInfo.ReplicaPlacement))
		if replicaPlacement.GetCopyCount() > len(locations) {
			underReplicatedVolumeLocations[vid] = locations
		}
	}

	if len(underReplicatedVolumeLocations) == 0 {
		return fmt.Errorf("no under replicated volumes")
	}

	if len(allLocations) == 0 {
		return fmt.Errorf("no data nodes at all")
	}

	// find the most under populated data nodes
	keepDataNodesSorted(allLocations)

	for vid, locations := range underReplicatedVolumeLocations {
		volumeInfo := replicatedVolumeInfo[vid]
		replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(volumeInfo.ReplicaPlacement))
		foundNewLocation := false
		for _, dst := range allLocations {
			// check whether data nodes satisfy the constraints
			if dst.dataNode.FreeVolumeCount > 0 && satisfyReplicaPlacement(replicaPlacement, locations, dst) {
				// ask the volume server to replicate the volume
				sourceNodes := underReplicatedVolumeLocations[vid]
				sourceNode := sourceNodes[rand.Intn(len(sourceNodes))]
				foundNewLocation = true
				fmt.Fprintf(writer, "replicating volume %d %s from %s to dataNode %s ...\n", volumeInfo.Id, replicaPlacement, sourceNode.dataNode.Id, dst.dataNode.Id)

				if !takeAction {
					break
				}

				err := operation.WithVolumeServerClient(dst.dataNode.Id, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
					_, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
						VolumeId:       volumeInfo.Id,
						SourceDataNode: sourceNode.dataNode.Id,
					})
					return replicateErr
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
			fmt.Fprintf(writer, "failed to place volume %d replica as %s, existing:%+v\n", volumeInfo.Id, replicaPlacement, locations)
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
func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, existingLocations []location, possibleLocation location) bool {

	existingDataNodes := make(map[string]int)
	for _, loc := range existingLocations {
		existingDataNodes[loc.String()] += 1
	}
	sameDataNodeCount := existingDataNodes[possibleLocation.String()]
	// avoid duplicated volume on the same data node
	if sameDataNodeCount > 0 {
		return false
	}

	existingDataCenters := make(map[string]int)
	for _, loc := range existingLocations {
		existingDataCenters[loc.DataCenter()] += 1
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
	for _, loc := range existingLocations {
		if loc.DataCenter() != possibleLocation.DataCenter() {
			continue
		}
		existingRacks[loc.Rack()] += 1
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
