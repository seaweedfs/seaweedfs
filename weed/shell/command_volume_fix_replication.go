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

func satisfyReplicaPlacement(replicaPlacement *super_block.ReplicaPlacement, existingLocations []location, possibleLocation location) bool {

	existingDataCenters := make(map[string]bool)
	existingRacks := make(map[string]bool)
	existingDataNodes := make(map[string]bool)
	for _, loc := range existingLocations {
		existingDataCenters[loc.DataCenter()] = true
		existingRacks[loc.Rack()] = true
		existingDataNodes[loc.String()] = true
	}

	if replicaPlacement.DiffDataCenterCount >= len(existingDataCenters) {
		// check dc, good if different from any existing data centers
		_, found := existingDataCenters[possibleLocation.DataCenter()]
		return !found
	} else if replicaPlacement.DiffRackCount >= len(existingRacks) {
		// check rack, good if different from any existing racks
		_, found := existingRacks[possibleLocation.Rack()]
		return !found
	} else if replicaPlacement.SameRackCount >= len(existingDataNodes) {
		// check data node, good if different from any existing data nodes
		_, found := existingDataNodes[possibleLocation.String()]
		return !found
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
