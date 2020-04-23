package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-collection ALL|EACH_COLLECTION|<collection_name>] [-force] [-dataCenter=<data_center_name>]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			balanceWritableVolumes()
			balanceReadOnlyVolumes()
		}
	}

	func balanceWritableVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for hasMovedOneVolume {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}
	func balanceReadOnlyVolumes(){
		//similar to balanceWritableVolumes
	}

`
}

func (c *commandVolumeBalance) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("collection", "EACH_COLLECTION", "collection name, or use \"ALL_COLLECTIONS\" across collections, \"EACH_COLLECTION\" for each collection")
	dc := balanceCommand.String("dataCenter", "", "only apply the balancing for this dataCenter")
	applyBalancing := balanceCommand.Bool("force", false, "apply the balancing plan.")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	typeToNodes := collectVolumeServersByType(resp.TopologyInfo, *dc)

	for maxVolumeCount, volumeServers := range typeToNodes {
		if len(volumeServers) < 2 {
			fmt.Printf("only 1 node is configured max %d volumes, skipping balancing\n", maxVolumeCount)
			continue
		}
		if *collection == "EACH_COLLECTION" {
			collections, err := ListCollectionNames(commandEnv, true, false)
			if err != nil {
				return err
			}
			for _, c := range collections {
				if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, c, *applyBalancing); err != nil {
					return err
				}
			}
		} else if *collection == "ALL_COLLECTIONS" {
			if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, "ALL_COLLECTIONS", *applyBalancing); err != nil {
				return err
			}
		} else {
			if err = balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, *collection, *applyBalancing); err != nil {
				return err
			}
		}

	}
	return nil
}

func balanceVolumeServers(commandEnv *CommandEnv, nodes []*Node, volumeSizeLimit uint64, collection string, applyBalancing bool) error {

	// balance writable volumes
	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return !v.ReadOnly && v.Size < volumeSizeLimit
		})
	}
	if err := balanceSelectedVolume(commandEnv, nodes, sortWritableVolumes, applyBalancing); err != nil {
		return err
	}

	// balance readable volumes
	for _, n := range nodes {
		n.selectVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL_COLLECTIONS" {
				if v.Collection != collection {
					return false
				}
			}
			return v.ReadOnly || v.Size >= volumeSizeLimit
		})
	}
	if err := balanceSelectedVolume(commandEnv, nodes, sortReadOnlyVolumes, applyBalancing); err != nil {
		return err
	}

	return nil
}

func collectVolumeServersByType(t *master_pb.TopologyInfo, selectedDataCenter string) (typeToNodes map[uint64][]*Node) {
	typeToNodes = make(map[uint64][]*Node)
	for _, dc := range t.DataCenterInfos {
		if selectedDataCenter != "" && dc.Id != selectedDataCenter {
			continue
		}
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				typeToNodes[dn.MaxVolumeCount] = append(typeToNodes[dn.MaxVolumeCount], &Node{
					info: dn,
					dc:   dc.Id,
					rack: r.Id,
				})
			}
		}
	}
	return
}

type Node struct {
	info            *master_pb.DataNodeInfo
	selectedVolumes map[uint32]*master_pb.VolumeInformationMessage
	dc              string
	rack            string
}

func sortWritableVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Size < volumes[j].Size
	})
}

func sortReadOnlyVolumes(volumes []*master_pb.VolumeInformationMessage) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Id < volumes[j].Id
	})
}

func balanceSelectedVolume(commandEnv *CommandEnv, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), applyBalancing bool) error {
	selectedVolumeCount := 0
	for _, dn := range nodes {
		selectedVolumeCount += len(dn.selectedVolumes)
	}

	idealSelectedVolumes := ceilDivide(selectedVolumeCount, len(nodes))

	hasMove := true

	for hasMove {
		hasMove = false
		sort.Slice(nodes, func(i, j int) bool {
			// TODO sort by free volume slots???
			return len(nodes[i].selectedVolumes) < len(nodes[j].selectedVolumes)
		})
		emptyNode, fullNode := nodes[0], nodes[len(nodes)-1]
		if len(fullNode.selectedVolumes) > idealSelectedVolumes && len(emptyNode.selectedVolumes)+1 <= idealSelectedVolumes {

			// sort the volumes to move
			var candidateVolumes []*master_pb.VolumeInformationMessage
			for _, v := range fullNode.selectedVolumes {
				candidateVolumes = append(candidateVolumes, v)
			}
			sortCandidatesFn(candidateVolumes)

			for _, v := range candidateVolumes {
				if v.ReplicaPlacement > 0 {
					if fullNode.dc != emptyNode.dc && fullNode.rack != emptyNode.rack {
						// TODO this logic is too simple, but should work most of the time
						// Need a correct algorithm to handle all different cases
						continue
					}
				}
				if _, found := emptyNode.selectedVolumes[v.Id]; !found {
					if err := moveVolume(commandEnv, v, fullNode, emptyNode, applyBalancing); err == nil {
						delete(fullNode.selectedVolumes, v.Id)
						emptyNode.selectedVolumes[v.Id] = v
						hasMove = true
						break
					} else {
						return err
					}
				}
			}
		}
	}
	return nil
}

func moveVolume(commandEnv *CommandEnv, v *master_pb.VolumeInformationMessage, fullNode *Node, emptyNode *Node, applyBalancing bool) error {
	collectionPrefix := v.Collection + "_"
	if v.Collection == "" {
		collectionPrefix = ""
	}
	fmt.Fprintf(os.Stdout, "moving volume %s%d %s => %s\n", collectionPrefix, v.Id, fullNode.info.Id, emptyNode.info.Id)
	if applyBalancing {
		return LiveMoveVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(v.Id), fullNode.info.Id, emptyNode.info.Id, 5*time.Second)
	}
	return nil
}

func (node *Node) selectVolumes(fn func(v *master_pb.VolumeInformationMessage) bool) {
	node.selectedVolumes = make(map[uint32]*master_pb.VolumeInformationMessage)
	for _, v := range node.info.VolumeInfos {
		if fn(v) {
			node.selectedVolumes[v.Id] = v
		}
	}
}
