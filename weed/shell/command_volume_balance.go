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
	commands = append(commands, &commandVolumeBalance{})
}

type commandVolumeBalance struct {
}

func (c *commandVolumeBalance) Name() string {
	return "volume.balance"
}

func (c *commandVolumeBalance) Help() string {
	return `balance all volumes among volume servers

	volume.balance [-c collectionName] [-f]

	Algorithm:
	For each type of volume server (different max volume count limit){
		balanceWritableVolumes()
		balanceReadOnlyVolumes()
	}

	func balanceWritableVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by{
				local writable volumes
			}
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 < idealWritableVolumes {
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

func (c *commandVolumeBalance) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	balanceCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := balanceCommand.String("c", "", "collection name. use \"ALL\" for all collections")
	applyBalancing := balanceCommand.Bool("f", false, "apply the balancing plan.")
	if err = balanceCommand.Parse(args); err != nil {
		return nil
	}

	var resp *master_pb.VolumeListResponse
	ctx := context.Background()
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	typeToNodes := collectVolumeServersByType(resp.TopologyInfo)
	for _, volumeServers := range typeToNodes {
		if len(volumeServers) < 2 {
			continue
		}
		balanceVolumeServers(commandEnv, volumeServers, resp.VolumeSizeLimitMb*1024*1024, *collection, *applyBalancing)
	}
	return nil
}

func balanceVolumeServers(commandEnv *commandEnv, dataNodeInfos []*master_pb.DataNodeInfo, volumeSizeLimit uint64, collection string, applyBalancing bool) {
	var nodes []*Node
	for _, dn := range dataNodeInfos {
		nodes = append(nodes, &Node{
			info: dn,
		})
	}

	// balance writable volumes
	for _, n := range nodes {
		n.prepareVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL" {
				if v.Collection != collection {
					return false
				}
			}
			return !v.ReadOnly && v.Size < volumeSizeLimit
		})
	}
	balanceSelectedVolume(commandEnv, nodes, sortWritableVolumes, applyBalancing)

	// balance readable volumes
	for _, n := range nodes {
		n.prepareVolumes(func(v *master_pb.VolumeInformationMessage) bool {
			if collection != "ALL" {
				if v.Collection != collection {
					return false
				}
			}
			return v.ReadOnly || v.Size >= volumeSizeLimit
		})
	}
	balanceSelectedVolume(commandEnv, nodes, sortReadOnlyVolumes, applyBalancing)
}

func collectVolumeServersByType(t *master_pb.TopologyInfo) (typeToNodes map[uint64][]*master_pb.DataNodeInfo) {
	typeToNodes = make(map[uint64][]*master_pb.DataNodeInfo)
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				typeToNodes[dn.MaxVolumeCount] = append(typeToNodes[dn.MaxVolumeCount], dn)
			}
		}
	}
	return
}

type Node struct {
	info            *master_pb.DataNodeInfo
	selectedVolumes map[uint32]*master_pb.VolumeInformationMessage
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

func balanceSelectedVolume(commandEnv *commandEnv, nodes []*Node, sortCandidatesFn func(volumes []*master_pb.VolumeInformationMessage), applyBalancing bool) {
	selectedVolumeCount := 0
	for _, dn := range nodes {
		selectedVolumeCount += len(dn.selectedVolumes)
	}

	idealSelectedVolumes := selectedVolumeCount / len(nodes)

	hasMove := true

	for hasMove {
		hasMove = false
		sort.Slice(nodes, func(i, j int) bool {
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
				if _, found := emptyNode.selectedVolumes[v.Id]; !found {
					moveVolume(commandEnv, v, fullNode, emptyNode, applyBalancing)
					delete(fullNode.selectedVolumes, v.Id)
					emptyNode.selectedVolumes[v.Id] = v
					hasMove = true
					break
				}
			}
		}
	}

}

func moveVolume(commandEnv *commandEnv, v *master_pb.VolumeInformationMessage, fullNode *Node, emptyNode *Node, applyBalancing bool) {
	collectionPrefix := v.Collection + "_"
	if v.Collection == "" {
		collectionPrefix = ""
	}
	fmt.Fprintf(os.Stdout, "moving volume %s%d %s => %s\n", collectionPrefix, v.Id, fullNode.info.Id, emptyNode.info.Id)
	if applyBalancing {
		ctx := context.Background()
		LiveMoveVolume(ctx, commandEnv.option.GrpcDialOption, needle.VolumeId(v.Id), fullNode.info.Id, emptyNode.info.Id, 5*time.Second)
	}
}

func (node *Node) prepareVolumes(fn func(v *master_pb.VolumeInformationMessage) bool) {
	node.selectedVolumes = make(map[uint32]*master_pb.VolumeInformationMessage)
	for _, v := range node.info.VolumeInfos {
		if fn(v) {
			node.selectedVolumes[v.Id] = v
		}
	}
}
