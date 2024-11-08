package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandClusterPs{})
}

type commandClusterPs struct {
}

func (c *commandClusterPs) Name() string {
	return "cluster.ps"
}

func (c *commandClusterPs) Help() string {
	return `check current cluster process status

	cluster.ps

`
}

func (c *commandClusterPs) HasTag(CommandTag) bool {
	return false
}

func (c *commandClusterPs) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	clusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = clusterPsCommand.Parse(args); err != nil {
		return nil
	}

	var filerNodes []*master_pb.ListClusterNodesResponse_ClusterNode
	var mqBrokerNodes []*master_pb.ListClusterNodesResponse_ClusterNode

	// get the list of filers
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})
		if err != nil {
			return err
		}

		filerNodes = resp.ClusterNodes
		return err
	})
	if err != nil {
		return
	}

	// get the list of message queue brokers
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})
		if err != nil {
			return err
		}

		mqBrokerNodes = resp.ClusterNodes
		return err
	})
	if err != nil {
		return
	}

	if len(mqBrokerNodes) > 0 {
		fmt.Fprintf(writer, "* message queue brokers %d\n", len(mqBrokerNodes))
		for _, node := range mqBrokerNodes {
			fmt.Fprintf(writer, "  * %s (%v)\n", node.Address, node.Version)
			if node.DataCenter != "" {
				fmt.Fprintf(writer, "    DataCenter: %v\n", node.DataCenter)
			}
			if node.Rack != "" {
				fmt.Fprintf(writer, "    Rack: %v\n", node.Rack)
			}
		}
	}

	filerSignatures := make(map[*master_pb.ListClusterNodesResponse_ClusterNode]int32)
	fmt.Fprintf(writer, "* filers %d\n", len(filerNodes))
	for _, node := range filerNodes {
		fmt.Fprintf(writer, "  * %s (%v) %v\n", node.Address, node.Version, time.Unix(0, node.CreatedAtNs).UTC())
		if node.DataCenter != "" {
			fmt.Fprintf(writer, "    DataCenter: %v\n", node.DataCenter)
		}
		if node.Rack != "" {
			fmt.Fprintf(writer, "    Rack: %v\n", node.Rack)
		}
		pb.WithFilerClient(false, 0, pb.ServerAddress(node.Address), commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err == nil {
				if resp.FilerGroup != "" {
					fmt.Fprintf(writer, "    filer group: %s\n", resp.FilerGroup)
				}
				fmt.Fprintf(writer, "    signature: %d\n", resp.Signature)
				filerSignatures[node] = resp.Signature
			} else {
				fmt.Fprintf(writer, "    failed to connect: %v\n", err)
			}
			return err
		})
	}
	for _, node := range filerNodes {
		pb.WithFilerClient(false, 0, pb.ServerAddress(node.Address), commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			fmt.Fprintf(writer, "* filer %s metadata sync time\n", node.Address)
			selfSignature := filerSignatures[node]
			for peer, peerSignature := range filerSignatures {
				if selfSignature == peerSignature {
					continue
				}
				if resp, err := client.KvGet(context.Background(), &filer_pb.KvGetRequest{Key: filer.GetPeerMetaOffsetKey(peerSignature)}); err == nil && len(resp.Value) == 8 {
					lastTsNs := int64(util.BytesToUint64(resp.Value))
					fmt.Fprintf(writer, "    %s: %v\n", peer.Address, time.Unix(0, lastTsNs).UTC())
				}
			}
			return nil
		})
	}

	// collect volume servers
	var volumeServers []pb.ServerAddress
	t, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				volumeServers = append(volumeServers, pb.NewServerAddressFromDataNode(dn))
			}
		}
	}

	fmt.Fprintf(writer, "* volume servers %d\n", len(volumeServers))
	for _, dc := range t.DataCenterInfos {
		fmt.Fprintf(writer, "  * data center: %s\n", dc.Id)
		for _, r := range dc.RackInfos {
			fmt.Fprintf(writer, "    * rack: %s\n", r.Id)
			for _, dn := range r.DataNodeInfos {
				pb.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(dn), commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
					resp, err := client.VolumeServerStatus(context.Background(), &volume_server_pb.VolumeServerStatusRequest{})
					if err == nil {
						fmt.Fprintf(writer, "      * %s (%v)\n", dn.Id, resp.Version)
					}
					return err
				})
			}
		}
	}

	return nil
}
