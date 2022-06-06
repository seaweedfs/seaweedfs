package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"io"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
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

func (c *commandClusterPs) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	clusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = clusterPsCommand.Parse(args); err != nil {
		return nil
	}

	var filerNodes []*master_pb.ListClusterNodesResponse_ClusterNode

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

	fmt.Fprintf(writer, "the cluster has %d filers\n", len(filerNodes))
	for _, node := range filerNodes {
		fmt.Fprintf(writer, "  * %s (%v)\n", node.Address, node.Version)
		pb.WithFilerClient(false, pb.ServerAddress(node.Address), commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err == nil {
				if resp.FilerGroup != "" {
					fmt.Fprintf(writer, "    filer group: %s\n", resp.FilerGroup)
				}
				fmt.Fprintf(writer, "    signature: %d\n", resp.Signature)
			} else {
				fmt.Fprintf(writer, "    failed to connect: %v\n", err)
			}
			return err
		})
	}

	return nil
}
