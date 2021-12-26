package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/cluster"
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

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})

		fmt.Fprintf(writer, "the cluster has %d filers\n", len(resp.ClusterNodes))
		for _, node := range resp.ClusterNodes {
			fmt.Fprintf(writer, "  * %s (%v)\n", node.Address, node.Version)
		}
		return err
	})
	if err != nil {
		return
	}

	return nil
}
