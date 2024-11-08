package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandRaftClusterPs{})
}

type commandRaftClusterPs struct {
}

func (c *commandRaftClusterPs) Name() string {
	return "cluster.raft.ps"
}

func (c *commandRaftClusterPs) Help() string {
	return `check current raft cluster status

	cluster.raft.ps
`
}

func (c *commandRaftClusterPs) HasTag(CommandTag) bool {
	return false
}

func (c *commandRaftClusterPs) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	raftClusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = raftClusterPsCommand.Parse(args); err != nil {
		return nil
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.RaftListClusterServers(context.Background(), &master_pb.RaftListClusterServersRequest{})
		if err != nil {
			return fmt.Errorf("raft list cluster: %v", err)
		}
		fmt.Fprintf(writer, "the raft cluster has %d servers\n", len(resp.ClusterServers))
		for _, server := range resp.ClusterServers {
			suffrage := server.Suffrage
			if server.IsLeader {
				suffrage = "Leader"
			}
			fmt.Fprintf(writer, "  * %s %s (%s)\n", server.Id, server.Address, suffrage)
		}

		return nil
	})

	return err

}
