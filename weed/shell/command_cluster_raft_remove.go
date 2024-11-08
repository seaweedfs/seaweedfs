package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandRaftServerRemove{})
}

type commandRaftServerRemove struct {
}

func (c *commandRaftServerRemove) Name() string {
	return "cluster.raft.remove"
}

func (c *commandRaftServerRemove) Help() string {
	return `remove a server from the raft cluster

	Example:
		cluster.raft.remove -id <server_name>
`
}

func (c *commandRaftServerRemove) HasTag(CommandTag) bool {
	return false
}

func (c *commandRaftServerRemove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	raftServerAddCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	serverId := raftServerAddCommand.String("id", "", "server id")
	if err = raftServerAddCommand.Parse(args); err != nil {
		return nil
	}

	if *serverId == "" {
		return fmt.Errorf("empty server id")
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.RaftRemoveServer(context.Background(), &master_pb.RaftRemoveServerRequest{
			Id:    *serverId,
			Force: true,
		})
		if err != nil {
			return fmt.Errorf("raft remove server: %v", err)
		}
		println("removed server", *serverId)
		return nil
	})

	return err

}
