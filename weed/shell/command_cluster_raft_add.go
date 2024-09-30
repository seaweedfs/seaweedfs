package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandRaftServerAdd{})
}

type commandRaftServerAdd struct {
}

func (c *commandRaftServerAdd) Name() string {
	return "cluster.raft.add"
}

func (c *commandRaftServerAdd) Help() string {
	return `add a server to the raft cluster

	Example:
		cluster.raft.add -id <server_name> -address <server_host:port> -voter
`
}

func (c *commandRaftServerAdd) HasTag(CommandTag) bool {
	return false
}

func (c *commandRaftServerAdd) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	raftServerAddCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	serverId := raftServerAddCommand.String("id", "", "server id")
	serverAddress := raftServerAddCommand.String("address", "", "server grpc address")
	serverVoter := raftServerAddCommand.Bool("voter", true, "assign it a vote")
	if err = raftServerAddCommand.Parse(args); err != nil {
		return nil
	}

	if *serverId == "" || *serverAddress == "" {
		return fmt.Errorf("empty server id or address")
	}

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		_, err := client.RaftAddServer(context.Background(), &master_pb.RaftAddServerRequest{
			Id:      *serverId,
			Address: *serverAddress,
			Voter:   *serverVoter,
		})
		if err != nil {
			return fmt.Errorf("raft add server: %v", err)
		}
		println("added server", *serverId)
		return nil
	})

	return err

}
