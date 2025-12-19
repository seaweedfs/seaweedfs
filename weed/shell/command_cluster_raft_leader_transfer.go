package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandRaftLeaderTransfer{})
}

type commandRaftLeaderTransfer struct{}

func (c *commandRaftLeaderTransfer) Name() string {
	return "cluster.raft.leader.transfer"
}

func (c *commandRaftLeaderTransfer) Help() string {
	return `transfer raft leadership to another master server

	This command initiates a graceful leadership transfer from the current
	leader to another server. Use this before performing maintenance on
	the current leader to reduce errors in filers and other components.

	Examples:
		# Transfer to any eligible follower (auto-selection)
		cluster.raft.leader.transfer

		# Transfer to a specific server
		cluster.raft.leader.transfer -id <server_id> -address <server_grpc_address>

	Notes:
		- This command must be sent to the current leader
		- The target server must be a voting member of the raft cluster
		- Use 'cluster.raft.ps' to list available servers and identify the leader
`
}

func (c *commandRaftLeaderTransfer) HasTag(CommandTag) bool {
	return false
}

func (c *commandRaftLeaderTransfer) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	leaderTransferCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	targetId := leaderTransferCommand.String("id", "", "target server id (optional)")
	targetAddress := leaderTransferCommand.String("address", "", "target server grpc address (required if -id is specified)")

	if err := leaderTransferCommand.Parse(args); err != nil {
		return nil
	}

	// Validate: if id is specified, address must also be specified
	if *targetId != "" && *targetAddress == "" {
		return fmt.Errorf("-address is required when -id is specified")
	}

	// First, show current cluster status
	fmt.Fprintf(writer, "Checking current raft cluster status...\n")

	var currentLeader string
	err := commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.RaftListClusterServers(ctx, &master_pb.RaftListClusterServersRequest{})
		if err != nil {
			return fmt.Errorf("failed to list cluster servers: %v", err)
		}

		if len(resp.ClusterServers) == 0 {
			fmt.Fprintf(writer, "No raft cluster configured (single master mode)\n")
			return fmt.Errorf("leadership transfer not available in single master mode")
		}

		fmt.Fprintf(writer, "Raft cluster has %d servers:\n", len(resp.ClusterServers))
		for _, server := range resp.ClusterServers {
			suffix := ""
			if server.IsLeader {
				suffix = " <- current leader"
				currentLeader = server.Id
			}
			fmt.Fprintf(writer, "  %s %s [%s]%s\n", server.Id, server.Address, server.Suffrage, suffix)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if currentLeader == "" {
		return fmt.Errorf("no leader found in cluster")
	}

	// Perform the transfer
	targetDesc := "any eligible follower"
	if *targetId != "" {
		targetDesc = fmt.Sprintf("server %s (%s)", *targetId, *targetAddress)
	}
	fmt.Fprintf(writer, "\nTransferring leadership from %s to %s...\n", currentLeader, targetDesc)

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.RaftLeadershipTransfer(ctx, &master_pb.RaftLeadershipTransferRequest{
			TargetId:      *targetId,
			TargetAddress: *targetAddress,
		})
		if err != nil {
			return fmt.Errorf("leadership transfer failed: %v", err)
		}

		fmt.Fprintf(writer, "Leadership successfully transferred.\n")
		fmt.Fprintf(writer, "  Previous leader: %s\n", resp.PreviousLeader)
		fmt.Fprintf(writer, "  New leader: %s\n", resp.NewLeader)
		return nil
	})

	if err != nil {
		fmt.Fprintf(writer, "\nLeadership transfer failed: %v\n", err)
		fmt.Fprintf(writer, "\nTroubleshooting:\n")
		fmt.Fprintf(writer, "  - Ensure you are connected to the current leader\n")
		fmt.Fprintf(writer, "  - Ensure target server is a voting member (use 'cluster.raft.ps')\n")
		fmt.Fprintf(writer, "  - Ensure target server is healthy and reachable\n")
		return err
	}

	return nil
}

