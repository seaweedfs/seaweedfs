package multi_master

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// A filer only learns about its peers from the cluster node updates on its
// KeepConnected stream, and those are broadcast to whoever is connected at that
// moment. A filer that reconnects has to be told the membership again, or it
// never subscribes to the peers that registered while it was away.
func TestKeepConnectedSendsExistingFilers(t *testing.T) {
	mc := StartMasterCluster(t)

	leaderIdx, leaderAddr := mc.FindLeader()
	if leaderIdx < 0 {
		t.Fatal("no leader")
	}
	master := pb.ServerAddress(leaderAddr)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	const existingFiler = "127.0.0.1:18888"
	const joiningFiler = "127.0.0.1:18889"

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	err := pb.WithMasterClient(ctx, true, master, dialOption, false, func(client master_pb.SeaweedClient) error {
		stream, err := client.KeepConnected(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&master_pb.KeepConnectedRequest{
			ClientType:    cluster.FilerType,
			ClientAddress: existingFiler,
		}); err != nil {
			return err
		}
		if err := waitForClusterNode(ctx, client, existingFiler); err != nil {
			return err
		}

		return pb.WithMasterClient(ctx, true, master, dialOption, false, func(joining master_pb.SeaweedClient) error {
			joiningCtx, cancelJoining := context.WithTimeout(ctx, waitTimeout)
			defer cancelJoining()

			joiningStream, err := joining.KeepConnected(joiningCtx)
			if err != nil {
				return err
			}
			if err := joiningStream.Send(&master_pb.KeepConnectedRequest{
				ClientType:    cluster.FilerType,
				ClientAddress: joiningFiler,
			}); err != nil {
				return err
			}
			for i := 0; ; i++ {
				resp, err := joiningStream.Recv()
				if err != nil {
					return err
				}
				// a client only reads the volume locations out of the first
				// message, an update sent ahead of them would be dropped
				if i == 0 && resp.VolumeLocation == nil {
					return fmt.Errorf("first message is not a volume location: %+v", resp)
				}
				if update := resp.ClusterNodeUpdate; update != nil && update.IsAdd && update.Address == existingFiler {
					return nil
				}
			}
		})
	})
	if err != nil {
		mc.DumpLogs()
		t.Fatalf("a joining filer was not told about %s: %v", existingFiler, err)
	}
}

func waitForClusterNode(ctx context.Context, client master_pb.SeaweedClient, address string) error {
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{ClientType: cluster.FilerType})
		if err != nil {
			return err
		}
		for _, node := range resp.ClusterNodes {
			if node.Address == address {
				return nil
			}
		}
		time.Sleep(waitTick)
	}
	return context.DeadlineExceeded
}
