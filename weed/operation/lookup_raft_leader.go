package operation

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

// LookupRaftLeaderMaster returns the current raft leader by querying configured
// master peers. Followers answer GetMasterConfiguration with the leader address;
// topology mutations such as VolumeMarkReadonly must run on that node.
//
// The volume server's heartbeat peer (GetMaster) may still be a follower after an
// election until the heartbeat loop reconnects.
func LookupRaftLeaderMaster(ctx context.Context, masters []pb.ServerAddress, grpcDialOption grpc.DialOption) (pb.ServerAddress, error) {
	if len(masters) == 0 {
		return "", fmt.Errorf("no master peers configured")
	}
	var lastErr error
	for _, peer := range masters {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var leader pb.ServerAddress
		err := WithMasterServerClient(ctx, false, peer, grpcDialOption, func(client master_pb.SeaweedClient) error {
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			resp, err := client.GetMasterConfiguration(callCtx, &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return err
			}
			if resp.Leader == "" {
				return fmt.Errorf("master %s returned empty leader", peer)
			}
			leader = pb.ServerAddress(resp.Leader)
			return nil
		})
		if err == nil && leader != "" {
			return leader, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return "", fmt.Errorf("lookup raft leader from %d master peer(s): %w", len(masters), lastErr)
	}
	return "", fmt.Errorf("lookup raft leader: no leader from %d master peer(s)", len(masters))
}
