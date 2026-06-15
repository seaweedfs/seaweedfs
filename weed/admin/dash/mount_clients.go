package dash

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// mountClientTypes are the metadata-subscriber client types that represent a
// FUSE/VFS mount: the Go `weed mount` ("mount") and the Rust VFS ("sw-vfs").
var mountClientTypes = []string{"mount", "sw-vfs"}

// GetMountClients queries every filer for its connected mount subscribers and
// aggregates them. Each filer only knows the clients connected to itself, so a
// cluster-wide view requires fanning out. An unreachable filer is skipped.
func (s *AdminServer) GetMountClients() (*MountClientsData, error) {
	var clients []MountClient

	for _, filerAddr := range s.GetAllFilers() {
		err := pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddr), s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.ListMetadataSubscribers(context.Background(), &filer_pb.ListMetadataSubscribersRequest{
				ClientTypes: mountClientTypes,
			})
			if err != nil {
				return err
			}
			for _, sub := range resp.Subscribers {
				clients = append(clients, MountClient{
					ClientName:   sub.ClientName,
					ClientType:   sub.ClientType,
					Address:      sub.Address,
					PathPrefix:   sub.PathPrefix,
					ClientId:     sub.ClientId,
					ConnectedAt:  time.Unix(0, sub.ConnectedAtNs),
					FilerAddress: sub.FilerAddress,
				})
			}
			return nil
		})
		if err != nil {
			glog.Warningf("list mount clients from filer %s: %v", filerAddr, err)
			continue
		}
	}

	sort.Slice(clients, func(i, j int) bool {
		if clients[i].Address != clients[j].Address {
			return clients[i].Address < clients[j].Address
		}
		return clients[i].PathPrefix < clients[j].PathPrefix
	})

	return &MountClientsData{
		MountClients:      clients,
		TotalMountClients: len(clients),
		LastUpdated:       time.Now(),
	}, nil
}
