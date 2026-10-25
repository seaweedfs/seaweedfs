package dash

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// mountClientTypes are the metadata-subscriber client types that represent a
// FUSE/VFS mount: the Go `weed mount` ("mount") and the Rust VFS ("sw-vfs").
var mountClientTypes = []string{"mount", "sw-vfs"}

// mountClientsFilerTimeout bounds each per-filer ListMetadataSubscribers call so
// a slow or unreachable filer can't stall the admin dashboard.
const mountClientsFilerTimeout = 5 * time.Second

// GetMountClients queries every filer for its connected mount subscribers and
// aggregates them. Each filer only knows the clients connected to itself, so a
// cluster-wide view requires fanning out. The filers are queried concurrently,
// each under a short timeout, and an unreachable filer is skipped.
func (s *AdminServer) GetMountClients() (*MountClientsData, error) {
	var (
		mu      sync.Mutex
		clients []MountClient
		wg      sync.WaitGroup
	)

	for _, filerAddr := range s.GetAllFilers() {
		wg.Add(1)
		go func(filerAddr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), mountClientsFilerTimeout)
			defer cancel()
			err := pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddr), s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				resp, err := client.ListMetadataSubscribers(ctx, &filer_pb.ListMetadataSubscribersRequest{
					ClientTypes: mountClientTypes,
				})
				if err != nil {
					return err
				}
				mu.Lock()
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
				mu.Unlock()
				return nil
			})
			if err != nil {
				glog.Warningf("list mount clients from filer %s: %v", filerAddr, err)
			}
		}(filerAddr)
	}
	wg.Wait()

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
