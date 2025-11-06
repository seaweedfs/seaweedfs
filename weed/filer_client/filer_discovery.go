package filer_client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

const (
	// FilerDiscoveryInterval is the interval for refreshing filer list from masters
	FilerDiscoveryInterval = 30 * time.Second
	// InitialDiscoveryInterval is the faster interval for initial discovery
	InitialDiscoveryInterval = 5 * time.Second
	// InitialDiscoveryRetries is the number of fast retries during startup
	InitialDiscoveryRetries = 6 // 6 retries * 5 seconds = 30 seconds total
)

// FilerDiscoveryService handles dynamic discovery and refresh of filers from masters
type FilerDiscoveryService struct {
	masters        []pb.ServerAddress
	grpcDialOption grpc.DialOption
	filers         []pb.ServerAddress
	filersMutex    sync.RWMutex
	refreshTicker  *time.Ticker
	stopChan       chan struct{}
	wg             sync.WaitGroup
	initialRetries int
}

// NewFilerDiscoveryService creates a new filer discovery service
func NewFilerDiscoveryService(masters []pb.ServerAddress, grpcDialOption grpc.DialOption) *FilerDiscoveryService {
	return &FilerDiscoveryService{
		masters:        masters,
		grpcDialOption: grpcDialOption,
		filers:         make([]pb.ServerAddress, 0),
		stopChan:       make(chan struct{}),
	}
}

// No need for convertHTTPToGRPC - pb.ServerAddress.ToGrpcAddress() already handles this

// discoverFilersFromMaster discovers filers from a single master
func (fds *FilerDiscoveryService) discoverFilersFromMaster(masterAddr pb.ServerAddress) ([]pb.ServerAddress, error) {
	// Convert HTTP master address to gRPC address (HTTP port + 10000)
	grpcAddr := masterAddr.ToGrpcAddress()

	conn, err := grpc.NewClient(grpcAddr, fds.grpcDialOption)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master at %s: %v", grpcAddr, err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
		ClientType: cluster.FilerType,
	})
	if err != nil {
		glog.Errorf("FILER DISCOVERY: ListClusterNodes failed for master %s: %v", masterAddr, err)
		return nil, fmt.Errorf("failed to list filers from master %s: %v", masterAddr, err)
	}

	var filers []pb.ServerAddress
	for _, node := range resp.ClusterNodes {
		// Return HTTP address (lock client will convert to gRPC when needed)
		filers = append(filers, pb.ServerAddress(node.Address))
	}

	return filers, nil
}

// refreshFilers discovers filers from all masters and updates the filer list
func (fds *FilerDiscoveryService) refreshFilers() {
	glog.V(2).Info("Refreshing filer list from masters")

	var allFilers []pb.ServerAddress
	var discoveryErrors []error

	// Try each master to discover filers
	for _, masterAddr := range fds.masters {
		filers, err := fds.discoverFilersFromMaster(masterAddr)
		if err != nil {
			discoveryErrors = append(discoveryErrors, err)
			glog.V(1).Infof("Failed to discover filers from master %s: %v", masterAddr, err)
			continue
		}

		allFilers = append(allFilers, filers...)
		glog.V(2).Infof("Discovered %d filers from master %s", len(filers), masterAddr)
	}

	// Deduplicate filers
	filerSet := make(map[pb.ServerAddress]bool)
	for _, filer := range allFilers {
		filerSet[filer] = true
	}

	uniqueFilers := make([]pb.ServerAddress, 0, len(filerSet))
	for filer := range filerSet {
		uniqueFilers = append(uniqueFilers, filer)
	}

	// Update the filer list
	fds.filersMutex.Lock()
	oldCount := len(fds.filers)
	fds.filers = uniqueFilers
	newCount := len(fds.filers)
	fds.filersMutex.Unlock()

	if newCount > 0 {
		glog.V(1).Infof("Filer discovery successful: updated from %d to %d filers", oldCount, newCount)
	} else if len(discoveryErrors) > 0 {
		glog.Warningf("Failed to discover any filers from %d masters, keeping existing %d filers", len(fds.masters), oldCount)
	}
}

// GetFilers returns the current list of filers
func (fds *FilerDiscoveryService) GetFilers() []pb.ServerAddress {
	fds.filersMutex.RLock()
	defer fds.filersMutex.RUnlock()

	// Return a copy to avoid concurrent modification
	filers := make([]pb.ServerAddress, len(fds.filers))
	copy(filers, fds.filers)
	return filers
}

// Start begins the filer discovery service
func (fds *FilerDiscoveryService) Start() error {
	glog.V(1).Info("Starting filer discovery service")

	// Initial discovery
	fds.refreshFilers()

	// Start with faster discovery during startup
	fds.initialRetries = InitialDiscoveryRetries
	interval := InitialDiscoveryInterval
	if len(fds.GetFilers()) > 0 {
		// If we found filers immediately, use normal interval
		interval = FilerDiscoveryInterval
		fds.initialRetries = 0
	}

	// Start periodic refresh
	fds.refreshTicker = time.NewTicker(interval)
	fds.wg.Add(1)
	go func() {
		defer fds.wg.Done()
		for {
			select {
			case <-fds.refreshTicker.C:
				fds.refreshFilers()

				// Switch to normal interval after initial retries
				if fds.initialRetries > 0 {
					fds.initialRetries--
					if fds.initialRetries == 0 || len(fds.GetFilers()) > 0 {
						glog.V(1).Info("Switching to normal filer discovery interval")
						fds.refreshTicker.Stop()
						fds.refreshTicker = time.NewTicker(FilerDiscoveryInterval)
					}
				}
			case <-fds.stopChan:
				glog.V(1).Info("Filer discovery service stopping")
				return
			}
		}
	}()

	return nil
}

// Stop stops the filer discovery service
func (fds *FilerDiscoveryService) Stop() error {
	glog.V(1).Info("Stopping filer discovery service")

	close(fds.stopChan)
	if fds.refreshTicker != nil {
		fds.refreshTicker.Stop()
	}
	fds.wg.Wait()

	return nil
}
