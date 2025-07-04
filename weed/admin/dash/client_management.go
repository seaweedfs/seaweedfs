package dash

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// WithMasterClient executes a function with a master client connection
func (s *AdminServer) WithMasterClient(f func(client master_pb.SeaweedClient) error) error {
	masterAddr := pb.ServerAddress(s.masterAddress)

	return pb.WithMasterClient(false, masterAddr, s.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		return f(client)
	})
}

// WithFilerClient executes a function with a filer client connection
func (s *AdminServer) WithFilerClient(f func(client filer_pb.SeaweedFilerClient) error) error {
	filerAddr := s.GetFilerAddress()
	if filerAddr == "" {
		return fmt.Errorf("no filer available")
	}

	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddr), s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return f(client)
	})
}

// WithVolumeServerClient executes a function with a volume server client connection
func (s *AdminServer) WithVolumeServerClient(address pb.ServerAddress, f func(client volume_server_pb.VolumeServerClient) error) error {
	return operation.WithVolumeServerClient(false, address, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		return f(client)
	})
}

// GetFilerAddress returns a filer address, discovering from masters if needed
func (s *AdminServer) GetFilerAddress() string {
	// Discover filers from masters
	filers := s.getDiscoveredFilers()
	if len(filers) > 0 {
		return filers[0] // Return the first available filer
	}

	return ""
}

// getDiscoveredFilers returns cached filers or discovers them from masters
func (s *AdminServer) getDiscoveredFilers() []string {
	// Check if cache is still valid
	if time.Since(s.lastFilerUpdate) < s.filerCacheExpiration && len(s.cachedFilers) > 0 {
		return s.cachedFilers
	}

	// Discover filers from masters
	var filers []string
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})
		if err != nil {
			return err
		}

		for _, node := range resp.ClusterNodes {
			filers = append(filers, node.Address)
		}

		return nil
	})

	if err != nil {
		glog.Warningf("Failed to discover filers from master %s: %v", s.masterAddress, err)
		// Return cached filers even if expired, better than nothing
		return s.cachedFilers
	}

	// Update cache
	s.cachedFilers = filers
	s.lastFilerUpdate = time.Now()

	return filers
}

// GetAllFilers returns all discovered filers
func (s *AdminServer) GetAllFilers() []string {
	return s.getDiscoveredFilers()
}
