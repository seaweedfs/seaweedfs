package dash

import (
	"context"
	"fmt"
	"net"
	"os"
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
	return s.masterClient.WithClient(false, f)
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
		currentMaster := s.masterClient.GetMaster(context.Background())
		glog.Warningf("Failed to discover filers from master %s: %v", currentMaster, err)
		// Return cached filers even if expired, better than nothing
		return s.cachedFilers
	}

	// Update cache
	s.cachedFilers = filers
	s.lastFilerUpdate = time.Now()

	// Update IAM Client endpoint if needed
	if s.IamClient != nil {
		iamEndpoint := s.getIamEndpoint()
		if iamEndpoint != "" {
			s.IamClient.SetEndpoint(iamEndpoint)
		}
	}

	return filers
}

// getIamEndpoint returns the IAM endpoint (S3 API) based on discovered filers
func (s *AdminServer) getIamEndpoint() string {
	// Check if fixed endpoint is configured in env
	if envEndpoint := os.Getenv("IAM_ENDPOINT"); envEndpoint != "" {
		return envEndpoint
	}

	filers := s.getDiscoveredFilers()
	if len(filers) == 0 {
		return ""
	}

	// Use the first discovered filer but change port to 8333 (default S3/IAM port)
	// If the filer address has a port, we replace it.
	filerAddr := filers[0]
	host, _, err := net.SplitHostPort(filerAddr)
	if err != nil {
		// If no port, assume filerAddr is the host
		host = filerAddr
	}

	// Read IAM_PORT from environment or default to 8333
	iamPort := os.Getenv("IAM_PORT")
	if iamPort == "" {
		iamPort = "8333"
	}

	return fmt.Sprintf("http://%s:%s", host, iamPort)
}

// GetAllFilers returns all discovered filers
func (s *AdminServer) GetAllFilers() []string {
	return s.getDiscoveredFilers()
}
