package dash

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
)

type DashboardServer struct {
	masterAddresses []pb.ServerAddress
	filerAddress    pb.ServerAddress
	grpcDialOption  grpc.DialOption
	adminUser       string
	adminPassword   string
	cacheExpiration time.Duration
	lastCacheUpdate time.Time
	cachedTopology  *ClusterTopology
}

type ClusterTopology struct {
	Masters       []MasterNode   `json:"masters"`
	DataCenters   []DataCenter   `json:"datacenters"`
	VolumeServers []VolumeServer `json:"volume_servers"`
	TotalVolumes  int            `json:"total_volumes"`
	TotalFiles    int64          `json:"total_files"`
	TotalSize     int64          `json:"total_size"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type MasterNode struct {
	Address  string `json:"address"`
	IsLeader bool   `json:"is_leader"`
	Status   string `json:"status"`
}

type DataCenter struct {
	ID    string `json:"id"`
	Racks []Rack `json:"racks"`
}

type Rack struct {
	ID    string         `json:"id"`
	Nodes []VolumeServer `json:"nodes"`
}

type VolumeServer struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	DataCenter    string    `json:"datacenter"`
	Rack          string    `json:"rack"`
	PublicURL     string    `json:"public_url"`
	Volumes       int       `json:"volumes"`
	MaxVolumes    int       `json:"max_volumes"`
	DiskUsage     int64     `json:"disk_usage"`
	DiskCapacity  int64     `json:"disk_capacity"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Status        string    `json:"status"`
}

func NewDashboardServer(
	masterAddresses []pb.ServerAddress,
	filerAddress pb.ServerAddress,
	grpcDialOption grpc.DialOption,
	adminUser, adminPassword string,
) *DashboardServer {
	return &DashboardServer{
		masterAddresses: masterAddresses,
		filerAddress:    filerAddress,
		grpcDialOption:  grpcDialOption,
		adminUser:       adminUser,
		adminPassword:   adminPassword,
		cacheExpiration: 30 * time.Second,
	}
}

// WithMasterClient executes a function with a master client connection
func (s *DashboardServer) WithMasterClient(f func(client master_pb.SeaweedClient) error) error {
	return pb.WithMasterClient(false, s.masterAddresses[0], s.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		return f(client)
	})
}

// WithFilerClient executes a function with a filer client connection
func (s *DashboardServer) WithFilerClient(f func(client filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcFilerClient(false, 0, s.filerAddress, s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return f(client)
	})
}

// WithVolumeServerClient executes a function with a volume server client connection
func (s *DashboardServer) WithVolumeServerClient(address pb.ServerAddress, f func(client volume_server_pb.VolumeServerClient) error) error {
	return operation.WithVolumeServerClient(false, address, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		return f(client)
	})
}

// GetClusterTopology returns the current cluster topology with caching
func (s *DashboardServer) GetClusterTopology() (*ClusterTopology, error) {
	now := time.Now()
	if s.cachedTopology != nil && now.Sub(s.lastCacheUpdate) < s.cacheExpiration {
		return s.cachedTopology, nil
	}

	topology := &ClusterTopology{
		UpdatedAt: now,
	}

	// Get cluster status from master
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		// Process topology information
		dcMap := make(map[string]*DataCenter)
		rackMap := make(map[string]*Rack)

		for _, topologyInfo := range resp.TopologyInfo.DataCenterInfos {
			dc := &DataCenter{
				ID:    topologyInfo.Id,
				Racks: []Rack{},
			}
			dcMap[dc.ID] = dc

			for _, rackInfo := range topologyInfo.RackInfos {
				rack := &Rack{
					ID:    rackInfo.Id,
					Nodes: []VolumeServer{},
				}
				rackMap[fmt.Sprintf("%s-%s", dc.ID, rack.ID)] = rack

				for _, nodeInfo := range rackInfo.DataNodeInfos {
					// Calculate totals from all disk infos
					var totalVolumes, totalMaxVolumes int64
					for _, diskInfo := range nodeInfo.DiskInfos {
						totalVolumes += diskInfo.VolumeCount
						totalMaxVolumes += diskInfo.MaxVolumeCount
					}

					vs := VolumeServer{
						ID:            nodeInfo.Id,
						Address:       nodeInfo.Id,
						DataCenter:    dc.ID,
						Rack:          rack.ID,
						PublicURL:     nodeInfo.Id,
						Volumes:       int(totalVolumes),
						MaxVolumes:    int(totalMaxVolumes),
						LastHeartbeat: now,
						Status:        "active",
					}
					rack.Nodes = append(rack.Nodes, vs)
					topology.VolumeServers = append(topology.VolumeServers, vs)
					topology.TotalVolumes += vs.Volumes
				}
				dc.Racks = append(dc.Racks, *rack)
			}
			topology.DataCenters = append(topology.DataCenters, *dc)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get cluster topology: %v", err)
	}

	// Cache the result
	s.cachedTopology = topology
	s.lastCacheUpdate = now

	return topology, nil
}

// InvalidateCache forces a refresh of cached data
func (s *DashboardServer) InvalidateCache() {
	s.lastCacheUpdate = time.Time{}
	s.cachedTopology = nil
}
