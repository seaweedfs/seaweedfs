package dash

import (
	"context"
	"sort"
	"time"

	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type VectorNode struct {
	Address string `json:"address"`
	Version string `json:"version"`
	Status  string `json:"status"`
}

type VectorOperationsData struct {
	Engines            []VectorNode             `json:"engines"`
	DiscoveryError     string                   `json:"discovery_error,omitempty"`
	MaintenanceWorkers int                      `json:"maintenance_workers"`
	MaintenanceJobs    []adminplugin.TrackedJob `json:"maintenance_jobs"`
}

func (s *AdminServer) getVectorNodesStatus(ctx context.Context) VectorOperationsData {
	data := VectorOperationsData{Engines: []VectorNode{}}
	if s.masterClient == nil {
		data.DiscoveryError = "Master discovery unavailable"
		return data
	}
	var discoveryErr error
	for _, master := range s.masterClient.ListCachedMasters() {
		discoveryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := pb.GrpcDial(discoveryCtx, master.ToGrpcAddress(), false, s.grpcDialOption)
		if err == nil {
			response, callErr := master_pb.NewSeaweedClient(conn).ListClusterNodes(discoveryCtx, s.listClusterNodesRequest(cluster.VectorType))
			_ = conn.Close()
			err = callErr
			if err == nil {
				for _, node := range response.ClusterNodes {
					data.Engines = append(data.Engines, VectorNode{Address: node.Address, Version: node.Version, Status: "Registered"})
				}
			}
		}
		cancel()
		if err != nil {
			discoveryErr = err
		}
		if len(data.Engines) > 0 {
			break
		}
	}
	if len(data.Engines) == 0 && discoveryErr != nil {
		data.DiscoveryError = discoveryErr.Error()
		return data
	}
	var checks errgroup.Group
	checks.SetLimit(8)
	for i := range data.Engines {
		checks.Go(func() error {
			data.Engines[i].Status = vectorNodeStatus(ctx, data.Engines[i].Address, s.grpcDialOption)
			return nil
		})
	}
	_ = checks.Wait()
	sort.Slice(data.Engines, func(i, j int) bool { return data.Engines[i].Address < data.Engines[j].Address })
	return data
}

func vectorNodeStatus(ctx context.Context, address string, dialOption grpc.DialOption) string {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	conn, err := pb.GrpcDial(ctx, address, false, dialOption)
	if err != nil {
		return "Unreachable"
	}
	defer conn.Close()
	response, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "vector_pb.VectorService"})
	if status.Code(err) == codes.Unimplemented || status.Code(err) == codes.NotFound {
		return "Registered (health unavailable)"
	}
	if err != nil {
		return "Unreachable"
	}
	if response.Status == grpc_health_v1.HealthCheckResponse_SERVING {
		return "Ready"
	}
	return "Not ready"
}

func (s *AdminServer) GetVectorOperationsData(ctx context.Context) VectorOperationsData {
	data := s.getVectorNodesStatus(ctx)
	for _, worker := range s.GetPluginWorkers() {
		if capability := worker.Capabilities["lance_vector_index"]; capability != nil && capability.CanExecute {
			data.MaintenanceWorkers++
		}
	}
	data.MaintenanceJobs = s.ListPluginJobs("lance_vector_index", "", 5)
	return data
}
