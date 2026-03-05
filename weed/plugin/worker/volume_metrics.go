package pluginworker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
)

func collectVolumeMetricsFromMasters(
	ctx context.Context,
	masterAddresses []string,
	collectionFilter string,
	grpcDialOption grpc.DialOption,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	if grpcDialOption == nil {
		return nil, nil, fmt.Errorf("grpc dial option is not configured")
	}
	if len(masterAddresses) == 0 {
		return nil, nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	for _, masterAddress := range masterAddresses {
		response, err := fetchVolumeList(ctx, masterAddress, grpcDialOption)
		if err != nil {
			glog.Warningf("Plugin worker failed master volume list at %s: %v", masterAddress, err)
			continue
		}

		metrics, activeTopology, buildErr := buildVolumeMetrics(response, collectionFilter)
		if buildErr != nil {
			glog.Warningf("Plugin worker failed to build metrics from master %s: %v", masterAddress, buildErr)
			continue
		}
		return metrics, activeTopology, nil
	}

	return nil, nil, fmt.Errorf("failed to load topology from all provided masters")
}

func fetchVolumeList(ctx context.Context, address string, grpcDialOption grpc.DialOption) (*master_pb.VolumeListResponse, error) {
	var lastErr error
	for _, candidate := range masterAddressCandidates(address) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
		conn, err := pb.GrpcDial(dialCtx, candidate, false, grpcDialOption)
		cancelDial()
		if err != nil {
			lastErr = err
			continue
		}

		client := master_pb.NewSeaweedClient(conn)
		callCtx, cancelCall := context.WithTimeout(ctx, 10*time.Second)
		response, callErr := client.VolumeList(callCtx, &master_pb.VolumeListRequest{})
		cancelCall()
		_ = conn.Close()

		if callErr == nil {
			return response, nil
		}
		lastErr = callErr
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no valid master address candidate")
	}
	return nil, lastErr
}

func buildVolumeMetrics(
	response *master_pb.VolumeListResponse,
	collectionFilter string,
) ([]*workertypes.VolumeHealthMetrics, *topology.ActiveTopology, error) {
	if response == nil || response.TopologyInfo == nil {
		return nil, nil, fmt.Errorf("volume list response has no topology info")
	}

	activeTopology := topology.NewActiveTopology(10)
	if err := activeTopology.UpdateTopology(response.TopologyInfo); err != nil {
		return nil, nil, err
	}

	patterns := wildcard.CompileWildcardMatchers(collectionFilter)
	volumeSizeLimitBytes := uint64(response.VolumeSizeLimitMb) * 1024 * 1024
	now := time.Now()
	metrics := make([]*workertypes.VolumeHealthMetrics, 0, 256)

	for _, dc := range response.TopologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for diskType, diskInfo := range node.DiskInfos {
					for _, volume := range diskInfo.VolumeInfos {
						if !wildcard.MatchesAnyWildcard(patterns, volume.Collection) {
							continue
						}

						metric := &workertypes.VolumeHealthMetrics{
							VolumeID:         volume.Id,
							Server:           node.Id,
							ServerAddress:    string(pb.NewServerAddressFromDataNode(node)),
							DiskType:         diskType,
							DiskId:           volume.DiskId,
							DataCenter:       dc.Id,
							Rack:             rack.Id,
							Collection:       volume.Collection,
							Size:             volume.Size,
							DeletedBytes:     volume.DeletedByteCount,
							LastModified:     time.Unix(volume.ModifiedAtSecond, 0),
							ReplicaCount:     1,
							ExpectedReplicas: int(volume.ReplicaPlacement),
							IsReadOnly:       volume.ReadOnly,
						}
						if metric.Size > 0 {
							metric.GarbageRatio = float64(metric.DeletedBytes) / float64(metric.Size)
						}
						if volumeSizeLimitBytes > 0 {
							metric.FullnessRatio = float64(metric.Size) / float64(volumeSizeLimitBytes)
						}
						metric.Age = now.Sub(metric.LastModified)
						metrics = append(metrics, metric)
					}
				}
			}
		}
	}

	replicaCounts := make(map[uint32]int)
	for _, metric := range metrics {
		replicaCounts[metric.VolumeID]++
	}
	for _, metric := range metrics {
		metric.ReplicaCount = replicaCounts[metric.VolumeID]
	}

	return metrics, activeTopology, nil
}

func masterAddressCandidates(address string) []string {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return nil
	}
	candidateSet := map[string]struct{}{
		trimmed: {},
	}
	converted := pb.ServerToGrpcAddress(trimmed)
	candidateSet[converted] = struct{}{}

	candidates := make([]string, 0, len(candidateSet))
	for candidate := range candidateSet {
		candidates = append(candidates, candidate)
	}
	sort.Strings(candidates)
	return candidates
}
