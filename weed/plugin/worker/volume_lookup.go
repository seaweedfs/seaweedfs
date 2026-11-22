package pluginworker

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LookupVolumeLocations asks the masters for the current registered locations
// of a volume, bypassing any client-side caches. Execution handlers use it to
// re-check a proposal against the live topology, since a proposal can go
// stale between detection and execution. Returns the location URLs
// (host:port) the master reports; an empty slice means the volume is not
// registered anywhere.
func LookupVolumeLocations(ctx context.Context, masterAddresses []string, grpcDialOption grpc.DialOption, volumeID uint32) ([]string, error) {
	if grpcDialOption == nil {
		return nil, fmt.Errorf("grpc dial option is not configured")
	}
	if len(masterAddresses) == 0 {
		return nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	vid := strconv.FormatUint(uint64(volumeID), 10)
	var lastErr error
	for _, address := range masterAddresses {
		for _, candidate := range MasterAddressCandidates(address) {
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
			resp, callErr := client.LookupVolume(callCtx, &master_pb.LookupVolumeRequest{
				VolumeOrFileIds: []string{vid},
			})
			cancelCall()
			_ = conn.Close()
			if callErr != nil {
				lastErr = callErr
				continue
			}

			for _, vidLocation := range resp.VolumeIdLocations {
				if vidLocation.VolumeOrFileId != vid {
					continue
				}
				locations := make([]string, 0, len(vidLocation.Locations))
				for _, location := range vidLocation.Locations {
					locations = append(locations, location.Url)
				}
				return locations, nil
			}
			// The master answered but does not know the volume.
			return nil, nil
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no valid master address candidate")
	}
	return nil, lastErr
}

// LookupEcVolumeLocations asks the masters which servers currently hold EC
// shards of a volume. An empty slice means the master knows no shard of it.
func LookupEcVolumeLocations(ctx context.Context, masterAddresses []string, grpcDialOption grpc.DialOption, volumeID uint32) ([]pb.ServerAddress, error) {
	if grpcDialOption == nil {
		return nil, fmt.Errorf("grpc dial option is not configured")
	}
	if len(masterAddresses) == 0 {
		return nil, fmt.Errorf("no master addresses provided in cluster context")
	}

	notFound := fmt.Sprintf("ec volume %d not found", volumeID)
	var lastErr error
	for _, address := range masterAddresses {
		for _, candidate := range MasterAddressCandidates(address) {
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
			resp, callErr := client.LookupEcVolume(callCtx, &master_pb.LookupEcVolumeRequest{VolumeId: volumeID})
			cancelCall()
			_ = conn.Close()
			if callErr != nil {
				if strings.Contains(status.Convert(callErr).Message(), notFound) {
					return nil, nil
				}
				lastErr = callErr
				continue
			}

			var holders []pb.ServerAddress
			for _, shardLocations := range resp.ShardIdLocations {
				for _, location := range shardLocations.Locations {
					holder := pb.NewServerAddressFromLocation(location)
					if !slices.ContainsFunc(holders, holder.Equals) {
						holders = append(holders, holder)
					}
				}
			}
			return holders, nil
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no valid master address candidate")
	}
	return nil, lastErr
}
