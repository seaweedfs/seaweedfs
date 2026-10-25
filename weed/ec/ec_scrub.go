package ec

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ScrubEcVolumes asks each volume server to scrub its EC volumes (optionally
// narrowed to volumeIds) in the given mode, aggregating and reporting results.
func ScrubEcVolumes(env *Env, writer io.Writer, volumeServerAddrs []pb.ServerAddress, volumeIds []uint32, mode volume_server_pb.VolumeScrubMode, forceDeletedNeedlesCheck bool, maxParallelization int, showDetails bool) error {
	var brokenVolumesStr, brokenShardsStr []string
	var details []string
	var totalVolumes, brokenVolumes, brokenShards, totalFiles uint64
	var mu sync.Mutex

	ewg := util.NewErrorWaitGroup(maxParallelization)
	count := 0
	for _, addr := range volumeServerAddrs {
		ewg.Add(func() error {
			mu.Lock()
			count++
			fmt.Fprintf(writer, "Scrubbing %s (%d/%d)...\n", addr.String(), count, len(volumeServerAddrs))
			mu.Unlock()

			err := operation.WithVolumeServerClient(false, addr, env.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				res, err := volumeServerClient.ScrubEcVolume(context.Background(), &volume_server_pb.ScrubEcVolumeRequest{
					Mode:                     mode,
					VolumeIds:                volumeIds,
					ForceDeletedNeedlesCheck: forceDeletedNeedlesCheck,
				})
				if err != nil {
					return err
				}

				mu.Lock()
				defer mu.Unlock()

				totalVolumes += res.GetTotalVolumes()
				totalFiles += res.GetTotalFiles()
				brokenVolumes += uint64(len(res.GetBrokenVolumeIds()))
				brokenShards += uint64(len(res.GetBrokenShardInfos()))
				for _, d := range res.GetDetails() {
					details = append(details, fmt.Sprintf("[%s] %s", addr, d))
				}
				for _, vid := range res.GetBrokenVolumeIds() {
					brokenVolumesStr = append(brokenVolumesStr, fmt.Sprintf("%s:%v", addr, vid))
				}
				for _, si := range res.GetBrokenShardInfos() {
					brokenShardsStr = append(brokenShardsStr, fmt.Sprintf("%s:%v:%v", addr, si.VolumeId, si.ShardId))
				}

				return nil
			})
			return err
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	fmt.Fprintf(writer, "Scrubbed %d EC files and %d volumes on %d nodes\n", totalFiles, totalVolumes, len(volumeServerAddrs))
	if brokenVolumes != 0 {
		fmt.Fprintf(writer, "\nGot scrub failures on %d EC volumes and %d EC shards :(\n", brokenVolumes, brokenShards)
		fmt.Fprintf(writer, "Affected volumes: %s\n", strings.Join(brokenVolumesStr, ", "))
		if len(brokenShardsStr) != 0 {
			fmt.Fprintf(writer, "Affected shards:  %s\n", strings.Join(brokenShardsStr, ", "))
		}
		if showDetails && len(details) != 0 {
			fmt.Fprintf(writer, "Details:\n\t%s\n", strings.Join(details, "\n\t"))
		}
	}
	return nil
}
