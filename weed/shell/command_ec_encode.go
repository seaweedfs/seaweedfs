package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h]
	ec.encode [-collection=""] [-volumeId=<volume_id>]

	This command will:
	1. freeze one volume
	2. apply erasure coding to the volume
	3. (optionally) re-balance encoded shards across multiple volume servers

	The erasure coding is 10.4. So ideally you have more than 14 volume servers, and you can afford
	to lose 4 volume servers.

	If the number of volumes are not high, the worst case is that you only have 4 volume servers,
	and the shards are spread as 4,4,3,3, respectively. You can afford to lose one volume server.

	If you only have less than 4 volume servers, with erasure coding, at least you can afford to
	have 4 corrupted shard files.

	Re-balancing algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	maxParallelization := encodeCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	forceChanges := encodeCommand.Bool("force", false, "force the encoding even if the cluster has less than recommended 4 nodes")
	shardReplicaPlacement := encodeCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	applyBalancing := encodeCommand.Bool("rebalance", false, "re-balance EC shards after creation")

	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}
	rp, err := parseReplicaPlacementArg(commandEnv, *shardReplicaPlacement)
	if err != nil {
		return err
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	if !*forceChanges {
		var nodeCount int
		eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
			nodeCount++
		})
		if nodeCount < erasure_coding.ParityShardsCount {
			glog.V(0).Infof("skip erasure coding with %d nodes, less than recommended %d nodes", nodeCount, erasure_coding.ParityShardsCount)
			return nil
		}
	}

	var volumeIds []needle.VolumeId
	var balanceCollections []string
	if vid := needle.VolumeId(*volumeId); vid != 0 {
		// volumeId is provided
		volumeIds = append(volumeIds, vid)
		balanceCollections = collectCollectionsForVolumeIds(topologyInfo, volumeIds)
	} else {
		// apply to all volumes for the given collection
		volumeIds, err = collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
		if err != nil {
			return err
		}
		balanceCollections = []string{*collection}
	}

	// encode all requested volumes...
	if err = doEcEncode(commandEnv, *collection, volumeIds, *maxParallelization); err != nil {
		return fmt.Errorf("ec encode for volumes %v: %v", volumeIds, err)
	}
	// ...re-balance ec shards...
	if err := EcBalance(commandEnv, balanceCollections, "", rp, *maxParallelization, *applyBalancing); err != nil {
		return fmt.Errorf("re-balance ec shards for collection(s) %v: %v", balanceCollections, err)
	}
	// ...then delete original volumes.
	if err := doDeleteVolumes(commandEnv, volumeIds, *maxParallelization); err != nil {
		return fmt.Errorf("re-balance ec shards for collection(s) %v: %v", balanceCollections, err)
	}

	return nil
}

func volumeLocations(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, error) {
	res := map[needle.VolumeId][]wdclient.Location{}
	for _, vid := range volumeIds {
		ls, ok := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !ok {
			return nil, fmt.Errorf("volume %d not found", vid)
		}
		res[vid] = ls
	}

	return res, nil
}

func doEcEncode(commandEnv *CommandEnv, collection string, volumeIds []needle.VolumeId, maxParallelization int) error {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}
	locations, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return nil
	}

	// mark volumes as readonly
	ewg := NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		for _, l := range locations[vid] {
			ewg.Add(func() error {
				if err := markVolumeReplicaWritable(commandEnv.option.GrpcDialOption, vid, l, false, false); err != nil {
					return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, l.Url, err)
				}
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	// generate ec shards
	ewg.Reset()
	for i, vid := range volumeIds {
		target := locations[vid][i%len(locations[vid])]
		ewg.Add(func() error {
			if err := generateEcShards(commandEnv.option.GrpcDialOption, vid, collection, target.ServerAddress()); err != nil {
				return fmt.Errorf("generate ec shards for volume %d on %s: %v", vid, target.Url, err)
			}
			return nil
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	// mount all ec shards for the converted volume
	shardIds := make([]uint32, erasure_coding.TotalShardsCount)
	for i := range shardIds {
		shardIds[i] = uint32(i)
	}

	ewg.Reset()
	for _, vid := range volumeIds {
		target := locations[vid][0]
		ewg.Add(func() error {
			if err := mountEcShards(commandEnv.option.GrpcDialOption, collection, vid, target.ServerAddress(), shardIds); err != nil {
				return fmt.Errorf("mount ec shards for volume %d on %s: %v", vid, target.Url, err)
			}
			return nil
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	return nil
}

func doDeleteVolumes(commandEnv *CommandEnv, volumeIds []needle.VolumeId, maxParallelization int) error {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}
	locations, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return nil
	}

	ewg := NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		for _, l := range locations[vid] {
			ewg.Add(func() error {
				if err := deleteVolume(commandEnv.option.GrpcDialOption, vid, l.ServerAddress(), false); err != nil {
					return fmt.Errorf("deleteVolume %s volume %d: %v", l.Url, vid, err)
				}
				fmt.Printf("deleted volume %d from %s\n", vid, l.Url)
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	return nil
}

func generateEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress) error {

	fmt.Printf("generateEcShards %d (collection %q) on %s ...\n", volumeId, collection, sourceVolumeServer)

	err := operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		return genErr
	})

	return err

}

func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, selectedCollection string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {
	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes quiet for: %d seconds and %.1f%% full\n", quietSeconds, fullPercentage)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				// ignore remote volumes
				if v.RemoteStorageName != "" && v.RemoteStorageKey != "" {
					continue
				}
				if v.Collection == selectedCollection && v.ModifiedAtSecond+quietSeconds < nowUnixSeconds {
					if float64(v.Size) > fullPercentage/100*float64(volumeSizeLimitMb)*1024*1024 {
						if good, found := vidMap[v.Id]; found {
							if good {
								if diskInfo.FreeVolumeCount < 2 {
									glog.V(0).Infof("skip %s %d on %s, no free disk", v.Collection, v.Id, dn.Id)
									vidMap[v.Id] = false
								}
							}
						} else {
							if diskInfo.FreeVolumeCount < 2 {
								glog.V(0).Infof("skip %s %d on %s, no free disk", v.Collection, v.Id, dn.Id)
								vidMap[v.Id] = false
							} else {
								vidMap[v.Id] = true
							}
						}
					}
				}
			}
		}
	})

	for vid, good := range vidMap {
		if good {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	return
}
