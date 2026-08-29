package shell

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h] [-batchSize=10] [-verbose] [-sourceDiskType=<disk_type>] [-diskType=<disk_type>]
	ec.encode [-volumeId=<volume_id>|-volumeIds=<volume_id>,...] [-batchSize=10] [-verbose] [-diskType=<disk_type>]

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

	The -collection parameter is a comma-separated list of collection names, with
	"*" and "?" wildcards, and regex patterns:
	  - One collection: ec.encode -collection="mybucket"
	  - Several collections: ec.encode -collection="mybucket,otherbucket"
	  - Match by prefix: ec.encode -collection="bucket*"
	  - Match all collections: ec.encode -collection="*"
	  - The empty-named collection: ec.encode -collection="_default"

	Options:
	  -verbose: show detailed reasons why volumes are not selected for encoding
	  -sourceDiskType: filter source volumes by disk type (hdd, ssd, or empty for all)
	  -diskType: target disk type for EC shards (hdd, ssd, or empty for default hdd)
	  -batchSize: encode/rebalance/verify/delete this many volumes at a time (default 10; 0 = all in one batch)
	  -volumeIds: comma-separated volume IDs to encode

	Each batch is committed independently. If a later batch fails, earlier batches
	are already encoded and their original volumes deleted.

	Examples:
	  # Encode SSD volumes to SSD EC shards (same tier)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=ssd

	  # Encode SSD volumes to HDD EC shards (tier migration to cheaper storage)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=hdd

	  # Encode all volumes to SSD EC shards
	  ec.encode -collection=mybucket -diskType=ssd

	  # Encode selected volume IDs and delete originals after each batch
	  ec.encode -volumeIds=101,102,103 -batchSize=2

	Re-balancing algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	volumeIdsStr := encodeCommand.String("volumeIds", "", "comma-separated volume ids")
	collection := encodeCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty matches the collection with no name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	maxParallelization := encodeCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	batchSize := encodeCommand.Int("batchSize", DefaultEcBatchSize, "encode/re-balance/verify/delete up to this many volumes at a time (0 = all in one batch)")
	forceChanges := encodeCommand.Bool("force", false, "force the encoding even if the cluster has less than recommended 4 nodes")
	shardReplicaPlacement := encodeCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	sourceDiskTypeStr := encodeCommand.String("sourceDiskType", "", "filter source volumes by disk type (hdd, ssd, or empty for all)")
	diskTypeStr := encodeCommand.String("diskType", "", "target disk type for EC shards (hdd, ssd, or empty for default hdd)")
	applyBalancing := encodeCommand.Bool("rebalance", true, "re-balance EC shards after creation (default: true)")
	verbose := encodeCommand.Bool("verbose", false, "show detailed reasons why volumes are not selected for encoding")

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

	// Parse source disk type filter (optional)
	var sourceDiskType *types.DiskType
	if *sourceDiskTypeStr != "" {
		sdt := types.ToDiskType(*sourceDiskTypeStr)
		sourceDiskType = &sdt
	}

	// Parse target disk type for EC shards
	diskType := types.ToDiskType(*diskTypeStr)

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
	if *volumeId != 0 || strings.TrimSpace(*volumeIdsStr) != "" {
		if *volumeId != 0 && strings.TrimSpace(*volumeIdsStr) != "" {
			return fmt.Errorf("-volumeId and -volumeIds are mutually exclusive")
		}
		if *volumeId != 0 {
			volumeIds = append(volumeIds, needle.VolumeId(*volumeId))
		} else {
			volumeIds, err = parseVolumeIdsFlag(*volumeIdsStr)
			if err != nil {
				return err
			}
		}
	} else {
		// apply to all volumes for the given collection pattern (regex)
		volumeIds, _, err = collectVolumeIdsForEcEncode(commandEnv, *collection, sourceDiskType, *fullPercentage, *quietPeriod, *verbose)
		if err != nil {
			return err
		}
	}
	if len(volumeIds) == 0 {
		fmt.Println("No volumes, nothing to do.")
		return nil
	}
	if *batchSize < 0 {
		return fmt.Errorf("-batchSize must be >= 0")
	}

	batches := ec.ChunkVolumeIds(volumeIds, *batchSize)
	if len(batches) > 1 {
		fmt.Printf("Processing %d volumes in %d batch(es), batchSize=%d\n", len(volumeIds), len(batches), *batchSize)
	}
	for i, batchVolumeIds := range batches {
		if len(batches) > 1 {
			fmt.Printf("Starting EC encoding batch %d/%d with %d volumes: %v\n", i+1, len(batches), len(batchVolumeIds), batchVolumeIds)
		}
		if err := ec.ProcessEcEncodeBatch(commandEnv.ecEnv(), writer, batchVolumeIds, rp, diskType, *maxParallelization, *applyBalancing, *collection); err != nil {
			return fmt.Errorf("ec encode batch %d/%d for volumes %v: %w", i+1, len(batches), batchVolumeIds, err)
		}
	}
	if len(batches) > 1 {
		fmt.Printf("Successfully completed EC encoding for %d volumes in %d batch(es)\n", len(volumeIds), len(batches))
	}

	return nil
}
func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, collectionPattern string, sourceDiskType *types.DiskType, fullPercentage float64, quietPeriod time.Duration, verbose bool) (vids []needle.VolumeId, matchedCollections []string, err error) {
	// compile regex pattern for collection matching
	collectionMatcher, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes with collection pattern '%s', quiet for: %d seconds and %.1f%% full\n", collectionPattern, quietSeconds, fullPercentage)

	vids, matchedCollections = ec.SelectVolumeIdsFromTopology(topologyInfo, volumeSizeLimitMb, collectionMatcher, sourceDiskType, quietSeconds, nowUnixSeconds, fullPercentage, verbose)
	return
}
