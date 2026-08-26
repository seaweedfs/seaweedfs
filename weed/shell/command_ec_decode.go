package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/ec"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandEcDecode{})
}

type commandEcDecode struct {
}

func (c *commandEcDecode) Name() string {
	return "ec.decode"
}

func (c *commandEcDecode) Help() string {
	return `decode a erasure coded volume into a normal volume

	ec.decode [-collection=""] [-volumeId=<volume_id>] [-batchSize=10] [-diskType=<disk_type>] [-checkMinFreeSpace]

	The -collection parameter supports regular expressions for pattern matching:
	  - Use exact match: ec.decode -collection="^mybucket$"
	  - Match multiple buckets: ec.decode -collection="bucket.*"
	  - Match all collections: ec.decode -collection=".*"

	Options:
	  -diskType: source disk type where EC shards are stored (hdd, ssd, or empty for default hdd)
	  -checkMinFreeSpace: check min free space when selecting the decode target (default true)
	  -batchSize: decode this many volumes per topology refresh (default 10; 0 = one snapshot for all volumes)

	Examples:
	  # Decode EC shards from HDD (default)
	  ec.decode -collection=mybucket

	  # Decode EC shards from SSD
	  ec.decode -collection=mybucket -diskType=ssd

`
}

func (c *commandEcDecode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcDecode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	decodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := decodeCommand.Int("volumeId", 0, "the volume id")
	collection := decodeCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty matches the collection with no name")
	diskTypeStr := decodeCommand.String("diskType", "", "source disk type where EC shards are stored (hdd, ssd, or empty for default hdd)")
	checkMinFreeSpace := decodeCommand.Bool("checkMinFreeSpace", true, "check min free space when selecting the decode target")
	batchSize := decodeCommand.Int("batchSize", DefaultEcBatchSize, "decode up to this many volumes per topology refresh (0 = one snapshot for all volumes)")
	if err = decodeCommand.Parse(args); err != nil {
		return nil
	}
	if *batchSize < 0 {
		return fmt.Errorf("-batchSize must be >= 0")
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)
	diskType := types.ToDiskType(*diskTypeStr)
	env := commandEnv.ecEnv()

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	var diskUsageState *ec.DecodeDiskUsageState
	if *checkMinFreeSpace {
		diskUsageState = ec.NewDecodeDiskUsageState(topologyInfo, diskType)
	}

	// volumeId is provided
	if vid != 0 {
		return ec.DoEcDecode(env, topologyInfo, *collection, vid, diskType, *checkMinFreeSpace, diskUsageState)
	}

	// apply to all volumes in the collection
	collectionMatcher, err := compileCollectionPattern(*collection)
	if err != nil {
		return fmt.Errorf("invalid collection pattern '%s': %v", *collection, err)
	}
	volumeIds := ec.CollectEcShardIds(topologyInfo, collectionMatcher, diskType)
	fmt.Printf("ec decode volumes: %v\n", volumeIds)
	batches := chunkVolumeIds(volumeIds, *batchSize)
	for i, batch := range batches {
		if i > 0 {
			// earlier batches moved shards and created volumes; re-snapshot so
			// shard locations and free-space accounting stay accurate
			topologyInfo, _, err = collectTopologyInfo(commandEnv, 0)
			if err != nil {
				return err
			}
			if *checkMinFreeSpace {
				diskUsageState = ec.NewDecodeDiskUsageState(topologyInfo, diskType)
			}
		}
		if len(batches) > 1 {
			fmt.Printf("ec decode batch %d/%d: %v\n", i+1, len(batches), batch)
		}
		for _, vid := range batch {
			if err = ec.DoEcDecode(env, topologyInfo, *collection, vid, diskType, *checkMinFreeSpace, diskUsageState); err != nil {
				return err
			}
		}
	}

	return nil
}

func lookupVolumeIds(commandEnv *CommandEnv, volumeIds []string) (volumeIdLocations []*master_pb.LookupVolumeResponse_VolumeIdLocation, err error) {
	var resp *master_pb.LookupVolumeResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: volumeIds})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.VolumeIdLocations, nil
}
