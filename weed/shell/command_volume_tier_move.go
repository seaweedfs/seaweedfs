package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierMove{})
}

type commandVolumeTierMove struct {
}

func (c *commandVolumeTierMove) Name() string {
	return "volume.tier.upload"
}

func (c *commandVolumeTierMove) Help() string {
	return `change a volume from one disk type to another

	volume.tier.move -source=hdd -target=ssd [-collection=""] [-fullPercent=95] [-quietFor=1h]
	volume.tier.move -target=hdd [-collection=""] -volumeId=<volume_id>

`
}

func (c *commandVolumeTierMove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	source := tierCommand.String("fromDiskType", "", "the source disk type")
	target := tierCommand.String("toDiskType", "", "the target disk type")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if *source == *target {
		return fmt.Errorf("source tier %s is the same as target tier %s", *source, *target)
	}

	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		// return doVolumeTierMove(commandEnv, writer, *collection, vid, *dest, *keepLocalDatFile)
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds, err := collectVolumeIdsForTierChange(commandEnv, *source, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("tier move volumes: %v\n", volumeIds)

	return nil
}

func collectVolumeIdsForTierChange(commandEnv *CommandEnv, sourceTier string, selectedCollection string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect %s volumes quiet for: %d seconds\n", sourceTier, quietSeconds)

	vidMap := make(map[uint32]bool)
	eachDataNode(resp.TopologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Collection == selectedCollection && v.ModifiedAtSecond+quietSeconds < nowUnixSeconds && types.ToDiskType(v.DiskType) == types.ToDiskType(sourceTier) {
					if float64(v.Size) > fullPercentage/100*float64(resp.VolumeSizeLimitMb)*1024*1024 {
						vidMap[v.Id] = true
					}
				}
			}
		}
	})

	for vid := range vidMap {
		vids = append(vids, needle.VolumeId(vid))
	}

	return
}
