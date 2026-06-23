package shell

import (
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeMark{})
}

type commandVolumeMark struct {
}

func (c *commandVolumeMark) Name() string {
	return "volume.mark"
}

func (c *commandVolumeMark) Help() string {
	return `Mark volume writable or readonly from one volume server, or all volume replicas in one collection

		volume.mark -node <volume server host:port> -volumeId <volume id> -writable or -readonly
		volume.mark -collection <collection> -writable or -readonly
`
}

func (c *commandVolumeMark) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeMark) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volMarkCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMarkCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volMarkCommand.String("node", "", "the volume server <host>:<port>")
	collection := volMarkCommand.String("collection", "", "the collection name")
	writable := volMarkCommand.Bool("writable", false, "volume mark writable")
	readonly := volMarkCommand.Bool("readonly", false, "volume mark readonly")
	if err = volMarkCommand.Parse(args); err != nil {
		return nil
	}
	collectionSet, nodeSet, volumeIdSet := false, false, false
	volMarkCommand.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "collection":
			collectionSet = true
		case "node":
			nodeSet = true
		case "volumeId":
			volumeIdSet = true
		}
	})
	markWritable := false
	if (*writable && *readonly) || (!*writable && !*readonly) {
		return fmt.Errorf("use -readonly or -writable")
	} else if *writable {
		markWritable = true
	}

	if collectionSet {
		if *collection == "" {
			return fmt.Errorf("collection is required")
		}
		if nodeSet || volumeIdSet {
			return fmt.Errorf("cannot use -collection with -node or -volumeId")
		}
	} else if !nodeSet || !volumeIdSet || *nodeStr == "" || *volumeIdInt == 0 {
		return fmt.Errorf("use -node and -volumeId, or -collection")
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if collectionSet {
		topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
		if err != nil {
			return err
		}
		targets, err := collectVolumeMarkTargetsByCollection(topologyInfo, *collection)
		if err != nil {
			return err
		}
		for _, target := range targets {
			if err := markVolumeWritable(commandEnv.option.GrpcDialOption, target.volumeId, target.sourceVolumeServer, markWritable, true); err != nil {
				return fmt.Errorf("mark volume %d on %s: %w", target.volumeId, target.sourceVolumeServer, err)
			}
		}
		return nil
	}

	sourceVolumeServer := pb.ServerAddress(*nodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	return markVolumeWritable(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, markWritable, true)
}

type volumeMarkTarget struct {
	volumeId           needle.VolumeId
	sourceVolumeServer pb.ServerAddress
}

func collectVolumeMarkTargetsByCollection(topoInfo *master_pb.TopologyInfo, collection string) ([]volumeMarkTarget, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection is required")
	}

	var targets []volumeMarkTarget
	for _, dc := range topoInfo.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, dn := range rack.GetDataNodeInfos() {
				if dn == nil {
					continue
				}
				sourceVolumeServer := pb.NewServerAddressFromDataNode(dn)
				for _, diskInfo := range dn.GetDiskInfos() {
					for _, v := range diskInfo.GetVolumeInfos() {
						if v.GetCollection() == collection {
							targets = append(targets, volumeMarkTarget{
								volumeId:           needle.VolumeId(v.GetId()),
								sourceVolumeServer: sourceVolumeServer,
							})
						}
					}
				}
			}
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("collection %s has no volumes", collection)
	}
	return targets, nil
}
