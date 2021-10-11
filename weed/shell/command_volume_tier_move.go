package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierMove{})
}

type commandVolumeTierMove struct {
	activeServers     map[pb.ServerAddress]struct{}
	activeServersLock sync.Mutex
	activeServersCond *sync.Cond
}

func (c *commandVolumeTierMove) Name() string {
	return "volume.tier.move"
}

func (c *commandVolumeTierMove) Help() string {
	return `change a volume from one disk type to another

	volume.tier.move -fromDiskType=hdd -toDiskType=ssd [-collectionPattern=""] [-fullPercent=95] [-quietFor=1h]

	Even if the volume is replicated, only one replica will be changed and the rest replicas will be dropped.
	So "volume.fix.replication" and "volume.balance" should be followed.

`
}

func (c *commandVolumeTierMove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	c.activeServers = make(map[pb.ServerAddress]struct{})
	c.activeServersCond = sync.NewCond(new(sync.Mutex))

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collectionPattern := tierCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	source := tierCommand.String("fromDiskType", "", "the source disk type")
	target := tierCommand.String("toDiskType", "", "the target disk type")
	applyChange := tierCommand.Bool("force", false, "actually apply the changes")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	fromDiskType := types.ToDiskType(*source)
	toDiskType := types.ToDiskType(*target)

	if fromDiskType == toDiskType {
		return fmt.Errorf("source tier %s is the same as target tier %s", fromDiskType, toDiskType)
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv)
	if err != nil {
		return err
	}

	// collect all volumes that should change
	volumeIds, err := collectVolumeIdsForTierChange(commandEnv, topologyInfo, volumeSizeLimitMb, fromDiskType, *collectionPattern, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("tier move volumes: %v\n", volumeIds)

	_, allLocations := collectVolumeReplicaLocations(topologyInfo)
	for _, vid := range volumeIds {
		if err = c.doVolumeTierMove(commandEnv, writer, vid, toDiskType, allLocations, *applyChange); err != nil {
			fmt.Printf("tier move volume %d: %v\n", vid, err)
		}
	}

	return nil
}

func isOneOf(server string, locations []wdclient.Location) bool {
	for _, loc := range locations {
		if server == loc.Url {
			return true
		}
	}
	return false
}

func (c *commandVolumeTierMove) doVolumeTierMove(commandEnv *CommandEnv, writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, allLocations []location, applyChanges bool) (err error) {
	// find volume location
	locations, found := commandEnv.MasterClient.GetLocations(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	// find one server with the most empty volume slots with target disk type
	hasFoundTarget := false
	keepDataNodesSorted(allLocations, toDiskType)
	fn := capacityByFreeVolumeCount(toDiskType)
	wg := sync.WaitGroup{}
	for _, dst := range allLocations {
		if fn(dst.dataNode) > 0 && !hasFoundTarget {
			// ask the volume server to replicate the volume
			if isOneOf(dst.dataNode.Id, locations) {
				continue
			}
			var sourceVolumeServer pb.ServerAddress
			for _, loc := range locations {
				if loc.Url != dst.dataNode.Id {
					sourceVolumeServer = loc.ServerAddress()
				}
			}
			if sourceVolumeServer == "" {
				continue
			}
			fmt.Fprintf(writer, "moving volume %d from %s to %s with disk type %s ...\n", vid, sourceVolumeServer, dst.dataNode.Id, toDiskType.ReadableString())
			hasFoundTarget = true

			if !applyChanges {
				// adjust volume count
				dst.dataNode.DiskInfos[string(toDiskType)].VolumeCount++
				break
			}

			destServerAddress := pb.NewServerAddressFromDataNode(dst.dataNode)
			c.activeServersCond.L.Lock()
			_, isSourceActive := c.activeServers[sourceVolumeServer]
			_, isDestActive := c.activeServers[destServerAddress]
			for isSourceActive || isDestActive {
				c.activeServersCond.Wait()
				_, isSourceActive = c.activeServers[sourceVolumeServer]
				_, isDestActive = c.activeServers[destServerAddress]
			}
			c.activeServers[sourceVolumeServer] = struct{}{}
			c.activeServers[destServerAddress] = struct{}{}
			c.activeServersCond.L.Unlock()

			wg.Add(1)
			go func(dst location) {
				if err := c.doMoveOneVolume(commandEnv, writer, vid, toDiskType, locations, sourceVolumeServer, dst); err != nil {
					fmt.Fprintf(writer, "move volume %d %s => %s: %v\n", vid, sourceVolumeServer, dst.dataNode.Id, err)
				}
				delete(c.activeServers, sourceVolumeServer)
				delete(c.activeServers, destServerAddress)
				c.activeServersCond.Signal()
				wg.Done()
			}(dst)

		}
	}

	wg.Wait()

	if !hasFoundTarget {
		fmt.Fprintf(writer, "can not find disk type %s for volume %d\n", toDiskType.ReadableString(), vid)
	}

	return nil
}

func (c *commandVolumeTierMove) doMoveOneVolume(commandEnv *CommandEnv, writer io.Writer, vid needle.VolumeId, toDiskType types.DiskType, locations []wdclient.Location, sourceVolumeServer pb.ServerAddress, dst location) (err error) {

	// mark all replicas as read only
	if err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, false); err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, locations[0].Url, err)
	}
	if err = LiveMoveVolume(commandEnv.option.GrpcDialOption, writer, vid, sourceVolumeServer, pb.NewServerAddressFromDataNode(dst.dataNode), 5*time.Second, toDiskType.ReadableString(), true); err != nil {

		// mark all replicas as writable
		if err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, true); err != nil {
			glog.Errorf("mark volume %d as writable on %s: %v", vid, locations[0].Url, err)
		}

		return fmt.Errorf("move volume %d %s => %s : %v", vid, locations[0].Url, dst.dataNode.Id, err)
	}

	// adjust volume count
	dst.dataNode.DiskInfos[string(toDiskType)].VolumeCount++

	// remove the remaining replicas
	for _, loc := range locations {
		if loc.Url != dst.dataNode.Id && loc.ServerAddress() != sourceVolumeServer {
			if err = deleteVolume(commandEnv.option.GrpcDialOption, vid, loc.ServerAddress()); err != nil {
				fmt.Fprintf(writer, "failed to delete volume %d on %s: %v\n", vid, loc.Url, err)
			}
		}
	}
	return nil
}

func collectVolumeIdsForTierChange(commandEnv *CommandEnv, topologyInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, sourceTier types.DiskType, collectionPattern string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect %s volumes quiet for: %d seconds\n", sourceTier, quietSeconds)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				// check collection name pattern
				if collectionPattern != "" {
					matched, err := filepath.Match(collectionPattern, v.Collection)
					if err != nil {
						return
					}
					if !matched {
						continue
					}
				}

				if v.ModifiedAtSecond+quietSeconds < nowUnixSeconds && types.ToDiskType(v.DiskType) == sourceTier {
					if float64(v.Size) > fullPercentage/100*float64(volumeSizeLimitMb)*1024*1024 {
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
