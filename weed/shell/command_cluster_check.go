package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func init() {
	Commands = append(Commands, &commandClusterCheck{})
}

type commandClusterCheck struct {
}

func (c *commandClusterCheck) Name() string {
	return "cluster.check"
}

func (c *commandClusterCheck) Help() string {
	return `check current cluster network connectivity

	cluster.check

`
}

func (c *commandClusterCheck) HasTag(CommandTag) bool {
	return false
}

func (c *commandClusterCheck) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	clusterPsCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	if err = clusterPsCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "Topology volumeSizeLimit:%d MB%s\n", volumeSizeLimitMb, diskInfosToString(topologyInfo.DiskInfos))

	if len(topologyInfo.DiskInfos) == 0 {
		return fmt.Errorf("no disk type defined")
	}
	for diskType, diskInfo := range topologyInfo.DiskInfos {
		if diskInfo.MaxVolumeCount == 0 {
			return fmt.Errorf("no volume available for \"%s\" disk type", diskType)
		}
	}

	// collect filers
	var filers []pb.ServerAddress
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: *commandEnv.option.FilerGroup,
		})

		for _, node := range resp.ClusterNodes {
			filers = append(filers, pb.ServerAddress(node.Address))
		}
		return err
	})
	if err != nil {
		return
	}
	fmt.Fprintf(writer, "the cluster has %d filers: %+v\n", len(filers), filers)

	if len(filers) > 0 {
		genericDiskInfo, genericDiskInfoOk := topologyInfo.DiskInfos[""]
		hddDiskInfo, hddDiskInfoOk := topologyInfo.DiskInfos[types.HddType]

		if !genericDiskInfoOk && !hddDiskInfoOk {
			return fmt.Errorf("filer metadata logs need generic or hdd disk type to be defined")
		}

		if (genericDiskInfoOk && genericDiskInfo.MaxVolumeCount == 0) || (hddDiskInfoOk && hddDiskInfo.MaxVolumeCount == 0) {
			return fmt.Errorf("filer metadata logs need generic or hdd volumes to be available")
		}
	}

	// collect volume servers
	var volumeServers []pb.ServerAddress
	t, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				volumeServers = append(volumeServers, pb.NewServerAddressFromDataNode(dn))
			}
		}
	}
	fmt.Fprintf(writer, "the cluster has %d volume servers: %+v\n", len(volumeServers), volumeServers)

	// collect all masters
	var masters []pb.ServerAddress
	masters = append(masters, commandEnv.MasterClient.GetMasters(context.Background())...)

	// check from master to volume servers
	for _, master := range masters {
		for _, volumeServer := range volumeServers {
			fmt.Fprintf(writer, "checking master %s to volume server %s ... ", string(master), string(volumeServer))
			err := pb.WithMasterClient(false, master, commandEnv.option.GrpcDialOption, false, func(client master_pb.SeaweedClient) error {
				pong, err := client.Ping(context.Background(), &master_pb.PingRequest{
					Target:     string(volumeServer),
					TargetType: cluster.VolumeServerType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check between masters
	for _, sourceMaster := range masters {
		for _, targetMaster := range masters {
			if sourceMaster == targetMaster {
				continue
			}
			fmt.Fprintf(writer, "checking master %s to %s ... ", string(sourceMaster), string(targetMaster))
			err := pb.WithMasterClient(false, sourceMaster, commandEnv.option.GrpcDialOption, false, func(client master_pb.SeaweedClient) error {
				pong, err := client.Ping(context.Background(), &master_pb.PingRequest{
					Target:     string(targetMaster),
					TargetType: cluster.MasterType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from volume servers to masters
	for _, volumeServer := range volumeServers {
		for _, master := range masters {
			fmt.Fprintf(writer, "checking volume server %s to master %s ... ", string(volumeServer), string(master))
			err := pb.WithVolumeServerClient(false, volumeServer, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
				pong, err := client.Ping(context.Background(), &volume_server_pb.PingRequest{
					Target:     string(master),
					TargetType: cluster.MasterType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from filers to masters
	for _, filer := range filers {
		for _, master := range masters {
			fmt.Fprintf(writer, "checking filer %s to master %s ... ", string(filer), string(master))
			err := pb.WithFilerClient(false, 0, filer, commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				pong, err := client.Ping(context.Background(), &filer_pb.PingRequest{
					Target:     string(master),
					TargetType: cluster.MasterType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check from filers to volume servers
	for _, filer := range filers {
		for _, volumeServer := range volumeServers {
			fmt.Fprintf(writer, "checking filer %s to volume server %s ... ", string(filer), string(volumeServer))
			err := pb.WithFilerClient(false, 0, filer, commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				pong, err := client.Ping(context.Background(), &filer_pb.PingRequest{
					Target:     string(volumeServer),
					TargetType: cluster.VolumeServerType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check between volume servers
	for _, sourceVolumeServer := range volumeServers {
		for _, targetVolumeServer := range volumeServers {
			if sourceVolumeServer == targetVolumeServer {
				continue
			}
			fmt.Fprintf(writer, "checking volume server %s to %s ... ", string(sourceVolumeServer), string(targetVolumeServer))
			err := pb.WithVolumeServerClient(false, sourceVolumeServer, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
				pong, err := client.Ping(context.Background(), &volume_server_pb.PingRequest{
					Target:     string(targetVolumeServer),
					TargetType: cluster.VolumeServerType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	// check between filers, and need to connect to itself
	for _, sourceFiler := range filers {
		for _, targetFiler := range filers {
			fmt.Fprintf(writer, "checking filer %s to %s ... ", string(sourceFiler), string(targetFiler))
			err := pb.WithFilerClient(false, 0, sourceFiler, commandEnv.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
				pong, err := client.Ping(context.Background(), &filer_pb.PingRequest{
					Target:     string(targetFiler),
					TargetType: cluster.FilerType,
				})
				if err == nil {
					printTiming(writer, pong.StartTimeNs, pong.RemoteTimeNs, pong.StopTimeNs)
				}
				return err
			})
			if err != nil {
				fmt.Fprintf(writer, "%v\n", err)
			}
		}
	}

	return nil
}

func printTiming(writer io.Writer, startNs, remoteNs, stopNs int64) {
	roundTripTimeMs := float32(stopNs-startNs) / 1000000
	deltaTimeMs := float32(remoteNs-(startNs+stopNs)/2) / 1000000
	fmt.Fprintf(writer, "ok round trip %.3fms clock delta %.3fms\n", roundTripTimeMs, deltaTimeMs)
}
