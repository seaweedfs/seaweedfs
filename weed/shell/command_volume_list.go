package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"io"
)

func init() {
	commands = append(commands, &commandVolumeList{})
}

type commandVolumeList struct {
}

func (c *commandVolumeList) Name() string {
	return "volume.list"
}

func (c *commandVolumeList) Help() string {
	return `list all volumes

	This command list all volumes as a tree of dataCenter > rack > dataNode > volume.

`
}

func (c *commandVolumeList) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	var resp *master_pb.VolumeListResponse
	ctx := context.Background()
	err = commandEnv.masterClient.WithClient(ctx, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	writeTopologyInfo(writer, resp.TopologyInfo)
	return nil
}

func writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo) {
	fmt.Fprintf(writer, "Topology volume:%d/%d active:%d free:%d\n", t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount)
	for _, dc := range t.DataCenterInfos {
		writeDataCenterInfo(writer, dc)
	}
}
func writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo) {
	fmt.Fprintf(writer, "  DataCenter %s volume:%d/%d active:%d free:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount)
	for _, r := range t.RackInfos {
		writeRackInfo(writer, r)
	}
}
func writeRackInfo(writer io.Writer, t *master_pb.RackInfo) {
	fmt.Fprintf(writer, "    Rack %s volume:%d/%d active:%d free:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount)
	for _, dn := range t.DataNodeInfos {
		writeDataNodeInfo(writer, dn)
	}
}
func writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo) {
	fmt.Fprintf(writer, "      DataNode %s volume:%d/%d active:%d free:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount)
	for _, vi := range t.VolumeInfos {
		writeVolumeInformationMessage(writer, vi)
	}
}
func writeVolumeInformationMessage(writer io.Writer, t *master_pb.VolumeInformationMessage) {
	fmt.Fprintf(writer, "        volume %+v \n", t)
}
