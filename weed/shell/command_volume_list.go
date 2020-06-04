package shell

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"

	"io"
	"sort"
)

func init() {
	Commands = append(Commands, &commandVolumeList{})
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

func (c *commandVolumeList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return err
	}

	writeTopologyInfo(writer, resp.TopologyInfo, resp.VolumeSizeLimitMb)
	return nil
}

func writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo, volumeSizeLimitMb uint64) statistics {
	fmt.Fprintf(writer, "Topology volume:%d/%d active:%d free:%d remote:%d volumeSizeLimit:%d MB\n", t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount, t.RemoteVolumeCount, volumeSizeLimitMb)
	sort.Slice(t.DataCenterInfos, func(i, j int) bool {
		return t.DataCenterInfos[i].Id < t.DataCenterInfos[j].Id
	})
	var s statistics
	for _, dc := range t.DataCenterInfos {
		s = s.plus(writeDataCenterInfo(writer, dc))
	}
	fmt.Fprintf(writer, "%+v \n", s)
	return s
}
func writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo) statistics {
	fmt.Fprintf(writer, "  DataCenter %s volume:%d/%d active:%d free:%d remote:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount, t.RemoteVolumeCount)
	var s statistics
	sort.Slice(t.RackInfos, func(i, j int) bool {
		return t.RackInfos[i].Id < t.RackInfos[j].Id
	})
	for _, r := range t.RackInfos {
		s = s.plus(writeRackInfo(writer, r))
	}
	fmt.Fprintf(writer, "  DataCenter %s %+v \n", t.Id, s)
	return s
}
func writeRackInfo(writer io.Writer, t *master_pb.RackInfo) statistics {
	fmt.Fprintf(writer, "    Rack %s volume:%d/%d active:%d free:%d remote:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount, t.RemoteVolumeCount)
	var s statistics
	sort.Slice(t.DataNodeInfos, func(i, j int) bool {
		return t.DataNodeInfos[i].Id < t.DataNodeInfos[j].Id
	})
	for _, dn := range t.DataNodeInfos {
		s = s.plus(writeDataNodeInfo(writer, dn))
	}
	fmt.Fprintf(writer, "    Rack %s %+v \n", t.Id, s)
	return s
}
func writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo) statistics {
	fmt.Fprintf(writer, "      DataNode %s volume:%d/%d active:%d free:%d remote:%d\n", t.Id, t.VolumeCount, t.MaxVolumeCount, t.ActiveVolumeCount, t.FreeVolumeCount, t.RemoteVolumeCount)
	var s statistics
	sort.Slice(t.VolumeInfos, func(i, j int) bool {
		return t.VolumeInfos[i].Id < t.VolumeInfos[j].Id
	})
	for _, vi := range t.VolumeInfos {
		s = s.plus(writeVolumeInformationMessage(writer, vi))
	}
	for _, ecShardInfo := range t.EcShardInfos {
		fmt.Fprintf(writer, "        ec volume id:%v collection:%v shards:%v\n", ecShardInfo.Id, ecShardInfo.Collection, erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIds())
	}
	fmt.Fprintf(writer, "      DataNode %s %+v \n", t.Id, s)
	return s
}
func writeVolumeInformationMessage(writer io.Writer, t *master_pb.VolumeInformationMessage) statistics {
	fmt.Fprintf(writer, "        volume %+v \n", t)
	return newStatistics(t)
}

type statistics struct {
	Size             uint64
	FileCount        uint64
	DeletedFileCount uint64
	DeletedBytes     uint64
}

func newStatistics(t *master_pb.VolumeInformationMessage) statistics {
	return statistics{
		Size:             t.Size,
		FileCount:        t.FileCount,
		DeletedFileCount: t.DeleteCount,
		DeletedBytes:     t.DeletedByteCount,
	}
}

func (s statistics) plus(t statistics) statistics {
	return statistics{
		Size:             s.Size + t.Size,
		FileCount:        s.FileCount + t.FileCount,
		DeletedFileCount: s.DeletedFileCount + t.DeletedFileCount,
		DeletedBytes:     s.DeletedBytes + t.DeletedBytes,
	}
}

func (s statistics) String() string {
	if s.DeletedFileCount > 0 {
		return fmt.Sprintf("total size:%d file_count:%d deleted_file:%d deleted_bytes:%d", s.Size, s.FileCount, s.DeletedFileCount, s.DeletedBytes)
	}
	return fmt.Sprintf("total size:%d file_count:%d", s.Size, s.FileCount)
}
