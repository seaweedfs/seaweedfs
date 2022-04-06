package shell

import (
	"bytes"
	"flag"
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

	volumeListCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbosityLevel := volumeListCommand.Int("v", 5, "verbose mode: 0, 1, 2, 3, 4, 5")
	if err = volumeListCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	writeTopologyInfo(writer, topologyInfo, volumeSizeLimitMb, *verbosityLevel)
	return nil
}

func diskInfosToString(diskInfos map[string]*master_pb.DiskInfo) string {
	var buf bytes.Buffer
	for diskType, diskInfo := range diskInfos {
		if diskType == "" {
			diskType = "hdd"
		}
		fmt.Fprintf(&buf, " %s(volume:%d/%d active:%d free:%d remote:%d)", diskType, diskInfo.VolumeCount, diskInfo.MaxVolumeCount, diskInfo.ActiveVolumeCount, diskInfo.FreeVolumeCount, diskInfo.RemoteVolumeCount)
	}
	return buf.String()
}

func diskInfoToString(diskInfo *master_pb.DiskInfo) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "volume:%d/%d active:%d free:%d remote:%d", diskInfo.VolumeCount, diskInfo.MaxVolumeCount, diskInfo.ActiveVolumeCount, diskInfo.FreeVolumeCount, diskInfo.RemoteVolumeCount)
	return buf.String()
}

func writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo, volumeSizeLimitMb uint64, verbosityLevel int) statistics {
	output(verbosityLevel >= 0, writer, "Topology volumeSizeLimit:%d MB%s\n", volumeSizeLimitMb, diskInfosToString(t.DiskInfos))
	sort.Slice(t.DataCenterInfos, func(i, j int) bool {
		return t.DataCenterInfos[i].Id < t.DataCenterInfos[j].Id
	})
	var s statistics
	for _, dc := range t.DataCenterInfos {
		s = s.plus(writeDataCenterInfo(writer, dc, verbosityLevel))
	}
	output(verbosityLevel >= 0, writer, "%+v \n", s)
	return s
}
func writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 1, writer, "  DataCenter %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	sort.Slice(t.RackInfos, func(i, j int) bool {
		return t.RackInfos[i].Id < t.RackInfos[j].Id
	})
	for _, r := range t.RackInfos {
		s = s.plus(writeRackInfo(writer, r, verbosityLevel))
	}
	output(verbosityLevel >= 1, writer, "  DataCenter %s %+v \n", t.Id, s)
	return s
}
func writeRackInfo(writer io.Writer, t *master_pb.RackInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 2, writer, "    Rack %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	sort.Slice(t.DataNodeInfos, func(i, j int) bool {
		return t.DataNodeInfos[i].Id < t.DataNodeInfos[j].Id
	})
	for _, dn := range t.DataNodeInfos {
		s = s.plus(writeDataNodeInfo(writer, dn, verbosityLevel))
	}
	output(verbosityLevel >= 2, writer, "    Rack %s %+v \n", t.Id, s)
	return s
}
func writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 3, writer, "      DataNode %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	for _, diskInfo := range t.DiskInfos {
		s = s.plus(writeDiskInfo(writer, diskInfo, verbosityLevel))
	}
	output(verbosityLevel >= 3, writer, "      DataNode %s %+v \n", t.Id, s)
	return s
}

func writeDiskInfo(writer io.Writer, t *master_pb.DiskInfo, verbosityLevel int) statistics {
	var s statistics
	diskType := t.Type
	if diskType == "" {
		diskType = "hdd"
	}
	output(verbosityLevel >= 4, writer, "        Disk %s(%s)\n", diskType, diskInfoToString(t))
	sort.Slice(t.VolumeInfos, func(i, j int) bool {
		return t.VolumeInfos[i].Id < t.VolumeInfos[j].Id
	})
	for _, vi := range t.VolumeInfos {
		s = s.plus(writeVolumeInformationMessage(writer, vi, verbosityLevel))
	}
	for _, ecShardInfo := range t.EcShardInfos {
		output(verbosityLevel >= 5, writer, "          ec volume id:%v collection:%v shards:%v\n", ecShardInfo.Id, ecShardInfo.Collection, erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIds())
	}
	output(verbosityLevel >= 4, writer, "        Disk %s %+v \n", diskType, s)
	return s
}

func writeVolumeInformationMessage(writer io.Writer, t *master_pb.VolumeInformationMessage, verbosityLevel int) statistics {
	output(verbosityLevel >= 5, writer, "          volume %+v \n", t)
	return newStatistics(t)
}

func output(condition bool, w io.Writer, format string, a ...interface{}) {
	if condition {
		fmt.Fprintf(w, format, a...)
	}
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
