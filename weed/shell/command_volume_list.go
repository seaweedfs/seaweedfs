package shell

import (
	"bytes"
	"flag"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"io"
)

func init() {
	Commands = append(Commands, &commandVolumeList{})
}

type commandVolumeList struct {
	collectionPattern *string
	dataCenter        *string
	rack              *string
	dataNode          *string
	readonly          *bool
	writable          *bool
	volumeId          *uint64
	volumeSizeLimitMb uint64
}

func (c *commandVolumeList) Name() string {
	return "volume.list"
}

func (c *commandVolumeList) Help() string {
	return `list all volumes

	This command list all volumes as a tree of dataCenter > rack > dataNode > volume.

`
}

func (c *commandVolumeList) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeListCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbosityLevel := volumeListCommand.Int("v", 5, "verbose mode: 0, 1, 2, 3, 4, 5")
	c.collectionPattern = volumeListCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	c.readonly = volumeListCommand.Bool("readonly", false, "show only readonly volumes")
	c.writable = volumeListCommand.Bool("writable", false, "show only writable volumes")
	c.volumeId = volumeListCommand.Uint64("volumeId", 0, "show only volume id")
	c.dataCenter = volumeListCommand.String("dataCenter", "", "show volumes only from the specified data center")
	c.rack = volumeListCommand.String("rack", "", "show volumes only from the specified rack")
	c.dataNode = volumeListCommand.String("dataNode", "", "show volumes only from the specified data node")

	if err = volumeListCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	var topologyInfo *master_pb.TopologyInfo
	topologyInfo, c.volumeSizeLimitMb, err = collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	c.writeTopologyInfo(writer, topologyInfo, c.volumeSizeLimitMb, *verbosityLevel)
	return nil
}

func sortMapKey[T1 comparable, T2 any](m map[T1]T2) []T1 {
	keys := make([]T1, 0, len(m))
	for k, _ := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		switch v1 := any(keys[i]).(type) {
		case int:
			return v1 < any(keys[j]).(int)
		case string:
			if strings.Compare(v1, any(keys[j]).(string)) < 0 {
				return true
			}
			return false
		default:
			return false
		}
	})
	return keys
}

func diskInfosToString(diskInfos map[string]*master_pb.DiskInfo) string {
	var buf bytes.Buffer

	for _, diskType := range sortMapKey(diskInfos) {
		diskInfo := diskInfos[diskType]

		if diskType == "" {
			diskType = types.HddType
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

func (c *commandVolumeList) writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo, volumeSizeLimitMb uint64, verbosityLevel int) *statistics {
	output(verbosityLevel >= 0, writer, "Topology volumeSizeLimit:%d MB%s\n", volumeSizeLimitMb, diskInfosToString(t.DiskInfos))
	slices.SortFunc(t.DataCenterInfos, func(a, b *master_pb.DataCenterInfo) int {
		return strings.Compare(a.Id, b.Id)
	})
	s := newStatistics()
	for _, dc := range t.DataCenterInfos {
		if *c.dataCenter != "" && *c.dataCenter != dc.Id {
			continue
		}
		s.add(c.writeDataCenterInfo(writer, dc, verbosityLevel))
	}
	output(verbosityLevel >= 0, writer, "%+v \n", s)
	return s
}

func (c *commandVolumeList) writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo, verbosityLevel int) statistics {
	var s statistics
	slices.SortFunc(t.RackInfos, func(a, b *master_pb.RackInfo) int {
		return strings.Compare(a.Id, b.Id)
	})
	dataCenterInfoFound := false
	for _, r := range t.RackInfos {
		if *c.rack != "" && *c.rack != r.Id {
			continue
		}
		s.add(c.writeRackInfo(writer, r, verbosityLevel, func() {
			output(verbosityLevel >= 1, writer, "  DataCenter %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
		}))
		if !dataCenterInfoFound && !s.isEmpty() {
			dataCenterInfoFound = true
		}
	}
	output(dataCenterInfoFound && verbosityLevel >= 1, writer, "  DataCenter %s %+v \n", t.Id, s)
	return s
}

func (c *commandVolumeList) writeRackInfo(writer io.Writer, t *master_pb.RackInfo, verbosityLevel int, outCenterInfo func()) statistics {
	var s statistics
	slices.SortFunc(t.DataNodeInfos, func(a, b *master_pb.DataNodeInfo) int {
		return strings.Compare(a.Id, b.Id)
	})
	rackInfoFound := false
	for _, dn := range t.DataNodeInfos {
		if *c.dataNode != "" && *c.dataNode != dn.Id {
			continue
		}
		s.add(c.writeDataNodeInfo(writer, dn, verbosityLevel, func() {
			outCenterInfo()
			output(verbosityLevel >= 2, writer, "    Rack %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
		}))
		if !rackInfoFound && !s.isEmpty() {
			rackInfoFound = true
		}
	}
	output(rackInfoFound && verbosityLevel >= 2, writer, "    Rack %s %+v \n", t.Id, s)
	return s
}

func (c *commandVolumeList) writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo, verbosityLevel int, outRackInfo func()) statistics {
	var s statistics
	diskInfoFound := false
	for _, diskType := range sortMapKey(t.DiskInfos) {
		diskInfo := t.DiskInfos[diskType]
		s.add(c.writeDiskInfo(writer, diskInfo, verbosityLevel, func() {
			outRackInfo()
			output(verbosityLevel >= 3, writer, "      DataNode %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
		}))
		if !diskInfoFound && !s.isEmpty() {
			diskInfoFound = true
		}
	}
	output(diskInfoFound && verbosityLevel >= 3, writer, "      DataNode %s %+v \n", t.Id, s)
	return s
}

func (c *commandVolumeList) isNotMatchDiskInfo(readOnly bool, collection string, volumeId uint32, volumeSize int64) bool {
	if *c.readonly && !readOnly {
		return true
	}
	if *c.writable && (readOnly || volumeSize == -1 || c.volumeSizeLimitMb >= uint64(volumeSize)) {
		return true
	}
	if *c.collectionPattern != "" {
		var matched bool
		if *c.collectionPattern == CollectionDefault {
			matched = (collection == "")
		} else {
			matched, _ = filepath.Match(*c.collectionPattern, collection)
		}
		if !matched {
			return true
		}
	}
	if *c.volumeId > 0 && *c.volumeId != uint64(volumeId) {
		return true
	}
	return false
}

func (c *commandVolumeList) writeDiskInfo(writer io.Writer, t *master_pb.DiskInfo, verbosityLevel int, outNodeInfo func()) statistics {
	var s statistics
	diskType := t.Type
	if diskType == "" {
		diskType = types.HddType
	}
	slices.SortFunc(t.VolumeInfos, func(a, b *master_pb.VolumeInformationMessage) int {
		return int(a.Id) - int(b.Id)
	})
	volumeInfosFound := false

	for _, vi := range t.VolumeInfos {
		if c.isNotMatchDiskInfo(vi.ReadOnly, vi.Collection, vi.Id, int64(vi.Size)) {
			continue
		}
		if !volumeInfosFound {
			outNodeInfo()
			output(verbosityLevel >= 4, writer, "        Disk %s(%s) id:%d\n", diskType, diskInfoToString(t), t.DiskId)
			volumeInfosFound = true
		}
		s.add(writeVolumeInformationMessage(writer, vi, verbosityLevel))
	}
	ecShardInfoFound := false
	for _, ecShardInfo := range t.EcShardInfos {
		if c.isNotMatchDiskInfo(false, ecShardInfo.Collection, ecShardInfo.Id, -1) {
			continue
		}
		if !volumeInfosFound && !ecShardInfoFound {
			outNodeInfo()
			output(verbosityLevel >= 4, writer, "        Disk %s(%s) id:%d\n", diskType, diskInfoToString(t), t.DiskId)
			ecShardInfoFound = true
		}
		s.add(writeECShardInformationMessage(writer, ecShardInfo, verbosityLevel))
	}
	output((volumeInfosFound || ecShardInfoFound) && verbosityLevel >= 4, writer, "        Disk %s %+v \n", diskType, s)
	return s
}

func writeVolumeInformationMessage(writer io.Writer, t *master_pb.VolumeInformationMessage, verbosityLevel int) statistics {
	if verbosityLevel >= 5 {
		vi, err := storage.NewVolumeInfo(t)
		if err == nil {
			ttlString := ""
			if vi.Ttl != nil && vi.Ttl.Count > 0 {
				ttlString = " ttl:" + vi.Ttl.String()
			}
			readOnlyString := ""
			if vi.ReadOnly {
				readOnlyString = " readOnly:true"
			}
			output(true, writer, "          volume id:%d size:%d collection:%q file_count:%d replica_placement:%s version:%v%s%s modified_at_second:%d \n",
				vi.Id, vi.Size, vi.Collection, vi.FileCount, vi.ReplicaPlacement.String(), vi.Version, ttlString, readOnlyString, vi.ModifiedAtSecond)
		} else {
			output(true, writer, "          volume %+v \n", t)
		}
	}
	return statistics{
		Size:             t.Size,
		FileCount:        t.FileCount,
		DeletedFileCount: t.DeleteCount,
		DeletedBytes:     t.DeletedByteCount,
	}
}

func writeECShardInformationMessage(writer io.Writer, t *master_pb.VolumeEcShardInformationMessage, verbosityLevel int) statistics {
	var expireAtString string
	if t.ExpireAtSec > 0 {
		expireAtString = fmt.Sprintf("expireAt:%s", time.Unix(int64(t.ExpireAtSec), 0).Format("2006-01-02 15:04:05"))
	}

	// Build shard size information
	si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(t)
	var totalSize uint64
	var shardSizeInfo string

	if len(t.ShardSizes) > 0 {
		var shardDetails []string
		for _, shardId := range si.Ids() {
			size := uint64(si.Size(shardId))
			shardDetails = append(shardDetails, fmt.Sprintf("%d:%s", shardId, util.BytesToHumanReadable(uint64(size))))
			totalSize += size
		}
		shardSizeInfo = fmt.Sprintf(" sizes:[%s] total:%s", strings.Join(shardDetails, " "), util.BytesToHumanReadable(uint64(totalSize)))
	}

	output(verbosityLevel >= 5, writer, "          ec volume id:%v collection:%v shards:%v%s %s\n",
		t.Id, t.Collection, si.Ids(), shardSizeInfo, expireAtString)

	// TODO: add file counts once available in VolumeInformationMessage.
	return statistics{
		Size: totalSize,
	}
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

func newStatistics() *statistics {
	return &statistics{
		Size:             0,
		FileCount:        0,
		DeletedFileCount: 0,
		DeletedBytes:     0,
	}
}

func (s *statistics) add(t statistics) {
	s.Size += t.Size
	s.FileCount += t.FileCount
	s.DeletedFileCount += t.DeletedFileCount
	s.DeletedBytes += t.DeletedBytes
}

func (s *statistics) isEmpty() bool {
	return s.Size == 0 && s.FileCount == 0 && s.DeletedFileCount == 0 && s.DeletedBytes == 0
}

func (s *statistics) String() string {
	if s.DeletedFileCount > 0 {
		return fmt.Sprintf("total size:%d file_count:%d deleted_file:%d deleted_bytes:%d", s.Size, s.FileCount, s.DeletedFileCount, s.DeletedBytes)
	}
	return fmt.Sprintf("total size:%d file_count:%d", s.Size, s.FileCount)
}
