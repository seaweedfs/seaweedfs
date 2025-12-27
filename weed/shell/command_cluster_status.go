package shell

import (
	"context"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/dustin/go-humanize/english"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"io"
)

func init() {
	Commands = append(Commands, &commandClusterStatus{})
}

// Map of volume_id -> [volume replicas] with stat details.
type VolumeReplicaStats struct {
	Id       string
	VolumeId uint32

	Files        uint64
	FilesDeleted uint64
	TotalSize    uint64
}
type RegularVolumeStats map[uint32][]*VolumeReplicaStats

type commandClusterStatus struct{}
type ClusterStatusPrinter struct {
	writer             io.Writer
	writerMu           sync.Mutex
	humanize           bool
	maxParallelization int

	locked             bool
	collections        []string
	topology           *master_pb.TopologyInfo
	volumeSizeLimitMb  uint64
	regularVolumeStats RegularVolumeStats
}

func (c *commandClusterStatus) Name() string {
	return "cluster.status"
}

func (c *commandClusterStatus) Help() string {
	return `outputs a quick overview of the cluster status`
}

func (c *commandClusterStatus) HasTag(CommandTag) bool {
	return false
}

func (c *commandClusterStatus) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	flags := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	humanize := flags.Bool("humanize", true, "human-readable output")
	includeFiles := flags.Bool("files", false, "include detailed file metrics, from all volume servers")
	maxParallelization := flags.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")

	if err = flags.Parse(args); err != nil {
		return err
	}

	collections, err := ListCollectionNames(commandEnv, true, true)
	if err != nil {
		return err
	}
	topology, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	sp := &ClusterStatusPrinter{
		writer:             writer,
		humanize:           *humanize,
		maxParallelization: *maxParallelization,

		locked:            commandEnv.isLocked(),
		collections:       collections,
		topology:          topology,
		volumeSizeLimitMb: volumeSizeLimitMb,
	}
	if *includeFiles {
		if err := sp.loadFileStats(commandEnv); err != nil {
			return err
		}
	}

	sp.Print()

	return nil
}

func (sp *ClusterStatusPrinter) uint64(n uint64) string {
	if !sp.humanize {
		return fmt.Sprintf("%d", n)
	}
	return humanize.Comma(int64(n))
}

func (sp *ClusterStatusPrinter) int(n int) string {
	return sp.uint64(uint64(n))
}

func (sp *ClusterStatusPrinter) uint64Plural(n uint64, str string) string {
	if !sp.humanize {
		return fmt.Sprintf("%s(s)", str)
	}
	uin := math.MaxInt
	if n < math.MaxInt {
		uin = int(n)
	}
	return english.PluralWord(int(uin), str, "")
}

func (sp *ClusterStatusPrinter) plural(n int, str string) string {
	return sp.uint64Plural(uint64(n), str)
}

func (sp *ClusterStatusPrinter) bytes(b uint64) string {
	if !sp.humanize {
		return fmt.Sprintf("%d %s", b, sp.plural(int(b), "byte"))
	}
	return fmt.Sprintf("%s", humanize.Bytes(b))
}

func (sp *ClusterStatusPrinter) uint64Ratio(a, b uint64) string {
	var p float64
	if b != 0 {
		p = float64(a) / float64(b)
	}
	if !sp.humanize {
		return fmt.Sprintf("%.02f", p)
	}
	return fmt.Sprintf("%s", humanize.FtoaWithDigits(p, 2))
}

func (sp *ClusterStatusPrinter) intRatio(a, b int) string {
	return sp.uint64Ratio(uint64(a), uint64(b))
}

func (sp *ClusterStatusPrinter) uint64Pct(a, b uint64) string {
	var p float64
	if b != 0 {
		p = 100 * float64(a) / float64(b)
	}
	if !sp.humanize {
		return fmt.Sprintf("%.02f%%", p)
	}
	return fmt.Sprintf("%s%%", humanize.FtoaWithDigits(p, 2))
}

func (sp *ClusterStatusPrinter) intPct(a, b int) string {
	return sp.uint64Pct(uint64(a), uint64(b))
}

func (sp *ClusterStatusPrinter) write(format string, a ...any) {
	sp.writerMu.Lock()
	defer sp.writerMu.Unlock()

	format = strings.TrimRight(format, " ")
	if len(format) == 0 {
		format = "\n"
	}
	fmt.Fprintf(sp.writer, format, a...)

	last := format[len(format)-1:]
	if last != "\n" && last != "\r" {
		fmt.Fprint(sp.writer, "\n")
	}
}

func (sp *ClusterStatusPrinter) Print() {
	sp.write("")
	sp.printClusterInfo()
	sp.printVolumeInfo()
	sp.printStorageInfo()
	sp.printFilesInfo()
}

// TODO: collect stats for EC volumes as well
func (sp *ClusterStatusPrinter) loadFileStats(commandEnv *CommandEnv) error {
	sp.regularVolumeStats = RegularVolumeStats{}

	var mu sync.Mutex
	var progressTotal, progressDone uint64
	ewg := NewErrorWaitGroup(sp.maxParallelization)

	for _, dci := range sp.topology.DataCenterInfos {
		for _, ri := range dci.RackInfos {
			for _, dni := range ri.DataNodeInfos {
				for _, d := range dni.DiskInfos {
					mu.Lock()
					progressTotal += uint64(len(d.VolumeInfos))
					mu.Unlock()
					for _, v := range d.VolumeInfos {
						ewg.Add(func() error {
							// Collect regular volume stats
							err := operation.WithVolumeServerClient(false, pb.NewServerAddressWithGrpcPort(dni.Id, int(dni.GrpcPort)), commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
								resp, reqErr := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
									VolumeId: uint32(v.Id),
								})
								if reqErr != nil {
									return reqErr
								}

								mu.Lock()
								defer mu.Unlock()
								if resp != nil {
									if _, ok := sp.regularVolumeStats[v.Id]; !ok {
										sp.regularVolumeStats[v.Id] = []*VolumeReplicaStats{}
									}
									sp.regularVolumeStats[v.Id] = append(sp.regularVolumeStats[v.Id], &VolumeReplicaStats{
										Id:           dni.Id,
										VolumeId:     v.Id,
										Files:        resp.FileCount,
										FilesDeleted: resp.FileDeletedCount,
										TotalSize:    resp.VolumeSize,
									})
								}
								progressDone++
								return nil
							})
							if err != nil {
								return err
							}

							mu.Lock()
							sp.write("collecting file stats: %s     \r", sp.uint64Pct(progressDone, progressTotal))
							mu.Unlock()
							return nil
						})
					}
				}
			}
		}
	}

	err := ewg.Wait()
	sp.write("")
	return err
}

func (sp *ClusterStatusPrinter) printClusterInfo() {
	dcs := len(sp.topology.DataCenterInfos)

	racks := 0
	nodes := 0
	disks := 0
	for _, dci := range sp.topology.DataCenterInfos {
		racks += len(dci.RackInfos)
		for _, ri := range dci.RackInfos {
			for _, dni := range ri.DataNodeInfos {
				nodes++
				disks += len(dni.DiskInfos)
			}
		}
	}

	status := "unlocked"
	if sp.locked {
		status = "LOCKED"
	}

	sp.write("cluster:")
	sp.write("\tid:       %s", sp.topology.Id)
	sp.write("\tstatus:   %s", status)
	sp.write("\tnodes:    %s", sp.int(nodes))
	sp.write("\ttopology: %s %s, %s %s on %s %s",
		sp.int(dcs), sp.plural(dcs, "DC"),
		sp.int(disks), sp.plural(disks, "disk"),
		sp.int(racks), sp.plural(racks, "rack"))
	sp.write("")

}

func (sp *ClusterStatusPrinter) printVolumeInfo() {
	collections := len(sp.collections)
	var maxVolumes uint64
	volumeIds := map[needle.VolumeId]bool{}
	ecVolumeIds := map[needle.VolumeId]bool{}

	var replicas, roReplicas, rwReplicas, ecShards int

	for _, dci := range sp.topology.DataCenterInfos {
		for _, ri := range dci.RackInfos {
			for _, dni := range ri.DataNodeInfos {
				for _, di := range dni.DiskInfos {
					maxVolumes += uint64(di.MaxVolumeCount)
					for _, vi := range di.VolumeInfos {
						vid := needle.VolumeId(vi.Id)
						volumeIds[vid] = true
						replicas++
						if vi.ReadOnly {
							roReplicas++
						} else {
							rwReplicas++
						}
					}
					for _, eci := range di.EcShardInfos {
						vid := needle.VolumeId(eci.Id)
						ecVolumeIds[vid] = true
						ecShards += erasure_coding.ShardsCountFromVolumeEcShardInformationMessage(eci)
					}
				}
			}
		}
	}

	volumes := len(volumeIds)
	ecVolumes := len(ecVolumeIds)
	totalVolumes := volumes + ecVolumes

	sp.write("volumes:")
	sp.write("\ttotal:    %s %s, %s %s",
		sp.int(totalVolumes), sp.plural(totalVolumes, "volume"),
		sp.int(collections), sp.plural(collections, "collection"))
	sp.write("\tmax size: %s", sp.bytes(sp.volumeSizeLimitMb*1024*1024))
	sp.write("\tregular:  %s/%s %s on %s %s, %s writable (%s), %s read-only (%s)",
		sp.int(volumes), sp.uint64(maxVolumes), sp.plural(volumes, "volume"),
		sp.int(replicas), sp.plural(replicas, "replica"),
		sp.int(rwReplicas), sp.intPct(rwReplicas, replicas),
		sp.int(roReplicas), sp.intPct(roReplicas, replicas))
	sp.write("\tEC:       %s EC %s on %s %s (%s shards/volume)",
		sp.int(ecVolumes), sp.plural(ecVolumes, "volume"),
		sp.int(ecShards), sp.plural(ecShards, "shard"),
		sp.intRatio(ecShards, ecVolumes))
	sp.write("")

}

func (sp *ClusterStatusPrinter) printStorageInfo() {
	perVolumeSize := map[needle.VolumeId]uint64{}
	perEcVolumeSize := map[needle.VolumeId]uint64{}
	var rawVolumeSize, rawEcVolumeSize uint64

	for _, dci := range sp.topology.DataCenterInfos {
		for _, ri := range dci.RackInfos {
			for _, dni := range ri.DataNodeInfos {
				for _, di := range dni.DiskInfos {
					for _, vi := range di.VolumeInfos {
						vid := needle.VolumeId(vi.Id)
						perVolumeSize[vid] = vi.Size
						rawVolumeSize += vi.Size
					}
					for _, eci := range di.EcShardInfos {
						vid := needle.VolumeId(eci.Id)
						var size uint64
						for _, ss := range eci.ShardSizes {
							size += uint64(ss)
						}
						perEcVolumeSize[vid] += size
						rawEcVolumeSize += size
					}

				}
			}
		}
	}
	// normalize EC logical volume sizes given shard settings
	for vid := range perEcVolumeSize {
		perEcVolumeSize[vid] = perEcVolumeSize[vid] * erasure_coding.DataShardsCount / erasure_coding.TotalShardsCount
	}

	var volumeSize, ecVolumeSize uint64
	for _, s := range perVolumeSize {
		volumeSize += s
	}
	for _, s := range perEcVolumeSize {
		ecVolumeSize += s
	}
	totalSize := volumeSize + ecVolumeSize

	sp.write("storage:")
	sp.write("\ttotal:           %s", sp.bytes(totalSize))
	sp.write("\tregular volumes: %s", sp.bytes(volumeSize))
	sp.write("\tEC volumes:      %s", sp.bytes(ecVolumeSize))
	sp.write("\traw:             %s on volume replicas, %s on EC shards", sp.bytes(rawVolumeSize), sp.bytes(rawEcVolumeSize))
	sp.write("")
}

func (sp *ClusterStatusPrinter) printFilesInfo() {
	if len(sp.regularVolumeStats) == 0 {
		return
	}

	var regularFilesTotal, regularFilesDeleted, regularFilesSize uint64
	var regularFilesTotalRaw, regularFilesDeletedRaw, regularFilesSizeRaw uint64

	for _, replicaStats := range sp.regularVolumeStats {
		rc := uint64(len(replicaStats))

		var volumeFilesTotal, volumeFilesSize, volumeFilesDeleted uint64
		for _, rs := range replicaStats {
			regularFilesTotalRaw += rs.Files
			regularFilesSizeRaw += rs.TotalSize
			regularFilesDeletedRaw += rs.FilesDeleted

			volumeFilesTotal += rs.Files
			volumeFilesSize += rs.TotalSize
			volumeFilesDeleted += rs.FilesDeleted
		}
		regularFilesTotal += (volumeFilesTotal / rc)
		regularFilesSize += (volumeFilesSize / rc)
		regularFilesDeleted += (volumeFilesDeleted / rc)
	}

	regularFiles := regularFilesTotal - regularFilesDeleted
	regularFilesRaw := regularFilesTotalRaw - regularFilesDeletedRaw
	var avgFileSize uint64
	if regularFilesTotal != 0 {
		avgFileSize = regularFilesSize / regularFilesTotal
	}

	sp.write("files:")
	sp.write("\tregular:     %s %s, %s readable (%s), %s deleted (%s), avg %s per file",
		sp.uint64(regularFilesTotal), sp.uint64Plural(regularFilesTotal, "file"),
		sp.uint64(regularFiles), sp.uint64Pct(regularFiles, regularFilesTotal),
		sp.uint64(regularFilesDeleted), sp.uint64Pct(regularFilesDeleted, regularFilesTotal),
		sp.bytes(avgFileSize))
	sp.write("\tregular raw: %s %s, %s readable (%s), %s deleted (%s), %s total",
		sp.uint64(regularFilesTotalRaw), sp.uint64Plural(regularFilesTotalRaw, "file"),
		sp.uint64(regularFilesRaw), sp.uint64Pct(regularFilesRaw, regularFilesTotalRaw),
		sp.uint64(regularFilesDeletedRaw), sp.uint64Pct(regularFilesDeletedRaw, regularFilesTotalRaw),
		sp.bytes(regularFilesSizeRaw))
	sp.write("\tEC:          [no data]")
	sp.write("\tEC raw:      [no data]")
	sp.write("")
}
