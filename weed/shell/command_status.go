package shell

import (
	"flag"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"io"
)

func init() {
	Commands = append(Commands, &commandStatus{})
}

type commandStatus struct{}
type StatusPrinter struct {
	writer         io.Writer
	verbosityLevel int

	locked            bool
	collections       []string
	topology          *master_pb.TopologyInfo
	volumeSizeLimitMb uint64
}

func (c *commandStatus) Name() string {
	return "status"
}

func (c *commandStatus) Help() string {
	return `outputs a quick overview of the cluster status`
}

func (c *commandStatus) HasTag(CommandTag) bool {
	return false
}

func (c *commandStatus) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	flags := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbosityLevel := flags.Int("v", 5, "verbose mode: 0, 1, 2, 3, 4, 5")

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

	sp := &StatusPrinter{
		writer:         writer,
		verbosityLevel: *verbosityLevel,

		locked:            commandEnv.isLocked(),
		collections:       collections,
		topology:          topology,
		volumeSizeLimitMb: volumeSizeLimitMb,
	}
	sp.Print()

	return nil
}

// TODO: humanize figures in output
// TODO: add option to collect detailed file stats
func (sp *StatusPrinter) Print() {
	sp.write("")
	sp.printClusterInfo()
	sp.printVolumeInfo()
	sp.printStorageInfo()
}

func (sp *StatusPrinter) write(format string, a ...any) {
	fmt.Fprintf(sp.writer, strings.TrimRight(format, "\r\n "), a...)
	fmt.Fprint(sp.writer, "\n")
}

func (sp *StatusPrinter) printClusterInfo() {
	disks := len(sp.topology.DiskInfos)
	dcs := len(sp.topology.DataCenterInfos)

	racks := 0
	nodes := 0
	for _, dci := range sp.topology.DataCenterInfos {
		racks += len(dci.RackInfos)
		for _, ri := range dci.RackInfos {
			nodes += len(ri.DataNodeInfos)
		}
	}

	status := "unlocked"
	if sp.locked {
		status = "LOCKED"
	}

	sp.write("cluster:")
	sp.write("\tid:       %s", sp.topology.Id)
	sp.write("\tstatus:   %s", status)
	sp.write("\tnodes:    %d", nodes)
	sp.write("\ttopology: %d DC(s), %d disk(s) on %d rack(s)", dcs, disks, racks)
	sp.write("")
}

func (sp *StatusPrinter) printVolumeInfo() {
	collections := len(sp.collections)
	var maxVolumes uint64
	volumes := map[needle.VolumeId]bool{}
	ecVolumes := map[needle.VolumeId]bool{}

	var replicas, roReplicas, rwReplicas, ecShards uint64

	for _, dci := range sp.topology.DataCenterInfos {
		for _, ri := range dci.RackInfos {
			for _, dni := range ri.DataNodeInfos {
				for _, di := range dni.DiskInfos {
					maxVolumes += uint64(di.MaxVolumeCount)
					for _, vi := range di.VolumeInfos {
						vid := needle.VolumeId(vi.Id)
						volumes[vid] = true
						replicas++
						if vi.ReadOnly {
							roReplicas++
						} else {
							rwReplicas++
						}
					}
					for _, eci := range di.EcShardInfos {
						vid := needle.VolumeId(eci.Id)
						ecVolumes[vid] = true
						ecShards += uint64(erasure_coding.ShardBits(eci.EcIndexBits).ShardIdCount())
					}
				}
			}
		}
	}

	var roReplicasRatio, rwReplicasRatio, ecShardsPerVolume float64
	if replicas != 0 {
		roReplicasRatio = float64(roReplicas) / float64(replicas)
		rwReplicasRatio = float64(rwReplicas) / float64(replicas)
	}
	if len(ecVolumes) != 0 {
		ecShardsPerVolume = float64(ecShards) / float64(len(ecVolumes))
	}

	totalVolumes := len(volumes) + len(ecVolumes)

	sp.write("volumes:")
	sp.write("\ttotal:    %d volumes on %d collections", totalVolumes, collections)
	sp.write("\tmax size: %d bytes", sp.volumeSizeLimitMb*1024*1024)
	sp.write("\tregular:  %d/%d volumes on %d replicas, %d writable (%.02f%%), %d read-only (%.02f%%)", len(volumes), maxVolumes, replicas, rwReplicas, 100*rwReplicasRatio, roReplicas, 100*roReplicasRatio)
	sp.write("\tEC:       %d EC volumes on %d shards (%.02f shards/volume)", len(ecVolumes), ecShards, ecShardsPerVolume)
	sp.write("")
}

func (sp *StatusPrinter) printStorageInfo() {
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

	var volumeSize, ecVolumeSize uint64
	for _, s := range perVolumeSize {
		volumeSize += s
	}
	for _, s := range perEcVolumeSize {
		ecVolumeSize += s
	}

	totalSize := volumeSize + ecVolumeSize

	sp.write("storage:")
	sp.write("\ttotal:           %d bytes", totalSize)
	sp.write("\tregular volumes: %d bytes", volumeSize)
	sp.write("\tEC volumes:      %d bytes", ecVolumeSize)
	sp.write("\traw:             %d bytes on volume replicas, %d bytes on EC shard files", rawVolumeSize, rawEcVolumeSize)
	sp.write("")
}
