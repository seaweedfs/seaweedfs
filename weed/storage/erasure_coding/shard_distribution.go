package erasure_coding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ShardDistributionOptions controls DistributeEcShards concurrency.
// MaxShardTransfers:
//   - nil / unset: no global cap; one in-flight per (destNode, disk_id), or
//     per destNode (host:port, not rack) when any target leaves disk choice
//     to the receiver (disk_id 0)
//   - N<=0: historical sequential nested loops
//   - N>0: global cap of N, still at most one in-flight per key
type ShardDistributionOptions struct {
	MaxShardTransfers *int
	send              shardSender
}

type shardSender func(ctx context.Context, volumeID uint32, collection string, dialOption grpc.DialOption, destServer string, diskID uint32, filePath, shardType string) error

type shardJob struct {
	destNode  string
	shardType string
	filePath  string
	diskID    uint32
	hasDisk   bool
}

func shardSenderOrDefault(opts *ShardDistributionOptions) shardSender {
	if opts != nil && opts.send != nil {
		return opts.send
	}
	return sendShardFileToDestination
}

func shardTransferCap(opts *ShardDistributionOptions) (sequential bool, capN int) {
	if opts == nil || opts.MaxShardTransfers == nil {
		return false, 0
	}
	n := *opts.MaxShardTransfers
	if n <= 0 {
		return true, 0
	}
	return false, n
}

func lookupShardDisk(shardDisks map[string]map[string]uint32, destNode, shardType string) (uint32, bool) {
	if byShard, ok := shardDisks[destNode]; ok {
		if id, exists := byShard[shardType]; exists {
			return id, true
		}
	}
	return 0, false
}

func shardTransferKey(j shardJob) string {
	if j.hasDisk {
		return fmt.Sprintf("%s#%d", j.destNode, j.diskID)
	}
	return j.destNode
}

func ensureLogger(logger logger) logger {
	if logger == nil {
		return &glogFallbackLogger{}
	}
	return logger
}

type logger interface {
	Info(string, ...interface{})
	Warning(string, ...interface{})
	Error(string, ...interface{})
}

type withFieldLogger interface {
	WithFields(map[string]interface{}) logger
}

func withFields(log logger, fields map[string]interface{}) logger {
	if fields == nil {
		return log
	}
	if wf, ok := log.(withFieldLogger); ok {
		return wf.WithFields(fields)
	}

	val := reflect.ValueOf(log)
	method := val.MethodByName("WithFields")
	if !method.IsValid() {
		return log
	}
	methodType := method.Type()
	if methodType.NumIn() != 1 || methodType.NumOut() != 1 {
		return log
	}
	argType := methodType.In(0)
	mapType := reflect.TypeOf(map[string]interface{}{})
	var arg reflect.Value
	if mapType.AssignableTo(argType) {
		arg = reflect.ValueOf(fields)
	} else if mapType.ConvertibleTo(argType) {
		arg = reflect.ValueOf(fields).Convert(argType)
	} else {
		return log
	}
	result := method.Call([]reflect.Value{arg})[0]
	if result.IsValid() && result.CanInterface() {
		if enhanced, ok := result.Interface().(logger); ok {
			return enhanced
		}
	}
	return log
}

type glogFallbackLogger struct{}

func (g *glogFallbackLogger) Info(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Infof(msg, args...)
		return
	}
	glog.Info(msg)
}

func (g *glogFallbackLogger) Warning(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Warningf(msg, args...)
		return
	}
	glog.Warning(msg)
}

func (g *glogFallbackLogger) Error(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Errorf(msg, args...)
		return
	}
	glog.Error(msg)
}

// DistributeEcShards distributes locally generated EC shards to destination servers.
// Returns the shard assignment map used for mounting.
// opts nil is the default: one in-flight per dest disk/node, no global cap.
func DistributeEcShards(volumeID uint32, collection string, targets []*worker_pb.TaskTarget, shardFiles map[string]string, dialOption grpc.DialOption, logger logger, opts *ShardDistributionOptions) (map[string][]string, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("no targets specified for EC shard distribution")
	}

	if len(shardFiles) == 0 {
		return nil, fmt.Errorf("no shard files available for distribution")
	}

	log := ensureLogger(logger)

	shardAssignment := make(map[string][]string)
	// node → shardType → planner-assigned diskID. Metadata files (ecx/ecj/vif)
	// inherit the first data shard's disk so they land next to the shards.
	shardDisks := make(map[string]map[string]uint32)

	for _, target := range targets {
		if len(target.ShardIds) == 0 {
			continue
		}

		var assignedShards []string
		for _, shardId := range target.ShardIds {
			shardType := fmt.Sprintf("ec%02d", shardId)
			assignedShards = append(assignedShards, shardType)
		}

		if len(assignedShards) > 0 {
			if _, hasEcx := shardFiles["ecx"]; hasEcx {
				assignedShards = append(assignedShards, "ecx")
			}
			if _, hasEcj := shardFiles["ecj"]; hasEcj {
				assignedShards = append(assignedShards, "ecj")
			}
			if _, hasVif := shardFiles["vif"]; hasVif {
				assignedShards = append(assignedShards, "vif")
			}
			if _, hasEcsum := shardFiles["ecsum"]; hasEcsum {
				assignedShards = append(assignedShards, "ecsum")
			}
		}

		if shardDisks[target.Node] == nil {
			shardDisks[target.Node] = make(map[string]uint32)
		}
		for _, shardType := range assignedShards {
			if _, already := shardDisks[target.Node][shardType]; !already {
				shardDisks[target.Node][shardType] = target.DiskId
			}
		}

		existing := shardAssignment[target.Node]
		if len(existing) == 0 {
			shardAssignment[target.Node] = assignedShards
			continue
		}

		seen := make(map[string]struct{}, len(existing))
		for _, shard := range existing {
			seen[shard] = struct{}{}
		}
		for _, shard := range assignedShards {
			if _, ok := seen[shard]; ok {
				continue
			}
			seen[shard] = struct{}{}
			existing = append(existing, shard)
		}
		shardAssignment[target.Node] = existing
	}

	if len(shardAssignment) == 0 {
		return nil, fmt.Errorf("no shard assignments found from planning phase")
	}

	send := shardSenderOrDefault(opts)
	sequential, capN := shardTransferCap(opts)
	if sequential {
		if err := distributeEcShardsSequential(volumeID, collection, shardAssignment, shardFiles, shardDisks, dialOption, log, send); err != nil {
			return nil, err
		}
	} else if err := distributeEcShardsParallel(volumeID, collection, shardAssignment, shardFiles, shardDisks, dialOption, log, send, capN); err != nil {
		return nil, err
	}

	glog.V(1).Infof("Successfully distributed EC shards to %d destinations", len(shardAssignment))
	return shardAssignment, nil
}

func distributeEcShardsSequential(volumeID uint32, collection string, shardAssignment map[string][]string, shardFiles map[string]string, shardDisks map[string]map[string]uint32, dialOption grpc.DialOption, log logger, send shardSender) error {
	ctx := context.Background()
	for destNode, assignedShards := range shardAssignment {
		withFields(log, map[string]interface{}{
			"destination":     destNode,
			"assigned_shards": len(assignedShards),
			"shard_types":     assignedShards,
		}).Info("Starting shard distribution to destination server")

		var transferredBytes int64
		for _, shardType := range assignedShards {
			filePath, exists := shardFiles[shardType]
			if !exists {
				return fmt.Errorf("shard file %s not found for destination %s", shardType, destNode)
			}

			if info, err := os.Stat(filePath); err == nil {
				transferredBytes += info.Size()
				withFields(log, map[string]interface{}{
					"destination": destNode,
					"shard_type":  shardType,
					"file_path":   filePath,
					"size_bytes":  info.Size(),
					"size_kb":     float64(info.Size()) / 1024,
				}).Info("Starting shard file transfer")
			}

			diskID, _ := lookupShardDisk(shardDisks, destNode, shardType)
			if err := send(ctx, volumeID, collection, dialOption, destNode, diskID, filePath, shardType); err != nil {
				return fmt.Errorf("failed to send %s to %s: %w", shardType, destNode, err)
			}

			withFields(log, map[string]interface{}{
				"destination": destNode,
				"shard_type":  shardType,
			}).Info("Shard file transfer completed")
		}

		withFields(log, map[string]interface{}{
			"destination":        destNode,
			"shards_transferred": len(assignedShards),
			"total_bytes":        transferredBytes,
			"total_mb":           float64(transferredBytes) / (1024 * 1024),
		}).Info("All shards distributed to destination server")
	}
	return nil
}

func collectShardJobs(shardAssignment map[string][]string, shardFiles map[string]string, shardDisks map[string]map[string]uint32, log logger) (map[string][]shardJob, error) {
	// disk_id 0 leaves placement to the receiver's auto-select, which can land
	// on a disk another queued stream is pinned to — serialize every transfer
	// to that server under one key.
	autoPlacement := make(map[string]bool)
	for destNode, assignedShards := range shardAssignment {
		for _, shardType := range assignedShards {
			if diskID, ok := lookupShardDisk(shardDisks, destNode, shardType); ok && diskID == 0 {
				autoPlacement[destNode] = true
				break
			}
		}
	}

	byKey := make(map[string][]shardJob)
	for destNode, assignedShards := range shardAssignment {
		withFields(log, map[string]interface{}{
			"destination":     destNode,
			"assigned_shards": len(assignedShards),
			"shard_types":     assignedShards,
		}).Info("Starting shard distribution to destination server")

		for _, shardType := range assignedShards {
			filePath, exists := shardFiles[shardType]
			if !exists {
				return nil, fmt.Errorf("shard file %s not found for destination %s", shardType, destNode)
			}
			diskID, hasDisk := lookupShardDisk(shardDisks, destNode, shardType)
			job := shardJob{
				destNode:  destNode,
				shardType: shardType,
				filePath:  filePath,
				diskID:    diskID,
				hasDisk:   hasDisk,
			}
			key := shardTransferKey(job)
			if autoPlacement[destNode] {
				key = destNode
			}
			byKey[key] = append(byKey[key], job)
		}
	}
	return byKey, nil
}

func distributeEcShardsParallel(volumeID uint32, collection string, shardAssignment map[string][]string, shardFiles map[string]string, shardDisks map[string]map[string]uint32, dialOption grpc.DialOption, log logger, send shardSender, capN int) error {
	byKey, err := collectShardJobs(shardAssignment, shardFiles, shardDisks, log)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(context.Background())
	var sem chan struct{}
	if capN > 0 {
		sem = make(chan struct{}, capN)
	}

	for _, jobs := range byKey {
		jobs := jobs
		g.Go(func() error {
			for _, job := range jobs {
				if err := ctx.Err(); err != nil {
					return err
				}
				if sem != nil {
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}
					if err := ctx.Err(); err != nil {
						<-sem
						return err
					}
				}
				err := transferOneShard(ctx, volumeID, collection, dialOption, log, send, job)
				if err != nil {
					// Hold the slot so another key cannot start a send in the
					// window before errgroup cancels ctx.
					return err
				}
				if sem != nil {
					<-sem
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for destNode, assignedShards := range shardAssignment {
		var transferredBytes int64
		for _, shardType := range assignedShards {
			if filePath, ok := shardFiles[shardType]; ok {
				if info, statErr := os.Stat(filePath); statErr == nil {
					transferredBytes += info.Size()
				}
			}
		}
		withFields(log, map[string]interface{}{
			"destination":        destNode,
			"shards_transferred": len(assignedShards),
			"total_bytes":        transferredBytes,
			"total_mb":           float64(transferredBytes) / (1024 * 1024),
		}).Info("All shards distributed to destination server")
	}
	return nil
}

func transferOneShard(ctx context.Context, volumeID uint32, collection string, dialOption grpc.DialOption, log logger, send shardSender, job shardJob) error {
	if info, err := os.Stat(job.filePath); err == nil {
		withFields(log, map[string]interface{}{
			"destination": job.destNode,
			"shard_type":  job.shardType,
			"file_path":   job.filePath,
			"size_bytes":  info.Size(),
			"size_kb":     float64(info.Size()) / 1024,
		}).Info("Starting shard file transfer")
	}
	if err := send(ctx, volumeID, collection, dialOption, job.destNode, job.diskID, job.filePath, job.shardType); err != nil {
		return fmt.Errorf("failed to send %s to %s: %w", job.shardType, job.destNode, err)
	}
	withFields(log, map[string]interface{}{
		"destination": job.destNode,
		"shard_type":  job.shardType,
	}).Info("Shard file transfer completed")
	return nil
}

// MountEcShards mounts EC shards on destination servers using an assignment map.
// sourceDiskType is forwarded to VolumeEcShardsMount so the resulting EC volume
// reports under the source's disk type rather than the destination's (#9423).
func MountEcShards(volumeID uint32, collection string, shardAssignment map[string][]string, sourceDiskType string, dialOption grpc.DialOption, logger logger) error {
	if shardAssignment == nil {
		return fmt.Errorf("shard assignment not available for mounting")
	}

	log := ensureLogger(logger)

	var mountErrors []error
	for destNode, assignedShards := range shardAssignment {
		var shardIds []uint32
		var metadataFiles []string

		for _, shardType := range assignedShards {
			if strings.HasPrefix(shardType, "ec") && len(shardType) == 4 {
				var shardId uint32
				if _, err := fmt.Sscanf(shardType[2:], "%d", &shardId); err == nil {
					shardIds = append(shardIds, shardId)
				}
			} else {
				metadataFiles = append(metadataFiles, shardType)
			}
		}

		withFields(log, map[string]interface{}{
			"destination":    destNode,
			"shard_ids":      shardIds,
			"shard_count":    len(shardIds),
			"metadata_files": metadataFiles,
		}).Info("Starting EC shard mount operation")

		if len(shardIds) == 0 {
			withFields(log, map[string]interface{}{
				"destination":    destNode,
				"metadata_files": metadataFiles,
			}).Info("No EC shards to mount (only metadata files)")
			continue
		}

		err := operation.WithVolumeServerClient(false, pb.ServerAddress(destNode), dialOption,
			func(client volume_server_pb.VolumeServerClient) error {
				_, mountErr := client.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
					VolumeId:       volumeID,
					Collection:     collection,
					ShardIds:       shardIds,
					SourceDiskType: sourceDiskType,
				})
				return mountErr
			})

		if err != nil {
			mountErrors = append(mountErrors, fmt.Errorf("mount %s shards %v: %w", destNode, shardIds, err))
			withFields(log, map[string]interface{}{
				"destination": destNode,
				"shard_ids":   shardIds,
				"error":       err.Error(),
			}).Error("Failed to mount EC shards")
		} else {
			withFields(log, map[string]interface{}{
				"destination": destNode,
				"shard_ids":   shardIds,
				"volume_id":   volumeID,
				"collection":  collection,
			}).Info("Successfully mounted EC shards")
		}
	}

	if len(mountErrors) > 0 {
		return errors.Join(mountErrors...)
	}
	return nil
}

// diskID=0 leaves disk placement to the server's auto-select.
func sendShardFileToDestination(ctx context.Context, volumeID uint32, collection string, dialOption grpc.DialOption, destServer string, diskID uint32, filePath, shardType string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return operation.WithVolumeServerClient(false, pb.ServerAddress(destServer), dialOption,
		func(client volume_server_pb.VolumeServerClient) error {
			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open shard file %s: %v", filePath, err)
			}
			defer file.Close()

			fileInfo, err := file.Stat()
			if err != nil {
				return fmt.Errorf("failed to get file info for %s: %v", filePath, err)
			}

			var ext string
			var shardId uint32
			if shardType == "ecx" {
				ext = ".ecx"
				shardId = 0
			} else if shardType == "ecj" {
				ext = ".ecj"
				shardId = 0
			} else if shardType == "vif" {
				ext = ".vif"
				shardId = 0
			} else if shardType == "ecsum" {
				ext = BitrotSidecarExt
				shardId = 0
			} else if strings.HasPrefix(shardType, "ec") && len(shardType) == 4 {
				ext = "." + shardType
				fmt.Sscanf(shardType[2:], "%d", &shardId)
			} else {
				return fmt.Errorf("unknown shard type: %s", shardType)
			}

			stream, err := client.ReceiveFile(ctx)
			if err != nil {
				return fmt.Errorf("failed to create receive stream: %v", err)
			}

			err = stream.Send(&volume_server_pb.ReceiveFileRequest{
				Data: &volume_server_pb.ReceiveFileRequest_Info{
					Info: &volume_server_pb.ReceiveFileInfo{
						VolumeId:   volumeID,
						Ext:        ext,
						Collection: collection,
						IsEcVolume: true,
						ShardId:    shardId,
						FileSize:   uint64(fileInfo.Size()),
						DiskId:     diskID,
					},
				},
			})
			if err != nil {
				return fmt.Errorf("failed to send file info: %v", err)
			}

			buffer := make([]byte, 64*1024)
			for {
				n, readErr := file.Read(buffer)
				if n > 0 {
					err = stream.Send(&volume_server_pb.ReceiveFileRequest{
						Data: &volume_server_pb.ReceiveFileRequest_FileContent{
							FileContent: buffer[:n],
						},
					})
					if err != nil {
						return fmt.Errorf("failed to send file content: %v", err)
					}
				}
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					return fmt.Errorf("failed to read file: %v", readErr)
				}
			}

			resp, err := stream.CloseAndRecv()
			if err != nil {
				return fmt.Errorf("failed to close stream: %v", err)
			}

			if resp.Error != "" {
				return fmt.Errorf("server error: %s", resp.Error)
			}

			glog.V(2).Infof("Successfully sent %s (%d bytes) to %s", shardType, resp.BytesWritten, destServer)
			return nil
		})
}
