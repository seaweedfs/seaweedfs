package erasure_coding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/grpc"
)

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
	WithFields(map[string]interface{}) interface {
		Info(string, ...interface{})
		Warning(string, ...interface{})
		Error(string, ...interface{})
	}
}

func withFields(log logger, fields map[string]interface{}) logger {
	if fields == nil {
		return log
	}
	if wf, ok := log.(withFieldLogger); ok {
		if enhanced, ok := wf.WithFields(fields).(logger); ok {
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
func DistributeEcShards(volumeID uint32, collection string, targets []*worker_pb.TaskTarget, shardFiles map[string]string, dialOption grpc.DialOption, logger logger) (map[string][]string, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("no targets specified for EC shard distribution")
	}

	if len(shardFiles) == 0 {
		return nil, fmt.Errorf("no shard files available for distribution")
	}

	log := ensureLogger(logger)

	shardAssignment := make(map[string][]string)

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
				return nil, fmt.Errorf("shard file %s not found for destination %s", shardType, destNode)
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

			if err := sendShardFileToDestination(volumeID, collection, dialOption, destNode, filePath, shardType); err != nil {
				return nil, fmt.Errorf("failed to send %s to %s: %v", shardType, destNode, err)
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

	glog.V(1).Infof("Successfully distributed EC shards to %d destinations", len(shardAssignment))
	return shardAssignment, nil
}

// MountEcShards mounts EC shards on destination servers using an assignment map.
func MountEcShards(volumeID uint32, collection string, shardAssignment map[string][]string, dialOption grpc.DialOption, logger logger) error {
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
					VolumeId:   volumeID,
					Collection: collection,
					ShardIds:   shardIds,
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

// sendShardFileToDestination sends a single shard file to a destination server using ReceiveFile API.
func sendShardFileToDestination(volumeID uint32, collection string, dialOption grpc.DialOption, destServer, filePath, shardType string) error {
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
			} else if strings.HasPrefix(shardType, "ec") && len(shardType) == 4 {
				ext = "." + shardType
				fmt.Sscanf(shardType[2:], "%d", &shardId)
			} else {
				return fmt.Errorf("unknown shard type: %s", shardType)
			}

			stream, err := client.ReceiveFile(context.Background())
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
