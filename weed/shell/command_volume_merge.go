package shell

import (
	"container/heap"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

const mergeIdleTimeoutSeconds = 1

func init() {
	Commands = append(Commands, &commandVolumeMerge{})
}

type commandVolumeMerge struct{}

func (c *commandVolumeMerge) Name() string {
	return "volume.merge"
}

func (c *commandVolumeMerge) Help() string {
	return `merge replicas for a volume id in timestamp order into a fresh copy

	volume.merge -volumeId <volume id>

This command:
	1) marks the volume readonly on replicas (if not already)
	2) allocates a temporary copy on a third location
	3) merges replicas in append timestamp order, skipping duplicates
	4) replaces the original replicas with the merged volume
	5) restores writable state if it was writable before
`
}

func (c *commandVolumeMerge) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeMerge) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	mergeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := mergeCommand.Int("volumeId", 0, "the volume id")
	targetNodeStr := mergeCommand.String("target", "", "optional target volume server <host>:<port> for temporary merge output")
	noLock := mergeCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	if err = mergeCommand.Parse(args); err != nil {
		return nil
	}

	if *volumeIdInt == 0 {
		return fmt.Errorf("volumeId is required")
	}

	if *noLock {
		commandEnv.noLock = true
	} else if err = commandEnv.confirmIsLocked(args); err != nil {
		return err
	}

	volumeId := needle.VolumeId(*volumeIdInt)

	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	volumeReplicas, allLocations := collectVolumeReplicaLocations(topologyInfo)
	replicas := volumeReplicas[uint32(volumeId)]
	if len(replicas) < 2 {
		return fmt.Errorf("volume %d has %d replica(s); merge requires at least two", volumeId, len(replicas))
	}

	volumeInfo := replicas[0].info
	replicaPlacement, err := super_block.NewReplicaPlacementFromByte(byte(volumeInfo.ReplicaPlacement))
	if err != nil {
		return fmt.Errorf("parse replica placement for volume %d: %w", volumeId, err)
	}

	var targetServer pb.ServerAddress
	if *targetNodeStr != "" {
		targetServer = pb.ServerAddress(*targetNodeStr)
		if isReplicaServer(targetServer, replicas) {
			return fmt.Errorf("target %s already hosts volume %d", *targetNodeStr, volumeId)
		}
		if err = allocateMergeVolume(commandEnv.option.GrpcDialOption, targetServer, volumeInfo, replicaPlacement); err != nil {
			return err
		}
	} else {
		targetServer, err = allocateMergeVolumeOnThirdLocation(commandEnv.option.GrpcDialOption, allLocations, replicas, volumeInfo, replicaPlacement)
		if err != nil {
			return err
		}
	}

	cleanupTarget := true
	defer func() {
		if !cleanupTarget {
			return
		}
		_ = deleteVolume(commandEnv.option.GrpcDialOption, volumeId, targetServer, false)
	}()

	shouldRestoreWritable, err := ensureVolumeReadonly(commandEnv, replicas)
	if err != nil {
		return err
	}
	if shouldRestoreWritable {
		defer func() {
			_ = markReplicasWritable(commandEnv.option.GrpcDialOption, replicas, true, false)
		}()
	}

	sources := make([]needleStream, 0, len(replicas))
	for _, replica := range replicas {
		server := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		sources = append(sources, startTailNeedleStream(commandEnv.option.GrpcDialOption, volumeId, server))
	}

	mergeErr := operation.WithVolumeServerClient(false, targetServer, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		version := needle.Version(volumeInfo.Version)
		if version == 0 {
			version = needle.GetCurrentVersion()
		}
		seen := make(map[types.NeedleId]needleSeen)
		return mergeNeedleStreams(sources, func(streamIndex int, n *needle.Needle) error {
			ts := needleTimestamp(n)
			if prev, ok := seen[n.Id]; ok && prev.timestamp == ts && prev.streamIndex != streamIndex {
				return nil
			}
			seen[n.Id] = needleSeen{timestamp: ts, streamIndex: streamIndex}
			blob, size, err := needleBlobFromNeedle(n, version)
			if err != nil {
				return err
			}
			_, err = client.WriteNeedleBlob(context.Background(), &volume_server_pb.WriteNeedleBlobRequest{
				VolumeId:   uint32(volumeId),
				NeedleId:   uint64(n.Id),
				Size:       int32(size),
				NeedleBlob: blob,
			})
			return err
		})
	})
	if mergeErr != nil {
		return mergeErr
	}

	for _, replica := range replicas {
		sourceServer := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		if _, err = copyVolume(commandEnv.option.GrpcDialOption, writer, volumeId, sourceServer, targetServer, "", 0, false); err != nil {
			return err
		}
	}

	if err = deleteVolume(commandEnv.option.GrpcDialOption, volumeId, targetServer, false); err != nil {
		return err
	}
	cleanupTarget = false

	fmt.Fprintf(writer, "merged volume %d from %d replicas via %s\n", volumeId, len(replicas), targetServer)
	return nil
}

type needleStream interface {
	Next() (*needle.Needle, bool)
	Err() error
}

type tailNeedleStream struct {
	ch    <-chan *needle.Needle
	errMu sync.Mutex
	err   error
}

func (s *tailNeedleStream) Next() (*needle.Needle, bool) {
	n, ok := <-s.ch
	return n, ok
}

func (s *tailNeedleStream) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *tailNeedleStream) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

func startTailNeedleStream(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, server pb.ServerAddress) *tailNeedleStream {
	ch := make(chan *needle.Needle, 32)
	stream := &tailNeedleStream{ch: ch}
	go func() {
		err := operation.TailVolumeFromSource(server, grpcDialOption, volumeId, 0, mergeIdleTimeoutSeconds, func(n *needle.Needle) error {
			ch <- n
			return nil
		})
		close(ch)
		stream.setErr(err)
	}()
	return stream
}

type needleMergeItem struct {
	streamIndex int
	needle      *needle.Needle
	timestamp   uint64
}

type needleMergeHeap []needleMergeItem

func (h needleMergeHeap) Len() int { return len(h) }
func (h needleMergeHeap) Less(i, j int) bool {
	if h[i].timestamp == h[j].timestamp {
		return h[i].needle.Id < h[j].needle.Id
	}
	return h[i].timestamp < h[j].timestamp
}
func (h needleMergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *needleMergeHeap) Push(x any) {
	*h = append(*h, x.(needleMergeItem))
}
func (h *needleMergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func mergeNeedleStreams(streams []needleStream, consume func(int, *needle.Needle) error) error {
	h := &needleMergeHeap{}
	heap.Init(h)

	for i, stream := range streams {
		if n, ok := stream.Next(); ok {
			heap.Push(h, needleMergeItem{streamIndex: i, needle: n, timestamp: needleTimestamp(n)})
		}
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(needleMergeItem)
		if err := consume(item.streamIndex, item.needle); err != nil {
			return err
		}
		if n, ok := streams[item.streamIndex].Next(); ok {
			heap.Push(h, needleMergeItem{streamIndex: item.streamIndex, needle: n, timestamp: needleTimestamp(n)})
		}
	}

	for _, stream := range streams {
		if err := stream.Err(); err != nil {
			return err
		}
	}
	return nil
}

func needleTimestamp(n *needle.Needle) uint64 {
	if n.AppendAtNs != 0 {
		return n.AppendAtNs
	}
	if n.LastModified != 0 {
		return uint64(time.Unix(int64(n.LastModified), 0).UnixNano())
	}
	return 0
}

type needleSeen struct {
	timestamp   uint64
	streamIndex int
}

func needleBlobFromNeedle(n *needle.Needle, version needle.Version) ([]byte, types.Size, error) {
	file, err := os.CreateTemp("", "weed-needle-*.dat")
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	}()

	diskFile := backend.NewDiskFile(file)
	defer diskFile.Close()

	_, size, actualSize, err := n.Append(diskFile, version)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, actualSize)
	read, err := diskFile.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		return nil, 0, err
	}
	return buf[:read], size, nil
}

func allocateMergeVolumeOnThirdLocation(grpcDialOption grpc.DialOption, allLocations []location, replicas []*VolumeReplica, info *master_pb.VolumeInformationMessage, replicaPlacement *super_block.ReplicaPlacement) (pb.ServerAddress, error) {
	replicaNodes := map[string]struct{}{}
	for _, replica := range replicas {
		replicaNodes[replica.location.dataNode.Id] = struct{}{}
	}

	for _, loc := range allLocations {
		if _, exists := replicaNodes[loc.dataNode.Id]; exists {
			continue
		}
		if !locationHasDiskType(loc, info.DiskType) {
			continue
		}
		server := pb.NewServerAddressFromDataNode(loc.dataNode)
		if err := allocateMergeVolume(grpcDialOption, server, info, replicaPlacement); err != nil {
			continue
		}
		return server, nil
	}

	return "", fmt.Errorf("no third location available to merge volume %d", info.Id)
}

func allocateMergeVolume(grpcDialOption grpc.DialOption, server pb.ServerAddress, info *master_pb.VolumeInformationMessage, replicaPlacement *super_block.ReplicaPlacement) error {
	return operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{
			VolumeId:    info.Id,
			Collection:  info.Collection,
			Preallocate: 0,
			Replication: replicaPlacement.String(),
			Ttl:         needle.LoadTTLFromUint32(info.Ttl).String(),
			DiskType:    info.DiskType,
			Version:     info.Version,
		})
		return err
	})
}

func ensureVolumeReadonly(commandEnv *CommandEnv, replicas []*VolumeReplica) (bool, error) {
	shouldRestoreWritable := false
	for _, replica := range replicas {
		server := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		err := operation.WithVolumeServerClient(false, server, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{VolumeId: replica.info.Id})
			if err != nil {
				return err
			}
			if !resp.IsReadOnly {
				shouldRestoreWritable = true
			}
			return nil
		})
		if err != nil {
			return false, err
		}
	}
	if shouldRestoreWritable {
		if err := markReplicasWritable(commandEnv.option.GrpcDialOption, replicas, false, false); err != nil {
			return false, err
		}
	}
	return shouldRestoreWritable, nil
}

func isReplicaServer(target pb.ServerAddress, replicas []*VolumeReplica) bool {
	for _, replica := range replicas {
		if pb.NewServerAddressFromDataNode(replica.location.dataNode) == target {
			return true
		}
	}
	return false
}

func locationHasDiskType(loc location, diskType string) bool {
	for _, diskInfo := range loc.dataNode.DiskInfos {
		if diskInfo.Type == diskType {
			return true
		}
	}
	return false
}

func markReplicasWritable(grpcDialOption grpc.DialOption, replicas []*VolumeReplica, writable bool, persist bool) error {
	for _, replica := range replicas {
		server := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		err := operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			if writable {
				_, err := client.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{VolumeId: replica.info.Id})
				return err
			}
			_, err := client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: replica.info.Id, Persist: persist})
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
