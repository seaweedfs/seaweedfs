package shell

import (
	"bytes"
	"container/heap"
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
)

// mergeIdleTimeoutSeconds is the timeout for idle streams during needle tailing.
// This ensures that slow or stalled streams don't block the merge indefinitely.
// Set to 5 seconds to handle network congestion and avoid premature stream termination.
// Can be made configurable in the future if needed for different deployment scenarios.
const mergeIdleTimeoutSeconds = 5

// mergeDeduplicationWindowNs defines the time window for deduplication across replicas.
// Since the same needle ID can have different timestamps on different servers due to
// clock skew and replication lag, we deduplicate needles with the same ID within this window.
// Set to 5 seconds in nanoseconds to handle typical server clock differences.
const mergeDeduplicationWindowNs = 5 * time.Second

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

	writableReplicaIndices, err := ensureVolumeReadonly(commandEnv, replicas)
	if err != nil {
		return err
	}
	if len(writableReplicaIndices) > 0 {
		defer func() {
			// Only restore writable state for replicas that were originally writable
			writableReplicas := make([]*VolumeReplica, 0, len(writableReplicaIndices))
			for _, idx := range writableReplicaIndices {
				writableReplicas = append(writableReplicas, replicas[idx])
			}
			_ = markReplicasWritable(commandEnv.option.GrpcDialOption, writableReplicas, true, false)
		}()
	}

	done := make(chan struct{})
	defer close(done)

	sources := make([]needleStream, 0, len(replicas))
	for _, replica := range replicas {
		server := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		sources = append(sources, startTailNeedleStream(commandEnv.option.GrpcDialOption, volumeId, server, done))
	}

	mergeErr := operation.WithVolumeServerClient(false, targetServer, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		version := needle.Version(volumeInfo.Version)
		if version == 0 {
			version = needle.GetCurrentVersion()
		}
		return mergeNeedleStreams(sources, func(streamIndex int, n *needle.Needle) error {
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
		if _, err = copyVolume(commandEnv.option.GrpcDialOption, writer, volumeId, targetServer, sourceServer, "", 0, false); err != nil {
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

func startTailNeedleStream(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, server pb.ServerAddress, done <-chan struct{}) *tailNeedleStream {
	ch := make(chan *needle.Needle, 32)
	stream := &tailNeedleStream{ch: ch}
	go func() {
		err := operation.TailVolumeFromSource(server, grpcDialOption, volumeId, 0, mergeIdleTimeoutSeconds, func(n *needle.Needle) error {
			select {
			case ch <- n:
			case <-done:
				return fmt.Errorf("merge cancelled")
			}
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

	// Track seen needle IDs within a time window to skip cross-stream duplicates.
	// Needles with the same ID within mergeDeduplicationWindowNs are considered duplicates,
	// accounting for clock skew and replication lag across servers.
	seenAtTimestamp := make(map[types.NeedleId]struct{})
	var windowStartTimestamp uint64
	windowInitialized := false

	for h.Len() > 0 {
		item := heap.Pop(h).(needleMergeItem)
		ts := item.timestamp
		n := item.needle

		// Initialize window on first timestamp, or move to new window when outside current window
		if !windowInitialized {
			windowStartTimestamp = ts
			windowInitialized = true
		} else if ts > windowStartTimestamp+uint64(mergeDeduplicationWindowNs) {
			// Moving to a new window: clear the watermark to reduce memory usage.
			// This is safe because we only skip duplicates within the same time window.
			seenAtTimestamp = make(map[types.NeedleId]struct{})
			windowStartTimestamp = ts
		}

		// Skip cross-stream duplicates: if we've already seen this needle ID within this time window,
		// skip it. Newer timestamps (overwrites of the same ID) will still be processed in the next window.
		if _, exists := seenAtTimestamp[n.Id]; exists {
			// Get next needle from the same stream and continue
			if nextN, ok := streams[item.streamIndex].Next(); ok {
				heap.Push(h, needleMergeItem{streamIndex: item.streamIndex, needle: nextN, timestamp: needleTimestamp(nextN)})
			}
			continue
		}

		seenAtTimestamp[n.Id] = struct{}{}
		if err := consume(item.streamIndex, n); err != nil {
			return err
		}
		if nextN, ok := streams[item.streamIndex].Next(); ok {
			heap.Push(h, needleMergeItem{streamIndex: item.streamIndex, needle: nextN, timestamp: needleTimestamp(nextN)})
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

// memoryBackendFile implements backend.BackendStorageFile using an in-memory buffer
type memoryBackendFile struct {
	buf    *bytes.Buffer
	offset int64
}

func (m *memoryBackendFile) ReadAt(p []byte, off int64) (n int, err error) {
	data := m.buf.Bytes()
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n = copy(p, data[off:])
	if off+int64(n) < int64(len(data)) {
		return n, nil
	}
	return n, io.EOF
}

func (m *memoryBackendFile) WriteAt(p []byte, off int64) (n int, err error) {
	data := m.buf.Bytes()
	if off > int64(len(data)) {
		// Pad with zeros
		m.buf.Write(make([]byte, off-int64(len(data))))
	}
	if off == int64(len(data)) {
		return m.buf.Write(p)
	}
	// Overwrite existing data
	newData := make([]byte, off+int64(len(p)))
	copy(newData, data)
	copy(newData[off:], p)
	m.buf = bytes.NewBuffer(newData)
	return len(p), nil
}

func (m *memoryBackendFile) Truncate(off int64) error {
	data := m.buf.Bytes()
	if off > int64(len(data)) {
		m.buf.Write(make([]byte, off-int64(len(data))))
	} else {
		m.buf = bytes.NewBuffer(data[:off])
	}
	return nil
}

func (m *memoryBackendFile) Close() error {
	return nil
}

func (m *memoryBackendFile) GetStat() (datSize int64, modTime time.Time, err error) {
	return int64(m.buf.Len()), time.Now(), nil
}

func (m *memoryBackendFile) Name() string {
	return "memory"
}

func (m *memoryBackendFile) Sync() error {
	return nil
}

func newMemoryBackendFile() *memoryBackendFile {
	return &memoryBackendFile{
		buf:    &bytes.Buffer{},
		offset: 0,
	}
}

func needleBlobFromNeedle(n *needle.Needle, version needle.Version) ([]byte, types.Size, error) {
	// Use in-memory buffer for serialization to avoid expensive temporary file I/O
	memFile := newMemoryBackendFile()
	defer memFile.Close()

	_, size, actualSize, err := n.Append(memFile, version)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, actualSize)
	read, err := memFile.ReadAt(buf, 0)
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
			glog.V(1).Infof("failed to allocate merge volume on %s with replication %s: %v", server, replicaPlacement.String(), err)
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

// ensureVolumeReadonly marks all replicas as readonly and returns the indices of replicas that were writable
func ensureVolumeReadonly(commandEnv *CommandEnv, replicas []*VolumeReplica) ([]int, error) {
	var writableReplicaIndices []int
	for i, replica := range replicas {
		server := pb.NewServerAddressFromDataNode(replica.location.dataNode)
		err := operation.WithVolumeServerClient(false, server, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{VolumeId: replica.info.Id})
			if err != nil {
				return err
			}
			if !resp.IsReadOnly {
				writableReplicaIndices = append(writableReplicaIndices, i)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if len(writableReplicaIndices) > 0 {
		if err := markReplicasWritable(commandEnv.option.GrpcDialOption, replicas, false, false); err != nil {
			return nil, err
		}
	}
	return writableReplicaIndices, nil
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
