package pluginworker

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"google.golang.org/grpc"
)

type ecVacuumExecutor struct {
	grpcDialOption grpc.DialOption
	baseWorkingDir string
}

type ecShardConfig struct {
	DataShards   int
	ParityShards int
}

func newEcVacuumExecutor(grpcDialOption grpc.DialOption, baseWorkingDir string) *ecVacuumExecutor {
	return &ecVacuumExecutor{
		grpcDialOption: grpcDialOption,
		baseWorkingDir: strings.TrimSpace(baseWorkingDir),
	}
}

func (c ecShardConfig) totalShards() int {
	return c.DataShards + c.ParityShards
}

func (c ecShardConfig) toECContext(collection string, volumeId uint32) *erasure_coding.ECContext {
	return &erasure_coding.ECContext{
		Collection:   collection,
		VolumeId:     needle.VolumeId(volumeId),
		DataShards:   c.DataShards,
		ParityShards: c.ParityShards,
	}
}

func (e *ecVacuumExecutor) Run(
	ctx context.Context,
	volumeId uint32,
	collection string,
	masterAddresses []string,
	report func(progress float64, stage string, message string),
) error {
	if volumeId == 0 {
		return fmt.Errorf("volume id is required")
	}
	if strings.TrimSpace(collection) == "" {
		return fmt.Errorf("collection is required")
	}
	if len(masterAddresses) == 0 {
		return fmt.Errorf("master addresses are required")
	}
	if e.grpcDialOption == nil {
		return fmt.Errorf("grpc dial option is required")
	}

	baseDir := defaultEcVacuumWorkingDir(e.baseWorkingDir)
	taskDir := filepath.Join(baseDir, fmt.Sprintf("vol_%d_%d", volumeId, time.Now().Unix()))
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return fmt.Errorf("create working directory %s: %w", taskDir, err)
	}
	defer cleanupEcVacuumWorkingDir(taskDir)

	progress := func(p float64, stage, message string) {
		if report != nil {
			report(p, stage, message)
		}
	}

	progress(5, "locating_shards", "Locating EC shards from master")
	shardLocations, err := lookupEcShardLocations(ctx, masterAddresses, volumeId, e.grpcDialOption)
	if err != nil {
		return err
	}
	if len(shardLocations) == 0 {
		return fmt.Errorf("no EC shard locations found for volume %d", volumeId)
	}

	targets := buildEcShardTargets(volumeId, shardLocations)
	if len(targets) == 0 {
		return fmt.Errorf("no shard targets resolved for volume %d", volumeId)
	}
	servers := uniqueTargetServers(targets)

	baseFileName := erasure_coding.EcShardFileName(collection, taskDir, int(volumeId))

	progress(10, "loading_config", "Loading EC shard configuration")
	vifPath := baseFileName + ".vif"
	_, _ = copyEcFileFromAnyServer(ctx, servers, collection, volumeId, ".vif", vifPath, e.grpcDialOption, true)

	volumeInfo, shardConfig, err := loadEcShardConfigFromVif(vifPath)
	if err != nil {
		return err
	}
	if err := ensureAllEcShardsPresent(shardLocations, shardConfig.totalShards()); err != nil {
		return err
	}

	progress(20, "copy_metadata", "Copying EC index and journal")
	ecxPath := baseFileName + ".ecx"
	if _, err := copyEcFileFromAnyServer(ctx, servers, collection, volumeId, ".ecx", ecxPath, e.grpcDialOption, false); err != nil {
		return err
	}

	deletedNeedles, err := collectDedupedEcj(ctx, servers, collection, volumeId, taskDir, e.grpcDialOption)
	if err != nil {
		return err
	}
	ecjPath := baseFileName + ".ecj"
	if err := writeDedupedEcj(ecjPath, deletedNeedles); err != nil {
		return err
	}

	progress(35, "copy_shards", "Copying EC shard files")
	for shardId := 0; shardId < shardConfig.totalShards(); shardId++ {
		locations := shardLocations[uint32(shardId)]
		if len(locations) == 0 {
			return fmt.Errorf("missing shard %d for volume %d", shardId, volumeId)
		}
		ext := erasure_coding.ToExt(shardId)
		dest := baseFileName + ext
		if _, err := copyEcFileFromAnyServer(ctx, locations, collection, volumeId, ext, dest, e.grpcDialOption, false); err != nil {
			return err
		}
	}

	progress(50, "decoding", "Decoding EC shards into a normal volume")
	if err := decodeEcVolume(baseFileName, shardConfig.DataShards); err != nil {
		return err
	}

	progress(65, "vacuuming", "Vacuuming decoded volume")
	if err := vacuumLocalVolume(taskDir, collection, volumeId); err != nil {
		return err
	}

	progress(75, "encoding", "Re-encoding volume into EC shards")
	if err := refreshEcVolumeInfo(vifPath, volumeInfo, shardConfig, baseFileName+".dat"); err != nil {
		return err
	}
	if err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx"); err != nil {
		return fmt.Errorf("generate ecx from idx: %w", err)
	}
	if err := writeEmptyFile(ecjPath); err != nil {
		return err
	}
	if err := erasure_coding.WriteEcFilesWithContext(baseFileName, shardConfig.toECContext(collection, volumeId)); err != nil {
		return fmt.Errorf("generate ec shards: %w", err)
	}

	shardFiles := buildEcShardFiles(baseFileName, shardConfig.totalShards(), vifPath)
	shardAssignment := buildShardAssignment(targets, shardFiles)

	progress(85, "unmounting", "Unmounting existing EC shards")
	if err := unmountEcShards(ctx, targets, volumeId, e.grpcDialOption); err != nil {
		return err
	}

	progress(90, "distributing", "Distributing new EC shards")
	assigned, err := erasure_coding.DistributeEcShards(volumeId, collection, targets, shardFiles, e.grpcDialOption, nil)
	if err != nil {
		_ = erasure_coding.MountEcShards(volumeId, collection, shardAssignment, e.grpcDialOption, nil)
		return err
	}

	progress(95, "mounting", "Mounting new EC shards")
	if err := erasure_coding.MountEcShards(volumeId, collection, assigned, e.grpcDialOption, nil); err != nil {
		return err
	}

	progress(100, "completed", "EC vacuum completed")
	return nil
}

func defaultEcVacuumWorkingDir(baseWorkingDir string) string {
	dir := strings.TrimSpace(baseWorkingDir)
	if dir == "" {
		return filepath.Join(".", "ec_vacuum")
	}
	return filepath.Join(dir, "ec_vacuum")
}

func cleanupEcVacuumWorkingDir(taskDir string) {
	if taskDir == "" {
		return
	}
	patterns := []string{"*.dat", "*.idx", "*.ec*", "*.vif", "*.cpd", "*.cpx", "*.cpldb"}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(taskDir, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			_ = os.Remove(match)
		}
	}
	if err := os.RemoveAll(taskDir); err != nil {
		glog.V(2).Infof("Could not remove ec vacuum working directory %s: %v", taskDir, err)
	}
}

func lookupEcShardLocations(ctx context.Context, masterAddresses []string, volumeId uint32, grpcDialOption grpc.DialOption) (map[uint32][]pb.ServerAddress, error) {
	var lastErr error
	for _, masterAddress := range masterAddresses {
		var resp *master_pb.LookupEcVolumeResponse
		err := operation.WithMasterServerClient(false, pb.ServerAddress(masterAddress), grpcDialOption, func(client master_pb.SeaweedClient) error {
			var err error
			resp, err = client.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{VolumeId: volumeId})
			return err
		})
		if err != nil {
			lastErr = err
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty lookup response from master %s", masterAddress)
			continue
		}

		shardLocations := make(map[uint32][]pb.ServerAddress)
		for _, shardLoc := range resp.ShardIdLocations {
			if shardLoc == nil {
				continue
			}
			var locations []pb.ServerAddress
			for _, loc := range shardLoc.Locations {
				if loc == nil {
					continue
				}
				locations = append(locations, pb.NewServerAddressFromLocation(loc))
			}
			if len(locations) > 0 {
				shardLocations[shardLoc.ShardId] = locations
			}
		}
		return shardLocations, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("failed to lookup EC volume %d from masters", volumeId)
	}
	return nil, lastErr
}

func buildEcShardTargets(volumeId uint32, shardLocations map[uint32][]pb.ServerAddress) []*worker_pb.TaskTarget {
	targetsByNode := make(map[string]*worker_pb.TaskTarget)
	for shardId, locations := range shardLocations {
		for _, location := range locations {
			key := string(location)
			target := targetsByNode[key]
			if target == nil {
				target = &worker_pb.TaskTarget{
					Node:     key,
					VolumeId: volumeId,
				}
				targetsByNode[key] = target
			}
			target.ShardIds = append(target.ShardIds, shardId)
		}
	}

	nodes := make([]string, 0, len(targetsByNode))
	for node := range targetsByNode {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	targets := make([]*worker_pb.TaskTarget, 0, len(nodes))
	for _, node := range nodes {
		target := targetsByNode[node]
		if target == nil {
			continue
		}
		sort.Slice(target.ShardIds, func(i, j int) bool {
			return target.ShardIds[i] < target.ShardIds[j]
		})
		targets = append(targets, target)
	}
	return targets
}

func uniqueTargetServers(targets []*worker_pb.TaskTarget) []pb.ServerAddress {
	seen := make(map[string]struct{})
	var servers []pb.ServerAddress
	for _, target := range targets {
		if target == nil || strings.TrimSpace(target.Node) == "" {
			continue
		}
		if _, ok := seen[target.Node]; ok {
			continue
		}
		seen[target.Node] = struct{}{}
		servers = append(servers, pb.ServerAddress(target.Node))
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i] < servers[j]
	})
	return servers
}

func ensureAllEcShardsPresent(shardLocations map[uint32][]pb.ServerAddress, totalShards int) error {
	if totalShards <= 0 {
		return fmt.Errorf("invalid total shard count: %d", totalShards)
	}
	for shardId := 0; shardId < totalShards; shardId++ {
		if len(shardLocations[uint32(shardId)]) == 0 {
			return fmt.Errorf("missing EC shard %d (expected %d shards)", shardId, totalShards)
		}
	}
	return nil
}

func copyEcFileFromAnyServer(
	ctx context.Context,
	servers []pb.ServerAddress,
	collection string,
	volumeId uint32,
	ext string,
	destPath string,
	grpcDialOption grpc.DialOption,
	ignoreMissing bool,
) (bool, error) {
	var lastErr error
	for _, server := range servers {
		found, err := copyEcFileFromServer(ctx, server, collection, volumeId, ext, destPath, grpcDialOption, ignoreMissing)
		if err != nil {
			lastErr = err
			continue
		}
		if found {
			return true, nil
		}
	}
	if ignoreMissing {
		return false, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("failed to copy %s for volume %d from any server", ext, volumeId)
	}
	return false, lastErr
}

func copyEcFileFromServer(
	ctx context.Context,
	server pb.ServerAddress,
	collection string,
	volumeId uint32,
	ext string,
	destPath string,
	grpcDialOption grpc.DialOption,
	ignoreMissing bool,
) (bool, error) {
	var found bool
	err := operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		stream, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
			VolumeId:                 volumeId,
			Collection:               collection,
			Ext:                      ext,
			StopOffset:               uint64(math.MaxInt64),
			CompactionRevision:       math.MaxUint32,
			IsEcVolume:               true,
			IgnoreSourceFileNotFound: ignoreMissing,
		})
		if err != nil {
			return err
		}

		file, err := os.Create(destPath)
		if err != nil {
			return fmt.Errorf("create %s: %w", destPath, err)
		}
		defer file.Close()

		var modifiedSeen bool
		var bytesWritten int64
		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return recvErr
			}
			if resp != nil && resp.ModifiedTsNs != 0 {
				modifiedSeen = true
			}
			if len(resp.GetFileContent()) > 0 {
				n, writeErr := file.Write(resp.FileContent)
				if writeErr != nil {
					return writeErr
				}
				bytesWritten += int64(n)
			}
		}

		if !modifiedSeen && bytesWritten == 0 {
			_ = os.Remove(destPath)
			return nil
		}
		found = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func collectDedupedEcj(
	ctx context.Context,
	servers []pb.ServerAddress,
	collection string,
	volumeId uint32,
	workDir string,
	grpcDialOption grpc.DialOption,
) (map[types.NeedleId]struct{}, error) {
	entries := make(map[types.NeedleId]struct{})
	for i, server := range servers {
		tmpPath := filepath.Join(workDir, fmt.Sprintf("%d-%d.ecj", volumeId, i))
		found, err := copyEcFileFromServer(ctx, server, collection, volumeId, ".ecj", tmpPath, grpcDialOption, true)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if err := readEcjFileIntoSet(tmpPath, entries); err != nil {
			return nil, err
		}
		_ = os.Remove(tmpPath)
	}
	return entries, nil
}

func readEcjFileIntoSet(path string, entries map[types.NeedleId]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, types.NeedleIdSize)
	for {
		n, err := file.Read(buf)
		if n != types.NeedleIdSize {
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("unexpected ecj read size %d", n)
		}
		entries[types.BytesToNeedleId(buf)] = struct{}{}
	}
}

func writeDedupedEcj(path string, entries map[types.NeedleId]struct{}) error {
	keys := make([]uint64, 0, len(entries))
	for key := range entries {
		keys = append(keys, uint64(key))
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, types.NeedleIdSize)
	for _, key := range keys {
		types.NeedleIdToBytes(buf, types.NeedleId(key))
		if _, err := file.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func decodeEcVolume(baseFileName string, dataShards int) error {
	if dataShards <= 0 {
		return fmt.Errorf("invalid data shard count %d", dataShards)
	}
	datFileSize, err := erasure_coding.FindDatFileSize(baseFileName, baseFileName)
	if err != nil {
		return err
	}
	shardFileNames := make([]string, dataShards)
	for i := 0; i < dataShards; i++ {
		shardFileNames[i] = baseFileName + erasure_coding.ToExt(i)
	}
	if err := erasure_coding.WriteDatFile(baseFileName, datFileSize, shardFileNames); err != nil {
		return err
	}
	return erasure_coding.WriteIdxFileFromEcIndex(baseFileName)
}

func vacuumLocalVolume(workDir string, collection string, volumeId uint32) error {
	v, err := storage.NewVolume(
		workDir,
		workDir,
		collection,
		needle.VolumeId(volumeId),
		storage.NeedleMapInMemory,
		nil,
		nil,
		0,
		needle.GetCurrentVersion(),
		0,
		0,
	)
	if err != nil {
		return err
	}
	defer v.Close()
	if err := v.CompactByIndex(nil); err != nil {
		return err
	}
	return v.CommitCompact()
}

func refreshEcVolumeInfo(vifPath string, volumeInfo *volume_server_pb.VolumeInfo, shardConfig ecShardConfig, datPath string) error {
	if volumeInfo == nil {
		volumeInfo = &volume_server_pb.VolumeInfo{}
	}
	if volumeInfo.Version == 0 {
		volumeInfo.Version = uint32(needle.GetCurrentVersion())
	}
	if volumeInfo.EcShardConfig == nil {
		volumeInfo.EcShardConfig = &volume_server_pb.EcShardConfig{}
	}
	volumeInfo.EcShardConfig.DataShards = uint32(shardConfig.DataShards)
	volumeInfo.EcShardConfig.ParityShards = uint32(shardConfig.ParityShards)

	stat, err := os.Stat(datPath)
	if err != nil {
		return err
	}
	volumeInfo.DatFileSize = stat.Size()
	return volume_info.SaveVolumeInfo(vifPath, volumeInfo)
}

func buildEcShardFiles(baseFileName string, totalShards int, vifPath string) map[string]string {
	shardFiles := map[string]string{
		"ecx": baseFileName + ".ecx",
		"ecj": baseFileName + ".ecj",
	}
	if strings.TrimSpace(vifPath) != "" {
		shardFiles["vif"] = vifPath
	}
	for i := 0; i < totalShards; i++ {
		shardFiles[fmt.Sprintf("ec%02d", i)] = baseFileName + erasure_coding.ToExt(i)
	}
	return shardFiles
}

func buildShardAssignment(targets []*worker_pb.TaskTarget, shardFiles map[string]string) map[string][]string {
	assignment := make(map[string][]string)
	for _, target := range targets {
		if target == nil || strings.TrimSpace(target.Node) == "" {
			continue
		}
		var shardTypes []string
		for _, shardId := range target.ShardIds {
			shardTypes = append(shardTypes, fmt.Sprintf("ec%02d", shardId))
		}
		if len(shardTypes) == 0 {
			continue
		}
		if _, ok := shardFiles["ecx"]; ok {
			shardTypes = append(shardTypes, "ecx")
		}
		if _, ok := shardFiles["ecj"]; ok {
			shardTypes = append(shardTypes, "ecj")
		}
		if _, ok := shardFiles["vif"]; ok {
			shardTypes = append(shardTypes, "vif")
		}
		assignment[target.Node] = shardTypes
	}
	return assignment
}

func unmountEcShards(ctx context.Context, targets []*worker_pb.TaskTarget, volumeId uint32, grpcDialOption grpc.DialOption) error {
	for _, target := range targets {
		if target == nil || len(target.ShardIds) == 0 {
			continue
		}
		err := operation.WithVolumeServerClient(false, pb.ServerAddress(target.Node), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			_, unmountErr := client.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: volumeId,
				ShardIds: target.ShardIds,
			})
			return unmountErr
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func writeEmptyFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}

func loadEcShardConfigFromVif(vifPath string) (*volume_server_pb.VolumeInfo, ecShardConfig, error) {
	defaultConfig := ecShardConfig{
		DataShards:   erasure_coding.DataShardsCount,
		ParityShards: erasure_coding.ParityShardsCount,
	}
	if strings.TrimSpace(vifPath) == "" {
		return &volume_server_pb.VolumeInfo{}, defaultConfig, nil
	}
	volumeInfo, _, found, err := volume_info.MaybeLoadVolumeInfo(vifPath)
	if err != nil {
		return nil, ecShardConfig{}, err
	}
	if !found || volumeInfo == nil || volumeInfo.EcShardConfig == nil {
		return volumeInfo, defaultConfig, nil
	}
	data := int(volumeInfo.EcShardConfig.DataShards)
	parity := int(volumeInfo.EcShardConfig.ParityShards)
	if data <= 0 || parity <= 0 || data+parity > erasure_coding.MaxShardCount {
		return volumeInfo, defaultConfig, nil
	}
	return volumeInfo, ecShardConfig{DataShards: data, ParityShards: parity}, nil
}

func countEcxEntries(path string) (int, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if stat.Size() == 0 {
		return 0, nil
	}
	if stat.Size()%int64(types.NeedleMapEntrySize) != 0 {
		return 0, fmt.Errorf("unexpected ecx size %d in %s", stat.Size(), path)
	}
	return int(stat.Size() / int64(types.NeedleMapEntrySize)), nil
}
