package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	MasterAddresses    []string
	FilerURL           string
	PathPrefix         string
	Collection         string
	FileSizeBytes      int64
	BatchSize          int
	WriteInterval      time.Duration
	CleanupInterval    time.Duration
	EcMinAge           time.Duration
	MaxCleanupPerCycle int
	RequestTimeout     time.Duration
	MaxRuntime         time.Duration
	DryRun             bool
}

type runner struct {
	cfg config

	httpClient     *http.Client
	grpcDialOption grpc.DialOption

	mu            sync.Mutex
	sequence      int64
	ecFirstSeenAt map[uint32]time.Time
}

type ecVolumeInfo struct {
	Collection string
	NodeShards map[pb.ServerAddress][]uint32
}

type ecCleanupCandidate struct {
	VolumeID    uint32
	FirstSeenAt time.Time
	Info        *ecVolumeInfo
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid flags: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.MaxRuntime > 0 {
		runCtx, cancel := context.WithTimeout(ctx, cfg.MaxRuntime)
		defer cancel()
		ctx = runCtx
	}

	r := &runner{
		cfg:            cfg,
		httpClient:     &http.Client{Timeout: cfg.RequestTimeout},
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		ecFirstSeenAt:  make(map[uint32]time.Time),
	}

	log.Printf(
		"starting EC stress runner: masters=%s filer=%s prefix=%s collection=%s file_size=%d batch=%d write_interval=%s cleanup_interval=%s ec_min_age=%s max_cleanup=%d dry_run=%v",
		strings.Join(cfg.MasterAddresses, ","),
		cfg.FilerURL,
		cfg.PathPrefix,
		cfg.Collection,
		cfg.FileSizeBytes,
		cfg.BatchSize,
		cfg.WriteInterval,
		cfg.CleanupInterval,
		cfg.EcMinAge,
		cfg.MaxCleanupPerCycle,
		cfg.DryRun,
	)

	if err := r.run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatalf("runner stopped with error: %v", err)
	}

	log.Printf("runner stopped")
}

func loadConfig() (config, error) {
	var masters string
	cfg := config{}

	flag.StringVar(&masters, "masters", "127.0.0.1:9333", "comma-separated master server addresses")
	flag.StringVar(&cfg.FilerURL, "filer", "http://127.0.0.1:8888", "filer base URL")
	flag.StringVar(&cfg.PathPrefix, "path_prefix", "/tmp/ec-stress", "filer path prefix for generated files")
	flag.StringVar(&cfg.Collection, "collection", "ec_stress", "target collection for stress data")

	fileSizeMB := flag.Int("file_size_mb", 8, "size per generated file in MB")
	flag.IntVar(&cfg.BatchSize, "batch_size", 4, "files generated per write cycle")
	flag.DurationVar(&cfg.WriteInterval, "write_interval", 5*time.Second, "interval between write cycles")
	flag.DurationVar(&cfg.CleanupInterval, "cleanup_interval", 2*time.Minute, "interval between EC cleanup cycles")
	flag.DurationVar(&cfg.EcMinAge, "ec_min_age", 30*time.Minute, "minimum observed EC age before deletion")
	flag.IntVar(&cfg.MaxCleanupPerCycle, "max_cleanup_per_cycle", 4, "maximum EC volumes deleted per cleanup cycle")
	flag.DurationVar(&cfg.RequestTimeout, "request_timeout", 20*time.Second, "HTTP/gRPC request timeout")
	flag.DurationVar(&cfg.MaxRuntime, "max_runtime", 0, "maximum run duration; 0 means run until interrupted")
	flag.BoolVar(&cfg.DryRun, "dry_run", false, "log actions without deleting EC shards")
	flag.Parse()

	cfg.MasterAddresses = splitNonEmpty(masters)
	cfg.FileSizeBytes = int64(*fileSizeMB) * 1024 * 1024

	if len(cfg.MasterAddresses) == 0 {
		return cfg, fmt.Errorf("at least one master is required")
	}
	if cfg.FileSizeBytes <= 0 {
		return cfg, fmt.Errorf("file_size_mb must be positive")
	}
	if cfg.BatchSize <= 0 {
		return cfg, fmt.Errorf("batch_size must be positive")
	}
	if cfg.WriteInterval <= 0 {
		return cfg, fmt.Errorf("write_interval must be positive")
	}
	if cfg.CleanupInterval <= 0 {
		return cfg, fmt.Errorf("cleanup_interval must be positive")
	}
	if cfg.EcMinAge < 0 {
		return cfg, fmt.Errorf("ec_min_age must be zero or positive")
	}
	if cfg.MaxCleanupPerCycle <= 0 {
		return cfg, fmt.Errorf("max_cleanup_per_cycle must be positive")
	}
	if cfg.RequestTimeout <= 0 {
		return cfg, fmt.Errorf("request_timeout must be positive")
	}

	cfg.PathPrefix = ensureLeadingSlash(strings.TrimSpace(cfg.PathPrefix))
	cfg.Collection = strings.TrimSpace(cfg.Collection)

	cfg.FilerURL = strings.TrimRight(strings.TrimSpace(cfg.FilerURL), "/")
	if cfg.FilerURL == "" {
		return cfg, fmt.Errorf("filer URL is required")
	}

	if _, err := url.ParseRequestURI(cfg.FilerURL); err != nil {
		return cfg, fmt.Errorf("invalid filer URL %q: %w", cfg.FilerURL, err)
	}

	return cfg, nil
}

func (r *runner) run(ctx context.Context) error {
	writeTicker := time.NewTicker(r.cfg.WriteInterval)
	defer writeTicker.Stop()

	cleanupTicker := time.NewTicker(r.cfg.CleanupInterval)
	defer cleanupTicker.Stop()

	r.runWriteCycle(ctx)
	r.runCleanupCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-writeTicker.C:
			r.runWriteCycle(ctx)
		case <-cleanupTicker.C:
			r.runCleanupCycle(ctx)
		}
	}
}

func (r *runner) runWriteCycle(ctx context.Context) {
	for i := 0; i < r.cfg.BatchSize; i++ {
		if ctx.Err() != nil {
			return
		}
		if err := r.uploadOneFile(ctx); err != nil {
			log.Printf("upload failed: %v", err)
		}
	}
}

func (r *runner) uploadOneFile(ctx context.Context) error {
	sequence := r.nextSequence()
	filePath := path.Join(r.cfg.PathPrefix, fmt.Sprintf("ec-stress-%d-%d.bin", time.Now().UnixNano(), sequence))
	fileURL := r.cfg.FilerURL + filePath
	if r.cfg.Collection != "" {
		fileURL += "?collection=" + url.QueryEscape(r.cfg.Collection)
	}

	uploadCtx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	defer cancel()

	body := io.LimitReader(rand.Reader, r.cfg.FileSizeBytes)
	request, err := http.NewRequestWithContext(uploadCtx, http.MethodPut, fileURL, body)
	if err != nil {
		return err
	}
	request.ContentLength = r.cfg.FileSizeBytes
	request.Header.Set("Content-Type", "application/octet-stream")

	response, err := r.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	io.Copy(io.Discard, response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("upload %s returned %s", filePath, response.Status)
	}

	log.Printf("uploaded %s size=%d", filePath, r.cfg.FileSizeBytes)
	return nil
}

func (r *runner) runCleanupCycle(ctx context.Context) {
	volumeList, err := r.fetchVolumeList(ctx)
	if err != nil {
		log.Printf("cleanup skipped: fetch volume list failed: %v", err)
		return
	}
	if volumeList == nil || volumeList.TopologyInfo == nil {
		log.Printf("cleanup skipped: topology is empty")
		return
	}

	ecVolumes := collectEcVolumes(volumeList.TopologyInfo, r.cfg.Collection)
	candidates := r.selectCleanupCandidates(ecVolumes)
	if len(candidates) == 0 {
		log.Printf("cleanup: no EC volume candidate aged >= %s in collection=%q", r.cfg.EcMinAge, r.cfg.Collection)
		return
	}

	log.Printf("cleanup: deleting up to %d EC volumes (found=%d)", r.cfg.MaxCleanupPerCycle, len(candidates))
	deleted := 0
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			return
		}

		if r.cfg.DryRun {
			log.Printf(
				"cleanup dry-run: would delete EC volume=%d collection=%q seen_for=%s nodes=%d",
				candidate.VolumeID,
				candidate.Info.Collection,
				time.Since(candidate.FirstSeenAt).Round(time.Second),
				len(candidate.Info.NodeShards),
			)
			continue
		}

		if err := r.deleteEcVolume(ctx, candidate.VolumeID, candidate.Info); err != nil {
			log.Printf("cleanup volume=%d failed: %v", candidate.VolumeID, err)
			continue
		}

		deleted++
		r.mu.Lock()
		delete(r.ecFirstSeenAt, candidate.VolumeID)
		r.mu.Unlock()
		log.Printf("cleanup volume=%d completed", candidate.VolumeID)
	}

	log.Printf("cleanup finished: deleted=%d attempted=%d", deleted, len(candidates))
}

func (r *runner) fetchVolumeList(ctx context.Context) (*master_pb.VolumeListResponse, error) {
	var lastErr error
	for _, master := range r.cfg.MasterAddresses {
		masterAddress := strings.TrimSpace(master)
		if masterAddress == "" {
			continue
		}

		var response *master_pb.VolumeListResponse
		err := pb.WithMasterClient(false, pb.ServerAddress(masterAddress), r.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			callCtx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
			defer cancel()

			resp, callErr := client.VolumeList(callCtx, &master_pb.VolumeListRequest{})
			if callErr != nil {
				return callErr
			}
			response = resp
			return nil
		})
		if err == nil {
			return response, nil
		}

		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no valid master address")
	}
	return nil, lastErr
}

func collectEcVolumes(topology *master_pb.TopologyInfo, collection string) map[uint32]*ecVolumeInfo {
	normalizedCollection := strings.TrimSpace(collection)
	volumeShardSets := make(map[uint32]map[pb.ServerAddress]map[uint32]struct{})
	volumeCollection := make(map[uint32]string)

	for _, dc := range topology.GetDataCenterInfos() {
		for _, rack := range dc.GetRackInfos() {
			for _, node := range rack.GetDataNodeInfos() {
				server := pb.NewServerAddressFromDataNode(node)
				for _, disk := range node.GetDiskInfos() {
					for _, shardInfo := range disk.GetEcShardInfos() {
						if shardInfo == nil || shardInfo.Id == 0 {
							continue
						}
						if normalizedCollection != "" && strings.TrimSpace(shardInfo.Collection) != normalizedCollection {
							continue
						}

						shards := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(shardInfo).IdsUint32()
						if len(shards) == 0 {
							continue
						}

						perVolume := volumeShardSets[shardInfo.Id]
						if perVolume == nil {
							perVolume = make(map[pb.ServerAddress]map[uint32]struct{})
							volumeShardSets[shardInfo.Id] = perVolume
						}
						perNode := perVolume[server]
						if perNode == nil {
							perNode = make(map[uint32]struct{})
							perVolume[server] = perNode
						}
						for _, shardID := range shards {
							perNode[shardID] = struct{}{}
						}
						volumeCollection[shardInfo.Id] = shardInfo.Collection
					}
				}
			}
		}
	}

	result := make(map[uint32]*ecVolumeInfo, len(volumeShardSets))
	for volumeID, perNode := range volumeShardSets {
		info := &ecVolumeInfo{
			Collection: volumeCollection[volumeID],
			NodeShards: make(map[pb.ServerAddress][]uint32, len(perNode)),
		}
		for server, shardSet := range perNode {
			shardIDs := make([]uint32, 0, len(shardSet))
			for shardID := range shardSet {
				shardIDs = append(shardIDs, shardID)
			}
			sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
			info.NodeShards[server] = shardIDs
		}
		result[volumeID] = info
	}

	return result
}

func (r *runner) selectCleanupCandidates(ecVolumes map[uint32]*ecVolumeInfo) []ecCleanupCandidate {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for volumeID := range ecVolumes {
		if _, exists := r.ecFirstSeenAt[volumeID]; !exists {
			r.ecFirstSeenAt[volumeID] = now
		}
	}
	for volumeID := range r.ecFirstSeenAt {
		if _, exists := ecVolumes[volumeID]; !exists {
			delete(r.ecFirstSeenAt, volumeID)
		}
	}

	candidates := make([]ecCleanupCandidate, 0, len(ecVolumes))
	for volumeID, info := range ecVolumes {
		firstSeenAt := r.ecFirstSeenAt[volumeID]
		if r.cfg.EcMinAge > 0 && now.Sub(firstSeenAt) < r.cfg.EcMinAge {
			continue
		}
		candidates = append(candidates, ecCleanupCandidate{
			VolumeID:    volumeID,
			FirstSeenAt: firstSeenAt,
			Info:        info,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].FirstSeenAt.Equal(candidates[j].FirstSeenAt) {
			return candidates[i].VolumeID < candidates[j].VolumeID
		}
		return candidates[i].FirstSeenAt.Before(candidates[j].FirstSeenAt)
	})

	if len(candidates) > r.cfg.MaxCleanupPerCycle {
		candidates = candidates[:r.cfg.MaxCleanupPerCycle]
	}
	return candidates
}

func (r *runner) deleteEcVolume(ctx context.Context, volumeID uint32, info *ecVolumeInfo) error {
	if info == nil {
		return fmt.Errorf("ec volume %d has no topology info", volumeID)
	}

	failureCount := 0
	for server, shardIDs := range info.NodeShards {
		err := pb.WithVolumeServerClient(false, server, r.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			unmountCtx, unmountCancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
			defer unmountCancel()
			_, _ = client.VolumeEcShardsUnmount(unmountCtx, &volume_server_pb.VolumeEcShardsUnmountRequest{
				VolumeId: volumeID,
				ShardIds: shardIDs,
			})

			if len(shardIDs) > 0 {
				deleteCtx, deleteCancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
				defer deleteCancel()
				if _, err := client.VolumeEcShardsDelete(deleteCtx, &volume_server_pb.VolumeEcShardsDeleteRequest{
					VolumeId: volumeID,
					ShardIds: shardIDs,
				}); err != nil {
					return err
				}
			}

			finalDeleteCtx, finalDeleteCancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
			defer finalDeleteCancel()
			_, _ = client.VolumeDelete(finalDeleteCtx, &volume_server_pb.VolumeDeleteRequest{
				VolumeId: volumeID,
			})
			return nil
		})

		if err != nil {
			failureCount++
			log.Printf("cleanup volume=%d server=%s shards=%v failed: %v", volumeID, server, shardIDs, err)
		}
	}

	if failureCount == len(info.NodeShards) && failureCount > 0 {
		return fmt.Errorf("all shard deletions failed for volume %d", volumeID)
	}
	if failureCount > 0 {
		return fmt.Errorf("partial shard deletion failure for volume %d", volumeID)
	}
	return nil
}

func (r *runner) nextSequence() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sequence++
	return r.sequence
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func ensureLeadingSlash(value string) string {
	if value == "" {
		return "/"
	}
	if strings.HasPrefix(value, "/") {
		return value
	}
	return "/" + value
}
