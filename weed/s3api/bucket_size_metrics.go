package s3api

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	bucketSizeMetricsInterval = 1 * time.Minute
	listBucketPageSize        = 1000 // Page size for paginated bucket listing
	s3MetricsLockName         = "s3.leader"
)

// CollectionInfo holds collection statistics
// Used for both metrics collection and quota enforcement
type CollectionInfo struct {
	FileCount        float64
	DeleteCount      float64
	DeletedByteCount float64
	Size             float64 // Logical size (deduplicated by volume ID)
	PhysicalSize     float64 // Physical size (including all replicas)
	VolumeCount      int     // Logical volume count (deduplicated by volume ID)
}

// volumeKey uniquely identifies a volume for deduplication
type volumeKey struct {
	collection string
	volumeId   uint32
}

// startBucketSizeMetricsLoop periodically collects bucket size metrics and updates Prometheus gauges.
// Uses a distributed lock to ensure only one S3 instance collects metrics at a time.
// Should be called as a goroutine; stops when the provided context is cancelled.
func (s3a *S3ApiServer) startBucketSizeMetricsLoop(ctx context.Context) {
	// Initial delay to let the system stabilize
	select {
	case <-time.After(10 * time.Second):
	case <-ctx.Done():
		return
	}

	// Create lock client for distributed lock
	if len(s3a.option.Filers) == 0 {
		glog.V(1).Infof("No filers configured, skipping bucket size metrics collection")
		return
	}
	filer := s3a.option.Filers[0]
	lockClient := cluster.NewLockClient(s3a.option.GrpcDialOption, filer)
	owner := string(filer) + "-s3-metrics"

	// Start long-lived lock - this S3 instance will only collect metrics when it holds the lock
	lock := lockClient.StartLongLivedLock(s3MetricsLockName, owner, func(newLockOwner string) {
		glog.V(1).Infof("S3 bucket size metrics lock owner changed to: %s", newLockOwner)
	}, bucketSizeMetricsInterval/2)
	defer lock.Stop()

	ticker := time.NewTicker(bucketSizeMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			glog.V(1).Infof("Stopping bucket size metrics collection")
			return
		case <-ticker.C:
			// Only collect metrics if we hold the lock
			if lock.IsLocked() {
				s3a.collectAndUpdateBucketSizeMetrics(ctx)
			}
		}
	}
}

// collectAndUpdateBucketSizeMetrics collects bucket sizes from master topology
// and updates Prometheus metrics. Uses the same approach as quota enforcement.
func (s3a *S3ApiServer) collectAndUpdateBucketSizeMetrics(ctx context.Context) {
	// Collect collection info from master topology (same as quota enforcement)
	collectionInfos, err := s3a.collectCollectionInfoFromMaster(ctx)
	if err != nil {
		glog.V(2).Infof("Failed to collect collection info from master: %v", err)
		return
	}

	// Get list of buckets
	buckets, err := s3a.listBucketNames(ctx)
	if err != nil {
		glog.V(2).Infof("Failed to list buckets for size metrics: %v", err)
		return
	}

	// Map collections to buckets and update metrics
	for _, bucket := range buckets {
		collection := s3a.getCollectionName(bucket)
		if info, found := collectionInfos[collection]; found {
			stats.UpdateBucketSizeMetrics(bucket, info.Size, info.PhysicalSize, info.FileCount)
			glog.V(3).Infof("Updated bucket size metrics: bucket=%s, logicalSize=%.0f, physicalSize=%.0f, objects=%.0f",
				bucket, info.Size, info.PhysicalSize, info.FileCount)
		} else {
			// Bucket exists but no collection data (empty bucket)
			stats.UpdateBucketSizeMetrics(bucket, 0, 0, 0)
		}
	}
}

// collectCollectionInfoFromMaster queries the master for topology info and extracts collection sizes.
// This is the same approach used by shell command s3.bucket.quota.enforce.
func (s3a *S3ApiServer) collectCollectionInfoFromMaster(ctx context.Context) (map[string]*CollectionInfo, error) {
	if len(s3a.option.Masters) == 0 {
		return nil, fmt.Errorf("no masters configured")
	}

	// Convert masters slice to map for WithOneOfGrpcMasterClients
	masterMap := make(map[string]pb.ServerAddress)
	for _, master := range s3a.option.Masters {
		masterMap[string(master)] = master
	}

	// Connect to any available master and get volume list with topology
	collectionInfos := make(map[string]*CollectionInfo)

	err := pb.WithOneOfGrpcMasterClients(false, masterMap, s3a.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return fmt.Errorf("failed to get volume list: %w", err)
		}
		if resp == nil || resp.TopologyInfo == nil {
			return fmt.Errorf("empty topology info from master")
		}
		collectCollectionInfoFromTopology(resp.TopologyInfo, collectionInfos)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return collectionInfos, nil
}

// listBucketNames returns a list of all bucket names using pagination
func (s3a *S3ApiServer) listBucketNames(ctx context.Context) ([]string, error) {
	var buckets []string

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		lastFileName := ""
		for {
			request := &filer_pb.ListEntriesRequest{
				Directory:          s3a.option.BucketsPath,
				StartFromFileName:  lastFileName,
				Limit:              listBucketPageSize,
				InclusiveStartFrom: lastFileName == "",
			}

			stream, err := client.ListEntries(ctx, request)
			if err != nil {
				return err
			}

			entriesReceived := 0
			for {
				resp, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("error receiving bucket list entries: %w", err)
				}
				entriesReceived++
				if resp.Entry != nil {
					lastFileName = resp.Entry.Name
					if resp.Entry.IsDirectory {
						// Skip .uploads and other hidden directories
						if !strings.HasPrefix(resp.Entry.Name, ".") {
							buckets = append(buckets, resp.Entry.Name)
						}
					}
				}
			}

			// If we got fewer entries than the limit, we're done
			if entriesReceived < listBucketPageSize {
				break
			}
		}
		return nil
	})

	return buckets, err
}

// collectCollectionInfoFromTopology extracts collection info from topology.
// Deduplicates by volume ID to correctly handle missing replicas.
// Unlike dividing by copyCount (which would give wrong results if replicas are missing),
// we track seen volume IDs and only count each volume once for logical size/count.
func collectCollectionInfoFromTopology(t *master_pb.TopologyInfo, collectionInfos map[string]*CollectionInfo) {
	// Track which volumes we've already seen to deduplicate by volume ID
	seenVolumes := make(map[volumeKey]bool)

	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						c := vi.Collection
						cif, found := collectionInfos[c]
						if !found {
							cif = &CollectionInfo{}
							collectionInfos[c] = cif
						}

						// Always add to physical size (all replicas)
						cif.PhysicalSize += float64(vi.Size)

						// Check if we've already counted this volume for logical stats
						key := volumeKey{collection: c, volumeId: vi.Id}
						if seenVolumes[key] {
							// Already counted this volume, skip logical stats
							continue
						}
						seenVolumes[key] = true

						// First time seeing this volume - add to logical stats
						cif.Size += float64(vi.Size)
						cif.FileCount += float64(vi.FileCount)
						cif.DeleteCount += float64(vi.DeleteCount)
						cif.DeletedByteCount += float64(vi.DeletedByteCount)
						cif.VolumeCount++
					}
				}
			}
		}
	}
}
