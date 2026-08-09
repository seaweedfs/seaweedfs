package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
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
	Size             float64 // Single-copy volume size (deduplicated by volume ID), still counting un-vacuumed garbage
	PhysicalSize     float64 // Physical size (including all replicas)
	VolumeCount      int     // Logical volume count (deduplicated by volume ID)
}

// LogicalSize is the live data size: single-copy volume size minus the
// un-vacuumed deleted/overwritten bytes. Quota enforcement and the
// bucket_size_bytes metric use this so vacuum lag never counts against a
// bucket, matching the usage figure the Admin UI shows.
func (c *CollectionInfo) LogicalSize() float64 {
	if c.Size < c.DeletedByteCount {
		return 0
	}
	return c.Size - c.DeletedByteCount
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
	}, bucketSizeMetricsInterval)
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
	buckets, err := s3a.listBuckets(ctx)
	if err != nil {
		glog.V(2).Infof("Failed to list buckets for size metrics: %v", err)
		return
	}

	// Map collections to buckets and update metrics
	for _, bucket := range buckets {
		collection := s3a.getCollectionName(bucket.Name)
		if info, found := collectionInfos[collection]; found {
			stats.UpdateBucketSizeMetrics(bucket.Name, info.LogicalSize(), info.PhysicalSize, info.FileCount)
			glog.V(3).Infof("Updated bucket size metrics: bucket=%s, logicalSize=%.0f, physicalSize=%.0f, objects=%.0f",
				bucket.Name, info.LogicalSize(), info.PhysicalSize, info.FileCount)
		} else {
			// Bucket exists but no collection data (empty bucket)
			stats.UpdateBucketSizeMetrics(bucket.Name, 0, 0, 0)
		}
	}

	s3a.enforceBucketQuotas(ctx, buckets, collectionInfos)
}

// enforceBucketQuotas flips each bucket's read-only flag to match its quota,
// rewriting filer.conf only when a flag changes.
func (s3a *S3ApiServer) enforceBucketQuotas(ctx context.Context, buckets []*filer_pb.Entry, collectionInfos map[string]*CollectionInfo) {
	if len(s3a.option.Filers) == 0 {
		return
	}

	fc, err := filer.ReadFilerConfFromFilers(s3a.option.Filers, s3a.option.GrpcDialOption, nil)
	if err != nil {
		glog.V(1).Infof("read filer.conf for quota enforcement: %v", err)
		return
	}

	changed := false
	for _, bucket := range buckets {
		var size float64
		if info, found := collectionInfos[s3a.getCollectionName(bucket.Name)]; found {
			size = info.LogicalSize()
		}
		locPrefix := s3a.option.BucketsPath + "/" + bucket.Name + "/"
		readOnly, flipped := fc.ApplyBucketQuotaReadOnly(locPrefix, size, float64(bucket.Quota))
		stats.UpdateBucketQuotaMetrics(bucket.Name, float64(bucket.Quota), readOnly)
		if flipped {
			changed = true
			glog.V(0).Infof("bucket %s quota enforcement: readOnly=%v (size=%.0f quota=%d)", bucket.Name, readOnly, size, bucket.Quota)
		}
	}

	if !changed {
		return
	}

	var buf bytes.Buffer
	if err := fc.ToText(&buf); err != nil {
		glog.Errorf("serialize filer.conf for quota enforcement: %v", err)
		return
	}
	if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(ctx, client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf.Bytes())
	}); err != nil {
		glog.Errorf("save filer.conf for quota enforcement: %v", err)
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

	// Ask the master to summarise. Adding this up here instead would mean
	// being sent every volume in the cluster once a minute.
	collectionInfos := make(map[string]*CollectionInfo)

	err := pb.WithOneOfGrpcMasterClients(false, masterMap, s3a.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.CollectionStatistics(ctx, &master_pb.CollectionStatisticsRequest{})
		if err != nil {
			return fmt.Errorf("failed to get collection statistics: %w", err)
		}
		if resp == nil {
			return fmt.Errorf("empty collection statistics from master")
		}
		for _, c := range resp.Collections {
			collectionInfos[c.Collection] = &CollectionInfo{
				FileCount:        float64(c.FileCount),
				DeleteCount:      float64(c.DeleteCount),
				DeletedByteCount: float64(c.DeletedByteCount),
				Size:             float64(c.Size),
				PhysicalSize:     float64(c.PhysicalSize),
				VolumeCount:      int(c.VolumeCount),
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return collectionInfos, nil
}

// listBuckets returns all bucket directory entries using pagination.
func (s3a *S3ApiServer) listBuckets(ctx context.Context) ([]*filer_pb.Entry, error) {
	var buckets []*filer_pb.Entry

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
							buckets = append(buckets, resp.Entry)
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
