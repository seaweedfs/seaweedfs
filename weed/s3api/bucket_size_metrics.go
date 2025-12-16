package s3api

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

const (
	bucketSizeMetricsInterval = 1 * time.Minute
	listBucketPageSize        = 1000 // Page size for paginated bucket listing
)

// CollectionInfo holds collection statistics
type CollectionInfo struct {
	FileCount        float64
	DeleteCount      float64
	DeletedByteCount float64
	Size             float64 // Logical size (deduplicated across replicas)
	PhysicalSize     float64 // Physical size (including all replicas)
	VolumeCount      int
}

// StartBucketSizeMetricsCollection starts a background goroutine to periodically
// collect bucket size metrics and update Prometheus gauges
func (s3a *S3ApiServer) StartBucketSizeMetricsCollection() {
	go s3a.loopCollectBucketSizeMetrics()
}

func (s3a *S3ApiServer) loopCollectBucketSizeMetrics() {
	// Initial delay to let the system stabilize
	time.Sleep(10 * time.Second)

	for {
		s3a.collectAndUpdateBucketSizeMetrics()
		time.Sleep(bucketSizeMetricsInterval)
	}
}

func (s3a *S3ApiServer) collectAndUpdateBucketSizeMetrics() {
	// Method 1: Try to get collection info from master via topology (most accurate)
	collectionInfos, err := s3a.collectCollectionInfoFromMaster()
	if err != nil {
		glog.V(2).Infof("Failed to collect collection info from master: %v, falling back to filer statistics", err)
		// Method 2: Fallback to filer Statistics RPC
		s3a.collectBucketSizeFromFilerStats()
		return
	}

	// Get list of buckets
	buckets, err := s3a.listBucketNames()
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
			// Bucket exists but no collection data (empty bucket or different storage)
			stats.UpdateBucketSizeMetrics(bucket, 0, 0, 0)
		}
	}
}

// collectCollectionInfoFromMaster queries the master for topology info and extracts collection sizes
// This provides accurate logical vs physical size differentiation by accounting for replication
func (s3a *S3ApiServer) collectCollectionInfoFromMaster() (map[string]*CollectionInfo, error) {
	// Check if we have filer client access
	if s3a.filerClient == nil {
		return nil, fmt.Errorf("filerClient is nil")
	}

	// Get master addresses from filer configuration
	var masters []string
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		masters = resp.Masters
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get filer configuration: %w", err)
	}
	if len(masters) == 0 {
		return nil, fmt.Errorf("no masters found in filer configuration")
	}

	// Connect to master and get volume list with topology
	collectionInfos := make(map[string]*CollectionInfo)
	master := pb.ServerAddress(masters[0])

	err = pb.WithMasterClient(false, master, s3a.option.GrpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return fmt.Errorf("failed to get volume list from master %s: %w", master, err)
		}
		collectCollectionInfoFromTopology(resp.TopologyInfo, collectionInfos)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return collectionInfos, nil
}

// collectBucketSizeFromFilerStats uses filer Statistics RPC to get bucket sizes
// This is the fallback method when master topology is not available
func (s3a *S3ApiServer) collectBucketSizeFromFilerStats() {
	buckets, err := s3a.listBucketNames()
	if err != nil {
		glog.V(2).Infof("Failed to list buckets for size metrics: %v", err)
		return
	}

	for _, bucket := range buckets {
		collection := s3a.getCollectionName(bucket)

		err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.Statistics(context.Background(), &filer_pb.StatisticsRequest{
				Collection: collection,
			})
			if err != nil {
				return err
			}

			// Note: Filer Statistics doesn't separate logical vs physical size
			// Both will be set to UsedSize
			stats.UpdateBucketSizeMetrics(bucket, float64(resp.UsedSize), float64(resp.UsedSize), float64(resp.FileCount))
			glog.V(3).Infof("Updated bucket size metrics (via filer): bucket=%s, size=%d, objects=%d",
				bucket, resp.UsedSize, resp.FileCount)
			return nil
		})
		if err != nil {
			glog.V(3).Infof("Failed to get statistics for bucket %s: %v", bucket, err)
		}
	}
}

// listBucketNames returns a list of all bucket names using pagination
func (s3a *S3ApiServer) listBucketNames() ([]string, error) {
	var buckets []string

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		lastFileName := ""
		for {
			request := &filer_pb.ListEntriesRequest{
				Directory:          s3a.option.BucketsPath,
				StartFromFileName:  lastFileName,
				Limit:              listBucketPageSize,
				InclusiveStartFrom: lastFileName == "", // Only inclusive on first request
			}

			stream, err := client.ListEntries(context.Background(), request)
			if err != nil {
				return err
			}

			count := 0
			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				if resp.Entry != nil && resp.Entry.IsDirectory {
					// Skip .uploads and other hidden directories
					if !strings.HasPrefix(resp.Entry.Name, ".") {
						buckets = append(buckets, resp.Entry.Name)
					}
					lastFileName = resp.Entry.Name
					count++
				}
			}

			// If we got fewer entries than the limit, we're done
			if count < listBucketPageSize {
				break
			}
		}
		return nil
	})

	return buckets, err
}

// addToCollectionInfo adds volume info to collection statistics
// Similar to shell/command_collection_list.go:addToCollection
func addToCollectionInfo(collectionInfos map[string]*CollectionInfo, vif *master_pb.VolumeInformationMessage) {
	c := vif.Collection
	cif, found := collectionInfos[c]
	if !found {
		cif = &CollectionInfo{}
		collectionInfos[c] = cif
	}

	// Calculate replication factor for logical size deduplication
	replicaPlacement, err := super_block.NewReplicaPlacementFromByte(byte(vif.ReplicaPlacement))
	if err != nil || replicaPlacement == nil {
		glog.V(3).Infof("Invalid replica placement for collection %s, defaulting to 1 copy: %v", c, err)
		replicaPlacement = &super_block.ReplicaPlacement{} // GetCopyCount() returns 1 by default
	}
	copyCount := float64(replicaPlacement.GetCopyCount())
	if copyCount == 0 {
		copyCount = 1 // Prevent division by zero
	}

	// Logical size = physical size / copy count (deduplicated)
	cif.Size += float64(vif.Size) / copyCount
	cif.PhysicalSize += float64(vif.Size)
	cif.DeleteCount += float64(vif.DeleteCount) / copyCount
	cif.FileCount += float64(vif.FileCount) / copyCount
	cif.DeletedByteCount += float64(vif.DeletedByteCount) / copyCount
	cif.VolumeCount++
}

// collectCollectionInfoFromTopology extracts collection info from topology
// Similar to shell/command_collection_list.go:collectCollectionInfo
func collectCollectionInfoFromTopology(t *master_pb.TopologyInfo, collectionInfos map[string]*CollectionInfo) {
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						addToCollectionInfo(collectionInfos, vi)
					}
				}
			}
		}
	}
}
