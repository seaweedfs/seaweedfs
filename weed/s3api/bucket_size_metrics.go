package s3api

import (
	"context"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

const (
	bucketSizeMetricsInterval = 1 * time.Minute
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
func (s3a *S3ApiServer) collectCollectionInfoFromMaster() (map[string]*CollectionInfo, error) {
	// Check if we have master client access via filerClient
	if s3a.filerClient == nil {
		return nil, nil // No master access, will use fallback
	}

	// Query filer for collection list first
	var collections []string
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.CollectionList(context.Background(), &filer_pb.CollectionListRequest{
			IncludeNormalVolumes: true,
			IncludeEcVolumes:     true,
		})
		if err != nil {
			return err
		}
		for _, c := range resp.Collections {
			collections = append(collections, c.Name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Now get volume list from master via filer
	collectionInfos := make(map[string]*CollectionInfo)
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Use GetFilerConfiguration to check if we can access master info
		// The filer proxies the VolumeList request to master
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		
		// The filer doesn't expose VolumeList directly, so we need to use Statistics per collection
		for _, collection := range collections {
			statsResp, err := client.Statistics(context.Background(), &filer_pb.StatisticsRequest{
				Collection: collection,
			})
			if err != nil {
				glog.V(3).Infof("Failed to get statistics for collection %s: %v", collection, err)
				continue
			}
			
			collectionInfos[collection] = &CollectionInfo{
				FileCount:    float64(statsResp.FileCount),
				Size:         float64(statsResp.UsedSize),
				PhysicalSize: float64(statsResp.UsedSize), // Filer stats don't differentiate logical vs physical
			}
		}
		
		_ = resp // Used for connection check
		return nil
	})
	
	return collectionInfos, err
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

// listBucketNames returns a list of all bucket names
func (s3a *S3ApiServer) listBucketNames() ([]string, error) {
	var buckets []string
	
	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory:          s3a.option.BucketsPath,
			Limit:              100000,
			InclusiveStartFrom: true,
		}
		
		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return err
		}
		
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
	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(vif.ReplicaPlacement))
	copyCount := float64(replicaPlacement.GetCopyCount())
	
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

