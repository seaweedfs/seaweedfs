package s3api

import (
	"context"
	"fmt"
	"io"
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
// Used for both metrics collection and quota enforcement
type CollectionInfo struct {
	FileCount        float64
	DeleteCount      float64
	DeletedByteCount float64
	Size             float64 // Logical size (deduplicated across replicas)
	PhysicalSize     float64 // Physical size (including all replicas)
	VolumeCount      int
}

// StartBucketSizeMetricsCollection starts a background goroutine to periodically
// collect bucket size metrics and update Prometheus gauges.
// This runs the same collection logic used for quota enforcement.
// The goroutine will stop when the provided context is cancelled.
func (s3a *S3ApiServer) StartBucketSizeMetricsCollection(ctx context.Context) {
	go s3a.loopCollectBucketSizeMetrics(ctx)
}

func (s3a *S3ApiServer) loopCollectBucketSizeMetrics(ctx context.Context) {
	// Initial delay to let the system stabilize
	select {
	case <-time.After(10 * time.Second):
	case <-ctx.Done():
		return
	}

	for {
		s3a.collectAndUpdateBucketSizeMetrics()
		select {
		case <-time.After(bucketSizeMetricsInterval):
		case <-ctx.Done():
			glog.V(1).Infof("Stopping bucket size metrics collection")
			return
		}
	}
}

// collectAndUpdateBucketSizeMetrics collects bucket sizes from master topology
// and updates Prometheus metrics. Uses the same approach as quota enforcement.
func (s3a *S3ApiServer) collectAndUpdateBucketSizeMetrics() {
	// Collect collection info from master topology (same as quota enforcement)
	collectionInfos, err := s3a.collectCollectionInfoFromMaster()
	if err != nil {
		glog.V(2).Infof("Failed to collect collection info from master: %v", err)
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
			// Bucket exists but no collection data (empty bucket)
			stats.UpdateBucketSizeMetrics(bucket, 0, 0, 0)
		}
	}
}

// collectCollectionInfoFromMaster queries the master for topology info and extracts collection sizes.
// This is the same approach used by shell command s3.bucket.quota.enforce.
func (s3a *S3ApiServer) collectCollectionInfoFromMaster() (map[string]*CollectionInfo, error) {
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
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return fmt.Errorf("failed to get volume list: %w", err)
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
func (s3a *S3ApiServer) listBucketNames() ([]string, error) {
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

			stream, err := client.ListEntries(context.Background(), request)
			if err != nil {
				return err
			}

			count := 0
			for {
				resp, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("error receiving bucket list entries: %w", err)
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

// addToCollectionInfo adds volume info to collection statistics.
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
		replicaPlacement = &super_block.ReplicaPlacement{}
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

// collectCollectionInfoFromTopology extracts collection info from topology.
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
