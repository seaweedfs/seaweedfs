// Package s3api implements the S3 API for SeaweedFS.
// This file implements the Smart Object Storage API (SOSAPI) which enables
// enterprise backup software to automatically discover storage system
// capabilities and capacity information.
package s3api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// SOSAPI constants
const (
	// sosAPISystemFolder is the well-known folder path for SOSAPI system files.
	// This UUID-based path is part of the SOSAPI specification.
	sosAPISystemFolder = ".system-d26a9498-cb7c-4a87-a44a-8ae204f5ba6c"

	// sosAPISystemXML is the path to the system capabilities XML file.
	sosAPISystemXML = sosAPISystemFolder + "/system.xml"

	// sosAPICapacityXML is the path to the capacity information XML file.
	sosAPICapacityXML = sosAPISystemFolder + "/capacity.xml"

	// sosAPIClientUserAgent is a substring to detect SOSAPI-compatible backup clients.
	sosAPIClientUserAgent = "APN/1.0 Veeam/1.0"

	// sosAPIProtocolVersion is the SOSAPI protocol version supported.
	sosAPIProtocolVersion = `"1.0"`

	// sosAPIDefaultBlockSizeKB is the recommended block size in KB.
	// 4096 KB (4MB) is optimal for object storage workloads.
	sosAPIDefaultBlockSizeKB = 4096
)

// SystemInfo represents the system.xml response structure for SOSAPI.
// It describes the storage system's capabilities and recommendations.
type SystemInfo struct {
	XMLName              xml.Name `xml:"SystemInfo"`
	ProtocolVersion      string   `xml:"ProtocolVersion"`
	ModelName            string   `xml:"ModelName"`
	ProtocolCapabilities struct {
		CapacityInfo   bool `xml:"CapacityInfo"`
		UploadSessions bool `xml:"UploadSessions"`
		IAMSTS         bool `xml:"IAMSTS"`
	} `xml:"ProtocolCapabilities"`
	APIEndpoints          *APIEndpoints          `xml:"APIEndpoints,omitempty"`
	SystemRecommendations *SystemRecommendations `xml:"SystemRecommendations,omitempty"`
}

// APIEndpoints contains optional IAM and STS endpoint information.
type APIEndpoints struct {
	IAMEndpoint string `xml:"IAMEndpoint,omitempty"`
	STSEndpoint string `xml:"STSEndpoint,omitempty"`
}

// SystemRecommendations contains storage system performance recommendations.
type SystemRecommendations struct {
	S3ConcurrentTaskLimit    int `xml:"S3ConcurrentTaskLimit,omitempty"`
	S3MultiObjectDeleteLimit int `xml:"S3MultiObjectDeleteLimit,omitempty"`
	StorageCurrentTaskLimit  int `xml:"StorageCurrentTaskLimit,omitempty"`
	KBBlockSize              int `xml:"KbBlockSize"`
}

// CapacityInfo represents the capacity.xml response structure for SOSAPI.
// It provides real-time storage capacity information.
type CapacityInfo struct {
	XMLName   xml.Name `xml:"CapacityInfo"`
	Capacity  int64    `xml:"Capacity"`
	Available int64    `xml:"Available"`
	Used      int64    `xml:"Used"`
}

// isSOSAPIObject checks if the given object path is a SOSAPI virtual object.
// These objects don't physically exist but are generated on-demand.
func isSOSAPIObject(object string) bool {
	switch object {
	case sosAPISystemXML, sosAPICapacityXML:
		return true
	default:
		return false
	}
}

// isSOSAPIClient checks if the request comes from a SOSAPI-compatible client
// by examining the User-Agent header.
func isSOSAPIClient(r *http.Request) bool {
	userAgent := r.Header.Get("User-Agent")
	return strings.Contains(userAgent, sosAPIClientUserAgent)
}

// generateSystemXML creates the system.xml response containing storage system
// capabilities and recommendations.
func generateSystemXML() ([]byte, error) {
	si := SystemInfo{
		ProtocolVersion: sosAPIProtocolVersion,
		ModelName:       "\"SeaweedFS " + version.VERSION_NUMBER + "\"",
	}

	// Enable capacity reporting capability
	si.ProtocolCapabilities.CapacityInfo = true
	si.ProtocolCapabilities.UploadSessions = false
	si.ProtocolCapabilities.IAMSTS = false

	// Set recommended block size for optimal performance
	si.SystemRecommendations = &SystemRecommendations{
		KBBlockSize: sosAPIDefaultBlockSizeKB,
	}

	return xml.Marshal(&si)
}

// generateCapacityXML creates the capacity.xml response containing real-time
// storage capacity information.
func (s3a *S3ApiServer) generateCapacityXML(ctx context.Context, bucket string) ([]byte, error) {
	total, available, used, err := s3a.getCapacityInfo(ctx, bucket)
	if err != nil {
		glog.Warningf("SOSAPI: failed to get capacity info for bucket %s: %v, using defaults", bucket, err)
		// Return zero capacity on error
		total, available, used = 0, 0, 0
	}

	ci := CapacityInfo{
		Capacity:  total,
		Available: available,
		Used:      used,
	}

	return xml.Marshal(&ci)
}

// getCapacityInfo retrieves capacity information for the specific bucket.
// It checks bucket quota first, then falls back to cluster topology information.
// Returns capacity, available, and used bytes.
func (s3a *S3ApiServer) getCapacityInfo(ctx context.Context, bucket string) (capacity, available, used int64, err error) {
	// 1. Check if bucket has a quota
	// We use s3a.getEntry which is a helper in s3api_bucket_handlers.go
	var quota int64
	// getEntry communicates with filer, so errors here might mean filer connectivity issues or bucket not found
	// If bucket not found, we probably shouldn't be here (checked in handler), but safe to ignore
	if entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket); err == nil && entry != nil {
		quota = entry.Quota
	}

	// 2. Get cluster topology from master
	if len(s3a.option.Masters) == 0 {
		return 0, 0, 0, nil
	}

	masterMap := make(map[string]pb.ServerAddress)
	for _, master := range s3a.option.Masters {
		masterMap[string(master)] = master
	}

	// Connect to any available master and get volume list (topology)
	err = pb.WithOneOfGrpcMasterClients(false, masterMap, s3a.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(ctx, &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo == nil {
			return nil
		}

		// Calculate used size for the bucket by summing up volumes in the collection
		used = collectBucketUsageFromTopology(resp.TopologyInfo, bucket)

		// Calculate cluster capacity if no quota
		if quota > 0 {
			capacity = quota
			available = quota - used
			if available < 0 {
				available = 0
			}
		} else {
			// No quota - use cluster capacity
			clusterTotal, clusterAvailable := calculateClusterCapacity(resp.TopologyInfo, resp.VolumeSizeLimitMb)
			capacity = clusterTotal
			available = clusterAvailable
		}
		return nil
	})

	return capacity, available, used, err
}

// collectBucketUsageFromTopology sums up the size of all volumes belonging to the specified bucket (collection).
func collectBucketUsageFromTopology(t *master_pb.TopologyInfo, bucket string) (used int64) {
	seenVolumes := make(map[uint32]bool)
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, disk := range dn.DiskInfos {
					for _, vi := range disk.VolumeInfos {
						if vi.Collection == bucket {
							if !seenVolumes[vi.Id] {
								used += int64(vi.Size)
								seenVolumes[vi.Id] = true
							}
						}
					}
				}
			}
		}
	}
	return
}

// calculateClusterCapacity sums up the total and available capacity of the entire cluster.
func calculateClusterCapacity(t *master_pb.TopologyInfo, volumeSizeLimitMb uint64) (total, available int64) {
	volumeSize := int64(volumeSizeLimitMb) * 1024 * 1024
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, disk := range dn.DiskInfos {
					total += int64(disk.MaxVolumeCount) * volumeSize
					available += int64(disk.FreeVolumeCount) * volumeSize
				}
			}
		}
	}
	return
}

// handleSOSAPIGetObject handles GET requests for SOSAPI virtual objects.
// Returns true if the request was handled, false if it should proceed normally.
func (s3a *S3ApiServer) handleSOSAPIGetObject(w http.ResponseWriter, r *http.Request, bucket, object string) bool {
	if !isSOSAPIObject(object) {
		return false
	}

	var xmlData []byte
	var err error

	// Verify bucket exists
	if _, errCode := s3a.getBucketConfig(bucket); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return true
	}

	switch object {
	case sosAPISystemXML:
		xmlData, err = generateSystemXML()
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate system.xml: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(4).Infof("SOSAPI: serving system.xml for bucket %s", bucket)

	case sosAPICapacityXML:
		xmlData, err = s3a.generateCapacityXML(r.Context(), bucket)
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate capacity.xml: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(4).Infof("SOSAPI: serving capacity.xml for bucket %s", bucket)

	default:
		return false
	}

	// Prepend XML declaration
	xmlData = append([]byte(xml.Header), xmlData...)

	// Calculate ETag from content
	hash := md5.Sum(xmlData)
	etag := hex.EncodeToString(hash[:])

	// Set ETag header manually as ServeContent doesn't calculate it automatically
	w.Header().Set("ETag", "\""+etag+"\"")
	w.Header().Set("Content-Type", "application/xml")

	// Use http.ServeContent to handle Content-Length, Range, and Last-Modified
	http.ServeContent(w, r, object, time.Now().UTC(), bytes.NewReader(xmlData))

	return true
}

// handleSOSAPIHeadObject handles HEAD requests for SOSAPI virtual objects.
// Returns true if the request was handled, false if it should proceed normally.
func (s3a *S3ApiServer) handleSOSAPIHeadObject(w http.ResponseWriter, r *http.Request, bucket, object string) bool {
	if !isSOSAPIObject(object) {
		return false
	}

	var xmlData []byte
	var err error

	// Verify bucket exists
	if _, errCode := s3a.getBucketConfig(bucket); errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return true
	}

	switch object {
	case sosAPISystemXML:
		xmlData, err = generateSystemXML()
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate system.xml for HEAD: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(4).Infof("SOSAPI: serving system.xml HEAD for bucket %s", bucket)

	case sosAPICapacityXML:
		xmlData, err = s3a.generateCapacityXML(r.Context(), bucket)
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate capacity.xml for HEAD: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(4).Infof("SOSAPI: serving capacity.xml HEAD for bucket %s", bucket)

	default:
		return false
	}

	// Prepend XML declaration
	xmlData = append([]byte(xml.Header), xmlData...)

	// Calculate ETag from content
	hash := md5.Sum(xmlData)
	etag := hex.EncodeToString(hash[:])

	// Set response headers
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", "\""+etag+"\"")
	w.Header().Set("Content-Length", strconv.Itoa(len(xmlData)))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))

	// Do NOT handle Range requests for HEAD (following MinIO behavior)
	// Write status OK and return (no body)
	w.WriteHeader(http.StatusOK)

	return true
}
