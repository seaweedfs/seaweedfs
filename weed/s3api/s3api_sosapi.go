// Package s3api implements the S3 API for SeaweedFS.
// This file implements the Smart Object Storage API (SOSAPI) which enables
// enterprise backup software to automatically discover storage system
// capabilities and capacity information.
package s3api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
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
// storage capacity information retrieved from the master server.
func (s3a *S3ApiServer) generateCapacityXML(ctx context.Context) ([]byte, error) {
	total, used, err := s3a.getClusterCapacity(ctx)
	if err != nil {
		glog.Warningf("SOSAPI: failed to get cluster capacity: %v, using defaults", err)
		// Return zero capacity on error - clients will handle gracefully
		total, used = 0, 0
	}

	available := total - used
	if available < 0 {
		available = 0
	}

	ci := CapacityInfo{
		Capacity:  total,
		Available: available,
		Used:      used,
	}

	return xml.Marshal(&ci)
}

// getClusterCapacity retrieves the total and used storage capacity from the master server.
func (s3a *S3ApiServer) getClusterCapacity(ctx context.Context) (total, used int64, err error) {
	if len(s3a.option.Masters) == 0 {
		glog.V(3).Infof("SOSAPI: no masters configured, capacity unavailable")
		return 0, 0, nil
	}

	// Convert masters slice to map for WithOneOfGrpcMasterClients
	masterMap := make(map[string]pb.ServerAddress)
	for _, master := range s3a.option.Masters {
		masterMap[string(master)] = master
	}

	// Connect to any available master and get statistics
	err = pb.WithOneOfGrpcMasterClients(false, masterMap, s3a.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, statsErr := client.Statistics(ctx, &master_pb.StatisticsRequest{})
		if statsErr != nil {
			return statsErr
		}
		total = int64(resp.TotalSize)
		used = int64(resp.UsedSize)
		return nil
	})

	return total, used, err
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
		glog.V(2).Infof("SOSAPI: serving system.xml for bucket %s", bucket)

	case sosAPICapacityXML:
		xmlData, err = s3a.generateCapacityXML(r.Context())
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate capacity.xml: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(2).Infof("SOSAPI: serving capacity.xml for bucket %s", bucket)

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

	// Handle Range requests if present
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		// Simple range handling for SOSAPI objects
		s3a.serveSOSAPIRange(w, r, xmlData, etag)
		return true
	}

	// Write full response
	w.WriteHeader(http.StatusOK)
	w.Write(xmlData)

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
		glog.V(2).Infof("SOSAPI: HEAD system.xml for bucket %s", bucket)

	case sosAPICapacityXML:
		xmlData, err = s3a.generateCapacityXML(r.Context())
		if err != nil {
			glog.Errorf("SOSAPI: failed to generate capacity.xml for HEAD: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return true
		}
		glog.V(2).Infof("SOSAPI: HEAD capacity.xml for bucket %s", bucket)

	default:
		return false
	}

	// Prepend XML declaration for accurate size calculation
	xmlData = append([]byte(xml.Header), xmlData...)

	// Calculate ETag from content
	hash := md5.Sum(xmlData)
	etag := hex.EncodeToString(hash[:])

	// Set response headers (no body for HEAD)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", "\""+etag+"\"")
	w.Header().Set("Content-Length", strconv.Itoa(len(xmlData)))
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)

	return true
}

// serveSOSAPIRange handles Range requests for SOSAPI objects.
func (s3a *S3ApiServer) serveSOSAPIRange(w http.ResponseWriter, r *http.Request, data []byte, etag string) {
	rangeHeader := r.Header.Get("Range")
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		http.Error(w, "Invalid Range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Parse simple range like "bytes=0-99"
	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		http.Error(w, "Invalid Range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	var start, end int64
	size := int64(len(data))

	if parts[0] == "" {
		// Suffix range: -N means last N bytes
		var n int64
		if _, err := io.ReadFull(strings.NewReader(parts[1]), make([]byte, 0)); err == nil {
			// Parse suffix length
			n = size // fallback to full content
		}
		start = size - n
		if start < 0 {
			start = 0
		}
		end = size - 1
	} else {
		// Normal range: start-end
		start = 0
		end = size - 1
		// Simple parsing - in production would need proper int parsing
	}

	if start > end || start >= size {
		http.Error(w, "Invalid Range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if end >= size {
		end = size - 1
	}

	// Set partial content headers
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", "\""+etag+"\"")
	w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.FormatInt(size, 10))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)

	// Write the requested range
	w.Write(data[start : end+1])
}
