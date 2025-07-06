package dash

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// S3 Bucket management data structures for templates
type S3BucketsData struct {
	Username     string     `json:"username"`
	Buckets      []S3Bucket `json:"buckets"`
	TotalBuckets int        `json:"total_buckets"`
	TotalSize    int64      `json:"total_size"`
	LastUpdated  time.Time  `json:"last_updated"`
}

type CreateBucketRequest struct {
	Name         string `json:"name" binding:"required"`
	Region       string `json:"region"`
	QuotaSize    int64  `json:"quota_size"`    // Quota size in bytes
	QuotaUnit    string `json:"quota_unit"`    // Unit: MB, GB, TB
	QuotaEnabled bool   `json:"quota_enabled"` // Whether quota is enabled
}

// S3 Bucket Management Handlers

// ShowS3Buckets displays the Object Store buckets management page
func (s *AdminServer) ShowS3Buckets(c *gin.Context) {
	username := c.GetString("username")

	buckets, err := s.GetS3Buckets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get Object Store buckets: " + err.Error()})
		return
	}

	// Calculate totals
	var totalSize int64
	for _, bucket := range buckets {
		totalSize += bucket.Size
	}

	data := S3BucketsData{
		Username:     username,
		Buckets:      buckets,
		TotalBuckets: len(buckets),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}

	c.JSON(http.StatusOK, data)
}

// ShowBucketDetails displays detailed information about a specific bucket
func (s *AdminServer) ShowBucketDetails(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name is required"})
		return
	}

	details, err := s.GetBucketDetails(bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get bucket details: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, details)
}

// CreateBucket creates a new S3 bucket
func (s *AdminServer) CreateBucket(c *gin.Context) {
	var req CreateBucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Validate bucket name (basic validation)
	if len(req.Name) < 3 || len(req.Name) > 63 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name must be between 3 and 63 characters"})
		return
	}

	// Convert quota to bytes
	quotaBytes := convertQuotaToBytes(req.QuotaSize, req.QuotaUnit)

	err := s.CreateS3BucketWithQuota(req.Name, quotaBytes, req.QuotaEnabled)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create bucket: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":       "Bucket created successfully",
		"bucket":        req.Name,
		"quota_size":    req.QuotaSize,
		"quota_unit":    req.QuotaUnit,
		"quota_enabled": req.QuotaEnabled,
	})
}

// UpdateBucketQuota updates the quota settings for a bucket
func (s *AdminServer) UpdateBucketQuota(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name is required"})
		return
	}

	var req struct {
		QuotaSize    int64  `json:"quota_size"`
		QuotaUnit    string `json:"quota_unit"`
		QuotaEnabled bool   `json:"quota_enabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Convert quota to bytes
	quotaBytes := convertQuotaToBytes(req.QuotaSize, req.QuotaUnit)

	err := s.SetBucketQuota(bucketName, quotaBytes, req.QuotaEnabled)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update bucket quota: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Bucket quota updated successfully",
		"bucket":        bucketName,
		"quota_size":    req.QuotaSize,
		"quota_unit":    req.QuotaUnit,
		"quota_enabled": req.QuotaEnabled,
	})
}

// DeleteBucket deletes an S3 bucket
func (s *AdminServer) DeleteBucket(c *gin.Context) {
	bucketName := c.Param("bucket")
	if bucketName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Bucket name is required"})
		return
	}

	err := s.DeleteS3Bucket(bucketName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete bucket: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Bucket deleted successfully",
		"bucket":  bucketName,
	})
}

// ListBucketsAPI returns the list of buckets as JSON
func (s *AdminServer) ListBucketsAPI(c *gin.Context) {
	buckets, err := s.GetS3Buckets()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get buckets: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"buckets": buckets,
		"total":   len(buckets),
	})
}

// Helper function to convert quota size and unit to bytes
func convertQuotaToBytes(size int64, unit string) int64 {
	if size <= 0 {
		return 0
	}

	switch strings.ToUpper(unit) {
	case "TB":
		return size * 1024 * 1024 * 1024 * 1024
	case "GB":
		return size * 1024 * 1024 * 1024
	case "MB":
		return size * 1024 * 1024
	default:
		// Default to MB if unit is not recognized
		return size * 1024 * 1024
	}
}

// Helper function to convert bytes to appropriate unit and size
func convertBytesToQuota(bytes int64) (int64, string) {
	if bytes == 0 {
		return 0, "MB"
	}

	// Convert to TB if >= 1TB
	if bytes >= 1024*1024*1024*1024 && bytes%(1024*1024*1024*1024) == 0 {
		return bytes / (1024 * 1024 * 1024 * 1024), "TB"
	}

	// Convert to GB if >= 1GB
	if bytes >= 1024*1024*1024 && bytes%(1024*1024*1024) == 0 {
		return bytes / (1024 * 1024 * 1024), "GB"
	}

	// Convert to MB (default)
	return bytes / (1024 * 1024), "MB"
}

// SetBucketQuota sets the quota for a bucket
func (s *AdminServer) SetBucketQuota(bucketName string, quotaBytes int64, quotaEnabled bool) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Get the current bucket entry
		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/buckets",
			Name:      bucketName,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %v", err)
		}

		bucketEntry := lookupResp.Entry

		// Determine quota value (negative if disabled)
		var quota int64
		if quotaEnabled && quotaBytes > 0 {
			quota = quotaBytes
		} else if !quotaEnabled && quotaBytes > 0 {
			quota = -quotaBytes
		} else {
			quota = 0
		}

		// Update the quota
		bucketEntry.Quota = quota

		// Update the entry
		_, err = client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: "/buckets",
			Entry:     bucketEntry,
		})
		if err != nil {
			return fmt.Errorf("failed to update bucket quota: %v", err)
		}

		return nil
	})
}

// CreateS3BucketWithQuota creates a new S3 bucket with quota settings
func (s *AdminServer) CreateS3BucketWithQuota(bucketName string, quotaBytes int64, quotaEnabled bool) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// First ensure /buckets directory exists
		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/",
			Entry: &filer_pb.Entry{
				Name:        "buckets",
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: uint32(0755 | os.ModeDir), // Directory mode
					Uid:      uint32(1000),
					Gid:      uint32(1000),
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					TtlSec:   0,
				},
			},
		})
		// Ignore error if directory already exists
		if err != nil && !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "existing entry") {
			return fmt.Errorf("failed to create /buckets directory: %v", err)
		}

		// Check if bucket already exists
		_, err = client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/buckets",
			Name:      bucketName,
		})
		if err == nil {
			return fmt.Errorf("bucket %s already exists", bucketName)
		}

		// Determine quota value (negative if disabled)
		var quota int64
		if quotaEnabled && quotaBytes > 0 {
			quota = quotaBytes
		} else if !quotaEnabled && quotaBytes > 0 {
			quota = -quotaBytes
		} else {
			quota = 0
		}

		// Create bucket directory under /buckets
		_, err = client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/buckets",
			Entry: &filer_pb.Entry{
				Name:        bucketName,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: uint32(0755 | os.ModeDir), // Directory mode
					Uid:      filer_pb.OS_UID,
					Gid:      filer_pb.OS_GID,
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
					TtlSec:   0,
				},
				Quota: quota,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket directory: %v", err)
		}

		return nil
	})
}
