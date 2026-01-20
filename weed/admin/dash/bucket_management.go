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
	"github.com/seaweedfs/seaweedfs/weed/s3api"
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
	Name                string `json:"name" binding:"required"`
	Region              string `json:"region"`
	QuotaSize           int64  `json:"quota_size"`            // Quota size in bytes
	QuotaUnit           string `json:"quota_unit"`            // Unit: MB, GB, TB
	QuotaEnabled        bool   `json:"quota_enabled"`         // Whether quota is enabled
	VersioningEnabled   bool   `json:"versioning_enabled"`    // Whether versioning is enabled
	ObjectLockEnabled   bool   `json:"object_lock_enabled"`   // Whether object lock is enabled
	ObjectLockMode      string `json:"object_lock_mode"`      // Object lock mode: "GOVERNANCE" or "COMPLIANCE"
	SetDefaultRetention bool   `json:"set_default_retention"` // Whether to set default retention
	ObjectLockDuration  int32  `json:"object_lock_duration"`  // Default retention duration in days
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

	// Validate object lock settings
	if req.ObjectLockEnabled {
		// Object lock requires versioning to be enabled
		req.VersioningEnabled = true

		// Validate object lock mode
		if req.ObjectLockMode != "GOVERNANCE" && req.ObjectLockMode != "COMPLIANCE" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Object lock mode must be either GOVERNANCE or COMPLIANCE"})
			return
		}

		// Validate retention duration if default retention is enabled
		if req.SetDefaultRetention {
			if req.ObjectLockDuration <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Object lock duration must be greater than 0 days when default retention is enabled"})
				return
			}
		}
	}

	// Convert quota to bytes
	quotaBytes := convertQuotaToBytes(req.QuotaSize, req.QuotaUnit)

	err := s.CreateS3BucketWithObjectLock(req.Name, quotaBytes, req.QuotaEnabled, req.VersioningEnabled, req.ObjectLockEnabled, req.ObjectLockMode, req.SetDefaultRetention, req.ObjectLockDuration)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create bucket: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":              "Bucket created successfully",
		"bucket":               req.Name,
		"quota_size":           req.QuotaSize,
		"quota_unit":           req.QuotaUnit,
		"quota_enabled":        req.QuotaEnabled,
		"versioning_enabled":   req.VersioningEnabled,
		"object_lock_enabled":  req.ObjectLockEnabled,
		"object_lock_mode":     req.ObjectLockMode,
		"object_lock_duration": req.ObjectLockDuration,
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
		bucketsPath := s.getBucketsPath()
		// Get the current bucket entry
		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: bucketsPath,
			Name:      bucketName,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %w", err)
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
			Directory: bucketsPath,
			Entry:     bucketEntry,
		})
		if err != nil {
			return fmt.Errorf("failed to update bucket quota: %w", err)
		}

		return nil
	})
}

// CreateS3BucketWithQuota creates a new S3 bucket with quota settings
func (s *AdminServer) CreateS3BucketWithQuota(bucketName string, quotaBytes int64, quotaEnabled bool) error {
	return s.CreateS3BucketWithObjectLock(bucketName, quotaBytes, quotaEnabled, false, false, "", false, 0)
}

// CreateS3BucketWithObjectLock creates a new S3 bucket with quota, versioning, and object lock settings
func (s *AdminServer) CreateS3BucketWithObjectLock(bucketName string, quotaBytes int64, quotaEnabled, versioningEnabled, objectLockEnabled bool, objectLockMode string, setDefaultRetention bool, objectLockDuration int32) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		bucketsPath := s.getBucketsPath()
		// Extract base directory and bucket folder name
		bucketsFolderName := "buckets" // default
		if bucketsPath != "" && bucketsPath != "/" {
			// Extract last component of path
			parts := strings.Split(strings.Trim(bucketsPath, "/"), "/")
			if len(parts) > 0 {
				bucketsFolderName = parts[len(parts)-1]
			}
		}
		baseDir := "/"
		if len(bucketsPath) > len(bucketsFolderName)+1 {
			baseDir = bucketsPath[:len(bucketsPath)-len(bucketsFolderName)]
		}
		
		// First ensure buckets directory exists
		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: baseDir,
			Entry: &filer_pb.Entry{
				Name:        bucketsFolderName,
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
			return fmt.Errorf("failed to create /buckets directory: %w", err)
		}

		// Check if bucket already exists
		_, err = client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: bucketsPath,
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

		// Prepare bucket attributes with versioning and object lock metadata
		attributes := &filer_pb.FuseAttributes{
			FileMode: uint32(0755 | os.ModeDir), // Directory mode
			Uid:      filer_pb.OS_UID,
			Gid:      filer_pb.OS_GID,
			Crtime:   time.Now().Unix(),
			Mtime:    time.Now().Unix(),
			TtlSec:   0,
		}

		// Create extended attributes map for versioning
		extended := make(map[string][]byte)

		// Create bucket entry
		bucketEntry := &filer_pb.Entry{
			Name:        bucketName,
			IsDirectory: true,
			Attributes:  attributes,
			Extended:    extended,
			Quota:       quota,
		}

		// Handle versioning using shared utilities
		if err := s3api.StoreVersioningInExtended(bucketEntry, versioningEnabled); err != nil {
			return fmt.Errorf("failed to store versioning configuration: %w", err)
		}

		// Handle Object Lock configuration using shared utilities
		if objectLockEnabled {
			var duration int32 = 0
			if setDefaultRetention {
				// Validate Object Lock parameters only when setting default retention
				if err := s3api.ValidateObjectLockParameters(objectLockEnabled, objectLockMode, objectLockDuration); err != nil {
					return fmt.Errorf("invalid Object Lock parameters: %w", err)
				}
				duration = objectLockDuration
			}

			// Create Object Lock configuration using shared utility
			objectLockConfig := s3api.CreateObjectLockConfigurationFromParams(objectLockEnabled, objectLockMode, duration)

			// Store Object Lock configuration in extended attributes using shared utility
			if err := s3api.StoreObjectLockConfigurationInExtended(bucketEntry, objectLockConfig); err != nil {
				return fmt.Errorf("failed to store Object Lock configuration: %w", err)
			}
		}

		// Create bucket directory under buckets path
		_, err = client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: bucketsPath,
			Entry:     bucketEntry,
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket directory: %w", err)
		}

		return nil
	})
}
