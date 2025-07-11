package s3api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	s3_constants "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ObjectVersion represents a version of an S3 object
type ObjectVersion struct {
	VersionId      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   time.Time
	ETag           string
	Size           int64
	Entry          *filer_pb.Entry
}

// ListObjectVersionsResult represents the response for ListObjectVersions
type ListObjectVersionsResult struct {
	XMLName             xml.Name            `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult"`
	Name                string              `xml:"Name"`
	Prefix              string              `xml:"Prefix"`
	KeyMarker           string              `xml:"KeyMarker,omitempty"`
	VersionIdMarker     string              `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string              `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string              `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int                 `xml:"MaxKeys"`
	Delimiter           string              `xml:"Delimiter,omitempty"`
	IsTruncated         bool                `xml:"IsTruncated"`
	Versions            []VersionEntry      `xml:"Version,omitempty"`
	DeleteMarkers       []DeleteMarkerEntry `xml:"DeleteMarker,omitempty"`
	CommonPrefixes      []PrefixEntry       `xml:"CommonPrefixes,omitempty"`
}

// generateVersionId creates a unique version ID
func generateVersionId() string {
	// Generate a random 16-byte value
	randBytes := make([]byte, 16)
	if _, err := rand.Read(randBytes); err != nil {
		glog.Errorf("Failed to generate random bytes for version ID: %v", err)
		return ""
	}

	// Hash with current timestamp for uniqueness
	hash := sha256.Sum256(append(randBytes, []byte(fmt.Sprintf("%d", time.Now().UnixNano()))...))

	// Return first 32 characters of hex string (same length as AWS S3 version IDs)
	return hex.EncodeToString(hash[:])[:32]
}

// getVersionedObjectDir returns the directory path for storing object versions
func (s3a *S3ApiServer) getVersionedObjectDir(bucket, object string) string {
	return path.Join(s3a.option.BucketsPath, bucket, object+".versions")
}

// getVersionFileName returns the filename for a specific version
func (s3a *S3ApiServer) getVersionFileName(versionId string) string {
	return fmt.Sprintf("v_%s", versionId)
}

// createDeleteMarker creates a delete marker for versioned delete operations
func (s3a *S3ApiServer) createDeleteMarker(bucket, object string) (string, error) {
	versionId := generateVersionId()

	glog.V(2).Infof("createDeleteMarker: creating delete marker %s for %s/%s", versionId, bucket, object)

	// Create the version file name for the delete marker
	versionFileName := s3a.getVersionFileName(versionId)

	// Store delete marker in the .versions directory
	// Make sure to clean up the object path to remove leading slashes
	cleanObject := strings.TrimPrefix(object, "/")
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsDir := bucketDir + "/" + cleanObject + ".versions"

	// Create the delete marker entry in the .versions directory
	err := s3a.mkFile(versionsDir, versionFileName, nil, func(entry *filer_pb.Entry) {
		entry.Name = versionFileName
		entry.IsDirectory = false
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.Mtime = time.Now().Unix()
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
		entry.Extended[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	})
	if err != nil {
		return "", fmt.Errorf("failed to create delete marker in .versions directory: %v", err)
	}

	// Update the .versions directory metadata to indicate this delete marker is the latest version
	err = s3a.updateLatestVersionInDirectory(bucket, cleanObject, versionId, versionFileName)
	if err != nil {
		glog.Errorf("createDeleteMarker: failed to update latest version in directory: %v", err)
		return "", fmt.Errorf("failed to update latest version in directory: %v", err)
	}

	glog.V(2).Infof("createDeleteMarker: successfully created delete marker %s for %s/%s", versionId, bucket, object)
	return versionId, nil
}

// listObjectVersions lists all versions of an object
func (s3a *S3ApiServer) listObjectVersions(bucket, prefix, keyMarker, versionIdMarker, delimiter string, maxKeys int) (*ListObjectVersionsResult, error) {
	var allVersions []interface{} // Can contain VersionEntry or DeleteMarkerEntry

	// List all entries in bucket
	entries, _, err := s3a.list(path.Join(s3a.option.BucketsPath, bucket), prefix, keyMarker, false, uint32(maxKeys*2))
	if err != nil {
		return nil, err
	}

	// For each entry, check if it's a .versions directory
	for _, entry := range entries {
		if !entry.IsDirectory {
			continue
		}

		// Check if this is a .versions directory
		if !strings.HasSuffix(entry.Name, ".versions") {
			continue
		}

		// Extract object name from .versions directory name
		objectKey := strings.TrimSuffix(entry.Name, ".versions")

		versions, err := s3a.getObjectVersionList(bucket, objectKey)
		if err != nil {
			glog.Warningf("Failed to get versions for object %s: %v", objectKey, err)
			continue
		}

		for _, version := range versions {
			if version.IsDeleteMarker {
				deleteMarker := &DeleteMarkerEntry{
					Key:          objectKey,
					VersionId:    version.VersionId,
					IsLatest:     version.IsLatest,
					LastModified: version.LastModified,
					Owner:        CanonicalUser{ID: "unknown", DisplayName: "unknown"},
				}
				allVersions = append(allVersions, deleteMarker)
			} else {
				versionEntry := &VersionEntry{
					Key:          objectKey,
					VersionId:    version.VersionId,
					IsLatest:     version.IsLatest,
					LastModified: version.LastModified,
					ETag:         version.ETag,
					Size:         version.Size,
					Owner:        CanonicalUser{ID: "unknown", DisplayName: "unknown"},
					StorageClass: "STANDARD",
				}
				allVersions = append(allVersions, versionEntry)
			}
		}
	}

	// Sort by key, then by LastModified and VersionId
	sort.Slice(allVersions, func(i, j int) bool {
		var keyI, keyJ string
		var lastModifiedI, lastModifiedJ time.Time
		var versionIdI, versionIdJ string

		switch v := allVersions[i].(type) {
		case *VersionEntry:
			keyI = v.Key
			lastModifiedI = v.LastModified
			versionIdI = v.VersionId
		case *DeleteMarkerEntry:
			keyI = v.Key
			lastModifiedI = v.LastModified
			versionIdI = v.VersionId
		}

		switch v := allVersions[j].(type) {
		case *VersionEntry:
			keyJ = v.Key
			lastModifiedJ = v.LastModified
			versionIdJ = v.VersionId
		case *DeleteMarkerEntry:
			keyJ = v.Key
			lastModifiedJ = v.LastModified
			versionIdJ = v.VersionId
		}

		if keyI != keyJ {
			return keyI < keyJ
		}
		if !lastModifiedI.Equal(lastModifiedJ) {
			return lastModifiedI.After(lastModifiedJ)
		}
		return versionIdI < versionIdJ
	})

	// Build result
	result := &ListObjectVersionsResult{
		Name:        bucket,
		Prefix:      prefix,
		KeyMarker:   keyMarker,
		MaxKeys:     maxKeys,
		Delimiter:   delimiter,
		IsTruncated: len(allVersions) > maxKeys,
	}

	// Limit results
	if len(allVersions) > maxKeys {
		allVersions = allVersions[:maxKeys]
		result.IsTruncated = true

		// Set next markers
		switch v := allVersions[len(allVersions)-1].(type) {
		case *VersionEntry:
			result.NextKeyMarker = v.Key
			result.NextVersionIdMarker = v.VersionId
		case *DeleteMarkerEntry:
			result.NextKeyMarker = v.Key
			result.NextVersionIdMarker = v.VersionId
		}
	}

	// Add versions to result
	for _, version := range allVersions {
		switch v := version.(type) {
		case *VersionEntry:
			result.Versions = append(result.Versions, *v)
		case *DeleteMarkerEntry:
			result.DeleteMarkers = append(result.DeleteMarkers, *v)
		}
	}

	return result, nil
}

// getObjectVersionList returns all versions of a specific object
func (s3a *S3ApiServer) getObjectVersionList(bucket, object string) ([]*ObjectVersion, error) {
	var versions []*ObjectVersion

	glog.V(2).Infof("getObjectVersionList: looking for versions of %s/%s in .versions directory", bucket, object)

	// All versions are now stored in the .versions directory only
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := object + ".versions"
	glog.V(2).Infof("getObjectVersionList: checking versions directory %s", versionsObjectPath)

	// Get the .versions directory entry to read latest version metadata
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		// No versions directory exists, return empty list
		glog.V(2).Infof("getObjectVersionList: no versions directory found: %v", err)
		return versions, nil
	}

	// Get the latest version info from directory metadata
	var latestVersionId string
	if versionsEntry.Extended != nil {
		if latestVersionIdBytes, hasLatestVersionId := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]; hasLatestVersionId {
			latestVersionId = string(latestVersionIdBytes)
			glog.V(2).Infof("getObjectVersionList: latest version ID from directory metadata: %s", latestVersionId)
		}
	}

	// List all version files in the .versions directory
	entries, _, err := s3a.list(bucketDir+"/"+versionsObjectPath, "", "", false, 1000)
	if err != nil {
		glog.V(2).Infof("getObjectVersionList: failed to list version files: %v", err)
		return versions, nil
	}

	glog.V(2).Infof("getObjectVersionList: found %d entries in versions directory", len(entries))

	for i, entry := range entries {
		if entry.Extended == nil {
			glog.V(2).Infof("getObjectVersionList: entry %d has no Extended metadata, skipping", i)
			continue
		}

		versionIdBytes, hasVersionId := entry.Extended[s3_constants.ExtVersionIdKey]
		if !hasVersionId {
			glog.V(2).Infof("getObjectVersionList: entry %d has no version ID, skipping", i)
			continue
		}

		versionId := string(versionIdBytes)

		// Check if this version is the latest by comparing with directory metadata
		isLatest := (versionId == latestVersionId)

		isDeleteMarkerBytes, _ := entry.Extended[s3_constants.ExtDeleteMarkerKey]
		isDeleteMarker := string(isDeleteMarkerBytes) == "true"

		glog.V(2).Infof("getObjectVersionList: found version %s, isLatest=%v, isDeleteMarker=%v", versionId, isLatest, isDeleteMarker)

		version := &ObjectVersion{
			VersionId:      versionId,
			IsLatest:       isLatest,
			IsDeleteMarker: isDeleteMarker,
			LastModified:   time.Unix(entry.Attributes.Mtime, 0),
			Entry:          entry,
		}

		if !isDeleteMarker {
			// Try to get ETag from Extended attributes first
			if etagBytes, hasETag := entry.Extended[s3_constants.ExtETagKey]; hasETag {
				version.ETag = string(etagBytes)
			} else {
				// Fallback: calculate ETag from chunks
				version.ETag = s3a.calculateETagFromChunks(entry.Chunks)
			}
			version.Size = int64(entry.Attributes.FileSize)
		}

		versions = append(versions, version)
	}

	// Sort by modification time (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].LastModified.After(versions[j].LastModified)
	})

	glog.V(2).Infof("getObjectVersionList: returning %d total versions for %s/%s", len(versions), bucket, object)
	for i, version := range versions {
		glog.V(2).Infof("getObjectVersionList: version %d: %s (isLatest=%v, isDeleteMarker=%v)", i, version.VersionId, version.IsLatest, version.IsDeleteMarker)
	}

	return versions, nil
}

// calculateETagFromChunks calculates ETag from file chunks following S3 multipart rules
// This is a wrapper around filer.ETagChunks that adds quotes for S3 compatibility
func (s3a *S3ApiServer) calculateETagFromChunks(chunks []*filer_pb.FileChunk) string {
	if len(chunks) == 0 {
		return "\"\""
	}

	// Use the existing filer ETag calculation and add quotes for S3 compatibility
	etag := filer.ETagChunks(chunks)
	if etag == "" {
		return "\"\""
	}
	return fmt.Sprintf("\"%s\"", etag)
}

// getSpecificObjectVersion retrieves a specific version of an object
func (s3a *S3ApiServer) getSpecificObjectVersion(bucket, object, versionId string) (*filer_pb.Entry, error) {
	if versionId == "" {
		// Get current version
		return s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(object, "/"))
	}

	// Get specific version from .versions directory
	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	entry, err := s3a.getEntry(versionsDir, versionFile)
	if err != nil {
		return nil, fmt.Errorf("version %s not found: %v", versionId, err)
	}

	return entry, nil
}

// deleteSpecificObjectVersion deletes a specific version of an object
func (s3a *S3ApiServer) deleteSpecificObjectVersion(bucket, object, versionId string) error {
	if versionId == "" {
		return fmt.Errorf("version ID is required for version-specific deletion")
	}

	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	// Delete the specific version from .versions directory
	_, err := s3a.getEntry(versionsDir, versionFile)
	if err != nil {
		return fmt.Errorf("version %s not found: %v", versionId, err)
	}

	// Version exists, delete it
	deleteErr := s3a.rm(versionsDir, versionFile, true, false)
	if deleteErr != nil {
		// Check if file was already deleted by another process
		if _, checkErr := s3a.getEntry(versionsDir, versionFile); checkErr != nil {
			// File doesn't exist anymore, deletion was successful
			return nil
		}
		return fmt.Errorf("failed to delete version %s: %v", versionId, deleteErr)
	}
	return nil
}

// ListObjectVersionsHandler handles the list object versions request
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
func (s3a *S3ApiServer) ListObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectVersionsHandler %s", bucket)

	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Parse query parameters
	query := r.URL.Query()
	prefix := query.Get("prefix")
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	keyMarker := query.Get("key-marker")
	versionIdMarker := query.Get("version-id-marker")
	delimiter := query.Get("delimiter")

	maxKeysStr := query.Get("max-keys")
	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	// List versions
	result, err := s3a.listObjectVersions(bucket, prefix, keyMarker, versionIdMarker, delimiter, maxKeys)
	if err != nil {
		glog.Errorf("ListObjectVersionsHandler: %v", err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	writeSuccessResponseXML(w, r, result)
}

// getLatestObjectVersion finds the latest version of an object by reading .versions directory metadata
func (s3a *S3ApiServer) getLatestObjectVersion(bucket, object string) (*filer_pb.Entry, error) {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := object + ".versions"

	// Get the .versions directory entry to read latest version metadata
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get .versions directory: %v", err)
	}

	// Check if directory has latest version metadata
	if versionsEntry.Extended == nil {
		return nil, fmt.Errorf("no version metadata found in .versions directory for %s/%s", bucket, object)
	}

	latestVersionIdBytes, hasLatestVersionId := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]
	latestVersionFileBytes, hasLatestVersionFile := versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]

	if !hasLatestVersionId || !hasLatestVersionFile {
		return nil, fmt.Errorf("incomplete latest version metadata in .versions directory for %s/%s", bucket, object)
	}

	latestVersionId := string(latestVersionIdBytes)
	latestVersionFile := string(latestVersionFileBytes)

	glog.V(2).Infof("getLatestObjectVersion: found latest version %s (file: %s) for %s/%s", latestVersionId, latestVersionFile, bucket, object)

	// Get the actual latest version file entry
	latestVersionPath := versionsObjectPath + "/" + latestVersionFile
	latestVersionEntry, err := s3a.getEntry(bucketDir, latestVersionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version file %s: %v", latestVersionPath, err)
	}

	return latestVersionEntry, nil
}
