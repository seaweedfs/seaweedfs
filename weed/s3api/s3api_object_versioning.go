package s3api

import (
	"crypto/rand"
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

// S3ListObjectVersionsResult - Custom struct for S3 list-object-versions response
// This avoids conflicts with the XSD generated ListVersionsResult struct
// and ensures proper separation of versions and delete markers into arrays
type S3ListObjectVersionsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListVersionsResult"`

	Name                string `xml:"Name"`
	Prefix              string `xml:"Prefix,omitempty"`
	KeyMarker           string `xml:"KeyMarker,omitempty"`
	VersionIdMarker     string `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int    `xml:"MaxKeys"`
	Delimiter           string `xml:"Delimiter,omitempty"`
	IsTruncated         bool   `xml:"IsTruncated"`

	// These are the critical fields - arrays instead of single elements
	Versions      []VersionEntry      `xml:"Version,omitempty"`      // Array for versions
	DeleteMarkers []DeleteMarkerEntry `xml:"DeleteMarker,omitempty"` // Array for delete markers

	CommonPrefixes []PrefixEntry `xml:"CommonPrefixes,omitempty"`
	EncodingType   string        `xml:"EncodingType,omitempty"`
}

// Original struct - keeping for compatibility but will use S3ListObjectVersionsResult for XML response
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

// generateVersionId creates a unique version ID that preserves chronological order
func generateVersionId() string {
	// Use nanosecond timestamp to ensure chronological ordering
	// Format as 16-digit hex (first 16 chars of version ID)
	now := time.Now().UnixNano()
	timestampHex := fmt.Sprintf("%016x", now)

	// Generate random 8 bytes for uniqueness (last 16 chars of version ID)
	randBytes := make([]byte, 8)
	if _, err := rand.Read(randBytes); err != nil {
		glog.Errorf("Failed to generate random bytes for version ID: %v", err)
		// Fallback to timestamp-only if random generation fails
		return timestampHex + "0000000000000000"
	}

	// Combine timestamp (16 chars) + random (16 chars) = 32 chars total
	randomHex := hex.EncodeToString(randBytes)
	versionId := timestampHex + randomHex

	return versionId
}

// getVersionedObjectDir returns the directory path for storing object versions
func (s3a *S3ApiServer) getVersionedObjectDir(bucket, object string) string {
	return path.Join(s3a.option.BucketsPath, bucket, object+s3_constants.VersionsFolder)
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
	versionsDir := bucketDir + "/" + cleanObject + s3_constants.VersionsFolder

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
		return "", fmt.Errorf("failed to create delete marker in .versions directory: %w", err)
	}

	// Update the .versions directory metadata to indicate this delete marker is the latest version
	err = s3a.updateLatestVersionInDirectory(bucket, cleanObject, versionId, versionFileName)
	if err != nil {
		glog.Errorf("createDeleteMarker: failed to update latest version in directory: %v", err)
		return "", fmt.Errorf("failed to update latest version in directory: %w", err)
	}

	glog.V(2).Infof("createDeleteMarker: successfully created delete marker %s for %s/%s", versionId, bucket, object)
	return versionId, nil
}

// listObjectVersions lists all versions of an object
func (s3a *S3ApiServer) listObjectVersions(bucket, prefix, keyMarker, versionIdMarker, delimiter string, maxKeys int) (*S3ListObjectVersionsResult, error) {
	var allVersions []interface{} // Can contain VersionEntry or DeleteMarkerEntry

	glog.V(1).Infof("listObjectVersions: listing versions for bucket %s, prefix '%s'", bucket, prefix)

	// Track objects that have been processed to avoid duplicates
	processedObjects := make(map[string]bool)

	// Track version IDs globally to prevent duplicates throughout the listing
	seenVersionIds := make(map[string]bool)

	// Recursively find all .versions directories in the bucket
	bucketPath := path.Join(s3a.option.BucketsPath, bucket)
	err := s3a.findVersionsRecursively(bucketPath, "", &allVersions, processedObjects, seenVersionIds, bucket, prefix)
	if err != nil {
		glog.Errorf("listObjectVersions: findVersionsRecursively failed: %v", err)
		return nil, err
	}

	glog.V(1).Infof("listObjectVersions: found %d total versions", len(allVersions))

	// Sort by key, then by LastModified (newest first), then by VersionId for deterministic ordering
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

		// First sort by object key
		if keyI != keyJ {
			return keyI < keyJ
		}

		// Then by modification time (newest first) - but use nanosecond precision for ties
		timeDiff := lastModifiedI.Sub(lastModifiedJ)
		if timeDiff.Abs() > time.Millisecond {
			return lastModifiedI.After(lastModifiedJ)
		}

		// For very close timestamps (within 1ms), use version ID for deterministic ordering
		// Sort version IDs in reverse lexicographic order to maintain newest-first semantics
		return versionIdI > versionIdJ
	})

	// Build result using S3ListObjectVersionsResult to avoid conflicts with XSD structs
	result := &S3ListObjectVersionsResult{
		Name:        bucket,
		Prefix:      prefix,
		KeyMarker:   keyMarker,
		MaxKeys:     maxKeys,
		Delimiter:   delimiter,
		IsTruncated: len(allVersions) > maxKeys,
	}

	glog.V(1).Infof("listObjectVersions: building response with %d versions (truncated: %v)", len(allVersions), result.IsTruncated)

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

	// Always initialize empty slices so boto3 gets the expected fields even when empty
	result.Versions = make([]VersionEntry, 0)
	result.DeleteMarkers = make([]DeleteMarkerEntry, 0)

	// Add versions to result
	for i, version := range allVersions {
		switch v := version.(type) {
		case *VersionEntry:
			glog.V(2).Infof("listObjectVersions: adding version %d: key=%s, versionId=%s", i, v.Key, v.VersionId)
			result.Versions = append(result.Versions, *v)
		case *DeleteMarkerEntry:
			glog.V(2).Infof("listObjectVersions: adding delete marker %d: key=%s, versionId=%s", i, v.Key, v.VersionId)
			result.DeleteMarkers = append(result.DeleteMarkers, *v)
		}
	}

	glog.V(1).Infof("listObjectVersions: final result - %d versions, %d delete markers", len(result.Versions), len(result.DeleteMarkers))

	return result, nil
}

// findVersionsRecursively searches for all .versions directories and regular files recursively
func (s3a *S3ApiServer) findVersionsRecursively(currentPath, relativePath string, allVersions *[]interface{}, processedObjects map[string]bool, seenVersionIds map[string]bool, bucket, prefix string) error {
	// List entries in current directory with pagination
	startFrom := ""
	for {
		entries, _, err := s3a.list(currentPath, "", startFrom, false, filer.PaginationSize)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			// Track last entry name for pagination
			startFrom = entry.Name

			entryPath := path.Join(relativePath, entry.Name)

			// Skip if this doesn't match the prefix filter
			if normalizedPrefix := strings.TrimPrefix(prefix, "/"); normalizedPrefix != "" {
				// An entry is a candidate if:
				// 1. Its path is a match for the prefix.
				// 2. It is a directory that is an ancestor of the prefix path, so we must descend into it.

				// Condition 1: The entry's path starts with the prefix.
				isMatch := strings.HasPrefix(entryPath, normalizedPrefix)
				if !isMatch && entry.IsDirectory {
					// Also check if a directory entry matches a directory-style prefix (e.g., prefix "a/", entry "a").
					isMatch = strings.HasPrefix(entryPath+"/", normalizedPrefix)
				}

				// Condition 2: The prefix path starts with the entry's path (and it's a directory).
				canDescend := entry.IsDirectory && strings.HasPrefix(normalizedPrefix, entryPath)

				if !isMatch && !canDescend {
					continue
				}
			}

			if entry.IsDirectory {
				// Skip .uploads directory (multipart upload temporary files)
				if strings.HasPrefix(entry.Name, ".uploads") {
					continue
				}

				// Check if this is a .versions directory
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					// Extract object name from .versions directory name
					objectKey := strings.TrimSuffix(entryPath, s3_constants.VersionsFolder)
					normalizedObjectKey := removeDuplicateSlashes(objectKey)
					// Mark both keys as processed for backward compatibility
					processedObjects[objectKey] = true
					processedObjects[normalizedObjectKey] = true

					glog.V(2).Infof("Found .versions directory for object %s (normalized: %s)", objectKey, normalizedObjectKey)

					versions, err := s3a.getObjectVersionList(bucket, normalizedObjectKey)
					if err != nil {
						glog.Warningf("Failed to get versions for object %s (normalized: %s): %v", objectKey, normalizedObjectKey, err)
						continue
					}

					for _, version := range versions {
						// Check for duplicate version IDs and skip if already seen
						// Use normalized key for deduplication
						versionKey := normalizedObjectKey + ":" + version.VersionId
						if seenVersionIds[versionKey] {
							glog.Warningf("findVersionsRecursively: duplicate version %s for object %s detected, skipping", version.VersionId, normalizedObjectKey)
							continue
						}
						seenVersionIds[versionKey] = true

						if version.IsDeleteMarker {
							glog.V(4).Infof("Adding delete marker from .versions: objectKey=%s, versionId=%s, isLatest=%v, versionKey=%s",
								normalizedObjectKey, version.VersionId, version.IsLatest, versionKey)
							deleteMarker := &DeleteMarkerEntry{
								Key:          normalizedObjectKey, // Use normalized key for consistency
								VersionId:    version.VersionId,
								IsLatest:     version.IsLatest,
								LastModified: version.LastModified,
								Owner:        s3a.getObjectOwnerFromVersion(version, bucket, normalizedObjectKey),
							}
							*allVersions = append(*allVersions, deleteMarker)
						} else {
							glog.V(4).Infof("Adding version from .versions: objectKey=%s, versionId=%s, isLatest=%v, versionKey=%s",
								normalizedObjectKey, version.VersionId, version.IsLatest, versionKey)
							versionEntry := &VersionEntry{
								Key:          normalizedObjectKey, // Use normalized key for consistency
								VersionId:    version.VersionId,
								IsLatest:     version.IsLatest,
								LastModified: version.LastModified,
								ETag:         version.ETag,
								Size:         version.Size,
								Owner:        s3a.getObjectOwnerFromVersion(version, bucket, normalizedObjectKey),
								StorageClass: "STANDARD",
							}
							*allVersions = append(*allVersions, versionEntry)
						}
					}
				} else {
					// This is a regular directory - check if it's an explicit S3 directory object
					// Only include directories that were explicitly created via S3 API (have FolderMimeType)
					// This excludes implicit directories created when uploading files like "test1/a"
					if entry.Attributes.Mime == s3_constants.FolderMimeType {
						directoryKey := entryPath
						if !strings.HasSuffix(directoryKey, "/") {
							directoryKey += "/"
						}

						// Add directory as a version entry with VersionId "null" (following S3/Minio behavior)
						glog.V(2).Infof("findVersionsRecursively: found explicit S3 directory %s", directoryKey)

						// Calculate ETag for empty directory
						directoryETag := "\"d41d8cd98f00b204e9800998ecf8427e\""

						versionEntry := &VersionEntry{
							Key:          directoryKey,
							VersionId:    "null",
							IsLatest:     true,
							LastModified: time.Unix(entry.Attributes.Mtime, 0),
							ETag:         directoryETag,
							Size:         0, // Directories have size 0
							Owner:        s3a.getObjectOwnerFromEntry(entry),
							StorageClass: "STANDARD",
						}
						*allVersions = append(*allVersions, versionEntry)
					}

					// Recursively search subdirectories (regardless of whether they're explicit or implicit)
					fullPath := path.Join(currentPath, entry.Name)
					err := s3a.findVersionsRecursively(fullPath, entryPath, allVersions, processedObjects, seenVersionIds, bucket, prefix)
					if err != nil {
						glog.Warningf("Error searching subdirectory %s: %v", entryPath, err)
						continue
					}
				}
			} else {
				// This is a regular file - check if it's a pre-versioning object
				objectKey := entryPath

				// Normalize object key to ensure consistency with other version operations
				normalizedObjectKey := removeDuplicateSlashes(objectKey)

				// Skip if this object already has a .versions directory (already processed)
				// Check both normalized and original keys for backward compatibility
				if processedObjects[objectKey] || processedObjects[normalizedObjectKey] {
					glog.V(4).Infof("Skipping already processed object: objectKey=%s, normalizedObjectKey=%s, processedObjects[objectKey]=%v, processedObjects[normalizedObjectKey]=%v",
						objectKey, normalizedObjectKey, processedObjects[objectKey], processedObjects[normalizedObjectKey])
					continue
				}

				glog.V(4).Infof("Processing regular file: objectKey=%s, normalizedObjectKey=%s, NOT in processedObjects", objectKey, normalizedObjectKey)

				// This is a pre-versioning or suspended-versioning object
				// Check if this file has version metadata (ExtVersionIdKey)
				hasVersionMeta := false
				if entry.Extended != nil {
					if versionIdBytes, ok := entry.Extended[s3_constants.ExtVersionIdKey]; ok {
						hasVersionMeta = true
						glog.V(4).Infof("Regular file %s has version metadata: %s", normalizedObjectKey, string(versionIdBytes))
					}
				}

				// Check if a .versions directory exists for this object
				versionsObjectPath := normalizedObjectKey + s3_constants.VersionsFolder
				_, versionsErr := s3a.getEntry(currentPath, versionsObjectPath)
				if versionsErr == nil {
					// .versions directory exists
					glog.V(4).Infof("Found .versions directory for regular file %s, hasVersionMeta=%v", normalizedObjectKey, hasVersionMeta)

					// If this file has version metadata, it's a suspended versioning null version
					// Include it and it will be the latest
					if hasVersionMeta {
						glog.V(4).Infof("Including suspended versioning file %s (has version metadata)", normalizedObjectKey)
						// Continue to add it below
					} else {
						// No version metadata - this is a pre-versioning file
						// Skip it if there's already a null version in .versions
						versions, err := s3a.getObjectVersionList(bucket, normalizedObjectKey)
						if err == nil {
							hasNullVersion := false
							for _, v := range versions {
								if v.VersionId == "null" {
									hasNullVersion = true
									break
								}
							}
							if hasNullVersion {
								glog.V(4).Infof("Skipping pre-versioning file %s, null version exists in .versions", normalizedObjectKey)
								processedObjects[objectKey] = true
								processedObjects[normalizedObjectKey] = true
								continue
							}
						}
						glog.V(4).Infof("Including pre-versioning file %s (no null version in .versions)", normalizedObjectKey)
					}
				} else {
					glog.V(4).Infof("No .versions directory for regular file %s, hasVersionMeta=%v", normalizedObjectKey, hasVersionMeta)
				}

				// Add this file as a null version with IsLatest=true
				isLatest := true

				// Check for duplicate version IDs and skip if already seen
				// Use normalized key for deduplication to match how other version operations work
				versionKey := normalizedObjectKey + ":null"
				if seenVersionIds[versionKey] {
					glog.Warningf("findVersionsRecursively: duplicate null version for object %s detected (versionKey=%s), skipping", normalizedObjectKey, versionKey)
					continue
				}
				seenVersionIds[versionKey] = true

				etag := s3a.calculateETagFromChunks(entry.Chunks)

				glog.V(4).Infof("Adding null version from regular file: objectKey=%s, normalizedObjectKey=%s, versionKey=%s, isLatest=%v, hasVersionMeta=%v",
					objectKey, normalizedObjectKey, versionKey, isLatest, hasVersionMeta)

				versionEntry := &VersionEntry{
					Key:          normalizedObjectKey, // Use normalized key for consistency
					VersionId:    "null",
					IsLatest:     isLatest,
					LastModified: time.Unix(entry.Attributes.Mtime, 0),
					ETag:         etag,
					Size:         int64(entry.Attributes.FileSize),
					Owner:        s3a.getObjectOwnerFromEntry(entry),
					StorageClass: "STANDARD",
				}
				*allVersions = append(*allVersions, versionEntry)
			}
		}

		// If we got fewer entries than PaginationSize, we've reached the end
		if len(entries) < filer.PaginationSize {
			break
		}
	}

	return nil
}

// getObjectVersionList returns all versions of a specific object
func (s3a *S3ApiServer) getObjectVersionList(bucket, object string) ([]*ObjectVersion, error) {
	var versions []*ObjectVersion

	glog.V(2).Infof("getObjectVersionList: looking for versions of %s/%s in .versions directory", bucket, object)

	// All versions are now stored in the .versions directory only
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := object + s3_constants.VersionsFolder
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

	// Use a map to detect and prevent duplicate version IDs
	seenVersionIds := make(map[string]bool)

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

		// Check for duplicate version IDs and skip if already seen
		if seenVersionIds[versionId] {
			glog.Warningf("getObjectVersionList: duplicate version ID %s detected for object %s/%s, skipping", versionId, bucket, object)
			continue
		}
		seenVersionIds[versionId] = true

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

	// Don't sort here - let the main listObjectVersions function handle sorting consistently

	glog.V(2).Infof("getObjectVersionList: returning %d total versions for %s/%s (after deduplication from %d entries)", len(versions), bucket, object, len(entries))
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
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := removeDuplicateSlashes(object)

	if versionId == "" {
		// Get current version
		return s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(normalizedObject, "/"))
	}

	if versionId == "null" {
		// "null" version ID refers to pre-versioning objects stored as regular files
		bucketDir := s3a.option.BucketsPath + "/" + bucket
		entry, err := s3a.getEntry(bucketDir, normalizedObject)
		if err != nil {
			return nil, fmt.Errorf("null version object %s not found: %v", normalizedObject, err)
		}
		return entry, nil
	}

	// Get specific version from .versions directory
	versionsDir := s3a.getVersionedObjectDir(bucket, normalizedObject)
	versionFile := s3a.getVersionFileName(versionId)

	entry, err := s3a.getEntry(versionsDir, versionFile)
	if err != nil {
		return nil, fmt.Errorf("version %s not found: %v", versionId, err)
	}

	return entry, nil
}

// deleteSpecificObjectVersion deletes a specific version of an object
func (s3a *S3ApiServer) deleteSpecificObjectVersion(bucket, object, versionId string) error {
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := removeDuplicateSlashes(object)

	if versionId == "" {
		return fmt.Errorf("version ID is required for version-specific deletion")
	}

	if versionId == "null" {
		// Delete "null" version (pre-versioning object stored as regular file)
		bucketDir := s3a.option.BucketsPath + "/" + bucket
		cleanObject := strings.TrimPrefix(normalizedObject, "/")

		// Check if the object exists
		_, err := s3a.getEntry(bucketDir, cleanObject)
		if err != nil {
			// Object doesn't exist - this is OK for delete operations (idempotent)
			glog.V(2).Infof("deleteSpecificObjectVersion: null version object %s already deleted or doesn't exist", cleanObject)
			return nil
		}

		// Delete the regular file
		deleteErr := s3a.rm(bucketDir, cleanObject, true, false)
		if deleteErr != nil {
			// Check if file was already deleted by another process
			if _, checkErr := s3a.getEntry(bucketDir, cleanObject); checkErr != nil {
				// File doesn't exist anymore, deletion was successful
				return nil
			}
			return fmt.Errorf("failed to delete null version %s: %v", cleanObject, deleteErr)
		}
		return nil
	}

	versionsDir := s3a.getVersionedObjectDir(bucket, normalizedObject)
	versionFile := s3a.getVersionFileName(versionId)

	// Check if this is the latest version before attempting deletion (for potential metadata update)
	versionsEntry, dirErr := s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), normalizedObject+s3_constants.VersionsFolder)
	isLatestVersion := false
	if dirErr == nil && versionsEntry.Extended != nil {
		if latestVersionIdBytes, hasLatest := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]; hasLatest {
			isLatestVersion = (string(latestVersionIdBytes) == versionId)
		}
	}

	// Attempt to delete the version file
	// Note: We don't check if the file exists first to avoid race conditions
	// The deletion operation should be idempotent
	deleteErr := s3a.rm(versionsDir, versionFile, true, false)
	if deleteErr != nil {
		// Check if file was already deleted by another process (race condition handling)
		if _, checkErr := s3a.getEntry(versionsDir, versionFile); checkErr != nil {
			// File doesn't exist anymore, deletion was successful (another thread deleted it)
			glog.V(2).Infof("deleteSpecificObjectVersion: version %s for %s%s already deleted by another process", versionId, bucket, object)
			return nil
		}
		// File still exists but deletion failed for another reason
		return fmt.Errorf("failed to delete version %s: %v", versionId, deleteErr)
	}

	// If we deleted the latest version, update the .versions directory metadata to point to the new latest
	if isLatestVersion {
		err := s3a.updateLatestVersionAfterDeletion(bucket, object)
		if err != nil {
			glog.Warningf("deleteSpecificObjectVersion: failed to update latest version after deletion: %v", err)
			// Don't return error since the deletion was successful
		}
	}

	return nil
}

// updateLatestVersionAfterDeletion finds the new latest version after deleting the current latest
func (s3a *S3ApiServer) updateLatestVersionAfterDeletion(bucket, object string) error {
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	cleanObject := strings.TrimPrefix(object, "/")
	versionsObjectPath := cleanObject + s3_constants.VersionsFolder
	versionsDir := bucketDir + "/" + versionsObjectPath

	glog.V(1).Infof("updateLatestVersionAfterDeletion: updating latest version for %s/%s, listing %s", bucket, object, versionsDir)

	// List all remaining version files in the .versions directory
	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		glog.Errorf("updateLatestVersionAfterDeletion: failed to list versions in %s: %v", versionsDir, err)
		return fmt.Errorf("failed to list versions: %v", err)
	}

	glog.V(1).Infof("updateLatestVersionAfterDeletion: found %d entries in %s", len(entries), versionsDir)

	// Find the most recent remaining version (latest timestamp in version ID)
	var latestVersionId string
	var latestVersionFileName string

	for _, entry := range entries {
		if entry.Extended == nil {
			continue
		}

		versionIdBytes, hasVersionId := entry.Extended[s3_constants.ExtVersionIdKey]
		if !hasVersionId {
			continue
		}

		versionId := string(versionIdBytes)

		// Skip delete markers when finding latest content version
		isDeleteMarkerBytes, _ := entry.Extended[s3_constants.ExtDeleteMarkerKey]
		if string(isDeleteMarkerBytes) == "true" {
			continue
		}

		// Compare version IDs chronologically (our version IDs start with timestamp)
		if latestVersionId == "" || versionId > latestVersionId {
			glog.V(1).Infof("updateLatestVersionAfterDeletion: found newer version %s (file: %s)", versionId, entry.Name)
			latestVersionId = versionId
			latestVersionFileName = entry.Name
		} else {
			glog.V(1).Infof("updateLatestVersionAfterDeletion: skipping older version %s", versionId)
		}
	}

	// Update the .versions directory metadata
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		return fmt.Errorf("failed to get .versions directory: %v", err)
	}

	if versionsEntry.Extended == nil {
		versionsEntry.Extended = make(map[string][]byte)
	}

	if latestVersionId != "" {
		// Update metadata to point to new latest version
		versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey] = []byte(latestVersionId)
		versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey] = []byte(latestVersionFileName)
		glog.V(2).Infof("updateLatestVersionAfterDeletion: new latest version for %s/%s is %s", bucket, object, latestVersionId)
	} else {
		// No versions left, remove latest version metadata
		delete(versionsEntry.Extended, s3_constants.ExtLatestVersionIdKey)
		delete(versionsEntry.Extended, s3_constants.ExtLatestVersionFileNameKey)
		glog.V(2).Infof("updateLatestVersionAfterDeletion: no versions left for %s/%s", bucket, object)
	}

	// Update the .versions directory entry
	err = s3a.mkFile(bucketDir, versionsObjectPath, versionsEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
		updatedEntry.Extended = versionsEntry.Extended
		updatedEntry.Attributes = versionsEntry.Attributes
		updatedEntry.Chunks = versionsEntry.Chunks
	})
	if err != nil {
		return fmt.Errorf("failed to update .versions directory metadata: %v", err)
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
	originalPrefix := query.Get("prefix") // Keep original prefix for response
	prefix := originalPrefix              // Use for internal processing
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

	// Set the original prefix in the response (not the normalized internal prefix)
	result.Prefix = originalPrefix

	writeSuccessResponseXML(w, r, result)
}

// getLatestObjectVersion finds the latest version of an object by reading .versions directory metadata
func (s3a *S3ApiServer) getLatestObjectVersion(bucket, object string) (*filer_pb.Entry, error) {
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := removeDuplicateSlashes(object)

	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := normalizedObject + s3_constants.VersionsFolder

	glog.V(1).Infof("getLatestObjectVersion: looking for latest version of %s/%s (normalized: %s)", bucket, object, normalizedObject)

	// Get the .versions directory entry to read latest version metadata with retry logic for filer consistency
	var versionsEntry *filer_pb.Entry
	var err error
	maxRetries := 8
	for attempt := 1; attempt <= maxRetries; attempt++ {
		versionsEntry, err = s3a.getEntry(bucketDir, versionsObjectPath)
		if err == nil {
			break
		}

		if attempt < maxRetries {
			// Exponential backoff with higher base: 100ms, 200ms, 400ms, 800ms, 1600ms, 3200ms, 6400ms
			delay := time.Millisecond * time.Duration(100*(1<<(attempt-1)))
			time.Sleep(delay)
		}
	}

	if err != nil {
		// .versions directory doesn't exist - this can happen for objects that existed
		// before versioning was enabled on the bucket. Fall back to checking for a
		// regular (non-versioned) object file.
		glog.V(1).Infof("getLatestObjectVersion: no .versions directory for %s%s after %d attempts (error: %v), checking for pre-versioning object", bucket, normalizedObject, maxRetries, err)

		regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
		if regularErr != nil {
			glog.V(1).Infof("getLatestObjectVersion: no pre-versioning object found for %s%s (error: %v)", bucket, normalizedObject, regularErr)
			return nil, fmt.Errorf("failed to get %s%s .versions directory and no regular object found: %w", bucket, normalizedObject, err)
		}

		glog.V(1).Infof("getLatestObjectVersion: found pre-versioning object for %s/%s", bucket, normalizedObject)
		return regularEntry, nil
	}

	// Check if directory has latest version metadata - retry if missing due to race condition
	if versionsEntry.Extended == nil {
		// Retry a few times to handle the race condition where directory exists but metadata is not yet written
		metadataRetries := 3
		for metaAttempt := 1; metaAttempt <= metadataRetries; metaAttempt++ {
			// Small delay and re-read the directory
			time.Sleep(time.Millisecond * 100)
			versionsEntry, err = s3a.getEntry(bucketDir, versionsObjectPath)
			if err != nil {
				break
			}

			if versionsEntry.Extended != nil {
				break
			}
		}

		// If still no metadata after retries, fall back to pre-versioning object
		if versionsEntry.Extended == nil {
			glog.V(2).Infof("getLatestObjectVersion: no Extended metadata in .versions directory for %s%s after retries, checking for pre-versioning object", bucket, object)

			regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
			if regularErr != nil {
				return nil, fmt.Errorf("no version metadata in .versions directory and no regular object found for %s%s", bucket, normalizedObject)
			}

			glog.V(2).Infof("getLatestObjectVersion: found pre-versioning object for %s%s (no Extended metadata case)", bucket, object)
			return regularEntry, nil
		}
	}

	latestVersionIdBytes, hasLatestVersionId := versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey]
	latestVersionFileBytes, hasLatestVersionFile := versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]

	if !hasLatestVersionId || !hasLatestVersionFile {
		// No version metadata means all versioned objects have been deleted.
		// Fall back to checking for a pre-versioning object.
		glog.V(2).Infof("getLatestObjectVersion: no version metadata in .versions directory for %s/%s, checking for pre-versioning object", bucket, object)

		regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
		if regularErr != nil {
			return nil, fmt.Errorf("no version metadata in .versions directory and no regular object found for %s%s", bucket, normalizedObject)
		}

		glog.V(2).Infof("getLatestObjectVersion: found pre-versioning object for %s%s after version deletion", bucket, object)
		return regularEntry, nil
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

// getObjectOwnerFromVersion extracts object owner information from version entry metadata
func (s3a *S3ApiServer) getObjectOwnerFromVersion(version *ObjectVersion, bucket, objectKey string) CanonicalUser {
	// First try to get owner from the version entry itself
	if version.Entry != nil && version.Entry.Extended != nil {
		if ownerBytes, exists := version.Entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			ownerId := string(ownerBytes)
			ownerDisplayName := s3a.iam.GetAccountNameById(ownerId)
			return CanonicalUser{ID: ownerId, DisplayName: ownerDisplayName}
		}
	}

	// Fallback: try to get owner from the current version of the object
	// This handles cases where older versions might not have owner metadata
	if version.VersionId == "null" {
		// For null version, check the regular object file
		bucketDir := s3a.option.BucketsPath + "/" + bucket
		if entry, err := s3a.getEntry(bucketDir, objectKey); err == nil && entry.Extended != nil {
			if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
				ownerId := string(ownerBytes)
				ownerDisplayName := s3a.iam.GetAccountNameById(ownerId)
				return CanonicalUser{ID: ownerId, DisplayName: ownerDisplayName}
			}
		}
	} else {
		// For versioned objects, try to get from latest version metadata
		if latestVersion, err := s3a.getLatestObjectVersion(bucket, objectKey); err == nil && latestVersion.Extended != nil {
			if ownerBytes, exists := latestVersion.Extended[s3_constants.ExtAmzOwnerKey]; exists {
				ownerId := string(ownerBytes)
				ownerDisplayName := s3a.iam.GetAccountNameById(ownerId)
				return CanonicalUser{ID: ownerId, DisplayName: ownerDisplayName}
			}
		}
	}

	// Ultimate fallback: return anonymous if no owner found
	return CanonicalUser{ID: s3_constants.AccountAnonymousId, DisplayName: "anonymous"}
}

// getObjectOwnerFromEntry extracts object owner information from a file entry
func (s3a *S3ApiServer) getObjectOwnerFromEntry(entry *filer_pb.Entry) CanonicalUser {
	if entry != nil && entry.Extended != nil {
		if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			ownerId := string(ownerBytes)
			ownerDisplayName := s3a.iam.GetAccountNameById(ownerId)
			return CanonicalUser{ID: ownerId, DisplayName: ownerDisplayName}
		}
	}

	// Fallback: return anonymous if no owner found
	return CanonicalUser{ID: s3_constants.AccountAnonymousId, DisplayName: "anonymous"}
}
