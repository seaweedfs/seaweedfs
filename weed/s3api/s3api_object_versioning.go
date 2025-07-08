package s3api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"os"
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
	return versionId
}

// storeVersionedObject stores an object with versioning metadata
func (s3a *S3ApiServer) storeVersionedObject(bucket, object, versionId string, entry *filer_pb.Entry, isLatest bool) error {
	// Add version metadata to entry
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
	entry.Extended[s3_constants.ExtIsLatestKey] = []byte(strconv.FormatBool(isLatest))

	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	objectName := strings.TrimPrefix(object, "/")

	if isLatest {
		// Move existing current object to .versions directory (if it exists and has version metadata)
		err := s3a.moveCurrentObjectToVersions(bucket, object)
		if err != nil {
			glog.Warningf("Failed to move current object to versions: %v", err)
		}

		// Store the new version as the current object
		err = s3a.mkFile(bucketDir, objectName, entry.Chunks, func(currentEntry *filer_pb.Entry) {
			currentEntry.Name = objectName
			currentEntry.IsDirectory = entry.IsDirectory
			currentEntry.Attributes = entry.Attributes
			currentEntry.Extended = entry.Extended
		})
		if err != nil {
			return fmt.Errorf("failed to store current version %s/%s: %v", bucketDir, objectName, err)
		}
	} else {
		// Store non-latest version directly in .versions directory
		versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
		if err != nil {
			return err
		}

		versionEntry := &filer_pb.Entry{
			Name:        s3a.getVersionFileName(versionId),
			IsDirectory: entry.IsDirectory,
			Attributes:  entry.Attributes,
			Chunks:      entry.Chunks,
			Extended:    entry.Extended,
		}

		err = s3a.mkFile(versionsDir, versionEntry.Name, versionEntry.Chunks, func(entry *filer_pb.Entry) {
			entry.Name = versionEntry.Name
			entry.IsDirectory = versionEntry.IsDirectory
			entry.Attributes = versionEntry.Attributes
			entry.Extended = versionEntry.Extended
		})
		if err != nil {
			return fmt.Errorf("failed to store versioned object %s/%s: %v", versionsDir, versionId, err)
		}
	}

	return nil
}

// moveCurrentObjectToVersions moves the current object to .versions directory
func (s3a *S3ApiServer) moveCurrentObjectToVersions(bucket, object string) error {
	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	objectName := strings.TrimPrefix(object, "/")

	// Check if current object exists and has version metadata
	currentEntry, err := s3a.getEntry(bucketDir, objectName)
	if err != nil {
		// No current object exists, nothing to move
		return nil
	}

	// Only move if it has version metadata (i.e., it's a versioned object)
	if currentEntry.Extended == nil {
		// Not a versioned object, nothing to move
		return nil
	}

	versionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]
	if !hasVersionId {
		// Not a versioned object, nothing to move
		return nil
	}

	versionId := string(versionIdBytes)

	// Ensure the .versions directory exists
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		return err
	}

	// Update metadata to mark as not latest
	if currentEntry.Extended == nil {
		currentEntry.Extended = make(map[string][]byte)
	}
	currentEntry.Extended[s3_constants.ExtIsLatestKey] = []byte("false")

	// Preserve ETag if it doesn't exist (for older objects)
	if _, hasETag := currentEntry.Extended[s3_constants.ExtETagKey]; !hasETag {
		fallbackETag := "\"" + filer.ETagChunks(currentEntry.Chunks) + "\""
		currentEntry.Extended[s3_constants.ExtETagKey] = []byte(fallbackETag)
	}

	// Store in .versions directory
	versionFileName := s3a.getVersionFileName(versionId)
	err = s3a.mkFile(versionsDir, versionFileName, currentEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Name = versionFileName
		entry.IsDirectory = currentEntry.IsDirectory
		entry.Attributes = currentEntry.Attributes
		entry.Extended = currentEntry.Extended
	})
	if err != nil {
		return fmt.Errorf("failed to move object to versions: %v", err)
	}

	// Remove from current location (we'll replace it with the new version)
	err = s3a.rm(bucketDir, objectName, false, false)
	if err != nil {
		glog.Warningf("Failed to remove current object after moving to versions: %v", err)
	}

	return nil
}

// markPreviousVersionsAsNotLatest updates all previous versions to not be latest
func (s3a *S3ApiServer) markPreviousVersionsAsNotLatest(bucket, object string) error {
	versionsDir := s3a.getVersionedObjectDir(bucket, object)

	// Check if the versions directory exists
	versionsDirPath, versionsDirName := path.Split(versionsDir)
	exists, err := s3a.exists(versionsDirPath, versionsDirName, true)
	if err != nil || !exists {
		// Directory doesn't exist yet, which is fine for the first version
		return nil
	}

	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		// Directory might have been deleted or is empty, which is fine
		return nil
	}

	for _, entry := range entries {
		if entry.Extended != nil {
			// Only update if it's currently marked as latest
			if isLatestBytes, exists := entry.Extended[s3_constants.ExtIsLatestKey]; exists && string(isLatestBytes) == "true" {
				entry.Extended[s3_constants.ExtIsLatestKey] = []byte("false")
				err := s3a.updateEntry(versionsDir, entry)
				if err != nil {
					glog.Warningf("Failed to update version %s for object %s: %v", entry.Name, object, err)
				}
			}
		}
	}

	return nil
}

// createDeleteMarker creates a delete marker for versioned delete operations
func (s3a *S3ApiServer) createDeleteMarker(bucket, object string) (string, error) {
	versionId := generateVersionId()

	// Move existing current object to .versions directory (if it exists and has version metadata)
	err := s3a.moveCurrentObjectToVersions(bucket, object)
	if err != nil {
		glog.Warningf("Failed to move current object to versions before delete marker: %v", err)
	}

	// Create delete marker as the new current object
	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	objectName := strings.TrimPrefix(object, "/")

	err = s3a.mkFile(bucketDir, objectName, nil, func(entry *filer_pb.Entry) {
		entry.Name = objectName
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
		entry.Extended[s3_constants.ExtIsLatestKey] = []byte("true")
	})
	if err != nil {
		return "", fmt.Errorf("failed to create delete marker %s/%s: %v", bucketDir, objectName, err)
	}

	return versionId, nil
}

// listObjectVersions lists all versions of an object
func (s3a *S3ApiServer) listObjectVersions(bucket, prefix, keyMarker, versionIdMarker, delimiter string, maxKeys int) (*ListObjectVersionsResult, error) {
	var allVersions []interface{} // Can contain VersionEntry or DeleteMarkerEntry

	// List all objects in bucket
	entries, _, err := s3a.list(path.Join(s3a.option.BucketsPath, bucket), prefix, keyMarker, false, uint32(maxKeys*2))
	if err != nil {
		return nil, err
	}

	// For each object, collect its versions
	for _, entry := range entries {
		if entry.IsDirectory {
			continue
		}

		objectKey := entry.Name
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

	// First, check if there's a current object (might be a delete marker or latest version)
	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	objectName := strings.TrimPrefix(object, "/")
	currentEntry, err := s3a.getEntry(bucketDir, objectName)
	if err == nil && currentEntry.Extended != nil {
		// Check if current object has version metadata (delete marker or versioned object)
		if versionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]; hasVersionId {
			versionId := string(versionIdBytes)
			isLatestBytes, _ := currentEntry.Extended[s3_constants.ExtIsLatestKey]
			isLatest := string(isLatestBytes) == "true"
			isDeleteMarkerBytes, _ := currentEntry.Extended[s3_constants.ExtDeleteMarkerKey]
			isDeleteMarker := string(isDeleteMarkerBytes) == "true"

			version := &ObjectVersion{
				VersionId:      versionId,
				IsLatest:       isLatest,
				IsDeleteMarker: isDeleteMarker,
				LastModified:   time.Unix(currentEntry.Attributes.Mtime, 0),
				Entry:          currentEntry,
			}

			if !isDeleteMarker {
				// Try to get ETag from Extended attributes first
				if etagBytes, hasETag := currentEntry.Extended[s3_constants.ExtETagKey]; hasETag {
					version.ETag = string(etagBytes)
				} else {
					// Fallback: calculate ETag from chunks
					version.ETag = "\"" + filer.ETagChunks(currentEntry.Chunks) + "\""
				}
				version.Size = int64(currentEntry.Attributes.FileSize)
			}

			versions = append(versions, version)
		}
	}

	// Then, check the .versions directory for previous versions
	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		// No versions directory exists, return only current object versions
		goto sortAndReturn
	}

	for _, entry := range entries {
		if entry.Extended == nil {
			continue
		}

		versionIdBytes, hasVersionId := entry.Extended[s3_constants.ExtVersionIdKey]
		if !hasVersionId {
			continue
		}

		versionId := string(versionIdBytes)
		isLatestBytes, _ := entry.Extended[s3_constants.ExtIsLatestKey]
		isLatest := string(isLatestBytes) == "true"

		isDeleteMarkerBytes, _ := entry.Extended[s3_constants.ExtDeleteMarkerKey]
		isDeleteMarker := string(isDeleteMarkerBytes) == "true"

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
				version.ETag = "\"" + filer.ETagChunks(entry.Chunks) + "\""
			}
			version.Size = int64(entry.Attributes.FileSize)
		}

		versions = append(versions, version)
	}

sortAndReturn:
	// Sort by modification time (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].LastModified.After(versions[j].LastModified)
	})

	return versions, nil
}

// getSpecificObjectVersion retrieves a specific version of an object
func (s3a *S3ApiServer) getSpecificObjectVersion(bucket, object, versionId string) (*filer_pb.Entry, error) {
	if versionId == "" {
		// Get current version
		return s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(object, "/"))
	}

	// Get specific version
	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	return s3a.getEntry(versionsDir, s3a.getVersionFileName(versionId))
}

// deleteSpecificObjectVersion deletes a specific version of an object
func (s3a *S3ApiServer) deleteSpecificObjectVersion(bucket, object, versionId string) error {
	if versionId == "" {
		return fmt.Errorf("version ID is required for version-specific deletion")
	}

	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	objectName := strings.TrimPrefix(object, "/")

	// First check if the version ID matches the current object
	currentEntry, err := s3a.getEntry(bucketDir, objectName)
	if err == nil && currentEntry.Extended != nil {
		if currentVersionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]; hasVersionId {
			currentVersionId := string(currentVersionIdBytes)
			if currentVersionId == versionId {
				// This is the current version, delete it from main location
				return s3a.rm(bucketDir, objectName, true, false)
			}
		}
	}

	// If not the current version, check the .versions directory
	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	// Check if this version exists in .versions directory
	_, err = s3a.getEntry(versionsDir, versionFile)
	if err != nil {
		return err
	}

	// Delete the version from .versions directory
	return s3a.rm(versionsDir, versionFile, true, false)
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

// ensureVersionedDirectory ensures the .versions directory exists for an object
func (s3a *S3ApiServer) ensureVersionedDirectory(bucket, object string) (string, error) {
	versionsDir := s3a.getVersionedObjectDir(bucket, object)

	// Check cache first to avoid repeated creation attempts
	if s3a.versionDirCache.IsCached(versionsDir) {
		return versionsDir, nil
	}

	versionsDirPath, versionsDirName := path.Split(versionsDir)

	// Try to create the directory - this is idempotent and handles race conditions
	err := s3a.mkdir(versionsDirPath, versionsDirName, func(entry *filer_pb.Entry) {
		entry.IsDirectory = true // Explicitly set as directory
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileMode = uint32(0755 | os.ModeDir)
	})

	if err != nil {
		// Check if directory already exists (race condition)
		if exists, checkErr := s3a.exists(versionsDirPath, versionsDirName, true); checkErr == nil && exists {
			// Directory was created by another process, which is fine
			s3a.versionDirCache.Set(versionsDir)
			return versionsDir, nil
		}
		return versionsDir, fmt.Errorf("failed to create versions directory %s: %v", versionsDir, err)
	}

	// Cache the successful creation to prevent repeated attempts
	s3a.versionDirCache.Set(versionsDir)
	return versionsDir, nil
}
