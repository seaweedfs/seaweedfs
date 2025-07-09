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

	glog.V(2).Infof("moveCurrentObjectToVersions: attempting to move %s/%s", bucket, object)

	// Check if current object exists and has version metadata
	currentEntry, err := s3a.getEntry(bucketDir, objectName)
	if err != nil {
		// No current object exists, nothing to move
		glog.V(2).Infof("moveCurrentObjectToVersions: no current object exists for %s/%s: %v", bucket, object, err)
		return nil
	}

	// Only move if it has version metadata (i.e., it's a versioned object)
	if currentEntry.Extended == nil {
		// Not a versioned object, nothing to move
		glog.V(2).Infof("moveCurrentObjectToVersions: current object %s/%s has no Extended metadata, nothing to move", bucket, object)
		return nil
	}

	versionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]
	if !hasVersionId {
		// Not a versioned object, nothing to move
		glog.V(2).Infof("moveCurrentObjectToVersions: current object %s/%s has no version ID, nothing to move", bucket, object)
		return nil
	}

	versionId := string(versionIdBytes)
	glog.V(2).Infof("moveCurrentObjectToVersions: found current object %s/%s with version ID %s", bucket, object, versionId)

	// Ensure the .versions directory exists
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		glog.Errorf("moveCurrentObjectToVersions: failed to ensure versions directory for %s/%s: %v", bucket, object, err)
		return err
	}

	// Check if this version already exists in the .versions directory
	// This handles race conditions where multiple goroutines try to move the same object
	versionFileName := s3a.getVersionFileName(versionId)
	_, existsErr := s3a.getEntry(versionsDir, versionFileName)
	if existsErr == nil {
		// Version already exists in .versions directory, nothing to do
		// This is a race condition - another goroutine already moved this object
		glog.V(2).Infof("moveCurrentObjectToVersions: version %s already exists in .versions directory for %s/%s, race condition detected", versionId, bucket, object)
		return nil
	}

	glog.V(2).Infof("moveCurrentObjectToVersions: moving version %s to .versions directory for %s/%s", versionId, bucket, object)

	// Create a copy of the entry for the versions directory
	versionEntry := &filer_pb.Entry{
		Name:        versionFileName,
		IsDirectory: currentEntry.IsDirectory,
		Attributes:  currentEntry.Attributes,
		Chunks:      currentEntry.Chunks,
		Extended:    make(map[string][]byte),
	}

	// Copy extended attributes
	for k, v := range currentEntry.Extended {
		versionEntry.Extended[k] = v
	}

	// Update metadata to mark as not latest
	versionEntry.Extended[s3_constants.ExtIsLatestKey] = []byte("false")

	// Preserve ETag if it doesn't exist (for older objects)
	if _, hasETag := versionEntry.Extended[s3_constants.ExtETagKey]; !hasETag {
		fallbackETag := s3a.calculateETagFromChunks(versionEntry.Chunks)
		versionEntry.Extended[s3_constants.ExtETagKey] = []byte(fallbackETag)
	}

	// Store in .versions directory
	err = s3a.mkFile(versionsDir, versionFileName, versionEntry.Chunks, func(entry *filer_pb.Entry) {
		entry.Name = versionEntry.Name
		entry.IsDirectory = versionEntry.IsDirectory
		entry.Attributes = versionEntry.Attributes
		entry.Extended = versionEntry.Extended
	})
	if err != nil {
		// Check if another goroutine already created this version file
		if _, existsErr := s3a.getEntry(versionsDir, versionFileName); existsErr == nil {
			// Another goroutine already created this version, which is fine
			glog.V(2).Infof("moveCurrentObjectToVersions: another goroutine already created version %s for %s/%s", versionId, bucket, object)
			return nil
		}
		glog.Errorf("moveCurrentObjectToVersions: failed to move object to versions for %s/%s: %v", bucket, object, err)
		return fmt.Errorf("failed to move object to versions: %v", err)
	}

	// Successfully moved to versions directory
	glog.V(2).Infof("moveCurrentObjectToVersions: successfully moved version %s to .versions directory for %s/%s", versionId, bucket, object)
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
	if chunks == nil || len(chunks) == 0 {
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

	// First try to get specific version from .versions directory
	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	entry, err := s3a.getEntry(versionsDir, versionFile)
	if err == nil {
		return entry, nil
	}

	// If not found in .versions directory, check if it's the current version
	currentEntry, currentErr := s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(object, "/"))
	if currentErr != nil {
		// Neither found in .versions nor current location
		return nil, err
	}

	// Check if the current entry has the requested version ID
	if currentEntry.Extended != nil {
		if currentVersionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]; hasVersionId {
			if string(currentVersionIdBytes) == versionId {
				return currentEntry, nil
			}
		}
	}

	// Version not found anywhere
	return nil, err
}

// deleteSpecificObjectVersion deletes a specific version of an object
func (s3a *S3ApiServer) deleteSpecificObjectVersion(bucket, object, versionId string) error {
	if versionId == "" {
		return fmt.Errorf("version ID is required for version-specific deletion")
	}

	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	// First try to delete from .versions directory
	_, err := s3a.getEntry(versionsDir, versionFile)
	if err == nil {
		// Version exists in .versions directory, delete it
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

	// If not found in .versions directory, check if it's the current version
	currentPath := path.Join(s3a.option.BucketsPath, bucket)
	currentFile := strings.TrimPrefix(object, "/")

	currentEntry, currentErr := s3a.getEntry(currentPath, currentFile)
	if currentErr != nil {
		// Neither found in .versions nor current location
		return fmt.Errorf("version %s not found", versionId)
	}

	// Check if the current entry has the requested version ID
	if currentEntry.Extended != nil {
		if currentVersionIdBytes, hasVersionId := currentEntry.Extended[s3_constants.ExtVersionIdKey]; hasVersionId {
			if string(currentVersionIdBytes) == versionId {
				// This is the current version, delete it
				deleteErr := s3a.rm(currentPath, currentFile, true, false)
				if deleteErr != nil {
					// Check if file was already deleted by another process
					if _, checkErr := s3a.getEntry(currentPath, currentFile); checkErr != nil {
						// File doesn't exist anymore, deletion was successful
						return nil
					}
					return fmt.Errorf("failed to delete current version %s: %v", versionId, deleteErr)
				}
				return nil
			}
		}
	}

	// Version not found anywhere
	return fmt.Errorf("version %s not found", versionId)
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
			return versionsDir, nil
		}
		return versionsDir, fmt.Errorf("failed to create versions directory %s: %v", versionsDir, err)
	}

	return versionsDir, nil
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
