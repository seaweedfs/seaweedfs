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

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ObjectVersion represents a version of an object
type ObjectVersion struct {
	VersionId      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   time.Time
	ETag           string
	Size           int64
	Entry          *filer_pb.Entry
}

// ListObjectVersionsResult is the response for ListObjectVersions API
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

// generateVersionId creates a unique version ID for an object
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

	// Ensure the .versions directory exists before storing the versioned object
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		return err
	}

	// Store the versioned object
	err = s3a.touch(versionsDir, s3a.getVersionFileName(versionId), entry)
	if err != nil {
		glog.Errorf("Failed to store versioned object %s/%s: %v", versionsDir, versionId, err)
		return err
	}

	// If this is the latest version, also store/update the current version
	if isLatest {
		bucketDir := path.Join(s3a.option.BucketsPath, bucket)
		objectName := strings.TrimPrefix(object, "/")

		err = s3a.touch(bucketDir, objectName, entry)
		if err != nil {
			glog.Errorf("Failed to update current version %s/%s: %v", bucketDir, objectName, err)
			return err
		}
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
		glog.V(3).Infof("Versions directory %s does not exist yet, skipping mark previous versions", versionsDir)
		return nil
	}

	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		// Directory might have been deleted or is empty, which is fine
		glog.V(3).Infof("Could not list versions directory %s: %v", versionsDir, err)
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
				} else {
					glog.V(2).Infof("Marked version %s as not latest for object %s", entry.Name, object)
				}
			}
		}
	}

	return nil
}

// createDeleteMarker creates a delete marker for versioned delete operations
func (s3a *S3ApiServer) createDeleteMarker(bucket, object string) (string, error) {
	versionId := generateVersionId()

	// Create delete marker entry
	deleteMarkerEntry := &filer_pb.Entry{
		Name:        s3a.getVersionFileName(versionId),
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: time.Now().Unix(),
		},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey:    []byte(versionId),
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
			s3_constants.ExtIsLatestKey:     []byte("true"),
		},
	}

	// Mark previous versions as not latest
	err := s3a.markPreviousVersionsAsNotLatest(bucket, object)
	if err != nil {
		glog.Warningf("Failed to mark previous versions as not latest: %v", err)
	}

	// Ensure the .versions directory exists before storing the delete marker
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		return "", err
	}

	// Store delete marker
	err = s3a.touch(versionsDir, deleteMarkerEntry.Name, deleteMarkerEntry)
	if err != nil {
		glog.Errorf("Failed to store delete marker %s/%s: %v", versionsDir, deleteMarkerEntry.Name, err)
		return "", err
	}

	// Remove current object if it exists (logical deletion)
	s3a.rm(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(object, "/"), false, false)

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

		objectKey := "/" + entry.Name
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

	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	entries, _, err := s3a.list(versionsDir, "", "", false, 1000)
	if err != nil {
		// No versions directory exists, return empty list
		return versions, nil
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
			// Get ETag and size from entry
			if entry.Chunks != nil && len(entry.Chunks) > 0 {
				// Calculate ETag from chunks
				version.ETag = fmt.Sprintf("\"%x\"", entry.Attributes.Mtime) // Simplified ETag
			}
			version.Size = int64(entry.Attributes.FileSize)
		}

		versions = append(versions, version)
	}

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

	versionsDir := s3a.getVersionedObjectDir(bucket, object)
	versionFile := s3a.getVersionFileName(versionId)

	// Check if this version exists
	_, err := s3a.getEntry(versionsDir, versionFile)
	if err != nil {
		return err
	}

	// Delete the version
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
	versionsDirPath, versionsDirName := path.Split(versionsDir)

	// Check if directory already exists
	exists, err := s3a.exists(versionsDirPath, versionsDirName, true)
	if err != nil {
		glog.Errorf("Failed to check if versions directory exists %s: %v", versionsDir, err)
		return versionsDir, err
	}

	if exists {
		return versionsDir, nil
	}

	// Create the .versions directory
	err = s3a.mkdir(versionsDirPath, versionsDirName, func(entry *filer_pb.Entry) {
		// Set directory attributes
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileMode = 0755 // Standard directory permissions
	})

	if err != nil {
		// Handle race condition - directory might have been created by another process
		// Check again if it exists now
		exists, checkErr := s3a.exists(versionsDirPath, versionsDirName, true)
		if checkErr == nil && exists {
			glog.V(3).Infof("Versions directory %s was created by another process", versionsDir)
			return versionsDir, nil
		}

		glog.Errorf("Failed to create versions directory %s: %v", versionsDir, err)
		return versionsDir, err
	}

	glog.V(2).Infof("Created versions directory: %s", versionsDir)
	return versionsDir, nil
}
