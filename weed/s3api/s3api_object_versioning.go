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
	glog.Infof("🚀 STORE VERSIONED OBJECT START: bucket=%s, object=%s, versionId=%s, isLatest=%v", bucket, object, versionId, isLatest)

	// Add version metadata to entry
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	entry.Extended[s3_constants.ExtVersionIdKey] = []byte(versionId)
	entry.Extended[s3_constants.ExtIsLatestKey] = []byte(strconv.FormatBool(isLatest))

	// Ensure the .versions directory exists before storing the versioned object
	glog.Infof("🔧 Ensuring versions directory exists for bucket=%s, object=%s", bucket, object)
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		glog.Errorf("❌ ensureVersionedDirectory failed: %v", err)
		return err
	}
	glog.Infof("🔧 ensureVersionedDirectory returned: %s", versionsDir)

	// Double-check that the directory exists before trying to create the versioned object
	versionsDirPath, versionsDirName := path.Split(versionsDir)
	glog.Infof("🔍 VERIFICATION: Checking if versions directory exists before storing object")
	glog.Infof("🔍 Verifying path=%s, name=%s (full path: %s)", versionsDirPath, versionsDirName, versionsDir)
	exists, checkErr := s3a.exists(versionsDirPath, versionsDirName, true)
	glog.Infof("🔍 Verification result: exists=%v, checkErr=%v", exists, checkErr)

	if checkErr != nil || !exists {
		glog.Errorf("❌ VERIFICATION FAILED: Versions directory %s does not exist after creation attempt (exists=%v, err=%v, path=%s, name=%s)", versionsDir, exists, checkErr, versionsDirPath, versionsDirName)

		// List parent directory contents to see what actually exists
		glog.Infof("🔍 LISTING PARENT DIRECTORY: %s", versionsDirPath)
		parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
		// Try to create it one more time
		glog.Infof("🔧 RETRY: Attempting to create versions directory again: bucket=%s, object=%s", bucket, object)
		_, retryErr := s3a.ensureVersionedDirectory(bucket, object)
		if retryErr != nil {
			glog.Errorf("❌ RETRY FAILED: failed to create versions directory %s: %v", versionsDir, retryErr)
			return fmt.Errorf("failed to create versions directory %s: %v", versionsDir, retryErr)
		}
		// Check again
		glog.Infof("🔍 RETRY VERIFICATION: Checking if versions directory exists after retry: path=%s, name=%s", versionsDirPath, versionsDirName)
		exists, checkErr = s3a.exists(versionsDirPath, versionsDirName, true)
		glog.Infof("🔍 Retry verification result: exists=%v, checkErr=%v", exists, checkErr)
		if checkErr != nil || !exists {
			glog.Errorf("❌ RETRY VERIFICATION FAILED: versions directory %s still does not exist after retry (exists=%v, err=%v)", versionsDir, exists, checkErr)

			// List parent directory contents after retry failure
			glog.Infof("🔍 LISTING PARENT DIRECTORY AFTER RETRY: %s", versionsDirPath)
			parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
			if listErr != nil {
				glog.Errorf("❌ Failed to list parent directory after retry %s: %v", versionsDirPath, listErr)
			} else {
				glog.Infof("🔍 Parent directory %s contains %d entries after retry:", versionsDirPath, len(parentEntries))
				for i, entry := range parentEntries {
					entryType := "FILE"
					if entry.IsDirectory {
						entryType = "DIR"
					}
					glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
				}
				if len(parentEntries) == 0 {
					glog.Infof("🔍   (directory is empty)")
				}
			}

			return fmt.Errorf("versions directory %s still does not exist after retry (exists=%v, err=%v)", versionsDir, exists, checkErr)
		}
		glog.Infof("✅ RETRY SUCCESS: Successfully created versions directory on retry: %s", versionsDir)

		// List parent directory to confirm what exists during successful retry
		glog.Infof("🔍 LISTING PARENT DIRECTORY DURING SUCCESSFUL RETRY: %s", versionsDirPath)
		parentEntries, _, listErr = s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory during retry %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries during retry:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
	} else {
		glog.Infof("✅ VERIFICATION SUCCESS: Versions directory %s already exists", versionsDir)

		// List parent directory to confirm what exists during successful verification
		glog.Infof("🔍 LISTING PARENT DIRECTORY DURING SUCCESSFUL VERIFICATION: %s", versionsDirPath)
		parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory during verification %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries during verification:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
	}

	// Store the versioned object (create a copy with the version ID as the name)
	versionEntry := &filer_pb.Entry{
		Name:        s3a.getVersionFileName(versionId),
		IsDirectory: entry.IsDirectory,
		Attributes:  entry.Attributes,
		Chunks:      entry.Chunks,
		Extended:    entry.Extended,
	}

	glog.Infof("💾 STORING: About to store versioned object")
	glog.Infof("💾 Store path: %s", versionsDir)
	glog.Infof("💾 Store name: %s", versionEntry.Name)
	glog.Infof("💾 Full store path: %s/%s", versionsDir, versionEntry.Name)

	err = s3a.touch(versionsDir, versionEntry.Name, versionEntry)
	if err != nil {
		glog.Errorf("❌ STORE FAILED: Failed to store versioned object %s/%s: %v", versionsDir, versionId, err)
		return err
	}

	glog.Infof("✅ STORE SUCCESS: Successfully stored versioned object %s/%s", versionsDir, versionId)

	// If this is the latest version, also store/update the current version
	if isLatest {
		bucketDir := path.Join(s3a.option.BucketsPath, bucket)
		objectName := strings.TrimPrefix(object, "/")

		glog.Infof("💾 Updating current version: %s/%s", bucketDir, objectName)
		err = s3a.touch(bucketDir, objectName, entry)
		if err != nil {
			glog.Errorf("❌ Failed to update current version %s/%s: %v", bucketDir, objectName, err)
			return err
		}
		glog.Infof("✅ Successfully updated current version: %s/%s", bucketDir, objectName)
	}

	glog.Infof("🎉 STORE VERSIONED OBJECT COMPLETE: bucket=%s, object=%s, versionId=%s", bucket, object, versionId)
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
	glog.Infof("🚀 CREATE DELETE MARKER START: bucket=%s, object=%s, versionId=%s", bucket, object, versionId)

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
	glog.Infof("🔧 Marking previous versions as not latest for bucket=%s, object=%s", bucket, object)
	err := s3a.markPreviousVersionsAsNotLatest(bucket, object)
	if err != nil {
		glog.Warningf("⚠️ Failed to mark previous versions as not latest: %v", err)
	}

	// Ensure the .versions directory exists before storing the delete marker
	glog.Infof("🔧 Ensuring versions directory exists for delete marker: bucket=%s, object=%s", bucket, object)
	versionsDir, err := s3a.ensureVersionedDirectory(bucket, object)
	if err != nil {
		glog.Errorf("❌ ensureVersionedDirectory failed for delete marker: %v", err)
		return "", err
	}
	glog.Infof("🔧 ensureVersionedDirectory returned for delete marker: %s", versionsDir)

	// Double-check that the directory exists before trying to create the delete marker
	versionsDirPath, versionsDirName := path.Split(versionsDir)
	glog.Infof("🔍 DELETE MARKER VERIFICATION: Checking if versions directory exists before storing delete marker")
	glog.Infof("🔍 Verifying path=%s, name=%s (full path: %s)", versionsDirPath, versionsDirName, versionsDir)
	exists, checkErr := s3a.exists(versionsDirPath, versionsDirName, true)
	glog.Infof("🔍 Delete marker verification result: exists=%v, checkErr=%v", exists, checkErr)

	if checkErr != nil || !exists {
		glog.Errorf("❌ DELETE MARKER VERIFICATION FAILED: Versions directory %s does not exist after creation attempt (exists=%v, err=%v, path=%s, name=%s)", versionsDir, exists, checkErr, versionsDirPath, versionsDirName)

		// List parent directory contents to see what actually exists for delete marker
		glog.Infof("🔍 LISTING PARENT DIRECTORY FOR DELETE MARKER: %s", versionsDirPath)
		parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory for delete marker %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries for delete marker:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
		// Try to create it one more time
		glog.Infof("🔧 DELETE MARKER RETRY: Attempting to create versions directory again for delete marker: bucket=%s, object=%s", bucket, object)
		_, retryErr := s3a.ensureVersionedDirectory(bucket, object)
		if retryErr != nil {
			glog.Errorf("❌ DELETE MARKER RETRY FAILED: failed to create versions directory %s: %v", versionsDir, retryErr)
			return "", fmt.Errorf("failed to create versions directory %s: %v", versionsDir, retryErr)
		}
		// Check again
		glog.Infof("🔍 DELETE MARKER RETRY VERIFICATION: Checking if versions directory exists after retry: path=%s, name=%s", versionsDirPath, versionsDirName)
		exists, checkErr = s3a.exists(versionsDirPath, versionsDirName, true)
		glog.Infof("🔍 Delete marker retry verification result: exists=%v, checkErr=%v", exists, checkErr)
		if checkErr != nil || !exists {
			glog.Errorf("❌ DELETE MARKER RETRY VERIFICATION FAILED: versions directory %s still does not exist after retry (exists=%v, err=%v)", versionsDir, exists, checkErr)

			// List parent directory contents after delete marker retry failure
			glog.Infof("🔍 LISTING PARENT DIRECTORY AFTER DELETE MARKER RETRY: %s", versionsDirPath)
			parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
			if listErr != nil {
				glog.Errorf("❌ Failed to list parent directory after delete marker retry %s: %v", versionsDirPath, listErr)
			} else {
				glog.Infof("🔍 Parent directory %s contains %d entries after delete marker retry:", versionsDirPath, len(parentEntries))
				for i, entry := range parentEntries {
					entryType := "FILE"
					if entry.IsDirectory {
						entryType = "DIR"
					}
					glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
				}
				if len(parentEntries) == 0 {
					glog.Infof("🔍   (directory is empty)")
				}
			}

			return "", fmt.Errorf("versions directory %s still does not exist after retry (exists=%v, err=%v)", versionsDir, exists, checkErr)
		}
		glog.Infof("✅ DELETE MARKER RETRY SUCCESS: Successfully created versions directory on retry: %s", versionsDir)

		// List parent directory to confirm what exists during successful delete marker retry
		glog.Infof("🔍 LISTING PARENT DIRECTORY DURING SUCCESSFUL DELETE MARKER RETRY: %s", versionsDirPath)
		parentEntries, _, listErr = s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory during delete marker retry %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries during delete marker retry:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
	} else {
		glog.Infof("✅ DELETE MARKER VERIFICATION SUCCESS: Versions directory %s already exists for delete marker", versionsDir)

		// List parent directory to confirm what exists during successful delete marker verification
		glog.Infof("🔍 LISTING PARENT DIRECTORY DURING SUCCESSFUL DELETE MARKER VERIFICATION: %s", versionsDirPath)
		parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory during delete marker verification %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries during delete marker verification:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}
	}

	// Store delete marker
	glog.Infof("💾 DELETE MARKER STORING: About to store delete marker")
	glog.Infof("💾 Delete marker path: %s", versionsDir)
	glog.Infof("💾 Delete marker name: %s", deleteMarkerEntry.Name)
	glog.Infof("💾 Full delete marker path: %s/%s", versionsDir, deleteMarkerEntry.Name)

	err = s3a.touch(versionsDir, deleteMarkerEntry.Name, deleteMarkerEntry)
	if err != nil {
		glog.Errorf("❌ DELETE MARKER STORE FAILED: Failed to store delete marker %s/%s: %v", versionsDir, deleteMarkerEntry.Name, err)
		return "", err
	}

	glog.Infof("✅ DELETE MARKER STORE SUCCESS: Successfully stored delete marker %s/%s", versionsDir, deleteMarkerEntry.Name)

	// Remove current object if it exists (logical deletion)
	glog.Infof("🗑️ Removing current object (logical deletion): bucket=%s, object=%s", bucket, object)
	s3a.rm(path.Join(s3a.option.BucketsPath, bucket), strings.TrimPrefix(object, "/"), false, false)

	glog.Infof("🎉 CREATE DELETE MARKER COMPLETE: bucket=%s, object=%s, versionId=%s", bucket, object, versionId)
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

	glog.Infof("🔧 ensureVersionedDirectory START: bucket=%s, object=%s", bucket, object)
	glog.Infof("🔧 Directory paths: versionsDir=%s, path=%s, name=%s", versionsDir, versionsDirPath, versionsDirName)

	// Check if directory already exists
	glog.Infof("🔧 Checking if versions directory exists: path=%s, name=%s", versionsDirPath, versionsDirName)
	exists, err := s3a.exists(versionsDirPath, versionsDirName, true)
	if err != nil {
		glog.Errorf("❌ Failed to check if versions directory exists %s (path=%s, name=%s): %v",
			versionsDir, versionsDirPath, versionsDirName, err)
		return versionsDir, err
	}

	glog.Infof("🔧 Directory existence check result: exists=%v, err=%v", exists, err)
	if exists {
		glog.Infof("✅ Versions directory already exists: %s", versionsDir)

		// List existing directory contents to see what's already there
		glog.Infof("🔍 LISTING EXISTING VERSIONS DIRECTORY: %s", versionsDir)
		existingEntries, _, listErr := s3a.list(versionsDir, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list existing versions directory %s: %v", versionsDir, listErr)
		} else {
			glog.Infof("🔍 Existing versions directory %s contains %d entries:", versionsDir, len(existingEntries))
			for i, entry := range existingEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(existingEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}

		return versionsDir, nil
	}

	glog.Infof("🔧 Creating versions directory: %s (path=%s, name=%s)",
		versionsDir, versionsDirPath, versionsDirName)

	// Ensure the parent directory exists first
	parentPath, parentName := path.Split(strings.TrimSuffix(versionsDirPath, "/"))
	glog.Infof("🔧 Checking parent directory: parentPath=%s, parentName=%s", parentPath, parentName)
	if parentName != "" {
		glog.Infof("🔧 Verifying parent directory exists: path=%s, name=%s", parentPath, parentName)
		parentExists, parentErr := s3a.exists(parentPath, parentName, true)
		glog.Infof("🔧 Parent directory check result: exists=%v, err=%v", parentExists, parentErr)
		if parentErr != nil {
			glog.Errorf("❌ Failed to check parent directory %s/%s: %v", parentPath, parentName, parentErr)
			return versionsDir, parentErr
		}
		if !parentExists {
			glog.Errorf("❌ Parent directory does not exist: %s/%s", parentPath, parentName)
			return versionsDir, fmt.Errorf("parent directory does not exist: %s/%s", parentPath, parentName)
		}
		glog.Infof("✅ Parent directory exists: %s/%s", parentPath, parentName)
	} else {
		glog.Infof("🔧 No parent directory to check (root level)")
	}

	// Create the .versions directory
	glog.Infof("🔧 Calling mkdir: path=%s, name=%s (full path: %s)", versionsDirPath, versionsDirName, versionsDir)
	err = s3a.mkdir(versionsDirPath, versionsDirName, func(entry *filer_pb.Entry) {
		// Set directory attributes
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.Mtime = time.Now().Unix()
		entry.Attributes.FileMode = 0755 // Standard directory permissions
		glog.Infof("🔧 Setting directory attributes for %s: mtime=%d, mode=%o", versionsDir, entry.Attributes.Mtime, entry.Attributes.FileMode)
	})

	if err != nil {
		glog.Errorf("❌ mkdir failed for %s (path=%s, name=%s): %v", versionsDir, versionsDirPath, versionsDirName, err)

		// Handle race condition - directory might have been created by another process
		// Check again if it exists now
		glog.Infof("🔧 Checking if directory exists after mkdir failure: path=%s, name=%s", versionsDirPath, versionsDirName)
		exists, checkErr := s3a.exists(versionsDirPath, versionsDirName, true)
		glog.Infof("🔧 Post-failure existence check result: exists=%v, checkErr=%v", exists, checkErr)
		if checkErr == nil && exists {
			glog.Infof("✅ Versions directory %s was created by another process", versionsDir)
			return versionsDir, nil
		}

		// List parent directory contents when mkdir fails to see what actually exists
		glog.Infof("🔍 LISTING PARENT DIRECTORY AFTER MKDIR FAILURE: %s", versionsDirPath)
		parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
		if listErr != nil {
			glog.Errorf("❌ Failed to list parent directory after mkdir failure %s: %v", versionsDirPath, listErr)
		} else {
			glog.Infof("🔍 Parent directory %s contains %d entries after mkdir failure:", versionsDirPath, len(parentEntries))
			for i, entry := range parentEntries {
				entryType := "FILE"
				if entry.IsDirectory {
					entryType = "DIR"
				}
				glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
			}
			if len(parentEntries) == 0 {
				glog.Infof("🔍   (directory is empty)")
			}
		}

		glog.Errorf("❌ Failed to create versions directory %s (checkErr=%v, exists=%v): %v", versionsDir, checkErr, exists, err)
		return versionsDir, err
	}

	glog.Infof("✅ mkdir returned success for %s", versionsDir)

	// List parent directory to verify what was created
	glog.Infof("🔍 LISTING PARENT DIRECTORY AFTER SUCCESSFUL MKDIR: %s", versionsDirPath)
	parentEntries, _, listErr := s3a.list(versionsDirPath, "", "", false, 100)
	if listErr != nil {
		glog.Errorf("❌ Failed to list parent directory after mkdir %s: %v", versionsDirPath, listErr)
	} else {
		glog.Infof("🔍 Parent directory %s contains %d entries after mkdir:", versionsDirPath, len(parentEntries))
		for i, entry := range parentEntries {
			entryType := "FILE"
			if entry.IsDirectory {
				entryType = "DIR"
			}
			glog.Infof("🔍   [%d] %s: %s (size: %d, mtime: %d)", i+1, entryType, entry.Name, entry.Attributes.FileSize, entry.Attributes.Mtime)
		}
		if len(parentEntries) == 0 {
			glog.Infof("🔍   (directory is empty)")
		}
	}

	glog.Infof("✅ Successfully created versions directory: %s", versionsDir)
	return versionsDir, nil
}
