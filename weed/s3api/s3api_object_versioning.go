package s3api

// This file contains the core S3 versioning operations.
// Version ID format handling is in s3api_version_id.go

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"errors"
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

// ErrDeleteMarker is returned when the latest version is a delete marker (expected condition)
var ErrDeleteMarker = errors.New("latest version is a delete marker")

// clearCachedVersionMetadata clears only the version metadata fields (not ID/filename).
// Used by setCachedListMetadata to prevent stale values when updating.
func clearCachedVersionMetadata(extended map[string][]byte) {
	delete(extended, s3_constants.ExtLatestVersionSizeKey)
	delete(extended, s3_constants.ExtLatestVersionMtimeKey)
	delete(extended, s3_constants.ExtLatestVersionETagKey)
	delete(extended, s3_constants.ExtLatestVersionOwnerKey)
	delete(extended, s3_constants.ExtLatestVersionIsDeleteMarker)
}

// setCachedListMetadata caches list metadata in the .versions directory entry for single-scan efficiency
func setCachedListMetadata(versionsEntry, versionEntry *filer_pb.Entry) {
	if versionEntry == nil || versionsEntry == nil {
		return
	}
	if versionsEntry.Extended == nil {
		versionsEntry.Extended = make(map[string][]byte)
	}

	// Clear old cached metadata to prevent stale values
	// Note: We don't use clearCachedListMetadata here because it also clears
	// ExtLatestVersionIdKey and ExtLatestVersionFileNameKey, which are set by the caller
	clearCachedVersionMetadata(versionsEntry.Extended)

	// Size and Mtime
	if versionEntry.Attributes != nil {
		versionsEntry.Extended[s3_constants.ExtLatestVersionSizeKey] = []byte(strconv.FormatUint(versionEntry.Attributes.FileSize, 10))
		versionsEntry.Extended[s3_constants.ExtLatestVersionMtimeKey] = []byte(strconv.FormatInt(versionEntry.Attributes.Mtime, 10))
	}

	// ETag, Owner, DeleteMarker from Extended
	if versionEntry.Extended != nil {
		if etag, ok := versionEntry.Extended[s3_constants.ExtETagKey]; ok {
			versionsEntry.Extended[s3_constants.ExtLatestVersionETagKey] = etag
		}
		if owner, ok := versionEntry.Extended[s3_constants.ExtAmzOwnerKey]; ok {
			versionsEntry.Extended[s3_constants.ExtLatestVersionOwnerKey] = owner
		}
		if deleteMarker, ok := versionEntry.Extended[s3_constants.ExtDeleteMarkerKey]; ok {
			versionsEntry.Extended[s3_constants.ExtLatestVersionIsDeleteMarker] = deleteMarker
		} else {
			versionsEntry.Extended[s3_constants.ExtLatestVersionIsDeleteMarker] = []byte("false")
		}
	}
}

// clearCachedListMetadata removes all cached list metadata from the .versions directory entry
func clearCachedListMetadata(extended map[string][]byte) {
	if extended == nil {
		return
	}
	delete(extended, s3_constants.ExtLatestVersionIdKey)
	delete(extended, s3_constants.ExtLatestVersionFileNameKey)
	clearCachedVersionMetadata(extended)
}

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
// Note: We intentionally do not store the full filer_pb.Entry here to avoid
// retaining large Chunks arrays in memory during list operations.
type ObjectVersion struct {
	VersionId      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   time.Time
	ETag           string
	Size           int64
	OwnerID        string // Owner ID extracted from entry metadata
	StorageClass   string
}

// createDeleteMarker creates a delete marker for versioned delete operations
func (s3a *S3ApiServer) createDeleteMarker(bucket, object string) (string, error) {
	// Clean up the object path first
	cleanObject := strings.TrimPrefix(object, "/")

	// Check if .versions directory exists to determine format
	useInvertedFormat := s3a.getVersionIdFormat(bucket, cleanObject)
	versionId := generateVersionId(useInvertedFormat)

	glog.V(2).Infof("createDeleteMarker: creating delete marker %s for %s/%s (inverted=%v)", versionId, bucket, object, useInvertedFormat)

	// Create the version file name for the delete marker
	versionFileName := s3a.getVersionFileName(versionId)

	// Store delete marker in the .versions directory
	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsDir := bucketDir + "/" + cleanObject + s3_constants.VersionsFolder

	// Create the delete marker entry in the .versions directory
	deleteMarkerMtime := time.Now().Unix()
	deleteMarkerExtended := map[string][]byte{
		s3_constants.ExtVersionIdKey:    []byte(versionId),
		s3_constants.ExtDeleteMarkerKey: []byte("true"),
	}

	err := s3a.mkFile(versionsDir, versionFileName, nil, func(entry *filer_pb.Entry) {
		entry.IsDirectory = false
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.Mtime = deleteMarkerMtime
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		for k, v := range deleteMarkerExtended {
			entry.Extended[k] = v
		}
	})
	if err != nil {
		return "", fmt.Errorf("failed to create delete marker in .versions directory: %w", err)
	}

	// Update the .versions directory metadata to indicate this delete marker is the latest version
	// Pass deleteMarkerEntry to cache its metadata for single-scan list efficiency
	deleteMarkerEntry := &filer_pb.Entry{
		Name:        versionFileName,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime: deleteMarkerMtime,
		},
		Extended: deleteMarkerExtended,
	}
	err = s3a.updateLatestVersionInDirectory(bucket, cleanObject, versionId, versionFileName, deleteMarkerEntry)
	if err != nil {
		glog.Errorf("createDeleteMarker: failed to update latest version in directory: %v", err)
		return "", fmt.Errorf("failed to update latest version in directory: %w", err)
	}

	glog.V(2).Infof("createDeleteMarker: successfully created delete marker %s for %s/%s", versionId, bucket, object)
	return versionId, nil
}

// versionListItem represents an item in the unified version/prefix list
type versionListItem struct {
	key         string
	versionId   string
	isPrefix    bool
	versionData interface{} // *VersionEntry or *DeleteMarkerEntry
}

// listObjectVersions lists all versions of an object
func (s3a *S3ApiServer) listObjectVersions(bucket, prefix, keyMarker, versionIdMarker, delimiter string, maxKeys int) (*S3ListObjectVersionsResult, error) {
	// S3 API limits max-keys to 1000
	if maxKeys > 1000 {
		maxKeys = 1000
	}
	// Pre-allocate with capacity for maxKeys+1 to reduce reallocations
	// The extra 1 is for truncation detection
	allVersions := make([]interface{}, 0, maxKeys+1)

	glog.V(1).Infof("listObjectVersions: listing versions for bucket %s, prefix '%s', keyMarker '%s', versionIdMarker '%s'", bucket, prefix, keyMarker, versionIdMarker)

	// Track objects that have been processed to avoid duplicates
	processedObjects := make(map[string]bool)

	// Track version IDs globally to prevent duplicates throughout the listing
	seenVersionIds := make(map[string]bool)

	// Map to track common prefixes (deduplicated)
	commonPrefixes := make(map[string]bool)

	// Recursively find all .versions directories in the bucket
	// Pass keyMarker and versionIdMarker to enable efficient pagination (skip entries before marker)
	bucketPath := path.Join(s3a.option.BucketsPath, bucket)

	// Memory optimization: limit collection to maxKeys+1 versions.
	// This works correctly for objects using the NEW inverted-timestamp format, where
	// filesystem order (lexicographic) matches sorted order (newest-first).
	// For OLD format objects (raw timestamps), filesystem order is oldest-first, so
	// limiting collection may return older versions instead of newest. However:
	// - New objects going forward use the new format
	// - The alternative (collecting all) causes memory issues for buckets with many versions
	// - Pagination continues correctly; users can page through to see all versions
	maxCollect := maxKeys + 1 // +1 to detect truncation
	err := s3a.findVersionsRecursively(bucketPath, "", &allVersions, processedObjects, seenVersionIds, bucket, prefix, keyMarker, versionIdMarker, delimiter, commonPrefixes, maxCollect)
	if err != nil {
		glog.Errorf("listObjectVersions: findVersionsRecursively failed: %v", err)
		return nil, err
	}

	clear(processedObjects)
	clear(seenVersionIds)

	// Combine versions and prefixes into a single sorted list
	combinedList := s3a.buildSortedCombinedList(allVersions, commonPrefixes)
	glog.V(1).Infof("listObjectVersions: collected %d combined items (versions+prefixes)", len(combinedList))

	// Apply MaxKeys truncation and determine pagination markers
	truncatedList, nextKeyMarker, nextVersionIdMarker, isTruncated := s3a.truncateAndSetMarkers(combinedList, maxKeys)
	glog.V(1).Infof("listObjectVersions: after truncation - %d items (truncated: %v)", len(truncatedList), isTruncated)

	// Build the final response by splitting items back into their respective fields
	result := s3a.splitIntoResult(truncatedList, bucket, prefix, keyMarker, versionIdMarker, delimiter, maxKeys, isTruncated, nextKeyMarker, nextVersionIdMarker)
	glog.V(1).Infof("listObjectVersions: final result - %d versions, %d delete markers, %d common prefixes", len(result.Versions), len(result.DeleteMarkers), len(result.CommonPrefixes))

	return result, nil
}

// buildSortedCombinedList merges versions and common prefixes into a single list
// sorted lexicographically by key, with versions preceding prefixes for the same key.
func (s3a *S3ApiServer) buildSortedCombinedList(allVersions []interface{}, commonPrefixes map[string]bool) []versionListItem {
	combinedList := make([]versionListItem, 0, len(allVersions)+len(commonPrefixes))

	// Add versions
	for _, version := range allVersions {
		var key, versionId string
		switch v := version.(type) {
		case *VersionEntry:
			key = v.Key
			versionId = v.VersionId
		case *DeleteMarkerEntry:
			key = v.Key
			versionId = v.VersionId
		}
		combinedList = append(combinedList, versionListItem{
			key:         key,
			versionId:   versionId,
			isPrefix:    false,
			versionData: version,
		})
	}

	// Add common prefixes
	for prefix := range commonPrefixes {
		combinedList = append(combinedList, versionListItem{
			key:      prefix,
			isPrefix: true,
		})
	}

	// Single sort for the entire combined list
	sort.Slice(combinedList, func(i, j int) bool {
		if combinedList[i].key != combinedList[j].key {
			return combinedList[i].key < combinedList[j].key
		}
		// For same key, versions come before prefixes
		if combinedList[i].isPrefix != combinedList[j].isPrefix {
			return !combinedList[i].isPrefix
		}
		// For same key with both being versions, sort by version ID (newest first)
		return compareVersionIds(combinedList[i].versionId, combinedList[j].versionId) < 0
	})

	return combinedList
}

// truncateAndSetMarkers applies MaxKeys limit and determines pagination markers
func (s3a *S3ApiServer) truncateAndSetMarkers(combinedList []versionListItem, maxKeys int) (truncated []versionListItem, nextKeyMarker, nextVersionIdMarker string, isTruncated bool) {
	isTruncated = len(combinedList) > maxKeys
	if isTruncated && maxKeys > 0 {
		// Set markers from the last item we'll return
		lastItem := combinedList[maxKeys-1]
		nextKeyMarker = lastItem.key
		if !lastItem.isPrefix {
			nextVersionIdMarker = lastItem.versionId
		}
		// Truncate the list
		combinedList = combinedList[:maxKeys]
	}
	return combinedList, nextKeyMarker, nextVersionIdMarker, isTruncated
}

// splitIntoResult builds the final S3ListObjectVersionsResult from the combined list
func (s3a *S3ApiServer) splitIntoResult(combinedList []versionListItem, bucket, prefix, keyMarker, versionIdMarker, delimiter string, maxKeys int, isTruncated bool, nextKeyMarker, nextVersionIdMarker string) *S3ListObjectVersionsResult {
	result := &S3ListObjectVersionsResult{
		Name:                bucket,
		Prefix:              prefix,
		KeyMarker:           keyMarker,
		VersionIdMarker:     versionIdMarker,
		MaxKeys:             maxKeys,
		Delimiter:           delimiter,
		IsTruncated:         isTruncated,
		NextKeyMarker:       nextKeyMarker,
		NextVersionIdMarker: nextVersionIdMarker,
		Versions:            make([]VersionEntry, 0),
		DeleteMarkers:       make([]DeleteMarkerEntry, 0),
		CommonPrefixes:      make([]PrefixEntry, 0),
	}

	for _, item := range combinedList {
		if item.isPrefix {
			result.CommonPrefixes = append(result.CommonPrefixes, PrefixEntry{Prefix: item.key})
		} else {
			switch v := item.versionData.(type) {
			case *VersionEntry:
				result.Versions = append(result.Versions, *v)
			case *DeleteMarkerEntry:
				result.DeleteMarkers = append(result.DeleteMarkers, *v)
			}
		}
	}
	return result
}

// versionCollector holds state for collecting object versions during recursive traversal
type versionCollector struct {
	s3a              *S3ApiServer
	bucket           string
	prefix           string
	keyMarker        string
	versionIdMarker  string
	maxCollect       int
	allVersions      *[]interface{}
	processedObjects map[string]bool
	seenVersionIds   map[string]bool
	delimiter        string
	commonPrefixes   map[string]bool
}

// isFull returns true if we've collected enough versions
func (vc *versionCollector) isFull() bool {
	if vc.maxCollect <= 0 {
		return false
	}
	currentCount := len(*vc.allVersions)
	if vc.commonPrefixes != nil {
		currentCount += len(vc.commonPrefixes)
	}
	return currentCount >= vc.maxCollect
}

// matchesPrefixFilter checks if an entry path matches the prefix filter
func (vc *versionCollector) matchesPrefixFilter(entryPath string, isDirectory bool) bool {
	if vc.prefix == "" {
		return true
	}

	// Entry matches if its path starts with the prefix
	isMatch := strings.HasPrefix(entryPath, vc.prefix)
	if !isMatch && isDirectory {
		// Directory might match with trailing slash
		isMatch = strings.HasPrefix(entryPath+"/", vc.prefix)
	}

	// For directories, also check if we need to descend (prefix is deeper)
	canDescend := isDirectory && strings.HasPrefix(vc.prefix, entryPath)

	return isMatch || canDescend
}

// shouldSkipObjectForMarker returns true if the object should be skipped based on keyMarker
func (vc *versionCollector) shouldSkipObjectForMarker(objectKey string) bool {
	if vc.keyMarker == "" {
		return false
	}
	return objectKey < vc.keyMarker
}

// shouldSkipVersionForMarker returns true if a version should be skipped based on markers
// For the keyMarker object, skip versions that are newer than or equal to versionIdMarker
// (these were already returned in previous pages).
// Handles both old (raw timestamp) and new (inverted timestamp) version ID formats.
func (vc *versionCollector) shouldSkipVersionForMarker(objectKey, versionId string) bool {
	if vc.keyMarker == "" || objectKey != vc.keyMarker {
		return false
	}
	// Object matches keyMarker - apply version filtering
	if vc.versionIdMarker == "" {
		// When a keyMarker is provided without a versionIdMarker, S3 pagination
		// starts after the keyMarker object. Returning true here ensures that
		// all versions of the keyMarker object are skipped.
		return true
	}
	// Skip versions that are newer than or equal to versionIdMarker
	// compareVersionIds returns negative if versionId is newer than marker
	// We skip if versionId is newer (negative) or equal (zero) to the marker
	cmp := compareVersionIds(versionId, vc.versionIdMarker)
	return cmp <= 0
}

// addVersion adds a version or delete marker to results
func (vc *versionCollector) addVersion(version *ObjectVersion, objectKey string) {
	if version.IsDeleteMarker {
		deleteMarker := &DeleteMarkerEntry{
			Key:          objectKey,
			VersionId:    version.VersionId,
			IsLatest:     version.IsLatest,
			LastModified: version.LastModified,
			Owner:        vc.s3a.getObjectOwnerFromVersion(version, vc.bucket, objectKey),
		}
		*vc.allVersions = append(*vc.allVersions, deleteMarker)
	} else {
		versionEntry := &VersionEntry{
			Key:          objectKey,
			VersionId:    version.VersionId,
			IsLatest:     version.IsLatest,
			LastModified: version.LastModified,
			ETag:         version.ETag,
			Size:         version.Size,
			Owner:        vc.s3a.getObjectOwnerFromVersion(version, vc.bucket, objectKey),
			StorageClass: StorageClass(vc.s3a.getStorageClassFromExtended(entryExtended(version))),
		}
		*vc.allVersions = append(*vc.allVersions, versionEntry)
	}
}

// processVersionsDirectory handles a .versions directory entry
func (vc *versionCollector) processVersionsDirectory(entryPath string) error {
	objectKey := strings.TrimSuffix(entryPath, s3_constants.VersionsFolder)
	normalizedObjectKey := s3_constants.NormalizeObjectKey(objectKey)

	// Mark as processed
	vc.processedObjects[objectKey] = true
	vc.processedObjects[normalizedObjectKey] = true

	// Skip objects before keyMarker
	if vc.shouldSkipObjectForMarker(normalizedObjectKey) {
		glog.V(4).Infof("processVersionsDirectory: skipping object %s (before keyMarker %s)", normalizedObjectKey, vc.keyMarker)
		return nil
	}

	glog.V(2).Infof("processVersionsDirectory: found object %s", normalizedObjectKey)

	versions, err := vc.s3a.getObjectVersionList(vc.bucket, normalizedObjectKey)
	if err != nil {
		glog.Warningf("processVersionsDirectory: failed to get versions for %s: %v", normalizedObjectKey, err)
		return nil // Continue with other entries
	}

	for _, version := range versions {
		if vc.isFull() {
			return nil
		}

		versionKey := normalizedObjectKey + ":" + version.VersionId
		if vc.seenVersionIds[versionKey] {
			continue
		}

		// Skip versions that were already returned in previous pages
		if vc.shouldSkipVersionForMarker(normalizedObjectKey, version.VersionId) {
			continue
		}

		vc.seenVersionIds[versionKey] = true
		vc.addVersion(version, normalizedObjectKey)
	}

	return nil
}

// processExplicitDirectory handles an explicit S3 directory object
func (vc *versionCollector) processExplicitDirectory(entryPath string, entry *filer_pb.Entry) {
	directoryKey := entryPath
	if !strings.HasSuffix(directoryKey, "/") {
		directoryKey += "/"
	}

	// Skip directories at or before keyMarker
	if vc.keyMarker != "" && directoryKey <= vc.keyMarker {
		return
	}

	versionEntry := &VersionEntry{
		Key:          directoryKey,
		VersionId:    "null",
		IsLatest:     true,
		LastModified: time.Unix(entry.Attributes.Mtime, 0),
		ETag:         "\"d41d8cd98f00b204e9800998ecf8427e\"", // Empty content ETag
		Size:         0,
		Owner:        vc.s3a.getObjectOwnerFromEntry(entry),
		StorageClass: StorageClass(vc.s3a.getStorageClassFromExtended(entry.Extended)),
	}
	*vc.allVersions = append(*vc.allVersions, versionEntry)
}

// processRegularFile handles a regular file entry (pre-versioning or suspended-versioning object)
func (vc *versionCollector) processRegularFile(currentPath, entryPath string, entry *filer_pb.Entry) {
	objectKey := entryPath
	normalizedObjectKey := s3_constants.NormalizeObjectKey(objectKey)

	// Skip files before keyMarker
	if vc.shouldSkipObjectForMarker(normalizedObjectKey) {
		return
	}

	// For keyMarker match, skip if this null version was already returned
	if vc.shouldSkipVersionForMarker(normalizedObjectKey, "null") {
		return
	}

	// Skip if already processed via .versions directory
	if vc.processedObjects[objectKey] || vc.processedObjects[normalizedObjectKey] {
		return
	}

	// Check if this file has version metadata
	hasVersionMeta := entry.Extended != nil && entry.Extended[s3_constants.ExtVersionIdKey] != nil

	// Check if a .versions directory exists for this object
	versionsEntryName := entry.Name + s3_constants.VersionsFolder
	_, versionsErr := vc.s3a.getEntry(currentPath, versionsEntryName)
	if versionsErr == nil && !hasVersionMeta {
		// .versions exists but file has no version metadata - check for null version in .versions
		versions, err := vc.s3a.getObjectVersionList(vc.bucket, normalizedObjectKey)
		if err == nil {
			for _, v := range versions {
				if v.VersionId == "null" {
					// Null version exists in .versions, skip this file
					vc.processedObjects[objectKey] = true
					vc.processedObjects[normalizedObjectKey] = true
					return
				}
			}
		}
	}

	// Check for duplicate
	versionKey := normalizedObjectKey + ":null"
	if vc.seenVersionIds[versionKey] {
		return
	}
	vc.seenVersionIds[versionKey] = true

	versionEntry := &VersionEntry{
		Key:          normalizedObjectKey,
		VersionId:    "null",
		IsLatest:     true,
		LastModified: time.Unix(entry.Attributes.Mtime, 0),
		ETag:         vc.s3a.calculateETagFromChunks(entry.Chunks),
		Size:         int64(entry.Attributes.FileSize),
		Owner:        vc.s3a.getObjectOwnerFromEntry(entry),
		StorageClass: StorageClass(vc.s3a.getStorageClassFromExtended(entry.Extended)),
	}
	*vc.allVersions = append(*vc.allVersions, versionEntry)
}

// findVersionsRecursively searches for .versions directories and regular files recursively
// with efficient pagination support. It skips objects before keyMarker and applies versionIdMarker filtering.
// maxCollect limits the number of versions to collect for memory efficiency (must be > 0)
// delimiter and commonPrefixes are used to group keys that share a common prefix
func (s3a *S3ApiServer) findVersionsRecursively(currentPath, relativePath string, allVersions *[]interface{}, processedObjects map[string]bool, seenVersionIds map[string]bool, bucket, prefix, keyMarker, versionIdMarker, delimiter string, commonPrefixes map[string]bool, maxCollect int) error {
	vc := &versionCollector{
		s3a:              s3a,
		bucket:           bucket,
		prefix:           prefix,
		keyMarker:        keyMarker,
		versionIdMarker:  versionIdMarker,
		maxCollect:       maxCollect,
		allVersions:      allVersions,
		processedObjects: processedObjects,
		seenVersionIds:   seenVersionIds,
		delimiter:        delimiter,
		commonPrefixes:   commonPrefixes,
	}

	return vc.collectVersions(currentPath, relativePath)
}

// collectVersions recursively collects versions from the given path
func (vc *versionCollector) collectVersions(currentPath, relativePath string) error {
	startFrom := ""
	for {
		if vc.isFull() {
			return nil
		}

		entries, isLast, err := vc.s3a.list(currentPath, "", startFrom, false, filer.PaginationSize)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if vc.isFull() {
				return nil
			}
			startFrom = entry.Name
			entryPath := path.Join(relativePath, entry.Name)

			if !vc.matchesPrefixFilter(entryPath, entry.IsDirectory) {
				continue
			}

			// Group into common prefixes if delimiter is found after the prefix
			if vc.delimiter != "" {
				fullKey := entryPath
				if entry.IsDirectory {
					fullKey += "/"
				}
				if strings.HasPrefix(fullKey, vc.prefix) {
					remainder := fullKey[len(vc.prefix):]
					if idx := strings.Index(remainder, vc.delimiter); idx >= 0 {
						commonPrefix := vc.prefix + remainder[:idx+len(vc.delimiter)]

						// Add to CommonPrefixes set if it hasn't been returned yet
						if !vc.commonPrefixes[commonPrefix] {
							// Filter by keyMarker to ensure proper pagination behavior
							if vc.keyMarker != "" && commonPrefix <= vc.keyMarker {
								continue
							}
							if vc.isFull() {
								return nil
							}
							vc.commonPrefixes[commonPrefix] = true
						}

						// Skip further processing (recursion or addition) for this entry
						// because it has been rolled up into the CommonPrefix
						continue
					}
				}
			}

			if entry.IsDirectory {
				if err := vc.processDirectory(currentPath, entryPath, entry); err != nil {
					return err
				}
			} else {
				vc.processRegularFile(currentPath, entryPath, entry)
			}
		}

		if isLast {
			break
		}
	}
	return nil
}

// processDirectory handles directory entries
func (vc *versionCollector) processDirectory(currentPath, entryPath string, entry *filer_pb.Entry) error {
	// Skip .uploads directory
	if strings.HasPrefix(entry.Name, ".uploads") {
		return nil
	}

	// Handle .versions directory
	if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
		return vc.processVersionsDirectory(entryPath)
	}

	// Handle explicit S3 directory object
	if entry.Attributes.Mime == s3_constants.FolderMimeType {
		vc.processExplicitDirectory(entryPath, entry)
	}

	// Recursively search subdirectory
	fullPath := path.Join(currentPath, entry.Name)
	if err := vc.collectVersions(fullPath, entryPath); err != nil {
		glog.Warningf("Error searching subdirectory %s: %v", entryPath, err)
	}

	return nil
}

// getObjectVersionList returns all versions of a specific object
// Uses pagination to handle objects with more than 1000 versions
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

	// Use a map to detect and prevent duplicate version IDs
	seenVersionIds := make(map[string]bool)
	versionsDir := bucketDir + "/" + versionsObjectPath

	// Paginate through all version files in the .versions directory
	startFrom := ""
	const pageSize = 1000
	totalEntries := 0

	for {
		entries, isLast, err := s3a.list(versionsDir, "", startFrom, false, pageSize)
		if err != nil {
			glog.Warningf("getObjectVersionList: failed to list version files in %s: %v", versionsDir, err)
			return nil, err
		}

		totalEntries += len(entries)

		for i, entry := range entries {
			// Track last entry for pagination
			startFrom = entry.Name

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

			// Extract owner ID from entry metadata to avoid retaining full Entry with Chunks
			var ownerID string
			if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
				ownerID = string(ownerBytes)
			}

			version := &ObjectVersion{
				VersionId:      versionId,
				IsLatest:       isLatest,
				IsDeleteMarker: isDeleteMarker,
				LastModified:   time.Unix(entry.Attributes.Mtime, 0),
				OwnerID:        ownerID,
				StorageClass:   s3a.getStorageClassFromExtended(entry.Extended),
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

		// Stop if we've reached the last page
		if isLast || len(entries) < pageSize {
			break
		}
	}

	// Clear map to help GC
	clear(seenVersionIds)

	// Don't sort here - let the main listObjectVersions function handle sorting consistently

	glog.V(2).Infof("getObjectVersionList: returning %d total versions for %s/%s (after deduplication from %d entries)", len(versions), bucket, object, totalEntries)
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
	normalizedObject := s3_constants.NormalizeObjectKey(object)

	if versionId == "" {
		// Get current version
		return s3a.getEntry(path.Join(s3a.option.BucketsPath, bucket), normalizedObject)
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
	normalizedObject := s3_constants.NormalizeObjectKey(object)

	if versionId == "" {
		return fmt.Errorf("version ID is required for version-specific deletion")
	}

	if versionId == "null" {
		// Delete "null" version (pre-versioning object stored as regular file)
		bucketDir := s3a.option.BucketsPath + "/" + bucket

		// Check if the object exists
		_, err := s3a.getEntry(bucketDir, normalizedObject)
		if err != nil {
			// Object doesn't exist - this is OK for delete operations (idempotent)
			glog.V(2).Infof("deleteSpecificObjectVersion: null version object %s already deleted or doesn't exist", normalizedObject)
			return nil
		}

		// Delete the regular file
		deleteErr := s3a.rm(bucketDir, normalizedObject, true, false)
		if deleteErr != nil {
			// Check if file was already deleted by another process
			if _, checkErr := s3a.getEntry(bucketDir, normalizedObject); checkErr != nil {
				// File doesn't exist anymore, deletion was successful
				return nil
			}
			return fmt.Errorf("failed to delete null version %s: %v", normalizedObject, deleteErr)
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
			glog.V(2).Infof("deleteSpecificObjectVersion: version %s for %s/%s already deleted by another process", versionId, bucket, object)
			return nil
		}
		// File still exists but deletion failed for another reason
		return fmt.Errorf("failed to delete version %s: %v", versionId, deleteErr)
	}

	// If we deleted the latest version, update the .versions directory metadata to point to the new latest
	if isLatestVersion {
		err := s3a.updateLatestVersionAfterDeletion(bucket, normalizedObject)
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
	versionsObjectPath := object + s3_constants.VersionsFolder
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
	var latestVersionEntry *filer_pb.Entry

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

		// Compare version IDs chronologically using unified comparator (handles both old and new formats)
		// compareVersionIds returns negative if first arg is newer
		if latestVersionId == "" || compareVersionIds(versionId, latestVersionId) < 0 {
			glog.V(1).Infof("updateLatestVersionAfterDeletion: found newer version %s (file: %s)", versionId, entry.Name)
			latestVersionId = versionId
			latestVersionFileName = entry.Name
			latestVersionEntry = entry
		} else {
			glog.V(1).Infof("updateLatestVersionAfterDeletion: skipping older or equal version %s", versionId)
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

		// Update cached list metadata from the new latest version entry
		setCachedListMetadata(versionsEntry, latestVersionEntry)

		glog.V(2).Infof("updateLatestVersionAfterDeletion: new latest version for %s/%s is %s", bucket, object, latestVersionId)
		// Update the .versions directory entry with new latest version metadata
		err = s3a.mkFile(bucketDir, versionsObjectPath, versionsEntry.Chunks, func(updatedEntry *filer_pb.Entry) {
			updatedEntry.Extended = versionsEntry.Extended
			updatedEntry.Attributes = versionsEntry.Attributes
			updatedEntry.Chunks = versionsEntry.Chunks
		})
		if err != nil {
			return fmt.Errorf("failed to update .versions directory metadata: %v", err)
		}
	} else {
		// No versions left - delete the .versions metadata file entirely
		// This prevents clients from seeing an empty .versions file
		glog.V(2).Infof("updateLatestVersionAfterDeletion: no versions left for %s/%s, deleting .versions metadata file", bucket, object)

		err = s3a.rm(bucketDir, versionsObjectPath, true, false)
		if err != nil {
			glog.Warningf("updateLatestVersionAfterDeletion: failed to delete .versions metadata file for %s/%s: %v", bucket, object, err)
			// Don't return error - the versions are already deleted, this is just cleanup
		}
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
	prefix := strings.TrimPrefix(originalPrefix, "/")
	// Note: prefix is used for filtering relative to bucket root, so no leading slash needed

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
	return s3a.doGetLatestObjectVersion(bucket, object, 8)
}

func (s3a *S3ApiServer) doGetLatestObjectVersion(bucket, object string, maxRetries int) (*filer_pb.Entry, error) {
	// Normalize object path to ensure consistency with toFilerPath behavior
	normalizedObject := s3_constants.NormalizeObjectKey(object)

	bucketDir := s3a.option.BucketsPath + "/" + bucket
	versionsObjectPath := normalizedObject + s3_constants.VersionsFolder

	glog.V(1).Infof("doGetLatestObjectVersion: looking for latest version of %s/%s (normalized: %s, retries: %d)", bucket, object, normalizedObject, maxRetries)

	// Get the .versions directory entry to read latest version metadata with retry logic for filer consistency
	var versionsEntry *filer_pb.Entry
	var err error
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
		glog.V(1).Infof("getLatestObjectVersion: no .versions directory for %s/%s after %d attempts (error: %v), checking for pre-versioning object", bucket, normalizedObject, maxRetries, err)

		regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
		if regularErr != nil {
			glog.V(1).Infof("getLatestObjectVersion: no pre-versioning object found for %s/%s (error: %v)", bucket, normalizedObject, regularErr)
			return nil, fmt.Errorf("failed to get %s/%s .versions directory and no regular object found: %w", bucket, normalizedObject, err)
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
			glog.V(2).Infof("getLatestObjectVersion: no Extended metadata in .versions directory for %s/%s after retries, checking for pre-versioning object", bucket, object)

			regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
			if regularErr != nil {
				return nil, fmt.Errorf("no version metadata in .versions directory and no regular object found for %s/%s", bucket, normalizedObject)
			}

			glog.V(2).Infof("getLatestObjectVersion: found pre-versioning object for %s/%s (no Extended metadata case)", bucket, object)
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
			return nil, fmt.Errorf("no version metadata in .versions directory and no regular object found for %s/%s", bucket, normalizedObject)
		}

		glog.V(2).Infof("getLatestObjectVersion: found pre-versioning object for %s/%s after version deletion", bucket, object)
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

// getLatestVersionEntryFromDirectoryEntry creates a logical entry for list operations using cached metadata
// from the .versions directory entry. This achieves SINGLE-SCAN efficiency - no additional getEntry calls needed.
//
// For N versioned objects:
// - Before: N×1 to N×12 find operations per list
// - After: 0 extra find operations (all metadata cached in .versions directory)
//
// Returns ErrDeleteMarker if the latest version is a delete marker (expected condition, not an error).
func (s3a *S3ApiServer) getLatestVersionEntryFromDirectoryEntry(bucket, object string, versionsDirEntry *filer_pb.Entry) (*filer_pb.Entry, error) {
	// Defensive nil check
	if versionsDirEntry == nil {
		return nil, fmt.Errorf("nil .versions directory entry")
	}

	normalizedObject := s3_constants.NormalizeObjectKey(object)

	// Check if the directory entry has latest version metadata
	if versionsDirEntry.Extended == nil {
		return nil, fmt.Errorf("no Extended metadata in .versions directory entry")
	}

	latestVersionIdBytes, hasLatestVersionId := versionsDirEntry.Extended[s3_constants.ExtLatestVersionIdKey]
	if !hasLatestVersionId {
		return nil, fmt.Errorf("missing latest version ID metadata in .versions directory entry")
	}

	// Check if this is a delete marker (should not be shown in regular list)
	if isDeleteMarker, exists := versionsDirEntry.Extended[s3_constants.ExtLatestVersionIsDeleteMarker]; exists && string(isDeleteMarker) == "true" {
		return nil, ErrDeleteMarker
	}

	latestVersionId := string(latestVersionIdBytes)

	// Try to use cached metadata for zero-copy list (single-scan efficiency)
	sizeBytes, hasSize := versionsDirEntry.Extended[s3_constants.ExtLatestVersionSizeKey]
	mtimeBytes, hasMtime := versionsDirEntry.Extended[s3_constants.ExtLatestVersionMtimeKey]
	etagBytes, hasEtag := versionsDirEntry.Extended[s3_constants.ExtLatestVersionETagKey]

	if hasSize && hasMtime && hasEtag {
		size, sizeErr := strconv.ParseUint(string(sizeBytes), 10, 64)
		mtime, mtimeErr := strconv.ParseInt(string(mtimeBytes), 10, 64)
		if sizeErr == nil && mtimeErr == nil {
			// Use cached metadata - no getEntry call needed!
			glog.V(3).Infof("getLatestVersionEntryFromDirectoryEntry: using cached metadata for %s/%s (size=%d, mtime=%d)", bucket, normalizedObject, size, mtime)

			logicalEntry := &filer_pb.Entry{
				Name:        path.Base(normalizedObject),
				IsDirectory: false,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: size,
					Mtime:    mtime,
				},
				Extended: map[string][]byte{
					s3_constants.ExtVersionIdKey: []byte(latestVersionId),
					s3_constants.ExtETagKey:      etagBytes,
				},
			}

			// Attempt to parse the ETag and set it as Md5 attribute for compatibility with filer.ETag().
			// This is a partial fix for single-part uploads. Multipart ETags will still use ExtETagKey.
			if len(etagBytes) >= 2 && etagBytes[0] == '"' && etagBytes[len(etagBytes)-1] == '"' {
				unquotedEtag := etagBytes[1 : len(etagBytes)-1]
				if !bytes.Contains(unquotedEtag, []byte("-")) {
					if md5bytes, err := hex.DecodeString(string(unquotedEtag)); err == nil {
						logicalEntry.Attributes.Md5 = md5bytes
					}
				}
			}

			// Add owner if cached
			if ownerBytes, hasOwner := versionsDirEntry.Extended[s3_constants.ExtLatestVersionOwnerKey]; hasOwner {
				logicalEntry.Extended[s3_constants.ExtAmzOwnerKey] = ownerBytes
			}

			return logicalEntry, nil
		}
		glog.Warningf("getLatestVersionEntryFromDirectoryEntry: failed to parse cached metadata for %s/%s, falling back. sizeErr:%v, mtimeErr:%v", bucket, normalizedObject, sizeErr, mtimeErr)
	}

	// Fallback: fetch version file if cached metadata not available (for older versions)
	latestVersionFileBytes, hasLatestVersionFile := versionsDirEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]
	if !hasLatestVersionFile {
		return nil, fmt.Errorf("missing latest version file name metadata in .versions directory entry")
	}
	latestVersionFile := string(latestVersionFileBytes)

	glog.V(3).Infof("getLatestVersionEntryFromDirectoryEntry: fetching version file for %s/%s (no cached metadata)", bucket, normalizedObject)

	bucketDir := path.Join(s3a.option.BucketsPath, bucket)
	versionsObjectPath := path.Join(normalizedObject, s3_constants.VersionsFolder)
	latestVersionPath := path.Join(versionsObjectPath, latestVersionFile)
	latestVersionEntry, err := s3a.getEntry(bucketDir, latestVersionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest version file %s: %v", latestVersionPath, err)
	}

	// Check if this is a delete marker (should not be shown in regular list)
	if latestVersionEntry.Extended != nil {
		if deleteMarker, exists := latestVersionEntry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
			return nil, ErrDeleteMarker
		}
	}

	// Create a logical entry that appears at the object path (not the versioned path)
	logicalEntry := &filer_pb.Entry{
		Name:        path.Base(normalizedObject),
		IsDirectory: false,
		Attributes:  latestVersionEntry.Attributes,
		Extended:    latestVersionEntry.Extended,
		Chunks:      latestVersionEntry.Chunks,
	}

	return logicalEntry, nil
}

// getObjectOwnerFromVersion extracts object owner information from version metadata
func (s3a *S3ApiServer) getObjectOwnerFromVersion(version *ObjectVersion, bucket, objectKey string) CanonicalUser {
	// First try to get owner from the version's OwnerID field (extracted during listing)
	if version.OwnerID != "" {
		ownerDisplayName := s3a.iam.GetAccountNameById(version.OwnerID)
		return CanonicalUser{ID: version.OwnerID, DisplayName: ownerDisplayName}
	}

	// Fallback: fetch the specific version entry to get the owner
	// This handles cases where OwnerID wasn't populated during listing
	if specificVersionEntry, err := s3a.getSpecificObjectVersion(bucket, objectKey, version.VersionId); err == nil && specificVersionEntry.Extended != nil {
		if ownerBytes, exists := specificVersionEntry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			ownerId := string(ownerBytes)
			ownerDisplayName := s3a.iam.GetAccountNameById(ownerId)
			return CanonicalUser{ID: ownerId, DisplayName: ownerDisplayName}
		}
	}

	// Fallback: return anonymous if no owner found
	return CanonicalUser{ID: s3_constants.AccountAnonymousId, DisplayName: "anonymous"}
}

func entryExtended(v *ObjectVersion) map[string][]byte {
	return map[string][]byte{
		s3_constants.AmzStorageClass: []byte(v.StorageClass),
		s3_constants.ExtAmzOwnerKey:  []byte(v.OwnerID),
		s3_constants.ExtETagKey:      []byte(v.ETag),
	}
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
