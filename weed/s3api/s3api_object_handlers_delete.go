package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/filer"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	deleteMultipleObjectsLimit = 1000
)

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteObjectHandler %s %s", bucket, object)

	// Check for specific version ID in query parameters
	versionId := r.URL.Query().Get("versionId")

	// Get detailed versioning state for proper handling of suspended vs enabled versioning
	versioningState, err := s3a.getVersioningState(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	versioningEnabled := (versioningState == s3_constants.VersioningEnabled)
	versioningSuspended := (versioningState == s3_constants.VersioningSuspended)
	versioningConfigured := (versioningState != "")

	var auditLog *s3err.AccessLog
	if s3err.Logger != nil {
		auditLog = s3err.GetAccessLog(r, http.StatusNoContent, s3err.ErrNone)
	}

	if versioningConfigured {
		// Handle versioned delete based on specific versioning state
		if versionId != "" {
			// Delete specific version (same for both enabled and suspended)
			// Check object lock permissions before deleting specific version
			governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
			if err := s3a.enforceObjectLockProtections(r, bucket, object, versionId, governanceBypassAllowed); err != nil {
				glog.V(2).Infof("DeleteObjectHandler: object lock check failed for %s/%s: %v", bucket, object, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
				return
			}

			// Delete specific version
			err := s3a.deleteSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				glog.Errorf("Failed to delete specific version %s: %v", versionId, err)
				s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
				return
			}

			// Set version ID in response header
			w.Header().Set("x-amz-version-id", versionId)
		} else {
			// Delete without version ID - behavior depends on versioning state
			if versioningEnabled {
				// Enabled versioning: Create delete marker (logical delete)
				// AWS S3 behavior: Delete marker creation is NOT blocked by object retention
				// because it's a logical delete that doesn't actually remove the retained version
				deleteMarkerVersionId, err := s3a.createDeleteMarker(bucket, object)
				if err != nil {
					glog.Errorf("Failed to create delete marker: %v", err)
					s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
					return
				}

				// Set delete marker version ID in response header
				w.Header().Set("x-amz-version-id", deleteMarkerVersionId)
				w.Header().Set("x-amz-delete-marker", "true")
			} else if versioningSuspended {
				// Suspended versioning: Actually delete the "null" version object
				glog.V(2).Infof("DeleteObjectHandler: deleting null version for suspended versioning %s/%s", bucket, object)

				// Check object lock permissions before deleting "null" version
				governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
				if err := s3a.enforceObjectLockProtections(r, bucket, object, "null", governanceBypassAllowed); err != nil {
					glog.V(2).Infof("DeleteObjectHandler: object lock check failed for %s/%s: %v", bucket, object, err)
					s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
					return
				}

				// Delete the "null" version (the regular file)
				err := s3a.deleteSpecificObjectVersion(bucket, object, "null")
				if err != nil {
					glog.Errorf("Failed to delete null version: %v", err)
					s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
					return
				}

				// Note: According to AWS S3 spec, suspended versioning should NOT return version ID headers
				// The object is deleted but no version information is returned
			}
		}
	} else {
		// Handle regular delete (non-versioned)
		// Check object lock permissions before deleting object
		governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
		if err := s3a.enforceObjectLockProtections(r, bucket, object, "", governanceBypassAllowed); err != nil {
			glog.V(2).Infof("DeleteObjectHandler: object lock check failed for %s/%s: %v", bucket, object, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
			return
		}

		target := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object))
		dir, name := target.DirAndName()

		err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			if err := doDeleteEntry(client, dir, name, true, false); err != nil {
				return err
			}

			if s3a.option.AllowEmptyFolder {
				return nil
			}

			directoriesWithDeletion := make(map[string]int)
			if strings.LastIndex(object, "/") > 0 {
				directoriesWithDeletion[dir]++
				// purge empty folders, only checking folders with deletions
				for len(directoriesWithDeletion) > 0 {
					directoriesWithDeletion = s3a.doDeleteEmptyDirectories(client, directoriesWithDeletion)
				}
			}

			return nil
		})
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
	}

	if auditLog != nil {
		auditLog.Key = strings.TrimPrefix(object, "/")
		s3err.PostAccessLog(*auditLog)
	}

	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3DeletedObjectsCounter.WithLabelValues(bucket).Inc()
	w.WriteHeader(http.StatusNoContent)
}

// ObjectIdentifier represents an object to be deleted with its key name and optional version ID.
type ObjectIdentifier struct {
	Key                   string `xml:"Key"`
	VersionId             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId string `xml:"DeleteMarkerVersionId,omitempty"`
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectIdentifier `xml:"Object"`
}

// DeleteError structure.
type DeleteError struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []ObjectIdentifier `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

// DeleteMultipleObjectsHandler - Delete multiple objects
func (s3a *S3ApiServer) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {

	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("DeleteMultipleObjectsHandler %s", bucket)

	deleteXMLBytes, err := io.ReadAll(r.Body)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	deleteObjects := &DeleteObjectsRequest{}
	if err := xml.Unmarshal(deleteXMLBytes, deleteObjects); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	if len(deleteObjects.Objects) > deleteMultipleObjectsLimit {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxDeleteObjects)
		return
	}

	var deletedObjects []ObjectIdentifier
	var deleteErrors []DeleteError
	var auditLog *s3err.AccessLog

	directoriesWithDeletion := make(map[string]int)

	if s3err.Logger != nil {
		auditLog = s3err.GetAccessLog(r, http.StatusNoContent, s3err.ErrNone)
	}

	// Get detailed versioning state for proper handling of suspended vs enabled versioning
	versioningState, err := s3a.getVersioningState(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	versioningEnabled := (versioningState == s3_constants.VersioningEnabled)
	versioningSuspended := (versioningState == s3_constants.VersioningSuspended)
	versioningConfigured := (versioningState != "")

	s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		// delete file entries
		for _, object := range deleteObjects.Objects {
			if object.Key == "" {
				continue
			}

			// Check object lock permissions before deletion (only for versioned buckets)
			if versioningConfigured {
				// Validate governance bypass for this specific object
				governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object.Key)
				if err := s3a.enforceObjectLockProtections(r, bucket, object.Key, object.VersionId, governanceBypassAllowed); err != nil {
					glog.V(2).Infof("DeleteMultipleObjectsHandler: object lock check failed for %s/%s (version: %s): %v", bucket, object.Key, object.VersionId, err)
					deleteErrors = append(deleteErrors, DeleteError{
						Code:      s3err.GetAPIError(s3err.ErrAccessDenied).Code,
						Message:   s3err.GetAPIError(s3err.ErrAccessDenied).Description,
						Key:       object.Key,
						VersionId: object.VersionId,
					})
					continue
				}
			}

			var deleteVersionId string
			var isDeleteMarker bool

			if versioningConfigured {
				// Handle versioned delete based on specific versioning state
				if object.VersionId != "" {
					// Delete specific version (same for both enabled and suspended)
					err := s3a.deleteSpecificObjectVersion(bucket, object.Key, object.VersionId)
					if err != nil {
						deleteErrors = append(deleteErrors, DeleteError{
							Code:      "",
							Message:   err.Error(),
							Key:       object.Key,
							VersionId: object.VersionId,
						})
						continue
					}
					deleteVersionId = object.VersionId
				} else {
					// Delete without version ID - behavior depends on versioning state
					if versioningEnabled {
						// Enabled versioning: Create delete marker (logical delete)
						deleteMarkerVersionId, err := s3a.createDeleteMarker(bucket, object.Key)
						if err != nil {
							deleteErrors = append(deleteErrors, DeleteError{
								Code:      "",
								Message:   err.Error(),
								Key:       object.Key,
								VersionId: object.VersionId,
							})
							continue
						}
						deleteVersionId = deleteMarkerVersionId
						isDeleteMarker = true
					} else if versioningSuspended {
						// Suspended versioning: Actually delete the "null" version object
						glog.V(2).Infof("DeleteMultipleObjectsHandler: deleting null version for suspended versioning %s/%s", bucket, object.Key)

						err := s3a.deleteSpecificObjectVersion(bucket, object.Key, "null")
						if err != nil {
							deleteErrors = append(deleteErrors, DeleteError{
								Code:      "",
								Message:   err.Error(),
								Key:       object.Key,
								VersionId: "null",
							})
							continue
						}
						deleteVersionId = "null"
						// Note: For suspended versioning, we don't set isDeleteMarker=true
						// because we actually deleted the object, not created a delete marker
					}
				}

				// Add to successful deletions with version info
				deletedObject := ObjectIdentifier{
					Key:          object.Key,
					VersionId:    deleteVersionId,
					DeleteMarker: isDeleteMarker,
				}

				// For delete markers, also set DeleteMarkerVersionId field
				if isDeleteMarker {
					deletedObject.DeleteMarkerVersionId = deleteVersionId
					// Don't set VersionId for delete markers, use DeleteMarkerVersionId instead
					deletedObject.VersionId = ""
				}
				if !deleteObjects.Quiet {
					deletedObjects = append(deletedObjects, deletedObject)
				}
				if isDeleteMarker {
					// For delete markers, we don't need to track directories for cleanup
					continue
				}
			} else {
				// Handle non-versioned delete (original logic)
				lastSeparator := strings.LastIndex(object.Key, "/")
				parentDirectoryPath, entryName, isDeleteData, isRecursive := "", object.Key, true, false
				if lastSeparator > 0 && lastSeparator+1 < len(object.Key) {
					entryName = object.Key[lastSeparator+1:]
					parentDirectoryPath = "/" + object.Key[:lastSeparator]
				}
				parentDirectoryPath = fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, parentDirectoryPath)

				err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
				if err == nil {
					directoriesWithDeletion[parentDirectoryPath]++
					deletedObjects = append(deletedObjects, object)
				} else if strings.Contains(err.Error(), filer.MsgFailDelNonEmptyFolder) {
					deletedObjects = append(deletedObjects, object)
				} else {
					delete(directoriesWithDeletion, parentDirectoryPath)
					deleteErrors = append(deleteErrors, DeleteError{
						Code:      "",
						Message:   err.Error(),
						Key:       object.Key,
						VersionId: object.VersionId,
					})
				}
			}

			if auditLog != nil {
				auditLog.Key = object.Key
				s3err.PostAccessLog(*auditLog)
			}
		}

		if s3a.option.AllowEmptyFolder {
			return nil
		}

		// purge empty folders, only checking folders with deletions
		for len(directoriesWithDeletion) > 0 {
			directoriesWithDeletion = s3a.doDeleteEmptyDirectories(client, directoriesWithDeletion)
		}

		return nil
	})

	deleteResp := DeleteObjectsResponse{}
	if !deleteObjects.Quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = deleteErrors
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3DeletedObjectsCounter.WithLabelValues(bucket).Add(float64(len(deletedObjects)))

	writeSuccessResponseXML(w, r, deleteResp)

}

func (s3a *S3ApiServer) doDeleteEmptyDirectories(client filer_pb.SeaweedFilerClient, directoriesWithDeletion map[string]int) (newDirectoriesWithDeletion map[string]int) {
	var allDirs []string
	for dir := range directoriesWithDeletion {
		allDirs = append(allDirs, dir)
	}
	slices.SortFunc(allDirs, func(a, b string) int {
		return len(b) - len(a)
	})
	newDirectoriesWithDeletion = make(map[string]int)
	for _, dir := range allDirs {
		parentDir, dirName := util.FullPath(dir).DirAndName()
		if parentDir == s3a.option.BucketsPath {
			continue
		}
		if err := doDeleteEntry(client, parentDir, dirName, false, false); err != nil {
			glog.V(4).Infof("directory %s has %d deletion but still not empty: %v", dir, directoriesWithDeletion[dir], err)
		} else {
			newDirectoriesWithDeletion[parentDir]++
		}
	}
	return
}
