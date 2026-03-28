package s3api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	deleteMultipleObjectsLimit = 1000
)

type deleteMutationResult struct {
	versionId    string
	deleteMarker bool
}

func deleteErrorFromCode(code s3err.ErrorCode, key, versionId string) DeleteError {
	apiErr := s3err.GetAPIError(code)
	return DeleteError{
		Code:      apiErr.Code,
		Message:   apiErr.Description,
		Key:       key,
		VersionId: versionId,
	}
}

// isMissingDeleteConditionTarget normalizes missing-target detection for conditional deletes.
// Prefer errors.Is(err, filer_pb.ErrNotFound) and errors.Is(err, ErrDeleteMarker); keep the
// string-based fallback only as a defensive bridge for filer paths that still return plain text.
func isMissingDeleteConditionTarget(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrDeleteMarker) {
		return true
	}

	lowerErr := strings.ToLower(err.Error())
	return strings.Contains(lowerErr, "not found")
}

func (s3a *S3ApiServer) resolveDeleteConditionalEntry(bucket, object, versionId, versioningState string) (*filer_pb.Entry, error) {
	normalizedObject := s3_constants.NormalizeObjectKey(object)
	bucketDir := s3a.bucketDir(bucket)

	if versionId != "" {
		if versionId == "null" {
			return s3a.getEntry(bucketDir, normalizedObject)
		}
		return s3a.getEntry(s3a.getVersionedObjectDir(bucket, normalizedObject), s3a.getVersionFileName(versionId))
	}

	switch versioningState {
	case s3_constants.VersioningEnabled:
		entry, err := s3a.getLatestObjectVersion(bucket, normalizedObject)
		if err != nil {
			return nil, err
		}
		return normalizeConditionalTargetEntry(entry), nil
	default:
		entry, err := s3a.resolveObjectEntry(bucket, normalizedObject)
		if err != nil {
			return nil, err
		}
		return normalizeConditionalTargetEntry(entry), nil
	}
}

func (s3a *S3ApiServer) validateDeleteIfMatch(entry *filer_pb.Entry, ifMatch string, missingCode s3err.ErrorCode) s3err.ErrorCode {
	if ifMatch == "" {
		return s3err.ErrNone
	}
	if entry == nil {
		return missingCode
	}
	if ifMatch == "*" {
		return s3err.ErrNone
	}
	if !s3a.etagMatches(ifMatch, s3a.getObjectETag(entry)) {
		return s3err.ErrPreconditionFailed
	}
	return s3err.ErrNone
}

func (s3a *S3ApiServer) checkDeleteIfMatch(bucket, object, versionId, versioningState, ifMatch string, missingCode s3err.ErrorCode) s3err.ErrorCode {
	if ifMatch == "" {
		return s3err.ErrNone
	}

	entry, err := s3a.resolveDeleteConditionalEntry(bucket, object, versionId, versioningState)
	if err != nil {
		if isMissingDeleteConditionTarget(err) {
			return missingCode
		}
		glog.Errorf("checkDeleteIfMatch: failed to resolve %s/%s (versionId=%s): %v", bucket, object, versionId, err)
		return s3err.ErrInternalError
	}

	return s3a.validateDeleteIfMatch(entry, ifMatch, missingCode)
}

func (s3a *S3ApiServer) deleteVersionedObject(r *http.Request, bucket, object, versionId, versioningState string) (deleteMutationResult, s3err.ErrorCode) {
	var result deleteMutationResult

	switch {
	case versionId != "":
		versionEntry, versionLookupErr := s3a.getSpecificObjectVersion(bucket, object, versionId)
		if versionLookupErr == nil && versionEntry != nil && versionEntry.Extended != nil {
			if deleteMarker, ok := versionEntry.Extended[s3_constants.ExtDeleteMarkerKey]; ok && string(deleteMarker) == "true" {
				result.deleteMarker = true
			}
		}
		governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
		if err := s3a.enforceObjectLockProtections(r, bucket, object, versionId, governanceBypassAllowed); err != nil {
			glog.V(2).Infof("deleteVersionedObject: object lock check failed for %s/%s version %s: %v", bucket, object, versionId, err)
			return result, s3err.ErrAccessDenied
		}
		if err := s3a.deleteSpecificObjectVersion(bucket, object, versionId); err != nil {
			glog.Errorf("deleteVersionedObject: failed to delete specific version %s for %s/%s: %v", versionId, bucket, object, err)
			return result, s3err.ErrInternalError
		}
		result.versionId = versionId
		return result, s3err.ErrNone

	case versioningState == s3_constants.VersioningEnabled:
		deleteMarkerVersionId, err := s3a.createDeleteMarker(bucket, object)
		if err != nil {
			glog.Errorf("deleteVersionedObject: failed to create delete marker for %s/%s: %v", bucket, object, err)
			return result, s3err.ErrInternalError
		}
		result.versionId = deleteMarkerVersionId
		result.deleteMarker = true
		return result, s3err.ErrNone

	case versioningState == s3_constants.VersioningSuspended:
		governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
		if err := s3a.enforceObjectLockProtections(r, bucket, object, "null", governanceBypassAllowed); err != nil {
			glog.V(2).Infof("deleteVersionedObject: object lock check failed for %s/%s null version: %v", bucket, object, err)
			return result, s3err.ErrAccessDenied
		}
		if err := s3a.deleteSpecificObjectVersion(bucket, object, "null"); err != nil {
			glog.Errorf("deleteVersionedObject: failed to delete null version for %s/%s: %v", bucket, object, err)
			return result, s3err.ErrInternalError
		}
		return result, s3err.ErrNone
	}

	glog.Errorf("deleteVersionedObject: unsupported versioning state %q for %s/%s", versioningState, bucket, object)
	return result, s3err.ErrInternalError
}

func (s3a *S3ApiServer) deleteUnversionedObjectWithClient(client filer_pb.SeaweedFilerClient, bucket, object string) error {
	target := util.NewFullPath(s3a.bucketDir(bucket), object)
	dir, name := target.DirAndName()
	return deleteObjectEntry(client, dir, name, true, false)
}

func (s3a *S3ApiServer) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {

	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.Infof("DeleteObjectHandler %s %s", bucket, object)
	if err := s3a.validateTableBucketObjectPath(bucket, object); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrAccessDenied)
		return
	}

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

	versioningConfigured := (versioningState != "")

	var auditLog *s3err.AccessLog
	if s3err.Logger != nil {
		auditLog = s3err.GetAccessLog(r, http.StatusNoContent, s3err.ErrNone)
	}

	if ifMatchResult := s3a.checkDeleteIfMatch(bucket, object, versionId, versioningState, r.Header.Get(s3_constants.IfMatch), s3err.ErrPreconditionFailed); ifMatchResult != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, ifMatchResult)
		return
	}

	var deleteResult deleteMutationResult
	deleteCode := s3a.withObjectWriteLock(bucket, object, func() s3err.ErrorCode {
		return s3a.checkDeleteIfMatch(bucket, object, versionId, versioningState, r.Header.Get(s3_constants.IfMatch), s3err.ErrPreconditionFailed)
	}, func() s3err.ErrorCode {
		if versioningConfigured {
			result, errCode := s3a.deleteVersionedObject(r, bucket, object, versionId, versioningState)
			if errCode != s3err.ErrNone {
				return errCode
			}
			deleteResult = result
			return s3err.ErrNone
		}

		governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object)
		if err := s3a.enforceObjectLockProtections(r, bucket, object, "", governanceBypassAllowed); err != nil {
			glog.V(2).Infof("DeleteObjectHandler: object lock check failed for %s/%s: %v", bucket, object, err)
			return s3err.ErrAccessDenied
		}

		if err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return s3a.deleteUnversionedObjectWithClient(client, bucket, object)
		}); err != nil {
			glog.Errorf("DeleteObjectHandler: failed to delete %s/%s: %v", bucket, object, err)
			return s3err.ErrInternalError
		}

		return s3err.ErrNone
	})
	if deleteCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, deleteCode)
		return
	}

	if deleteResult.versionId != "" {
		w.Header().Set("x-amz-version-id", deleteResult.versionId)
	}
	if deleteResult.deleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
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
	ETag                  string `xml:"ETag,omitempty"`
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

	versioningConfigured := (versioningState != "")
	deletedCount := 0

	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// delete file entries
		for _, object := range deleteObjects.Objects {
			if object.Key == "" {
				continue
			}
			if err := s3a.validateTableBucketObjectPath(bucket, object.Key); err != nil {
				deleteErrors = append(deleteErrors, deleteErrorFromCode(s3err.ErrAccessDenied, object.Key, object.VersionId))
				continue
			}

			var deleteResult deleteMutationResult
			deleteCode := s3a.withObjectWriteLock(bucket, object.Key, func() s3err.ErrorCode {
				return s3a.checkDeleteIfMatch(bucket, object.Key, object.VersionId, versioningState, object.ETag, s3err.ErrNoSuchKey)
			}, func() s3err.ErrorCode {
				if versioningConfigured {
					result, errCode := s3a.deleteVersionedObject(r, bucket, object.Key, object.VersionId, versioningState)
					if errCode != s3err.ErrNone {
						return errCode
					}
					deleteResult = result
					return s3err.ErrNone
				}

				governanceBypassAllowed := s3a.evaluateGovernanceBypassRequest(r, bucket, object.Key)
				if err := s3a.enforceObjectLockProtections(r, bucket, object.Key, "", governanceBypassAllowed); err != nil {
					glog.V(2).Infof("DeleteMultipleObjectsHandler: object lock check failed for %s/%s: %v", bucket, object.Key, err)
					return s3err.ErrAccessDenied
				}

				if err := s3a.deleteUnversionedObjectWithClient(client, bucket, object.Key); err != nil {
					glog.Errorf("DeleteMultipleObjectsHandler: failed to delete %s/%s: %v", bucket, object.Key, err)
					return s3err.ErrInternalError
				}

				return s3err.ErrNone
			})
			if deleteCode != s3err.ErrNone {
				deleteErrors = append(deleteErrors, deleteErrorFromCode(deleteCode, object.Key, object.VersionId))
				continue
			}

			deletedCount++
			if !deleteObjects.Quiet {
				deletedObject := ObjectIdentifier{
					Key:       object.Key,
					VersionId: deleteResult.versionId,
				}
				if deleteResult.deleteMarker {
					deletedObject.DeleteMarker = true
					deletedObject.DeleteMarkerVersionId = deleteResult.versionId
					deletedObject.VersionId = ""
				}
				deletedObjects = append(deletedObjects, deletedObject)
			}

			if auditLog != nil {
				auditLog.Key = object.Key
				s3err.PostAccessLog(*auditLog)
			}
		}

		// Note: Empty folder cleanup is now handled asynchronously by EmptyFolderCleaner
		// which listens to metadata events and uses consistent hashing for coordination

		return nil
	})
	if err != nil {
		glog.Errorf("DeleteMultipleObjectsHandler: failed to initialize filer client for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	deleteResp := DeleteObjectsResponse{}
	if !deleteObjects.Quiet {
		deleteResp.DeletedObjects = deletedObjects
	}
	deleteResp.Errors = deleteErrors
	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3DeletedObjectsCounter.WithLabelValues(bucket).Add(float64(deletedCount))

	writeSuccessResponseXML(w, r, deleteResp)

}
