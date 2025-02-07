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

	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3DeletedObjectsCounter.WithLabelValues(bucket).Inc()
	w.WriteHeader(http.StatusNoContent)
}

// / ObjectIdentifier carries key name for the object to delete.
type ObjectIdentifier struct {
	ObjectName string `xml:"Key"`
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
	Code    string
	Message string
	Key     string
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
	s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		// delete file entries
		for _, object := range deleteObjects.Objects {
			if object.ObjectName == "" {
				continue
			}
			lastSeparator := strings.LastIndex(object.ObjectName, "/")
			parentDirectoryPath, entryName, isDeleteData, isRecursive := "", object.ObjectName, true, false
			if lastSeparator > 0 && lastSeparator+1 < len(object.ObjectName) {
				entryName = object.ObjectName[lastSeparator+1:]
				parentDirectoryPath = "/" + object.ObjectName[:lastSeparator]
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
					Code:    "",
					Message: err.Error(),
					Key:     object.ObjectName,
				})
			}
			if auditLog != nil {
				auditLog.Key = entryName
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
