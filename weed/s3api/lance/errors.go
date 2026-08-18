package lance

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// Lance Namespace error codes. The spec numbers them, so a client switches on
// the code rather than parsing the message.
const (
	codeUnsupported            = 0
	codeNamespaceNotFound      = 1
	codeNamespaceAlreadyExists = 2
	codeNamespaceNotEmpty      = 3
	codeTableNotFound          = 4
	codeTableAlreadyExists     = 5
	codeInvalidInput           = 13
	codeConcurrentModification = 14
	codePermissionDenied       = 15
	codeUnauthenticated        = 16
	codeInternal               = 18
)

type errorResponse struct {
	Error    string `json:"error,omitempty"`
	Code     int    `json:"code"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		glog.Warningf("lance: failed to encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status, code int, message string) {
	instance := ""
	if r != nil {
		instance = r.URL.Path
	}
	writeJSON(w, status, errorResponse{Error: message, Code: code, Instance: instance})
}

// writeStorageError translates an S3 Tables storage error into the Lance error
// model. A table bucket is the first namespace level here, so a missing bucket
// is a missing namespace rather than a missing catalog.
func writeStorageError(w http.ResponseWriter, r *http.Request, err error) {
	var storageErr *s3tables.S3TablesError
	if !errors.As(err, &storageErr) {
		writeError(w, r, http.StatusInternalServerError, codeInternal, err.Error())
		return
	}

	status, code := http.StatusInternalServerError, codeInternal
	switch storageErr.Type {
	case s3tables.ErrCodeNoSuchBucket, s3tables.ErrCodeNoSuchNamespace:
		status, code = http.StatusNotFound, codeNamespaceNotFound
	case s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchView:
		status, code = http.StatusNotFound, codeTableNotFound
	case s3tables.ErrCodeBucketAlreadyExists, s3tables.ErrCodeNamespaceAlreadyExists:
		status, code = http.StatusConflict, codeNamespaceAlreadyExists
	case s3tables.ErrCodeTableAlreadyExists, s3tables.ErrCodeViewAlreadyExists:
		status, code = http.StatusConflict, codeTableAlreadyExists
	case s3tables.ErrCodeBucketNotEmpty, s3tables.ErrCodeNamespaceNotEmpty:
		status, code = http.StatusConflict, codeNamespaceNotEmpty
	case s3tables.ErrCodeConflict:
		status, code = http.StatusConflict, codeConcurrentModification
	case s3tables.ErrCodeAccessDenied:
		status, code = http.StatusForbidden, codePermissionDenied
	case s3tables.ErrCodeInvalidRequest, s3tables.ErrCodeInvalidIcebergLayout:
		status, code = http.StatusBadRequest, codeInvalidInput
	}
	writeError(w, r, status, code, storageErr.Message)
}

// isNotFound reports whether a storage error means the object is absent, so
// exists-style handlers can answer without a second lookup.
func isNotFound(err error) bool {
	var storageErr *s3tables.S3TablesError
	if !errors.As(err, &storageErr) {
		return false
	}
	switch storageErr.Type {
	case s3tables.ErrCodeNoSuchBucket, s3tables.ErrCodeNoSuchNamespace,
		s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchView:
		return true
	}
	return false
}
