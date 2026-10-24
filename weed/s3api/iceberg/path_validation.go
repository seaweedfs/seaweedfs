package iceberg

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// validateRequestPath rejects Iceberg REST requests whose captured
// {prefix}/{namespace}/{table} mux vars would produce a parent-directory
// traversal when joined into a filer path. The iceberg router runs with
// SkipClean(true), so `..` survives routing; downstream path.Join calls
// (stageCreateMarkerDir, location builders, etc.) then collapse it and
// escape the table-bucket directory.
//
// {prefix} maps to a table-bucket name; {table} is a single path segment;
// {namespace} is unit-separator (0x1F) joined parts that get flattened into
// a single dotted name for the on-disk layout — each part is validated
// individually.
func validateRequestPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		// Use the comma-ok form so vars only checked when the matched route
		// actually captures them; when captured, an empty value is itself a
		// rejection because downstream path.Join would collapse it.
		if prefix, ok := vars["prefix"]; ok {
			if prefix == "" || !s3_constants.IsValidBucketName(prefix) {
				writeError(w, http.StatusBadRequest, "BadRequest", "invalid prefix")
				return
			}
		}
		if table, ok := vars["table"]; ok {
			if table == "" || !isValidNameSegment(table) {
				writeError(w, http.StatusBadRequest, "BadRequest", "invalid table name")
				return
			}
		}
		if ns, ok := vars["namespace"]; ok {
			if ns == "" {
				writeError(w, http.StatusBadRequest, "BadRequest", "invalid namespace")
				return
			}
			// Reject leading/trailing/consecutive unit separators so distinct
			// inputs cannot collapse to the same parsed namespace via
			// parseNamespace's empty-part filter.
			for _, part := range strings.Split(ns, "\x1F") {
				if part == "" || !isValidNameSegment(part) {
					writeError(w, http.StatusBadRequest, "BadRequest", "invalid namespace")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// isValidTablePath reports whether a "/"-separated table path (the part below
// the bucket) is free of segments that path.Join would collapse to escape the
// bucket directory. Empty segments (from leading/duplicate slashes) are
// ignored, but the path must resolve to at least one real segment so it does
// not collapse to the bucket-level metadata directory.
func isValidTablePath(tablePath string) bool {
	hasSegment := false
	for _, segment := range strings.Split(tablePath, "/") {
		if segment == "" {
			continue
		}
		if !isValidNameSegment(segment) {
			return false
		}
		hasSegment = true
	}
	return hasSegment
}

// isValidNameSegment rejects a single path-segment value (bucket prefix slot,
// table name, or one namespace part) that would be unsafe to embed in a filer
// path: `.`, `..`, embedded slash/backslash, or NUL.
func isValidNameSegment(s string) bool {
	if s == "" {
		return true
	}
	if s == "." || s == ".." {
		return false
	}
	return !strings.ContainsAny(s, "/\\\x00")
}

// confineMetadataLocation checks that a parsed s3 location stays within the
// authorized table bucket and rejects traversal segments that path.Join in
// saveMetadataBlob would collapse to escape it. The table path must resolve
// to at least one real segment so its metadata directory is table-specific.
func confineMetadataLocation(metadataBucket, metadataPath, bucketName string) error {
	if metadataBucket != bucketName {
		return fmt.Errorf("table location must be within bucket %s", bucketName)
	}
	if !isValidTablePath(metadataPath) {
		return fmt.Errorf("invalid table location path")
	}
	return nil
}
