package iceberg

import (
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
		if !s3_constants.IsValidBucketName(vars["prefix"]) {
			writeError(w, http.StatusBadRequest, "BadRequest", "invalid prefix")
			return
		}
		if !isValidNameSegment(vars["table"]) {
			writeError(w, http.StatusBadRequest, "BadRequest", "invalid table name")
			return
		}
		if ns := vars["namespace"]; ns != "" {
			for _, part := range strings.Split(ns, "\x1F") {
				if part == "" {
					continue
				}
				if !isValidNameSegment(part) {
					writeError(w, http.StatusBadRequest, "BadRequest", "invalid namespace")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
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
