package s3api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ResolveS3Action determines the specific S3 action from HTTP request context.
// This is the unified implementation used by both the bucket policy engine
// and the IAM integration for consistent action resolution.
//
// It examines the HTTP method, path, query parameters, and headers to determine
// the most specific S3 action string (e.g., "s3:DeleteObject", "s3:PutObjectTagging").
//
// Parameters:
//   - r: HTTP request containing method, URL, query params, and headers
//   - baseAction: Coarse-grained action constant (e.g., ACTION_WRITE, ACTION_READ)
//   - bucket: Bucket name from the request path
//   - object: Object key from the request path (may be empty for bucket operations)
//
// Returns:
//   - Specific S3 action string (e.g., "s3:DeleteObject")
//   - Empty string if no specific resolution is possible
func ResolveS3Action(r *http.Request, baseAction string, bucket string, object string) string {
	if r == nil {
		// No HTTP context available: fall back to coarse-grained mapping
		// This ensures consistent behavior and avoids returning empty strings
		return mapBaseActionToS3Format(baseAction)
	}

	method := r.Method
	query := r.URL.Query()

	// Determine if this is an object or bucket operation
	// Note: "/" is treated as bucket-level, not object-level
	hasObject := object != "" && object != "/"

	// Priority 1: Check for specific query parameters that indicate specific actions
	// These override everything else because they explicitly indicate the operation type
	if action := resolveFromQueryParameters(query, method, hasObject); action != "" {
		return action
	}

	// Priority 2: Handle basic operations based on method and resource type
	// Only use the result if a specific action was resolved; otherwise fall through to Priority 3
	if hasObject {
		if action := resolveObjectLevelAction(method, baseAction); action != "" {
			return action
		}
	} else if bucket != "" {
		if action := resolveBucketLevelAction(method, baseAction); action != "" {
			return action
		}
	}

	// Priority 3: Fallback to legacy action mapping
	return mapBaseActionToS3Format(baseAction)
}

// bucketQueryActions maps bucket-level query parameters to their corresponding S3 actions by HTTP method
var bucketQueryActions = map[string]map[string]string{
	"policy": {
		http.MethodGet:    s3_constants.S3_ACTION_GET_BUCKET_POLICY,
		http.MethodPut:    s3_constants.S3_ACTION_PUT_BUCKET_POLICY,
		http.MethodDelete: s3_constants.S3_ACTION_DELETE_BUCKET_POLICY,
	},
	"cors": {
		http.MethodGet:    s3_constants.S3_ACTION_GET_BUCKET_CORS,
		http.MethodPut:    s3_constants.S3_ACTION_PUT_BUCKET_CORS,
		http.MethodDelete: s3_constants.S3_ACTION_DELETE_BUCKET_CORS,
	},
	"lifecycle": {
		http.MethodGet:    s3_constants.S3_ACTION_GET_BUCKET_LIFECYCLE,
		http.MethodPut:    s3_constants.S3_ACTION_PUT_BUCKET_LIFECYCLE,
		http.MethodDelete: s3_constants.S3_ACTION_PUT_BUCKET_LIFECYCLE, // DELETE uses same permission as PUT
	},
	"versioning": {
		http.MethodGet: s3_constants.S3_ACTION_GET_BUCKET_VERSIONING,
		http.MethodPut: s3_constants.S3_ACTION_PUT_BUCKET_VERSIONING,
	},
	"notification": {
		http.MethodGet: s3_constants.S3_ACTION_GET_BUCKET_NOTIFICATION,
		http.MethodPut: s3_constants.S3_ACTION_PUT_BUCKET_NOTIFICATION,
	},
	"object-lock": {
		http.MethodGet: s3_constants.S3_ACTION_GET_BUCKET_OBJECT_LOCK,
		http.MethodPut: s3_constants.S3_ACTION_PUT_BUCKET_OBJECT_LOCK,
	},
}

// resolveFromQueryParameters checks query parameters to determine specific S3 actions
func resolveFromQueryParameters(query url.Values, method string, hasObject bool) string {
	// Multipart upload operations
	if query.Has("uploadId") && query.Has("partNumber") {
		if method == http.MethodPut {
			return s3_constants.S3_ACTION_UPLOAD_PART
		}
	}
	
	if query.Has("uploadId") {
		switch method {
		case http.MethodPost:
			return s3_constants.S3_ACTION_COMPLETE_MULTIPART
		case http.MethodDelete:
			return s3_constants.S3_ACTION_ABORT_MULTIPART
		case http.MethodGet:
			return s3_constants.S3_ACTION_LIST_PARTS
		}
	}
	
	if query.Has("uploads") {
		if method == http.MethodPost {
			return s3_constants.S3_ACTION_CREATE_MULTIPART
		} else if method == http.MethodGet {
			return s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS
		}
	}

	// ACL operations
	if query.Has("acl") {
		switch method {
		case http.MethodGet, http.MethodHead:
			if hasObject {
				return s3_constants.S3_ACTION_GET_OBJECT_ACL
			}
			return s3_constants.S3_ACTION_GET_BUCKET_ACL
		case http.MethodPut:
			if hasObject {
				return s3_constants.S3_ACTION_PUT_OBJECT_ACL
			}
			return s3_constants.S3_ACTION_PUT_BUCKET_ACL
		}
	}

	// Tagging operations
	if query.Has("tagging") {
		switch method {
		case http.MethodGet:
			if hasObject {
				return s3_constants.S3_ACTION_GET_OBJECT_TAGGING
			}
			return s3_constants.S3_ACTION_GET_BUCKET_TAGGING
		case http.MethodPut:
			if hasObject {
				return s3_constants.S3_ACTION_PUT_OBJECT_TAGGING
			}
			return s3_constants.S3_ACTION_PUT_BUCKET_TAGGING
		case http.MethodDelete:
			if hasObject {
				return s3_constants.S3_ACTION_DELETE_OBJECT_TAGGING
			}
			return s3_constants.S3_ACTION_DELETE_BUCKET_TAGGING
		}
	}
	
	// Versioning operations - distinguish between versionId (specific version) and versions (list versions)
	// versionId: Used to access/delete a specific version of an object (e.g., GET /bucket/key?versionId=xyz)
	if query.Has("versionId") {
		if hasObject {
			switch method {
			case http.MethodGet, http.MethodHead:
				return s3_constants.S3_ACTION_GET_OBJECT_VERSION
			case http.MethodDelete:
				return s3_constants.S3_ACTION_DELETE_OBJECT_VERSION
			}
		}
	}

	// versions: Used to list all versions of objects in a bucket (e.g., GET /bucket?versions)
	if query.Has("versions") {
		if method == http.MethodGet && !hasObject {
			return s3_constants.S3_ACTION_LIST_BUCKET_VERSIONS
		}
	}

	// Check bucket-level query parameters using data-driven approach
	for param, actions := range bucketQueryActions {
		if query.Has(param) {
			if action, ok := actions[method]; ok {
				return action
			}
		}
	}

	// Location (GET only)
	if query.Has("location") && method == http.MethodGet {
		return s3_constants.S3_ACTION_GET_BUCKET_LOCATION
	}
	
	// Object retention and legal hold operations (object-level only)
	if hasObject {
		if query.Has("retention") {
			switch method {
			case http.MethodGet:
				return s3_constants.S3_ACTION_GET_OBJECT_RETENTION
			case http.MethodPut:
				return s3_constants.S3_ACTION_PUT_OBJECT_RETENTION
			}
		}

		if query.Has("legal-hold") {
			switch method {
			case http.MethodGet:
				return s3_constants.S3_ACTION_GET_OBJECT_LEGAL_HOLD
			case http.MethodPut:
				return s3_constants.S3_ACTION_PUT_OBJECT_LEGAL_HOLD
			}
		}
	}

	// Batch delete - POST request with delete query parameter (bucket-level operation)
	if query.Has("delete") && method == http.MethodPost {
		return s3_constants.S3_ACTION_DELETE_OBJECT
	}

	return ""
}

// resolveObjectLevelAction determines the S3 action for object-level operations
func resolveObjectLevelAction(method string, baseAction string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		if baseAction == s3_constants.ACTION_READ {
			return s3_constants.S3_ACTION_GET_OBJECT
		}
		
	case http.MethodPut:
		if baseAction == s3_constants.ACTION_WRITE {
			// Note: CopyObject operations also use s3:PutObject permission (same as MinIO/AWS)
			// Copy requires s3:PutObject on destination and s3:GetObject on source
			return s3_constants.S3_ACTION_PUT_OBJECT
		}
		
	case http.MethodDelete:
		// CRITICAL: Map DELETE method to s3:DeleteObject
		// This fixes the architectural limitation where ACTION_WRITE was mapped to s3:PutObject
		if baseAction == s3_constants.ACTION_WRITE {
			return s3_constants.S3_ACTION_DELETE_OBJECT
		}
		
	case http.MethodPost:
		// POST without query params is typically multipart or form upload
		if baseAction == s3_constants.ACTION_WRITE {
			return s3_constants.S3_ACTION_PUT_OBJECT
		}
	}
	
	return ""
}

// resolveBucketLevelAction determines the S3 action for bucket-level operations
func resolveBucketLevelAction(method string, baseAction string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		if baseAction == s3_constants.ACTION_LIST {
			return s3_constants.S3_ACTION_LIST_BUCKET
		} else if baseAction == s3_constants.ACTION_READ {
			return s3_constants.S3_ACTION_LIST_BUCKET
		}
		
	case http.MethodPut:
		if baseAction == s3_constants.ACTION_WRITE {
			return s3_constants.S3_ACTION_CREATE_BUCKET
		}
		
	case http.MethodDelete:
		if baseAction == s3_constants.ACTION_DELETE_BUCKET {
			return s3_constants.S3_ACTION_DELETE_BUCKET
		}
		
	case http.MethodPost:
		// POST to bucket is typically form upload
		if baseAction == s3_constants.ACTION_WRITE {
			return s3_constants.S3_ACTION_PUT_OBJECT
		}
	}
	
	return ""
}

// mapBaseActionToS3Format converts coarse-grained base actions to S3 format
// This is the fallback when no specific resolution is found
func mapBaseActionToS3Format(baseAction string) string {
	// Handle actions that already have s3: prefix
	if strings.HasPrefix(baseAction, "s3:") {
		return baseAction
	}

	// Map coarse-grained actions to their most common S3 equivalent
	// Note: The s3_constants values ARE the string values (e.g., ACTION_READ = "Read")
	switch baseAction {
	case s3_constants.ACTION_READ: // "Read"
		return s3_constants.S3_ACTION_GET_OBJECT
	case s3_constants.ACTION_WRITE: // "Write"
		return s3_constants.S3_ACTION_PUT_OBJECT
	case s3_constants.ACTION_LIST: // "List"
		return s3_constants.S3_ACTION_LIST_BUCKET
	case s3_constants.ACTION_TAGGING: // "Tagging"
		return s3_constants.S3_ACTION_PUT_OBJECT_TAGGING
	case s3_constants.ACTION_ADMIN: // "Admin"
		return s3_constants.S3_ACTION_ALL
	case s3_constants.ACTION_READ_ACP: // "ReadAcp"
		return s3_constants.S3_ACTION_GET_OBJECT_ACL
	case s3_constants.ACTION_WRITE_ACP: // "WriteAcp"
		return s3_constants.S3_ACTION_PUT_OBJECT_ACL
	case s3_constants.ACTION_DELETE_BUCKET: // "DeleteBucket"
		return s3_constants.S3_ACTION_DELETE_BUCKET
	case s3_constants.ACTION_BYPASS_GOVERNANCE_RETENTION:
		return s3_constants.S3_ACTION_BYPASS_GOVERNANCE
	case s3_constants.ACTION_GET_OBJECT_RETENTION:
		return s3_constants.S3_ACTION_GET_OBJECT_RETENTION
	case s3_constants.ACTION_PUT_OBJECT_RETENTION:
		return s3_constants.S3_ACTION_PUT_OBJECT_RETENTION
	case s3_constants.ACTION_GET_OBJECT_LEGAL_HOLD:
		return s3_constants.S3_ACTION_GET_OBJECT_LEGAL_HOLD
	case s3_constants.ACTION_PUT_OBJECT_LEGAL_HOLD:
		return s3_constants.S3_ACTION_PUT_OBJECT_LEGAL_HOLD
	case s3_constants.ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG:
		return s3_constants.S3_ACTION_GET_BUCKET_OBJECT_LOCK
	case s3_constants.ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG:
		return s3_constants.S3_ACTION_PUT_BUCKET_OBJECT_LOCK
	default:
		// For unknown actions, prefix with s3: to maintain format consistency
		return "s3:" + baseAction
	}
}

