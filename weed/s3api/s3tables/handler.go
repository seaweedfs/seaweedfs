package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

const (
	TablesPath       = s3_constants.DefaultBucketsPath
	DefaultAccountID = "000000000000"
	DefaultRegion    = "us-east-1"

	// Extended entry attributes for metadata storage
	ExtendedKeyTableBucket     = s3_constants.ExtS3TablesPrefix + "tableBucket"
	ExtendedKeyMetadata        = s3_constants.ExtS3TablesPrefix + "metadata"
	ExtendedKeyMetadataVersion = s3_constants.ExtS3TablesPrefix + "metadataVersion"
	ExtendedKeyPolicy          = s3_constants.ExtS3TablesPrefix + "policy"
	ExtendedKeyTags            = s3_constants.ExtS3TablesPrefix + "tags"
	ExtendedKeyMaintenance     = s3_constants.ExtS3TablesPrefix + "maintenance"
	// Written by the maintenance worker, read by GetTableMaintenanceJobStatus.
	// Separate from ExtendedKeyMaintenance so worker and operator writes do not
	// contend on the same attribute.
	ExtendedKeyMaintenanceStatus = s3_constants.ExtS3TablesPrefix + "maintenanceStatus"
	ExtendedKeyEntryType         = s3_constants.ExtS3TablesPrefix + "entryType"
	// ExtendedKeyDeleting marks a catalog record claimed by an in-flight
	// create, delete or rename, so the claim survives until the operation
	// finishes or releases it. A table carries "create", "delete" or "rename";
	// a namespace being deleted carries the random claim token written by
	// claimForDelete.
	ExtendedKeyDeleting = s3_constants.ExtS3TablesPrefix + "deleting"

	// ExtendedKeyVectorPolicy is written by the S3 Vectors service on table
	// buckets. It lives here rather than in the vectors package so the catalog
	// can assert it in its update preconditions: a whole-entry write that does
	// not know the key could silently erase a concurrently saved vector policy.
	ExtendedKeyVectorPolicy = "s3vectors-policy"

	// Entry-type marker values for ExtendedKeyEntryType. Absent or "table" means
	// a table; views are stored like tables but tagged "view".
	EntryTypeTable = "table"
	EntryTypeView  = "view"

	// MaxRequestBodySize is the largest request body the handler reads (10MB).
	// In-process callers that send a table's full metadata check against it.
	MaxRequestBodySize = 10 * 1024 * 1024
)

var (
	ErrVersionTokenMismatch = errors.New("version token mismatch")
	ErrAccessDenied         = errors.New("access denied")
	ErrTableAlreadyExists   = errors.New("table already exists")
)

type ResourceType string

const (
	ResourceTypeBucket ResourceType = "bucket"
	ResourceTypeTable  ResourceType = "table"
)

// S3TablesHandler handles S3 Tables API requests
type S3TablesHandler struct {
	region          string
	accountID       string
	defaultAllow    bool // Whether to allow access by default (for zero-config IAM)
	trusted         bool // Trusted local tooling (shell/admin) bypasses authorization
	iamAuthorizer   IAMAuthorizer
	localAccountIDs func(string) bool
	canEncrypt      func() bool
}

// NewS3TablesHandler creates a new S3 Tables handler
func NewS3TablesHandler() *S3TablesHandler {
	return &S3TablesHandler{
		region:       DefaultRegion,
		accountID:    DefaultAccountID,
		defaultAllow: false,
	}
}

// SetRegion sets the AWS region for ARN generation
func (h *S3TablesHandler) SetRegion(region string) {
	if region != "" {
		h.region = region
	}
}

// SetAccountID sets the AWS account ID for ARN generation
func (h *S3TablesHandler) SetAccountID(accountID string) {
	if accountID != "" {
		h.accountID = accountID
	}
}

// SetDefaultAllow sets whether to allow access by default
func (h *S3TablesHandler) SetDefaultAllow(allow bool) {
	h.defaultAllow = allow
}

// SetTrusted lets local tooling that talks to the filer directly (shell, admin
// console) bypass authorization. HTTP-facing callers must not set it.
func (h *S3TablesHandler) SetTrusted(trusted bool) {
	h.trusted = trusted
}

// SetLocalAccountChecker injects the predicate that recognizes account IDs of
// configured local identities, so a stored owner that names one is never
// re-anchored to an OIDC subject.
func (h *S3TablesHandler) SetLocalAccountChecker(fn func(string) bool) {
	h.localAccountIDs = fn
}

// SetEncryptionChecker injects the predicate that tells whether the gateway
// holds an SSE-S3 key, so a bucket whose declared AES256 the write path
// applies is not created where every write would then fail.
func (h *S3TablesHandler) SetEncryptionChecker(fn func() bool) {
	h.canEncrypt = fn
}

// FilerClient interface for filer operations
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

// HandleRequest is the main entry point for S3 Tables API requests
func (h *S3TablesHandler) HandleRequest(w http.ResponseWriter, r *http.Request, filerClient FilerClient) {
	operation := r.Header.Get("X-Amz-Target")
	if operation != "" {
		if idx := strings.LastIndex(operation, "."); idx != -1 {
			operation = operation[idx+1:]
		}
	}
	if operation == "" {
		glog.V(1).Infof("S3Tables: missing X-Amz-Target header")
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "Missing X-Amz-Target header")
		return
	}

	glog.V(3).Infof("S3Tables: handling operation %s", operation)

	// the status the handler answers decides how its error is logged (logRequestError)
	sw := &statusRecorder{ResponseWriter: w}
	w = sw
	fence := h.fenceVectorNamespace(w, r, filerClient, operation)
	if fence != nil {
		w, filerClient = fence, fence
	}

	var err error
	switch operation {
	// Table Bucket operations
	case "CreateTableBucket":
		err = h.handleCreateTableBucket(w, r, filerClient)
	case "GetTableBucket":
		err = h.handleGetTableBucket(w, r, filerClient)
	case "ListTableBuckets":
		err = h.handleListTableBuckets(w, r, filerClient)
	case "DeleteTableBucket":
		err = h.handleDeleteTableBucket(w, r, filerClient)

	// Table Bucket Policy operations
	case "PutTableBucketPolicy":
		err = h.handlePutTableBucketPolicy(w, r, filerClient)
	case "GetTableBucketPolicy":
		err = h.handleGetTableBucketPolicy(w, r, filerClient)
	case "DeleteTableBucketPolicy":
		err = h.handleDeleteTableBucketPolicy(w, r, filerClient)

	// Namespace operations
	case "CreateNamespace":
		err = h.handleCreateNamespace(w, r, filerClient)
	case "GetNamespace":
		err = h.handleGetNamespace(w, r, filerClient)
	case "UpdateNamespace":
		err = h.handleUpdateNamespace(w, r, filerClient)
	case "ListNamespaces":
		err = h.handleListNamespaces(w, r, filerClient)
	case "DeleteNamespace":
		err = h.handleDeleteNamespace(w, r, filerClient)

	// Table operations
	case "CreateTable":
		err = h.handleCreateTable(w, r, filerClient)
	case "RegisterTable":
		err = h.handleRegisterTable(w, r, filerClient)
	case "GetTable":
		err = h.handleGetTable(w, r, filerClient)
	case "ListTables":
		err = h.handleListTables(w, r, filerClient)
	case "UpdateTable":
		err = h.handleUpdateTable(w, r, filerClient)
	case "DeleteTable":
		err = h.handleDeleteTable(w, r, filerClient)
	case "RenameTable":
		err = h.handleRenameTable(w, r, filerClient)

	// View operations
	case "CreateView":
		err = h.handleCreateView(w, r, filerClient)
	case "GetView":
		err = h.handleGetView(w, r, filerClient)
	case "ListViews":
		err = h.handleListViews(w, r, filerClient)
	case "UpdateView":
		err = h.handleUpdateView(w, r, filerClient)
	case "DeleteView":
		err = h.handleDeleteView(w, r, filerClient)
	case "RenameView":
		err = h.handleRenameView(w, r, filerClient)

	// Table Policy operations
	case "PutTablePolicy":
		err = h.handlePutTablePolicy(w, r, filerClient)
	case "GetTablePolicy":
		err = h.handleGetTablePolicy(w, r, filerClient)
	case "DeleteTablePolicy":
		err = h.handleDeleteTablePolicy(w, r, filerClient)

	// Maintenance configuration operations
	case "PutTableBucketMaintenanceConfiguration":
		err = h.handlePutTableBucketMaintenanceConfiguration(w, r, filerClient)
	case "GetTableBucketMaintenanceConfiguration":
		err = h.handleGetTableBucketMaintenanceConfiguration(w, r, filerClient)
	case "PutTableMaintenanceConfiguration":
		err = h.handlePutTableMaintenanceConfiguration(w, r, filerClient)
	case "GetTableMaintenanceConfiguration":
		err = h.handleGetTableMaintenanceConfiguration(w, r, filerClient)
	case "GetTableMaintenanceJobStatus":
		err = h.handleGetTableMaintenanceJobStatus(w, r, filerClient)

	// Tagging operations
	case "TagResource":
		err = h.handleTagResource(w, r, filerClient)
	case "ListTagsForResource":
		err = h.handleListTagsForResource(w, r, filerClient)
	case "UntagResource":
		err = h.handleUntagResource(w, r, filerClient)

	default:
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, fmt.Sprintf("Unknown operation: %s", operation))
		return
	}

	if fence != nil {
		fence.settle()
	}
	if err != nil {
		logRequestError(operation, sw.status, err)
	}
}

// A refusal the handler answered with a 4xx (access denied, not authorized,
// not found, already exists, not empty) is the API working as intended, not a
// server fault; it is logged at V(1). A 5xx, or an error returned without any
// status written, stays at ERROR.
var (
	logClientRefusal = func(format string, args ...interface{}) { glog.V(1).Infof(format, args...) }
	logServerError   = glog.Errorf
)

func logRequestError(operation string, status int, err error) {
	if status >= 400 && status < 500 {
		logClientRefusal("S3Tables: %s refused (%d): %v", operation, status, err)
		return
	}
	logServerError("S3Tables: error handling %s: %v", operation, err)
}

// statusRecorder remembers the first status written through it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap lets http.NewResponseController reach the underlying writer, so it
// flushes (or reports ErrNotSupported) exactly as that writer does.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Principal/authorization helpers

// getAccountID returns a stable caller identifier for ownership and permission checks.
// Reflection depends on the identity shape produced by JWT/STS auth (Account *struct{Id string},
// Claims map[string]interface{} containing string values for preferred_username/sub, and optional
// identity name/header values). Changing those fields without updating the reflection here will
// break the handler, so refactorers should replace this with a typed interface if needed.
func (h *S3TablesHandler) getAccountID(r *http.Request) string {
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw != nil {
		// Use reflection to access identity fields and avoid import cycles.
		val := reflect.ValueOf(identityRaw)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Kind() == reflect.Struct {
			// Prefer stable claims from JWT/STS identities. Only "sub" is guaranteed durable per OIDC;
			// preferred_username is ergonomic but can rotate and may orphan ownership data, while email
			// is explicitly excluded to avoid storing PII in metadata.
			if claims := identityClaimsMap(val); claims != nil {
				// aws:userid is the issuer-namespaced (sub, iss) hash; a bare
				// sub or preferred_username collides across providers.
				for _, claimKey := range []string{"aws:userid", "sub", "preferred_username"} {
					if principal := normalizePrincipalID(claimString(claims, claimKey)); principal != "" {
						return principal
					}
				}
			}

			accountField := val.FieldByName("Account")
			if accountField.IsValid() && !accountField.IsNil() {
				accountVal := accountField.Elem()
				if accountVal.Kind() == reflect.Struct {
					idField := accountVal.FieldByName("Id")
					if idField.IsValid() && idField.Kind() == reflect.String {
						if principal := normalizePrincipalID(idField.String()); principal != "" {
							// Account-less identities default to the admin account; only
							// keep it for real admins, else use the unique identity name.
							if principal != s3_constants.AccountAdminId || hasAdminAction(getIdentityActions(r)) {
								return principal
							}
						}
					}
				}
			}
		}
	}

	if identityName := s3_constants.GetIdentityNameFromContext(r); identityName != "" {
		if principal := normalizePrincipalID(identityName); principal != "" {
			return principal
		}
	}

	if accountID := r.Header.Get(s3_constants.AmzAccountId); accountID != "" {
		if principal := normalizePrincipalID(accountID); principal != "" {
			if principal != s3_constants.AccountAdminId || hasAdminAction(getIdentityActions(r)) {
				return principal
			}
		}
	}
	return h.accountID
}

// identityClaimsMap extracts the Claims map from an identity value, using
// reflection to avoid an import cycle with the s3api identity type.
func identityClaimsMap(val reflect.Value) map[string]interface{} {
	claimsField := val.FieldByName("Claims")
	if !claimsField.IsValid() || claimsField.Kind() != reflect.Map || claimsField.IsNil() || claimsField.Type().Key().Kind() != reflect.String {
		return nil
	}
	claims, ok := claimsField.Interface().(map[string]interface{})
	if !ok {
		return nil
	}
	return claims
}

func claimString(claims map[string]interface{}, key string) string {
	v, ok := claims[key]
	if !ok || v == nil {
		return ""
	}
	str, _ := v.(string)
	return str
}

// trustedIssuers returns the OIDC issuers the auth layer asserted into the
// identity's claims; they re-anchor legacy owner checks in checkPermission.
func (h *S3TablesHandler) trustedIssuers(r *http.Request) []string {
	identityRaw := s3_constants.GetIdentityFromContext(r)
	if identityRaw == nil {
		return nil
	}
	val := reflect.ValueOf(identityRaw)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}
	return strings.Fields(claimString(identityClaimsMap(val), "seaweed:oidc-issuers"))
}

// normalizePrincipalID collapses ARN and identity strings to a key that is stable within a single account.
// WARNING: this assumes identity names are unique per account; distinct principals such as
// arn:aws:iam::111:user/alice and arn:aws:iam::222:user/alice will both normalize to "alice".
// If future work adds multi-account support, revisit this function to include the account ID or full ARN
// so ownership checks remain correct.
func normalizePrincipalID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	// If this is an ARN (common for assumed roles), use the trailing segment as a
	// stable-ish principal key instead of embedding the full ARN in ownership fields.
	if strings.HasPrefix(id, "arn:") {
		if idx := strings.LastIndex(id, "/"); idx >= 0 && idx+1 < len(id) {
			return strings.TrimSpace(id[idx+1:])
		}
		if idx := strings.LastIndex(id, ":"); idx >= 0 && idx+1 < len(id) {
			return strings.TrimSpace(id[idx+1:])
		}
		return strings.TrimSpace(id)
	}
	return id
}

// getIdentityActions extracts the action list from the identity object in the request context.
// Uses reflection to avoid import cycles with s3api package.
func getIdentityActions(r *http.Request) []string {
	val, ok := getIdentityStructValue(r)
	if !ok {
		return nil
	}
	actionsField := val.FieldByName("Actions")
	if !actionsField.IsValid() || actionsField.Kind() != reflect.Slice {
		return nil
	}

	// Convert actions to string slice
	actions := make([]string, actionsField.Len())
	for i := 0; i < actionsField.Len(); i++ {
		action := actionsField.Index(i)
		// Action is likely a custom type (e.g., type Action string)
		// Convert to string using String() or direct string conversion
		if action.Kind() == reflect.String {
			actions[i] = action.String()
		} else if action.CanInterface() {
			// Try to convert via fmt.Sprint
			actions[i] = fmt.Sprint(action.Interface())
		}
	}
	return actions
}

// Request/Response helpers

func (h *S3TablesHandler) readRequestBody(r *http.Request, v interface{}) error {
	defer r.Body.Close()

	// Limit request body size to prevent unbounded reads
	limitedReader := io.LimitReader(r.Body, MaxRequestBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}

	// Check if body exceeds size limit
	if len(body) > MaxRequestBodySize {
		return fmt.Errorf("request body too large: exceeds maximum size of %d bytes", MaxRequestBodySize)
	}

	if len(body) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("failed to decode request: %w", err)
	}

	return nil
}

// Response writing helpers

func (h *S3TablesHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	if data != nil {
		if err := json.NewEncoder(w).Encode(data); err != nil {
			glog.Errorf("S3Tables: failed to encode response: %v", err)
		}
	}
}

func (h *S3TablesHandler) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	errorResponse := map[string]interface{}{
		"__type":  code,
		"message": message,
	}
	if err := json.NewEncoder(w).Encode(errorResponse); err != nil {
		glog.Errorf("S3Tables: failed to encode error response: %v", err)
	}
}

// ARN generation helpers

func (h *S3TablesHandler) generateTableBucketARN(ownerAccountID, bucketName string) string {
	return buildARN(h.region, ownerAccountID, fmt.Sprintf("bucket/%s", bucketName))
}

func (h *S3TablesHandler) generateTableARN(ownerAccountID, bucketName, tableID string) string {
	return buildARN(h.region, ownerAccountID, fmt.Sprintf("bucket/%s/table/%s", bucketName, tableID))
}

func (h *S3TablesHandler) generateViewARN(ownerAccountID, bucketName, viewID string) string {
	return buildARN(h.region, ownerAccountID, fmt.Sprintf("bucket/%s/view/%s", bucketName, viewID))
}

// generateS3BucketARN builds the plain S3 ARN used for IAM resource matching.
func (h *S3TablesHandler) generateS3BucketARN(bucketName string) string {
	return fmt.Sprintf("arn:%s:s3:::%s", arnPartitionForRegion(h.region), bucketName)
}

func isAuthError(err error) bool {
	var authErr *AuthError
	return errors.As(err, &authErr) || errors.Is(err, ErrAccessDenied)
}

// authorizeAbsentTarget reports whether the caller may learn the target
// resolved to nothing, so a caller without a grant gets the same response a
// denied existing target would give. The bucket named by the target still
// applies whatever metadata and policy load; with nothing readable to
// evaluate, only an identity-level grant on the operation, the admin, or a
// default-allow deployment passes.
func (h *S3TablesHandler) authorizeAbsentTarget(r *http.Request, filerClient FilerClient, operation, bucketName, namespace, name, resourceARN string) bool {
	principal := h.getAccountID(r)
	if principal == "" {
		return false
	}

	var bucketOwner, bucketPolicy string
	var bucketTags map[string]string
	if bucketName != "" {
		bucketPath := GetTableBucketPath(bucketName)
		_ = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			if data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata); err == nil {
				var meta tableBucketMetadata
				if json.Unmarshal(data, &meta) == nil {
					bucketOwner = meta.OwnerAccountID
				}
			}
			if policy, err := h.readPolicy(r, client, bucketPath); err == nil {
				bucketPolicy = policy
			}
			bucketTags, _ = h.readTags(r.Context(), client, bucketPath)
			return nil
		})
	}

	bucketARN := h.generateTableBucketARN(bucketOwner, bucketName)
	// Judge the target by the canonical ARN the surviving bucket resolves to:
	// the request ARN's account and region are the caller's to choose and must
	// not decide whether a policy Deny applies.
	canonicalARN := resourceARN
	if bucketOwner != "" {
		if name != "" {
			canonicalARN = h.generateTableARN(bucketOwner, bucketName, namespace+"/"+name)
		} else {
			canonicalARN = bucketARN
		}
	}
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespace,
		TableName:        name,
		TableBucketTags:  bucketTags,
		IdentityActions:  getIdentityActions(r),
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if resourceARN != "" &&
		matchesDenyStatement(operation, principal, bucketPolicy, canonicalARN, ctx) {
		return false
	}
	if CheckTableOrBucketPermission(operation, principal, "", "", canonicalARN,
		bucketOwner, bucketPolicy, bucketARN, ctx) {
		return true
	}
	return bucketOwner == "" && checkPermission(operation, principal, "", "", resourceARN, ctx)
}

func (h *S3TablesHandler) readTags(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) (map[string]string, error) {
	data, err := h.getExtendedAttribute(ctx, client, path, ExtendedKeyTags)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return nil, nil
		}
		return nil, err
	}
	tags := make(map[string]string)
	if err := json.Unmarshal(data, &tags); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tags: %w", err)
	}
	return tags, nil
}

func mapKeys(tags map[string]string) []string {
	if len(tags) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	return keys
}
