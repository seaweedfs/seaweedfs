package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// handleCreateTableBucket creates a new table bucket
func (h *S3TablesHandler) handleCreateTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req CreateTableBucketRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Validate bucket name
	if err := validateBucketName(req.Name); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// A bucket is a catalog, and a catalog serves one protocol. Saying which one
	// at creation is what lets everything downstream - the endpoint the UI
	// shows, the tables the bucket accepts - be answered without opening a table.
	bucketFormat := FormatIceberg
	if req.Format != "" {
		normalized, ok := NormalizeFormat(req.Format)
		if !ok {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, fmt.Sprintf("unsupported format %q", req.Format))
			return fmt.Errorf("invalid format")
		}
		bucketFormat = normalized
	}

	if err := ValidateEncryptionConfiguration(req.EncryptionConfiguration, "sseAlgorithm"); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	identityPolicyNames := getIdentityPolicyNames(r)
	useIAM := h.shouldUseIAM(r, identityActions, identityPolicyNames)
	useLegacy := !useIAM
	if useIAM {
		allowed, err := h.authorizeIAMAction(r, identityPolicyNames, "CreateTableBucket", h.generateTableBucketARN(principal, req.Name), h.generateS3BucketARN(req.Name))
		if err != nil {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table buckets")
			return NewAuthError("CreateTableBucket", principal, "not authorized to create table buckets")
		}
		if !allowed {
			if h.defaultAllow && len(identityActions) == 0 && len(identityPolicyNames) == 0 {
				useLegacy = true
			} else {
				h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table buckets")
				return NewAuthError("CreateTableBucket", principal, "not authorized to create table buckets")
			}
		}
	}
	if useLegacy {
		owner := h.accountID
		if owner == "" {
			owner = DefaultAccountID
		}
		if !CheckPermissionWithContext("CreateTableBucket", principal, owner, "", "", &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table buckets")
			return NewAuthError("CreateTableBucket", principal, "not authorized to create table buckets")
		}
	}

	if len(req.EncryptionConfiguration) > 0 && IsCatalogOnlyFormat(bucketFormat) && h.canEncrypt != nil && !h.canEncrypt() {
		msg := "AES256 encryption needs an SSE-S3 key on the S3 gateway: set s3.sse.kek or s3.sse.key in security.toml"
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, msg)
		return errors.New(msg)
	}

	bucketPath := GetTableBucketPath(req.Name)

	// Check if bucket already exists and ensure no conflict with object store buckets
	tableBucketExists := false
	s3BucketExists := false
	bucketsPath := s3_constants.DefaultBucketsPath
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(r.Context(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		if resp.DirBuckets != "" {
			bucketsPath = resp.DirBuckets
		}
		entryResp, err := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: bucketsPath,
			Name:      req.Name,
		})
		if err != nil {
			if !errors.Is(err, filer_pb.ErrNotFound) {
				return err
			}
			return nil
		}
		if entryResp != nil && entryResp.Entry != nil {
			if IsTableBucketEntry(entryResp.Entry) {
				tableBucketExists = true
			} else {
				s3BucketExists = true
			}
		}
		return nil
	})

	if err != nil {
		glog.Errorf("S3Tables: failed to check bucket existence: %v", err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to check bucket existence")
		return err
	}

	if tableBucketExists {
		h.writeError(w, http.StatusConflict, ErrCodeBucketAlreadyExists, fmt.Sprintf("table bucket %s already exists", req.Name))
		return fmt.Errorf("bucket already exists")
	}
	if s3BucketExists {
		h.writeError(w, http.StatusConflict, ErrCodeBucketAlreadyExists, fmt.Sprintf("bucket %s already exists", req.Name))
		return fmt.Errorf("bucket already exists")
	}

	// Create the bucket directory and set metadata as extended attributes
	now := time.Now()
	metadata := &tableBucketMetadata{
		Name:                    req.Name,
		CreatedAt:               now,
		OwnerAccountID:          principal,
		Format:                  bucketFormat,
		EncryptionConfiguration: req.EncryptionConfiguration,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		glog.Errorf("S3Tables: failed to marshal metadata: %v", err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal metadata")
		return err
	}

	var taken error
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Ensure root tables directory exists
		if !h.entryExists(r.Context(), client, TablesPath) {
			if err := h.createDirectory(r.Context(), client, TablesPath); err != nil {
				return fmt.Errorf("failed to create root tables directory: %w", err)
			}
		}

		extended := map[string][]byte{
			ExtendedKeyTableBucket: []byte("true"),
			ExtendedKeyMetadata:    metadataBytes,
		}
		if len(req.Tags) > 0 {
			tagsBytes, err := json.Marshal(req.Tags)
			if err != nil {
				return fmt.Errorf("failed to marshal tags: %w", err)
			}
			extended[ExtendedKeyTags] = tagsBytes
		}
		if err := h.createDirectoryExclusive(r.Context(), client, bucketPath, extended); err != nil {
			if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
				taken = err // an answer, not a filer failure
				return nil
			}
			return err
		}

		return nil
	})
	if err == nil && taken != nil {
		err = taken
	}

	if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		h.writeError(w, http.StatusConflict, ErrCodeBucketAlreadyExists, fmt.Sprintf("table bucket %s already exists", req.Name))
		return err
	}
	if err != nil {
		glog.Errorf("S3Tables: failed to create table bucket %s: %v", req.Name, err)
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create table bucket")
		return err
	}

	resp := &CreateTableBucketResponse{
		ARN: h.generateTableBucketARN(metadata.OwnerAccountID, req.Name),
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}
