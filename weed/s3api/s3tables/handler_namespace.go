package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// errOwnerlessBucket marks a table bucket that has the marker but no
// metadata: damaged, and answered with the operation's own denial rather than
// letting the caller name itself the bucket's owner before any check runs.
var errOwnerlessBucket = fmt.Errorf("table bucket has no owner metadata")

// mayRepairOwnerlessBucket reports whether the caller may repair a bucket in
// that state: the admin or a trusted internal handler. hasAdminAction also
// answers s3tables:*, an operation grant, not administrator status.
func (h *S3TablesHandler) mayRepairOwnerlessBucket(r *http.Request) bool {
	if h.trusted || h.getAccountID(r) == s3_constants.AccountAdminId {
		return true
	}
	for _, action := range getIdentityActions(r) {
		if action == s3_constants.ACTION_ADMIN {
			return true
		}
	}
	return false
}

// handleCreateNamespace creates a new namespace in a table bucket
func (h *S3TablesHandler) handleCreateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	glog.V(3).Infof("S3Tables: handleCreateNamespace called")
	var req CreateNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	if len(req.Namespace) == 0 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "namespace is required")
		return fmt.Errorf("namespace is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Check if table bucket exists
	bucketPath := GetTableBucketPath(bucketName)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	ownerAccountID := h.getAccountID(r)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				dir, name := splitPath(bucketPath)
				entryResp, lookupErr := filer_pb.LookupEntry(r.Context(), client, &filer_pb.LookupDirectoryEntryRequest{
					Directory: dir,
					Name:      name,
				})
				if lookupErr != nil {
					return lookupErr
				}
				if entryResp.Entry == nil || !IsTableBucketEntry(entryResp.Entry) {
					return filer_pb.ErrNotFound
				}
				if !h.mayRepairOwnerlessBucket(r) {
					return errOwnerlessBucket
				}
				// Repair the marker-only entry under the entry lock so a
				// creator finishing its own create in the meantime keeps its
				// owner: adopt whatever metadata is there, write only when it
				// is still absent.
				if err := h.mutateEntryExtended(r.Context(), client, bucketPath, func(extended map[string][]byte) error {
					if data, ok := extended[ExtendedKeyMetadata]; ok {
						return json.Unmarshal(data, &bucketMetadata)
					}
					bucketMetadata = tableBucketMetadata{
						Name:           bucketName,
						CreatedAt:      time.Now(),
						OwnerAccountID: ownerAccountID,
					}
					metadataBytes, err := json.Marshal(&bucketMetadata)
					if err != nil {
						return fmt.Errorf("failed to marshal bucket metadata: %w", err)
					}
					extended[ExtendedKeyMetadata] = metadataBytes
					return nil
				}); err != nil {
					return err
				}
			} else {
				return err
			}
		} else if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, errOwnerlessBucket) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
			return ErrAccessDenied
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			if !h.authorizeAbsentTarget(r, filerClient, "CreateNamespace", bucketName, namespaceName, "", "") {
				h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
				return ErrAccessDenied
			}
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table bucket: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	glog.Infof("S3Tables: CreateNamespace permission check - principal=%s, owner=%s, actions=%v", principal, bucketMetadata.OwnerAccountID, identityActions)
	if !CheckPermissionWithContext("CreateNamespace", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		glog.Infof("S3Tables: Permission denied for CreateNamespace - principal=%s, owner=%s", principal, bucketMetadata.OwnerAccountID)
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create namespace in this bucket")
		return ErrAccessDenied
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	// The authorization above holds for this bucket only. One deleted and
	// created again under the same name since then is another bucket, and is
	// treated as gone by both checks below.
	isAuthorizedBucket := isTableBucketCreatedAt(bucketMetadata.CreatedAt)

	// A delete that has claimed the bucket is scanning it for namespaces, so
	// refuse before writing anything. Checked after the authorization above, so
	// denied callers never see the delete.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.checkParentOpen(r.Context(), client, bucketPath, isAuthorizedBucket)
	})
	if h.writeBucketClosedError(w, err, bucketName) {
		return err
	}

	// Check if namespace already exists. One still marked unfinished does not:
	// its create either confirmed it or lost it, and the mark left behind would
	// wedge the name, so this create takes it over the way a table's does.
	exists := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, namespacePath)
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		_, hasMetadata := entry.Extended[ExtendedKeyMetadata]
		exists = hasMetadata && !isUnfinishedCreate(entry.Extended)
		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		return err
	}
	if exists {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return fmt.Errorf("namespace already exists")
	}

	// Create the namespace with bucket owner to maintain consistency
	// (authorization above ensures the caller has permission to create in this bucket)
	now := time.Now()
	metadata := &namespaceMetadata{
		Namespace:      req.Namespace,
		CreatedAt:      now,
		OwnerAccountID: bucketMetadata.OwnerAccountID,
		Properties:     req.Properties,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// The unfinished mark keeps a table or view from being created in the
	// namespace until the bucket check below has passed: one created before it
	// would be left under a namespace that check then backs out.
	attrs := map[string][]byte{
		ExtendedKeyMetadata: metadataBytes,
		ExtendedKeyDeleting: []byte(createClaim),
	}
	var taken error
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := h.createDirectoryExclusive(r.Context(), client, namespacePath, attrs)
		if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
			// A directory is already there. If it is a leftover — a delete kept
			// it for the data under it and cleared its catalog attributes —
			// take it back; claimCatalogEntry refuses a real namespace.
			err = h.retakeDirectory(r.Context(), client, namespacePath, attrs)
			if errors.Is(err, ErrNameTaken) {
				taken = filer_pb.ErrEntryAlreadyExists // an answer, not a filer failure
				return nil
			}
		}
		if err != nil {
			return err
		}
		// A delete may have claimed the bucket since the check above, after it
		// scanned for namespaces and before this write. Confirm the bucket once
		// more now that the namespace is there: a delete claiming it later scans
		// the namespace, marked or not, and one that claimed it already makes
		// this back out what it wrote. The mark comes off only after that check
		// and the bucket is not checked again: a table created in the window
		// between the two would be left under a namespace the second check backs out.
		// A bucket recreated since the authorization fails the check as gone, so
		// the namespace is backed out of it rather than left with this owner.
		return h.confirmParentOnce(r.Context(), client, bucketPath, isAuthorizedBucket, namespacePath, attrs, namespacePath)
	})
	if err == nil && taken != nil {
		err = taken
	}
	if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return err
	}
	if errors.Is(err, ErrConcurrentUpdate) || errors.Is(err, filer_pb.ErrNotFound) {
		h.writeError(w, http.StatusConflict, ErrCodeConflict, "namespace changed during create, retry the request")
		return err
	}
	if h.writeBucketClosedError(w, err, bucketName) {
		return err
	}
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create namespace")
		return err
	}

	resp := &CreateNamespaceResponse{
		Namespace:      req.Namespace,
		TableBucketARN: req.TableBucketARN,
		Properties:     req.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// writeBucketClosedError answers a create refused because its bucket is being
// deleted or was deleted, and reports whether err was one of those.
func (h *S3TablesHandler) writeBucketClosedError(w http.ResponseWriter, err error, bucketName string) bool {
	switch {
	case errors.Is(err, errParentDeleting):
		h.writeError(w, http.StatusConflict, ErrCodeConflict, fmt.Sprintf("table bucket %s is being deleted", bucketName))
	case errors.Is(err, errParentGone):
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
	case errors.Is(err, errBackOutFailed):
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("table bucket %s was deleted during the request and the new namespace may remain", bucketName))
	default:
		return false
	}
	return true
}

// handleGetNamespace gets details of a namespace
func (h *S3TablesHandler) handleGetNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Get namespace and bucket policy
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readNamespaceMetadataAttr(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	resp := &GetNamespaceResponse{
		Namespace:      metadata.Namespace,
		CreatedAt:      metadata.CreatedAt,
		OwnerAccountID: metadata.OwnerAccountID,
		Properties:     metadata.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleUpdateNamespace replaces the stored properties of an existing namespace.
// AWS S3 Tables namespaces have no properties; this backs the Iceberg REST
// catalog's namespace-property updates, which carry the merged map.
func (h *S3TablesHandler) handleUpdateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req UpdateNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readNamespaceMetadataAttr(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return err
		}

		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("UpdateNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	metadata.Properties = req.Properties
	metadataBytes, err := json.Marshal(&metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, namespacePath, func(extended map[string][]byte) error {
			extended[ExtendedKeyMetadata] = metadataBytes
			return nil
		})
	})
	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "namespace changed during update, retry the request")
			return err
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to update namespace")
		return err
	}

	resp := &UpdateNamespaceResponse{
		Namespace:      metadata.Namespace,
		TableBucketARN: req.TableBucketARN,
		Properties:     metadata.Properties,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListNamespaces lists all namespaces in a table bucket
func (h *S3TablesHandler) handleListNamespaces(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListNamespacesRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	maxNamespaces := req.MaxNamespaces
	if maxNamespaces <= 0 {
		maxNamespaces = 100
	}

	bucketPath := GetTableBucketPath(bucketName)

	// Check permission (check bucket ownership)
	var bucketMetadata tableBucketMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &bucketMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("ListNamespaces", principal, bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return ErrAccessDenied
	}

	var namespaces []NamespaceSummary

	lastFileName := req.ContinuationToken
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(namespaces) < maxNamespaces {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
				Limit:              uint32(maxNamespaces * 2),
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "" || lastFileName == req.ContinuationToken,
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, respErr := resp.Recv()
				if respErr != nil {
					if respErr == io.EOF {
						break
					}
					return respErr
				}
				if entry.Entry == nil {
					continue
				}

				// Skip the start item if it was included in the previous page
				if len(namespaces) == 0 && req.ContinuationToken != "" && entry.Entry.Name == req.ContinuationToken {
					continue
				}

				hasMore = true
				lastFileName = entry.Entry.Name

				if !entry.Entry.IsDirectory {
					continue
				}

				// Skip hidden entries
				if strings.HasPrefix(entry.Entry.Name, ".") {
					continue
				}

				// Apply prefix filter
				if req.Prefix != "" && !strings.HasPrefix(entry.Entry.Name, req.Prefix) {
					continue
				}

				// Read metadata from extended attribute. A namespace its create has
				// not confirmed is not one yet.
				data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
				if !ok || isUnfinishedCreate(entry.Entry.Extended) {
					continue
				}

				var metadata namespaceMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					continue
				}

				if metadata.OwnerAccountID != bucketMetadata.OwnerAccountID {
					continue
				}

				namespaces = append(namespaces, NamespaceSummary{
					Namespace: metadata.Namespace,
					CreatedAt: metadata.CreatedAt,
				})

				if len(namespaces) >= maxNamespaces {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			namespaces = []NamespaceSummary{}
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespaces: %v", err))
			return err
		}
	}

	paginationToken := ""
	if len(namespaces) >= maxNamespaces {
		paginationToken = lastFileName
	}

	resp := &ListNamespacesResponse{
		Namespaces:        namespaces,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteNamespace deletes a namespace from a table bucket
func (h *S3TablesHandler) handleDeleteNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req DeleteNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := GetNamespacePath(bucketName, namespaceName)
	bucketPath := GetTableBucketPath(bucketName)

	// Check if namespace exists and get metadata for permission check
	var metadata namespaceMetadata
	var bucketPolicy string
	var bucketTags map[string]string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readNamespaceMetadataAttr(r.Context(), client, namespacePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get namespace metadata: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("DeleteNamespace", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		return ErrAccessDenied
	}

	// Claim before scanning, so a create that writes after the scan sees it.
	// A namespace whose create has not confirmed it is absent, and claiming it
	// would replace the create's mark: a refused delete then lifts the claim and
	// leaves the namespace looking confirmed before its create checked the bucket.
	var claim string
	var claimed map[string][]byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var claimErr error
		claim, claimed, claimErr = h.claimForDelete(r.Context(), client, namespacePath, isFinishedNamespaceEntry)
		return claimErr
	})
	if err != nil {
		switch {
		case errors.Is(err, filer_pb.ErrNotFound):
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		case errors.Is(err, ErrConcurrentUpdate):
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "namespace changed during delete, retry the request")
		default:
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to claim namespace for delete: %v", err))
		}
		return err
	}
	releaseClaim := func() {
		_ = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			h.releaseDeleteClaim(r.Context(), client, namespacePath, claim)
			return nil
		})
	}

	// Check if namespace is empty
	hasChildren := false
	keepDir := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		lastFileName := ""
		for !hasChildren {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          namespacePath,
				Limit:              10,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "",
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, err := resp.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				if entry.Entry == nil {
					continue
				}
				hasMore = true
				lastFileName = entry.Entry.Name
				// Hidden entries are never children.
				if !strings.HasPrefix(entry.Entry.Name, ".") {
					// A record still marked by its create is absent to readers and
					// cannot confirm under the claim this delete holds, so it is no
					// child — the delete removes it with the namespace.
					if isUnfinishedCreate(entry.Entry.Extended) {
						continue
					}
					// Only consider it a child if it's a table (has metadata), a file,
					// or a record an in-flight operation claimed — a rename's cleared
					// source still holds its claim, and removing its directory takes
					// the data the destination keeps pointing at. Bare directories
					// left by dropped tables or S3 clients should not prevent the
					// namespace from being deleted.
					_, hasMetadata := entry.Entry.Extended[ExtendedKeyMetadata]
					_, claimed := entry.Entry.Extended[ExtendedKeyDeleting]
					if hasMetadata || claimed || !entry.Entry.IsDirectory {
						hasChildren = true
						break
					}
				}
				// A bare directory with files under it, hidden or not, is a
				// renamed-away table's data, which the destination still points
				// at — a table's location may sit under any directory of the
				// namespace: the namespace directory stays, minus its catalog
				// record. One such directory settles it, so the rest are not walked.
				if entry.Entry.IsDirectory && !keepDir {
					holdsFiles, err := h.dirHoldsFiles(r.Context(), client, path.Join(namespacePath, entry.Entry.Name))
					if err != nil {
						return err
					}
					keepDir = holdsFiles
				}
			}
			if !hasMore {
				break
			}
		}

		return nil
	})

	if err != nil {
		releaseClaim()
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", flattenNamespace(req.Namespace)))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list namespace entries: %v", err))
		}
		return err
	}

	if hasChildren {
		releaseClaim()
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceNotEmpty, "namespace is not empty")
		return fmt.Errorf("namespace not empty")
	}

	// Delete the namespace, or stop it being one while the data under it stays.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if keepDir {
			return h.keepAsLeftover(r.Context(), client, namespacePath, claimed)
		}
		return h.deleteDirectoryIfMatches(r.Context(), client, namespacePath, &filer_pb.Entry{Extended: claimed})
	})

	if err != nil {
		releaseClaim()
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "namespace changed during delete, retry the request")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete namespace")
		}
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
