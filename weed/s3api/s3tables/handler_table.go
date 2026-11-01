package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateTable creates a new table in a namespace
func (h *S3TablesHandler) handleCreateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req CreateTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.Format == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "format is required")
		return fmt.Errorf("format is required")
	}

	// Validate format
	if req.Format != FormatIceberg && req.Format != FormatLance {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, fmt.Sprintf("unsupported format %q", req.Format))
		return fmt.Errorf("invalid format")
	}

	target, err := h.authorizeCreateTable(w, r, filerClient, req.TableBucketARN, req.Namespace, req.Name, req.Tags, false)
	if err != nil {
		return err
	}
	bucketName, namespaceName, tableName := target.bucketName, target.namespaceName, target.tableName

	// A bucket declares the format it holds, and a table of another format would
	// be invisible to the catalog serving it. A bucket made before the
	// declaration existed has none, and keeps taking anything.
	if target.bucketFormat != "" && target.bucketFormat != req.Format {
		message := fmt.Sprintf("table bucket %s holds %s tables", bucketName, target.bucketFormat)
		h.writeError(w, http.StatusConflict, ErrCodeConflict, message)
		return fmt.Errorf("%s", message)
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)

	// Check if a table or view already exists at this name. Names are unique
	// across tables and views in a namespace.
	var existingMetadata tableMetadataInternal
	var existingIsView bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if isUnfinishedCreate(entry.Extended) {
			// Another create wrote this record and may still back it out, so
			// it is not an existing table; the claim below refuses the name.
			return filer_pb.ErrNotFound
		}
		if EntryType(entry.Extended) == EntryTypeView {
			existingIsView = true
			return nil
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return fmt.Errorf("%w: %s", ErrAttributeNotFound, ExtendedKeyMetadata)
		}
		if unmarshalErr := json.Unmarshal(data, &existingMetadata); unmarshalErr != nil {
			return fmt.Errorf("failed to parse existing table metadata: %w", unmarshalErr)
		}
		return nil
	})

	if err == nil {
		if existingIsView {
			h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("a view named %s already exists", tableName))
			return fmt.Errorf("view name conflict: %s", tableName)
		}
		// Creating a table that already exists is idempotent, but only for the
		// same format. Handing a Lance client an Iceberg table's location, or the
		// reverse, has it write one format's files into the other's directory.
		if existingMetadata.Format != "" && existingMetadata.Format != req.Format {
			h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists,
				fmt.Sprintf("a %s table named %s already exists", existingMetadata.Format, tableName))
			return fmt.Errorf("format conflict: %s", tableName)
		}
		tableARN := h.generateTableARN(existingMetadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
		h.writeJSON(w, http.StatusOK, &CreateTableResponse{
			TableARN:         tableARN,
			VersionToken:     existingMetadata.VersionToken,
			MetadataLocation: existingMetadata.MetadataLocation,
		})
		return nil
	} else if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrAttributeNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		return err
	}

	if err := ValidateMetadataLocation(req.MetadataLocation, bucketName, namespaceName); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Create the table
	now := time.Now()
	versionToken := generateVersionToken()

	metadata := &tableMetadataInternal{
		Name:             tableName,
		Namespace:        namespaceName,
		Format:           req.Format,
		CreatedAt:        now,
		ModifiedAt:       now,
		OwnerAccountID:   target.ownerAccountID, // Inherit namespace owner for consistency
		VersionToken:     versionToken,
		MetadataVersion:  max(req.MetadataVersion, 1),
		MetadataLocation: req.MetadataLocation,
		Metadata:         req.Metadata,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal table metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	var tagsBytes []byte
	if len(req.Tags) > 0 {
		if tagsBytes, err = json.Marshal(req.Tags); err != nil {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal tags")
			return fmt.Errorf("failed to marshal tags: %w", err)
		}
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Ensure table directory exists (may already be created by object storage clients)
		if err := h.ensureDirectory(r.Context(), client, tablePath); err != nil {
			return err
		}

		// Create data subdirectory for Iceberg files
		dataPath := tablePath + "/data"
		if err := h.ensureDirectory(r.Context(), client, dataPath); err != nil {
			return err
		}

		attrs := map[string][]byte{
			ExtendedKeyMetadata:  metadataBytes,
			ExtendedKeyEntryType: []byte(EntryTypeTable),
			ExtendedKeyDeleting:  []byte(createClaim),
		}
		if len(tagsBytes) > 0 {
			attrs[ExtendedKeyTags] = tagsBytes
		}
		if err := h.claimCatalogEntry(r.Context(), client, tablePath, attrs); err != nil {
			return err
		}
		return h.confirmParent(r.Context(), client, GetNamespacePath(bucketName, namespaceName), isNamespaceEntry, tablePath, attrs, dataPath, tablePath)
	})

	if err != nil {
		if errors.Is(err, ErrNameTaken) {
			h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("table %s already exists", tableName))
			return err
		}
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table changed during create, retry the request")
			return err
		}
		if h.writeNamespaceClosedError(w, err, namespaceName) {
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create table")
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)

	resp := &CreateTableResponse{
		TableARN:         tableARN,
		VersionToken:     versionToken,
		MetadataLocation: metadata.MetadataLocation,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// metadataVersionFromLocation parses the version N from a metadata location.
// SeaweedFS writes v{N}.metadata.json; Iceberg engines (Spark/Trino/Flink/Java)
// write {NNNNN}-{uuid}.metadata.json with a zero-padded leading version. Returns
// 1 when no version can be parsed.
func metadataVersionFromLocation(metadataLocation string) int {
	name := metadataLocation
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, ".metadata.json")
	// v{N} form
	if v, err := strconv.Atoi(strings.TrimPrefix(name, "v")); err == nil && v > 0 {
		return v
	}
	// v{N}-{unique} form, written when a commit finds v{N} already staged
	if trimmed := strings.TrimPrefix(name, "v"); trimmed != name {
		if idx := strings.IndexByte(trimmed, '-'); idx != -1 {
			if v, err := strconv.Atoi(trimmed[:idx]); err == nil && v > 0 {
				return v
			}
		}
	}
	// {NNNNN}-{uuid} form: the leading integer before the first '-'
	if idx := strings.IndexByte(name, '-'); idx != -1 {
		if v, err := strconv.Atoi(name[:idx]); err == nil && v > 0 {
			return v
		}
	}
	return 1
}

// handleRegisterTable registers an existing Iceberg metadata.json under a new
// catalog entry. Unlike CreateTable it does not generate metadata: it points the
// table at the caller-supplied MetadataLocation.
func (h *S3TablesHandler) handleRegisterTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req RegisterTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.MetadataLocation == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "metadataLocation is required")
		return fmt.Errorf("metadataLocation is required")
	}

	target, err := h.authorizeCreateTable(w, r, filerClient, req.TableBucketARN, req.Namespace, req.Name, nil, false)
	if err != nil {
		return err
	}
	bucketName, namespaceName, tableName := target.bucketName, target.namespaceName, target.tableName

	if err := ValidateMetadataLocation(req.MetadataLocation, bucketName, namespaceName); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)

	// Table must be absent; a record still marked by its unfinished create is.
	var existingMetadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		entry, err := h.lookupEntry(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if isUnfinishedCreate(entry.Extended) {
			return filer_pb.ErrNotFound
		}
		data, ok := entry.Extended[ExtendedKeyMetadata]
		if !ok {
			return ErrAttributeNotFound
		}
		return json.Unmarshal(data, &existingMetadata)
	})
	if err == nil {
		h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("table %s already exists", tableName))
		return fmt.Errorf("table %s already exists", tableName)
	} else if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrAttributeNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		return err
	}

	now := time.Now()
	versionToken := generateVersionToken()
	metadata := &tableMetadataInternal{
		Name:             tableName,
		Namespace:        namespaceName,
		Format:           FormatIceberg,
		CreatedAt:        now,
		ModifiedAt:       now,
		OwnerAccountID:   target.ownerAccountID,
		VersionToken:     versionToken,
		MetadataVersion:  metadataVersionFromLocation(req.MetadataLocation),
		MetadataLocation: req.MetadataLocation,
		Metadata:         req.Metadata,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal table metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	attrs := map[string][]byte{
		ExtendedKeyMetadata: metadataBytes,
		ExtendedKeyDeleting: []byte(createClaim),
	}
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if err := h.ensureDirectory(r.Context(), client, tablePath); err != nil {
			return err
		}
		if err := h.claimCatalogEntry(r.Context(), client, tablePath, attrs); err != nil {
			return err
		}
		return h.confirmParent(r.Context(), client, GetNamespacePath(bucketName, namespaceName), isNamespaceEntry, tablePath, attrs, tablePath)
	})
	if err != nil {
		if errors.Is(err, ErrNameTaken) {
			h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("table %s already exists", tableName))
			return err
		}
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table changed during register, retry the request")
			return err
		}
		if h.writeNamespaceClosedError(w, err, namespaceName) {
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to register table")
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	h.writeJSON(w, http.StatusOK, &RegisterTableResponse{
		TableARN:         tableARN,
		VersionToken:     versionToken,
		MetadataLocation: metadata.MetadataLocation,
	})
	return nil
}

// handleGetTable gets details of a table
func (h *S3TablesHandler) handleGetTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req GetTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	var bucketName, namespace, tableName string
	var err error

	// Support getting by ARN or by bucket/namespace/name
	if req.TableARN != "" {
		bucketName, namespace, tableName, err = parseTableFromARN(req.TableARN)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	} else if req.TableBucketARN != "" && len(req.Namespace) > 0 && req.Name != "" {
		bucketName, err = parseBucketNameFromARN(req.TableBucketARN)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		namespace, err = validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		tableName, err = validateTableName(req.Name)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	} else {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "either tableARN or (tableBucketARN, namespace, name) is required")
		return fmt.Errorf("missing required parameters")
	}

	tablePath := GetTablePath(bucketName, namespace, tableName)

	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readTableMetadata(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}
		return nil
	})

	if err != nil {
		// A directory without the table-metadata xattr is not a table (e.g. a renamed-away source).
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table: %v", err))
		}
		return err
	}

	// Authorize access to the table using policy framework
	accountID := h.getAccountID(r)
	bucketPath := GetTableBucketPath(bucketName)
	tablePolicy := ""
	bucketPolicy := ""
	bucketTags := map[string]string{}
	tableTags := map[string]string{}
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Fetch bucket metadata to use correct owner for bucket policy evaluation
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %v", err)
		}

		// Fetch table policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err == nil {
			tablePolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch table policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, tablePath); err != nil {
			return err
		} else if tags != nil {
			tableTags = tags
		}

		// Fetch bucket policy if it exists
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		} else if tags != nil {
			bucketTags = tags
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespace+"/"+tableName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	identityActions := getIdentityActions(r)
	allowed := CheckTableOrBucketPermission("GetTable", accountID,
		metadata.OwnerAccountID, tablePolicy, tableARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        namespace,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			ResourceTags:     tableTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		})

	if !allowed {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return ErrAccessDenied
	}

	resp := &GetTableResponse{
		Name:             metadata.Name,
		TableARN:         tableARN,
		Namespace:        expandNamespace(metadata.Namespace),
		Format:           metadata.Format,
		CreatedAt:        metadata.CreatedAt,
		ModifiedAt:       metadata.ModifiedAt,
		OwnerAccountID:   metadata.OwnerAccountID,
		MetadataLocation: metadata.MetadataLocation,
		MetadataVersion:  metadata.MetadataVersion,
		VersionToken:     metadata.VersionToken,
		Metadata:         metadata.Metadata,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListTables lists all tables in a namespace or bucket
func (h *S3TablesHandler) handleListTables(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req ListTablesRequest
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

	maxTables := req.MaxTables
	if maxTables <= 0 {
		maxTables = 100
	}
	// Cap to prevent uint32 overflow when used in uint32(maxTables*2)
	const maxTablesLimit = 1000
	if maxTables > maxTablesLimit {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "MaxTables exceeds maximum allowed value")
		return fmt.Errorf("invalid maxTables value: %d", maxTables)
	}

	// Pre-validate namespace before calling WithFilerClient to return 400 on validation errors
	var namespaceName string
	if len(req.Namespace) > 0 {
		var err error
		namespaceName, err = validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
	}

	var tables []TableSummary
	var paginationToken string

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var err error
		accountID := h.getAccountID(r)

		if len(req.Namespace) > 0 {
			// Namespace has already been validated above
			namespacePath := GetNamespacePath(bucketName, namespaceName)
			bucketPath := GetTableBucketPath(bucketName)
			var nsMeta namespaceMetadata
			var bucketMeta tableBucketMetadata
			var namespacePolicy, bucketPolicy string
			bucketTags := map[string]string{}

			// Fetch namespace metadata and policy
			targetAbsent := false
			data, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
			if err != nil {
				if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrAttributeNotFound) {
					return err
				}
				targetAbsent = true
			} else if err := json.Unmarshal(data, &nsMeta); err != nil {
				return err
			}

			// Fetch namespace policy if it exists
			policyData, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyPolicy)
			if err == nil {
				namespacePolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) && !errors.Is(err, filer_pb.ErrNotFound) {
				return fmt.Errorf("failed to fetch namespace policy: %w", err)
			}

			// Fetch bucket metadata and policy
			data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
			if err == nil {
				if err := json.Unmarshal(data, &bucketMeta); err != nil {
					return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
				}
			} else if errors.Is(err, filer_pb.ErrNotFound) {
				targetAbsent = true
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket metadata: %w", err)
			}

			policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
			if err == nil {
				bucketPolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) && !errors.Is(err, filer_pb.ErrNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
			if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
				if !errors.Is(err, filer_pb.ErrNotFound) {
					return fmt.Errorf("failed to read bucket tags: %w", err)
				}
			} else if tags != nil {
				bucketTags = tags
			}

			if targetAbsent {
				if !h.authorizeAbsentTarget(r, &singleClient{client}, "ListTables", bucketName, namespaceName, "", "") {
					return ErrAccessDenied
				}
				return filer_pb.ErrNotFound
			}

			bucketARN := h.generateTableBucketARN(bucketMeta.OwnerAccountID, bucketName)
			identityActions := getIdentityActions(r)
			if !CheckTableOrBucketPermission("ListTables", accountID,
				nsMeta.OwnerAccountID, namespacePolicy, bucketARN,
				bucketMeta.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
					TrustedIssuers:   h.trustedIssuers(r),
					IsLocalAccountID: h.localAccountIDs,
					TableBucketName:  bucketName,
					Namespace:        namespaceName,
					TableBucketTags:  bucketTags,
					IdentityActions:  identityActions,
					DefaultAllow:     h.defaultAllowFor(r),
				}) {
				return ErrAccessDenied
			}

			tables, paginationToken, err = h.listTablesInNamespaceWithClient(r, client, bucketName, namespaceName, req.Prefix, req.ContinuationToken, maxTables)
			if err != nil {
				return fmt.Errorf("list tables in namespace %v: %w", namespaceName, err)
			}
		} else {
			// List tables across all namespaces in bucket
			bucketPath := GetTableBucketPath(bucketName)
			var bucketMeta tableBucketMetadata
			var bucketPolicy string
			bucketTags := map[string]string{}

			// Fetch bucket metadata and policy
			data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
			if err != nil {
				if !errors.Is(err, filer_pb.ErrNotFound) && !errors.Is(err, ErrAttributeNotFound) {
					return fmt.Errorf("failed to fetch bucket metadata: %w", err)
				}
				if !h.authorizeAbsentTarget(r, &singleClient{client}, "ListTables", bucketName, "", "", "") {
					return ErrAccessDenied
				}
				return filer_pb.ErrNotFound
			}
			if err := json.Unmarshal(data, &bucketMeta); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}

			// Fetch bucket policy if it exists
			policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
			if err == nil {
				bucketPolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
			if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
				return fmt.Errorf("failed to read bucket tags: %w", err)
			} else if tags != nil {
				bucketTags = tags
			}

			bucketARN := h.generateTableBucketARN(bucketMeta.OwnerAccountID, bucketName)
			identityActions := getIdentityActions(r)
			if !CheckPermissionWithContext("ListTables", accountID, bucketMeta.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
				TrustedIssuers:   h.trustedIssuers(r),
				IsLocalAccountID: h.localAccountIDs,
				TableBucketName:  bucketName,
				TableBucketTags:  bucketTags,
				IdentityActions:  identityActions,
				DefaultAllow:     h.defaultAllowFor(r),
			}) {
				return ErrAccessDenied
			}

			tables, paginationToken, err = h.listTablesInAllNamespaces(r, client, bucketName, req.Prefix, req.ContinuationToken, maxTables)
			if err != nil {
				return fmt.Errorf("list tables in all namespaces: %w", err)
			}
		}
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// If the bucket or namespace directory is not found, return an empty result
			tables = []TableSummary{}
			paginationToken = ""
		} else if isAuthError(err) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "Access Denied")
			return err
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list tables: %v", err))
			return err
		}
	}

	resp := &ListTablesResponse{
		Tables:            tables,
		ContinuationToken: paginationToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// listTablesInNamespaceWithClient lists tables in a specific namespace
func (h *S3TablesHandler) listTablesInNamespaceWithClient(r *http.Request, client filer_pb.SeaweedFilerClient, bucketName, namespaceName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	namespacePath := GetNamespacePath(bucketName, namespaceName)
	return h.listTablesWithClient(r, client, namespacePath, bucketName, namespaceName, prefix, continuationToken, maxTables)
}

func (h *S3TablesHandler) listTablesWithClient(r *http.Request, client filer_pb.SeaweedFilerClient, dirPath, bucketName, namespaceName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	var tables []TableSummary
	lastFileName := continuationToken
	ctx := r.Context()

	for len(tables) < maxTables {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dirPath,
			Limit:              uint32(maxTables * 2),
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "" || lastFileName == continuationToken,
		})
		if err != nil {
			return nil, "", err
		}

		hasMore := false
		for {
			entry, respErr := resp.Recv()
			if respErr != nil {
				if respErr == io.EOF {
					break
				}
				return nil, "", respErr
			}
			if entry.Entry == nil {
				continue
			}

			// Skip the start item if it was included in the previous page
			if len(tables) == 0 && continuationToken != "" && entry.Entry.Name == continuationToken {
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
			if prefix != "" && !strings.HasPrefix(entry.Entry.Name, prefix) {
				continue
			}

			// Views share the table layout; exclude them from table listings,
			// and a record its create has not confirmed is not a table yet.
			if EntryType(entry.Entry.Extended) == EntryTypeView || isUnfinishedCreate(entry.Entry.Extended) {
				continue
			}

			// Read table metadata from extended attribute
			data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
			if !ok {
				continue
			}

			var metadata tableMetadataInternal
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}

			// Note: Authorization (ownership or policy-based access) is checked at the handler level
			// before calling this function. This filter is removed to allow policy-based sharing.
			// The caller has already been verified to have ListTables permission for this namespace/bucket.

			tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+entry.Entry.Name)

			tables = append(tables, TableSummary{
				Name:             entry.Entry.Name,
				TableARN:         tableARN,
				Namespace:        expandNamespace(namespaceName),
				Format:           metadata.Format,
				CreatedAt:        metadata.CreatedAt,
				ModifiedAt:       metadata.ModifiedAt,
				MetadataLocation: metadata.MetadataLocation,
			})

			if len(tables) >= maxTables {
				return tables, lastFileName, nil
			}
		}

		if !hasMore {
			break
		}
	}

	if len(tables) < maxTables {
		lastFileName = ""
	}
	return tables, lastFileName, nil
}

func (h *S3TablesHandler) listTablesInAllNamespaces(r *http.Request, client filer_pb.SeaweedFilerClient, bucketName, prefix, continuationToken string, maxTables int) ([]TableSummary, string, error) {
	bucketPath := GetTableBucketPath(bucketName)
	ctx := r.Context()

	var continuationNamespace string
	var startTableName string
	if continuationToken != "" {
		if parts := strings.SplitN(continuationToken, "/", 2); len(parts) == 2 {
			continuationNamespace = parts[0]
			startTableName = parts[1]
		} else {
			continuationNamespace = continuationToken
		}
	}

	var tables []TableSummary
	lastNamespace := continuationNamespace
	for {
		// List namespaces in batches
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          bucketPath,
			Limit:              100,
			StartFromFileName:  lastNamespace,
			InclusiveStartFrom: (lastNamespace == continuationNamespace && startTableName != "") || (lastNamespace == "" && continuationNamespace == ""),
		})
		if err != nil {
			return nil, "", err
		}

		hasMore := false
		for {
			entry, respErr := resp.Recv()
			if respErr != nil {
				if respErr == io.EOF {
					break
				}
				return nil, "", respErr
			}
			if entry.Entry == nil {
				continue
			}

			hasMore = true
			lastNamespace = entry.Entry.Name

			if !entry.Entry.IsDirectory || strings.HasPrefix(entry.Entry.Name, ".") {
				continue
			}

			namespace := entry.Entry.Name
			tableNameFilter := ""
			if namespace == continuationNamespace {
				tableNameFilter = startTableName
			}

			nsTables, nsToken, err := h.listTablesInNamespaceWithClient(r, client, bucketName, namespace, prefix, tableNameFilter, maxTables-len(tables))
			if err != nil {
				return nil, "", fmt.Errorf("list tables in namespace %s: %w", namespace, err)
			}

			tables = append(tables, nsTables...)

			if namespace == continuationNamespace {
				startTableName = ""
			}

			if len(tables) >= maxTables {
				paginationToken := namespace + "/" + nsToken
				if nsToken == "" {
					// If we hit the limit exactly at the end of a namespace, the next token should be the next namespace
					paginationToken = namespace // This will start from the NEXT namespace in the outer loop
				}
				return tables, paginationToken, nil
			}
		}

		if !hasMore {
			break
		}
	}

	return tables, "", nil
}

// handleDeleteTable deletes a table from a namespace
func (h *S3TablesHandler) handleDeleteTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {

	var req DeleteTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return fmt.Errorf("missing required parameters")
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

	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)

	// Check if table exists and enforce VersionToken if provided
	var metadata tableMetadataInternal
	var tablePolicy string
	var bucketPolicy string
	var bucketTags map[string]string
	var tableTags map[string]string
	var bucketMetadata tableBucketMetadata
	var tableEntry *filer_pb.Entry
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// One lookup feeds both the version-token check and the conditional
		// delete below: a second read could land on a replacement entry while
		// the token was validated against the old one.
		tableEntry, err = h.lookupEntry(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		data, err := tableMetadata(tableEntry)
		if err != nil {
			return err
		}

		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		// Fetch table policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				// No table policy set; proceed with empty policy
			} else {
				return fmt.Errorf("failed to fetch table policy: %w", err)
			}
		} else {
			tablePolicy = string(policyData)
		}

		tableTags, err = h.readTags(r.Context(), client, tablePath)
		if err != nil {
			return err
		}

		bucketPath := GetTableBucketPath(bucketName)
		data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %w", err)
		}
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err != nil {
			if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
		} else {
			bucketPolicy = string(policyData)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		} else if errors.Is(err, ErrVersionTokenMismatch) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "version token mismatch")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	allowed := CheckTableOrBucketPermission("DeleteTable", principal,
		metadata.OwnerAccountID, tablePolicy, tableARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        namespaceName,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			ResourceTags:     tableTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		})
	if !allowed {
		if h.canReadEntry("GetTable", principal, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, tablePolicy, bucketPolicy, tableARN, bucketARN, bucketName, namespaceName, tableName, bucketTags, tableTags, identityActions, r) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table")
		} else {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		}
		return NewAuthError("DeleteTable", principal, "not authorized to delete table")
	}
	if req.VersionToken != "" && metadata.VersionToken != req.VersionToken {
		h.writeError(w, http.StatusConflict, ErrCodeConflict, "version token mismatch")
		return ErrVersionTokenMismatch
	}

	// Delete the table
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		dataPath := TableDataDirFromMetadataLocation(metadata.MetadataLocation)
		if dataPath != "" && dataPath != tablePath && strings.HasPrefix(dataPath+"/", GetTableBucketPath(bucketName)+"/") {
			// Refuse to purge a data path that is an ancestor of the table's own
			// name path (e.g. corrupt metadata resolving to the bucket or
			// namespace root): the bucket-scope check above still admits the
			// bucket root, and a recursive delete there would take out unrelated
			// tables.
			if strings.HasPrefix(tablePath+"/", dataPath+"/") {
				return fmt.Errorf("refusing to delete table %s: data path %q is an ancestor of catalog path %q", tableName, dataPath, tablePath)
			}
			// The location is caller-supplied, so it may name a sibling that is
			// still a live catalog entry. Only this table's authorization was
			// checked; a decoupled location has had its catalog attributes
			// stripped, so a surviving marker means the path belongs elsewhere.
			// The marker lives on the entry's name path, so ancestors of the
			// data path are checked as well: a location inside another table's
			// directory resolves above any marker the data path itself would
			// carry.
			for dir := path.Dir(dataPath); dir != GetTableBucketPath(bucketName) && strings.HasPrefix(dir, GetTableBucketPath(bucketName)+"/"); dir = path.Dir(dir) {
				ancestor, lookupErr := h.lookupEntry(r.Context(), client, dir)
				if lookupErr != nil {
					if errors.Is(lookupErr, filer_pb.ErrNotFound) {
						continue
					}
					return fmt.Errorf("refusing to delete table %s: cannot read data path %q: %w", tableName, dataPath, lookupErr)
				}
				if _, ok := ancestor.Extended[ExtendedKeyEntryType]; ok {
					return fmt.Errorf("refusing to delete table %s: data path %q is inside another catalog entry", tableName, dataPath)
				}
				var ownerMeta tableMetadataInternal
				if data := ancestor.Extended[ExtendedKeyMetadata]; json.Unmarshal(data, &ownerMeta) == nil && ownerMeta.Name != "" {
					return fmt.Errorf("refusing to delete table %s: data path %q is inside another catalog entry", tableName, dataPath)
				}
			}
			switch _, markerErr := h.getExtendedAttribute(r.Context(), client, dataPath, ExtendedKeyMetadata); {
			case markerErr == nil:
				return fmt.Errorf("refusing to delete table %s: data path %q is another catalog entry", tableName, dataPath)
			case !errors.Is(markerErr, ErrAttributeNotFound) && !errors.Is(markerErr, filer_pb.ErrNotFound):
				return fmt.Errorf("refusing to delete table %s: cannot read data path %q: %w", tableName, dataPath, markerErr)
			}
			// Decoupled table (renamed, or created over a leftover): its data
			// lives elsewhere. Purge the data, then clear the catalog marker
			// without deleting the name path -- it may still hold another
			// table's data that was left when this name was reused.
			if req.VersionToken != "" && tableEntry != nil {
				// A token caller owns exactly the record it read. Claim it
				// under the captured attributes before any data is removed:
				// a rename or delete+recreate that moved the record aborts
				// here, while it still points at live data. The claim holds
				// the name through the purge, a failed purge releases it so
				// the intact record can be retried, and the marker comes off
				// under a compare on the claimed record.
				snapshot := snapshotExtended(tableEntry.Extended)
				if err := h.claimCatalogRecord(r.Context(), client, tablePath, snapshot, "delete"); err != nil {
					if errors.Is(err, ErrConcurrentUpdate) {
						return ErrVersionTokenMismatch
					}
					return err
				}
				claimed := snapshotExtended(tableEntry.Extended)
				claimed[ExtendedKeyDeleting] = []byte("delete")
				if err := h.deleteDirectory(r.Context(), client, dataPath); err != nil {
					if relErr := h.releaseCatalogRecord(context.WithoutCancel(r.Context()), client, tablePath, claimed); relErr != nil {
						glog.Warningf("delete table %s: release claim: %v", tablePath, relErr)
					}
					return err
				}
				err := h.removeExtendedAttributesIf(context.WithoutCancel(r.Context()), client, tablePath, claimed,
					ExtendedKeyMetadata, ExtendedKeyMetadataVersion, ExtendedKeyPolicy, ExtendedKeyTags, ExtendedKeyEntryType,
					ExtendedKeyMaintenance, ExtendedKeyMaintenanceStatus, ExtendedKeyDeleting)
				if err != nil {
					if errors.Is(err, ErrConcurrentUpdate) {
						return ErrVersionTokenMismatch
					}
					return err
				}
				return nil
			}
			// A no-token decoupled delete claims the record like the token path
			// above: a rename holding the record turns the delete away instead
			// of losing its data to the purge.
			entry, err := h.lookupEntry(r.Context(), client, tablePath)
			if err != nil {
				return err
			}
			// Authorization ran on the first read; only that same record may be
			// purged.
			if err := ensureAuthorizedTable(entry, metadata); err != nil {
				return err
			}
			// dataPath was resolved on the earlier read, but the claim and the
			// purge run on this one: an update that moved the location in
			// between must send the delete back instead of purging a stale dir.
			if purge, err := tablePurgeDir(entry, tablePath, bucketName); err != nil {
				return err
			} else if purge != dataPath {
				return ErrConcurrentUpdate
			}
			snapshot := snapshotExtended(entry.Extended)
			if err := h.claimCatalogRecord(r.Context(), client, tablePath, snapshot, "delete"); err != nil {
				return err
			}
			claimed := snapshotExtended(entry.Extended)
			claimed[ExtendedKeyDeleting] = []byte("delete")
			if err := h.deleteDirectory(r.Context(), client, dataPath); err != nil {
				if relErr := h.releaseCatalogRecord(context.WithoutCancel(r.Context()), client, tablePath, claimed); relErr != nil {
					glog.Warningf("delete table %s: release claim: %v", tablePath, relErr)
				}
				return err
			}
			return h.removeExtendedAttributesIf(context.WithoutCancel(r.Context()), client, tablePath, claimed,
				ExtendedKeyMetadata, ExtendedKeyMetadataVersion, ExtendedKeyPolicy, ExtendedKeyTags, ExtendedKeyEntryType,
				ExtendedKeyMaintenance, ExtendedKeyMaintenanceStatus, ExtendedKeyDeleting)
		}
		// Colocated table: the name path holds the data. A token caller's
		// delete carries the captured attributes as a filer precondition, so
		// compare and removal are one operation under the path lock: an xattr
		// write that keeps Mtime, or a delete+recreate in between, refuses it.
		if req.VersionToken != "" && tableEntry != nil {
			if claim := string(tableEntry.Extended[ExtendedKeyDeleting]); claim != "" && claim != "delete" {
				return ErrVersionTokenMismatch
			}
			if err := h.deleteDirectoryIfMatches(r.Context(), client, tablePath, tableEntry); err != nil {
				if errors.Is(err, ErrConcurrentUpdate) {
					return ErrVersionTokenMismatch
				}
				return err
			}
			return nil
		}
		// The no-token delete claims the record before removing it, so a
		// rename or another delete holding the claim loses the record instead
		// of the directory under it.
		entry, err := h.lookupEntry(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := ensureAuthorizedTable(entry, metadata); err != nil {
			return err
		}
		// Same staleness check as the decoupled path: a location update that
		// landed since the authorized read turns this delete away, or it would
		// remove the name path the record no longer stores its data under.
		if purge, err := tablePurgeDir(entry, tablePath, bucketName); err != nil {
			return err
		} else if purge != tablePath {
			return ErrConcurrentUpdate
		}
		claimed := snapshotExtended(entry.Extended)
		claimed[ExtendedKeyDeleting] = []byte("delete")
		if err := h.claimCatalogRecord(r.Context(), client, tablePath, snapshotExtended(entry.Extended), "delete"); err != nil {
			return err
		}
		if err := h.deleteDirectoryIfMatches(r.Context(), client, tablePath, &filer_pb.Entry{Extended: claimed}); err != nil {
			if relErr := h.releaseCatalogRecord(context.WithoutCancel(r.Context()), client, tablePath, claimed); relErr != nil {
				glog.Warningf("delete table %s: release claim: %v", tablePath, relErr)
			}
			return err
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrVersionTokenMismatch) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "version token mismatch")
			return err
		}
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table changed during delete, retry the request")
			return err
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}

// renamedTableAttributes are the catalog attributes a rename carries to the new
// name and clears from the old one.
var renamedTableAttributes = []string{
	ExtendedKeyMetadata,
	ExtendedKeyMetadataVersion,
	ExtendedKeyPolicy,
	ExtendedKeyTags,
	ExtendedKeyMaintenance,
	ExtendedKeyMaintenanceStatus,
	ExtendedKeyEntryType,
}

// catalogEntryKind describes the entry a rename operates on, so tables and
// views share one implementation of the catalog-only move.
type catalogEntryKind struct {
	entryType string
	noun      string
	renameOp  string
	createOp  string
	readOp    string
	// tagsForRead mirrors whether the read handler supplies resource tags to
	// the policy check, so the denied-write visibility test cannot accept a
	// tag-conditioned allow the real read would not evaluate.
	tagsForRead  bool
	notFoundCode string
	existsCode   string
	// resourceARN builds the ARN a policy scoped to this entry would name, so a
	// view is authorized against its view ARN and not a table ARN.
	resourceARN func(h *S3TablesHandler, ownerAccountID, bucketName, id string) string
}

var (
	tableEntryKind = catalogEntryKind{
		entryType:    EntryTypeTable,
		noun:         "table",
		renameOp:     "RenameTable",
		createOp:     "CreateTable",
		readOp:       "GetTable",
		tagsForRead:  true,
		notFoundCode: ErrCodeNoSuchTable,
		existsCode:   ErrCodeTableAlreadyExists,
		resourceARN: func(h *S3TablesHandler, ownerAccountID, bucketName, id string) string {
			return h.generateTableARN(ownerAccountID, bucketName, id)
		},
	}
	viewEntryKind = catalogEntryKind{
		entryType:    EntryTypeView,
		noun:         "view",
		renameOp:     "RenameView",
		createOp:     "CreateView",
		readOp:       "GetView",
		tagsForRead:  false,
		notFoundCode: ErrCodeNoSuchView,
		existsCode:   ErrCodeViewAlreadyExists,
		resourceARN: func(h *S3TablesHandler, ownerAccountID, bucketName, id string) string {
			return h.generateViewARN(ownerAccountID, bucketName, id)
		},
	}
)

// readTableMetadata returns the entry's table-metadata xattr. A view stored at
// the same name is reported absent, the way GetTable and the renamer's
// entry-type check already treat kind mismatches.
func (h *S3TablesHandler) readTableMetadata(ctx context.Context, client filer_pb.SeaweedFilerClient, tablePath string) ([]byte, error) {
	entry, err := h.lookupEntry(ctx, client, tablePath)
	if err != nil {
		return nil, err
	}
	return tableMetadata(entry)
}

// tableMetadata extracts the table-metadata xattr from an entry. A view stored
// at the same name is reported absent, the way GetTable and the renamer's
// entry-type check already treat kind mismatches, and so is a record its
// create has not finished writing. A directory without the record is no table
// either, and reports the same absence.
func tableMetadata(entry *filer_pb.Entry) ([]byte, error) {
	if EntryType(entry.Extended) == EntryTypeView || isUnfinishedCreate(entry.Extended) {
		return nil, filer_pb.ErrNotFound
	}
	data, ok := entry.Extended[ExtendedKeyMetadata]
	if !ok {
		// A directory without the record is not a table (e.g. a renamed-away
		// source), so it reads as a missing entry.
		return nil, fmt.Errorf("%w: %w: %s", filer_pb.ErrNotFound, ErrAttributeNotFound, ExtendedKeyMetadata)
	}
	return data, nil
}

// ensureAuthorizedTable reports whether entry is still the table record a
// delete authorized on its first read. A view, an unfinished create or a record
// whose metadata is gone is absent; a record with another version token took
// the name since, and the authorization does not cover it.
func ensureAuthorizedTable(entry *filer_pb.Entry, authorized tableMetadataInternal) error {
	data, err := tableMetadata(entry)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return filer_pb.ErrNotFound
		}
		return err
	}
	var current tableMetadataInternal
	if err := json.Unmarshal(data, &current); err != nil {
		return fmt.Errorf("failed to unmarshal table metadata: %w", err)
	}
	if current.VersionToken != authorized.VersionToken {
		return ErrConcurrentUpdate
	}
	return nil
}

// tablePurgeDir resolves where a delete must purge from the metadata stored on
// entry, the way handleDeleteTable resolved dataPath on its authorized read: a
// location inside the bucket and off the name path is decoupled, anything else
// leaves the data under the name path.
func tablePurgeDir(entry *filer_pb.Entry, tablePath, bucketName string) (string, error) {
	data, ok := entry.Extended[ExtendedKeyMetadata]
	if !ok {
		return "", filer_pb.ErrNotFound
	}
	var metadata tableMetadataInternal
	if err := json.Unmarshal(data, &metadata); err != nil {
		return "", fmt.Errorf("failed to unmarshal table metadata: %w", err)
	}
	dataPath := TableDataDirFromMetadataLocation(metadata.MetadataLocation)
	if dataPath != "" && dataPath != tablePath && strings.HasPrefix(dataPath+"/", GetTableBucketPath(bucketName)+"/") {
		return dataPath, nil
	}
	return tablePath, nil
}

// canReadEntry reports whether the principal may read the catalog entry a
// denied write would touch. A denied write is then answered with 403 — the
// refusal leaks nothing a reader could not load anyway — while a caller that
// cannot see the entry keeps the not-found veil.
func (h *S3TablesHandler) canReadEntry(readOp, principal, tableOwner, bucketOwner, tablePolicy, bucketPolicy, tableARN, bucketARN, bucketName, namespace, name string, bucketTags, tableTags map[string]string, identityActions []string, r *http.Request) bool {
	ctx := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespace,
		TableName:        name,
		TableBucketTags:  bucketTags,
		ResourceTags:     tableTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	return CheckTableOrBucketPermission(readOp, principal,
		tableOwner, tablePolicy, tableARN,
		bucketOwner, bucketPolicy, bucketARN, ctx)
}

// handleRenameTable moves a table's catalog entry to a new namespace/name within
// the same bucket. It is catalog-only: the metadata.json and data files stay put,
// the destination keeps the source's MetadataLocation, and the source name is
// soft-deleted in place (its catalog xattrs are dropped, its data is left intact).
func (h *S3TablesHandler) handleRenameTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	return h.renameCatalogEntry(w, r, filerClient, tableEntryKind)
}

// handleRenameView is handleRenameTable for views, which live in the same
// namespace directory under the same name rules.
func (h *S3TablesHandler) handleRenameView(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	return h.renameCatalogEntry(w, r, filerClient, viewEntryKind)
}

// markSourceGone tags a missing source path so the rename reports it under the
// source name instead of the closed-parent mapping naming the destination.
func markSourceGone(err error) error {
	if errors.Is(err, filer_pb.ErrNotFound) {
		return fmt.Errorf("%w: %w", errSourceGone, err)
	}
	return err
}

func (h *S3TablesHandler) renameCatalogEntry(w http.ResponseWriter, r *http.Request, filerClient FilerClient, kind catalogEntryKind) error {
	var req RenameTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || len(req.SourceNamespace) == 0 || req.SourceName == "" || len(req.DestNamespace) == 0 || req.DestName == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, sourceNamespace, sourceName, destNamespace, and destName are required")
		return fmt.Errorf("missing required parameters")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	srcNamespace, err := validateNamespace(req.SourceNamespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	srcName, err := validateTableName(req.SourceName)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	destNamespace, err := validateNamespace(req.DestNamespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}
	destName, err := validateTableName(req.DestName)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	srcPath := GetTablePath(bucketName, srcNamespace, srcName)
	destPath := GetTablePath(bucketName, destNamespace, destName)

	var metadata tableMetadataInternal
	var metadataVersionXattr []byte
	var maintenanceXattr []byte
	var maintenanceStatusXattr []byte
	var tablePolicy string
	var bucketPolicy string
	var bucketTags map[string]string
	var tableTags map[string]string
	var bucketMetadata tableBucketMetadata
	var srcExtended map[string][]byte
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, srcPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}

		if versionData, err := h.getExtendedAttribute(r.Context(), client, srcPath, ExtendedKeyMetadataVersion); err == nil {
			metadataVersionXattr = versionData
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch metadata version: %w", err)
		}

		policyData, err := h.getExtendedAttribute(r.Context(), client, srcPath, ExtendedKeyPolicy)
		if err == nil {
			tablePolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch table policy: %w", err)
		}
		tableTags, err = h.readTags(r.Context(), client, srcPath)
		if err != nil {
			return err
		}

		srcEntry, err := h.lookupEntry(r.Context(), client, srcPath)
		if err != nil {
			return err
		}
		srcExtended = srcEntry.Extended

		// The maintenance configuration and its last-run status belong to the
		// table, so they move with it; leaving them behind re-enables
		// maintenance under the new name and leaks the old settings onto
		// whatever is created at the old one.
		for _, attr := range []struct {
			key  string
			dest *[]byte
		}{
			{ExtendedKeyMaintenance, &maintenanceXattr},
			{ExtendedKeyMaintenanceStatus, &maintenanceStatusXattr},
		} {
			data, err := h.getExtendedAttribute(r.Context(), client, srcPath, attr.key)
			if err == nil {
				*attr.dest = data
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch %s: %w", attr.key, err)
			}
		}

		bucketPath := GetTableBucketPath(bucketName)
		data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %w", err)
		}
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %w", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, kind.notFoundCode, fmt.Sprintf("%s %s not found", kind.noun, srcName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check %s: %v", kind.noun, err))
		}
		return err
	}

	// Tables and views share the namespace directory, so a rename must not pick
	// up the other kind under the same name, nor a record its create may still
	// back out.
	if EntryType(srcExtended) != kind.entryType || isUnfinishedCreate(srcExtended) {
		h.writeError(w, http.StatusNotFound, kind.notFoundCode, fmt.Sprintf("%s %s not found", kind.noun, srcName))
		return fmt.Errorf("%s %s not found", kind.noun, srcName)
	}

	tableARN := kind.resourceARN(h, metadata.OwnerAccountID, bucketName, srcNamespace+"/"+srcName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	allowed := CheckTableOrBucketPermission(kind.renameOp, principal,
		metadata.OwnerAccountID, tablePolicy, tableARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        srcNamespace,
			TableName:        srcName,
			TableBucketTags:  bucketTags,
			ResourceTags:     tableTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		})
	if !allowed {
		readTags := tableTags
		if !kind.tagsForRead {
			readTags = nil
		}
		if h.canReadEntry(kind.readOp, principal, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, tablePolicy, bucketPolicy, tableARN, bucketARN, bucketName, srcNamespace, srcName, bucketTags, readTags, identityActions, r) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to rename "+kind.noun)
		} else {
			h.writeError(w, http.StatusNotFound, kind.notFoundCode, fmt.Sprintf("%s %s not found", kind.noun, srcName))
		}
		return NewAuthError(kind.renameOp, principal, "not authorized to rename "+kind.noun)
	}

	// Require the destination namespace to exist. Whether the destination name
	// is taken is only recorded here; reporting the conflict before the
	// destination authorization check would disclose it to denied callers.
	destNamespacePath := GetNamespacePath(bucketName, destNamespace)
	var destNamespaceMetadata namespaceMetadata
	var destNamespacePolicy string
	var destExists bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, destNamespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &destNamespaceMetadata); err != nil {
			return fmt.Errorf("failed to unmarshal destination namespace metadata: %w", err)
		}
		policyData, err := h.getExtendedAttribute(r.Context(), client, destNamespacePath, ExtendedKeyPolicy)
		if err == nil {
			destNamespacePolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch destination namespace policy: %w", err)
		}
		if destEntry, err := h.lookupEntry(r.Context(), client, destPath); err == nil {
			_, hasMetadata := destEntry.Extended[ExtendedKeyMetadata]
			destExists = hasMetadata && !isUnfinishedCreate(destEntry.Extended)
		} else if !errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", destNamespace))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check destination: %v", err))
		}
		return err
	}

	// Renaming places the table into the destination namespace, so the principal
	// must also be allowed to create a table there (the source check alone lets a
	// caller move tables into namespaces they don't control). Denials report the
	// same not-found as a missing destination namespace.
	if !CheckTableOrBucketPermission(kind.createOp, principal,
		destNamespaceMetadata.OwnerAccountID, destNamespacePolicy, bucketARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        destNamespace,
			TableName:        destName,
			TableBucketTags:  bucketTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", destNamespace))
		return NewAuthError(kind.renameOp, principal, "not authorized to create "+kind.noun+" in the destination namespace")
	}

	if destExists {
		h.writeError(w, http.StatusConflict, kind.existsCode, fmt.Sprintf("%s %s already exists", kind.noun, destName))
		return ErrTableAlreadyExists
	}
	if err := h.refuseClaimedNamespace(w, r, filerClient, destNamespacePath, destNamespace); err != nil {
		return err
	}

	metadata.Name = destName
	metadata.Namespace = destNamespace
	metadata.ModifiedAt = time.Now()

	metadataBytes, err := json.Marshal(&metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal "+kind.noun+" metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Claim the source record before publishing the destination: a delete or
	// a second rename that wins the claim backs out here — while no
	// destination entry exists — instead of leaving one behind pointing at
	// data a delete already removed.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		if err := h.claimCatalogRecord(r.Context(), client, srcPath, snapshotExtended(srcExtended), "rename"); err != nil {
			return markSourceGone(err)
		}
		claimed := snapshotExtended(srcExtended)
		claimed[ExtendedKeyDeleting] = []byte("rename")
		releaseClaim := func(err error) error {
			if relErr := h.releaseCatalogRecord(context.WithoutCancel(r.Context()), client, srcPath, claimed); relErr != nil {
				glog.Warningf("rename %s: release claim: %v", srcPath, relErr)
			}
			return err
		}
		if err := h.ensureDirectory(r.Context(), client, destPath); err != nil {
			return releaseClaim(err)
		}
		written := map[string][]byte{
			ExtendedKeyMetadata:  metadataBytes,
			ExtendedKeyEntryType: []byte(kind.entryType),
			ExtendedKeyDeleting:  []byte(renameDestClaim),
		}
		if len(metadataVersionXattr) > 0 {
			written[ExtendedKeyMetadataVersion] = metadataVersionXattr
		}
		if len(tableTags) > 0 {
			tagsBytes, err := json.Marshal(tableTags)
			if err != nil {
				return releaseClaim(fmt.Errorf("failed to marshal tags: %w", err))
			}
			written[ExtendedKeyTags] = tagsBytes
		}
		if tablePolicy != "" {
			written[ExtendedKeyPolicy] = []byte(tablePolicy)
		}
		if len(maintenanceXattr) > 0 {
			written[ExtendedKeyMaintenance] = maintenanceXattr
		}
		if len(maintenanceStatusXattr) > 0 {
			written[ExtendedKeyMaintenanceStatus] = maintenanceStatusXattr
		}
		// The destination goes up marked like a create, but under the rename's
		// own claim so a create cannot adopt a record the rename still owes a
		// publish or a keep. It is confirmed only while its namespace stays
		// open: a delete that claims the namespace during the rename must find
		// this record refused, not take a renamed table whose source the
		// soft-delete below already released.
		if err := h.claimCatalogEntry(r.Context(), client, destPath, written); err != nil {
			return releaseClaim(err)
		}
		// The destination stays marked — and so invisible to every other
		// operation — until the source clears. Confirming it first would publish
		// a record that a failed source clear would then remove from under
		// callers that already saw it.
		backOutDest := func(err error) error {
			ctx := context.WithoutCancel(r.Context())
			keys := make([]string, 0, len(written))
			for key := range written {
				keys = append(keys, key)
			}
			if backErr := h.removeExtendedAttributesIf(ctx, client, destPath, written, keys...); backErr != nil && !errors.Is(backErr, filer_pb.ErrNotFound) {
				glog.Warningf("rename %s: back out destination: %v", destPath, backErr)
			}
			h.removeLeftoverDirectory(ctx, client, destPath)
			return releaseClaim(err)
		}
		destParent, err := h.openParent(r.Context(), client, destNamespacePath, isNamespaceEntry)
		if err != nil {
			return backOutDest(err)
		}
		// Soft-delete the source catalog identity in place: drop its catalog xattrs
		// so the name stops resolving while the metadata/ and data/ children stay put
		// (manifests embed absolute paths, so the data must not move). The rename's
		// claim stays on the cleared record until the commit lands: a namespace
		// delete scanning it in between must still refuse, or it removes the
		// directory the destination keeps pointing at.
		err = h.removeExtendedAttributesIf(r.Context(), client, srcPath, claimed, renamedTableAttributes...)
		if err != nil {
			return backOutDest(markSourceGone(err))
		}
		// Commit on an uncanceled context so a client cancel cannot wedge the
		// record half-published. Only this rename can touch its marked record,
		// so the compare cannot lose; the namespace check then runs once more,
		// since a delete claiming it after the first check scans the still-
		// marked destination, skips it, and removes it under a rename that
		// already answered success. On a filer error the source's cleared
		// attributes go back rather than dropping the table's catalog identity
		// entirely.
		commitCtx := context.WithoutCancel(r.Context())
		destConfirmed := false
		if err = h.removeExtendedAttributesIf(commitCtx, client, destPath, written, ExtendedKeyDeleting); err == nil {
			destConfirmed = true
			err = h.parentStillOpen(commitCtx, client, destNamespacePath, isNamespaceEntry, destParent)
		}
		if err != nil {
			if destConfirmed {
				// The recheck failed after the record went live: strip it, so
				// the failed rename leaves no confirmed destination behind.
				confirmed := make(map[string][]byte, len(written))
				keys := make([]string, 0, len(written))
				for key, data := range written {
					if key == ExtendedKeyDeleting {
						continue
					}
					confirmed[key] = data
					keys = append(keys, key)
				}
				if backErr := h.removeExtendedAttributesIf(commitCtx, client, destPath, confirmed, keys...); backErr != nil && !errors.Is(backErr, filer_pb.ErrNotFound) {
					glog.Warningf("rename %s: remove confirmed destination: %v", destPath, backErr)
				} else {
					h.removeLeftoverDirectory(commitCtx, client, destPath)
				}
			}
			cleared := snapshotExtended(claimed)
			for _, key := range renamedTableAttributes {
				delete(cleared, key)
			}
			if rErr := h.mutateEntryExtended(commitCtx, client, srcPath, func(extended map[string][]byte) error {
				for key, want := range cleared {
					if !bytes.Equal(extended[key], want) {
						return fmt.Errorf("%w: %s changed on %s", ErrConcurrentUpdate, key, srcPath)
					}
				}
				// A create that reused the name since the clear owns it now:
				// restoring over its attributes would clobber a live table.
				for _, key := range renamedTableAttributes {
					if _, ok := extended[key]; ok {
						return fmt.Errorf("%w: %s recreated on %s", ErrConcurrentUpdate, key, srcPath)
					}
				}
				for key, data := range srcExtended {
					if key != ExtendedKeyDeleting {
						extended[key] = data
					}
				}
				delete(extended, ExtendedKeyDeleting)
				return nil
			}); rErr != nil {
				glog.Warningf("rename %s: restore source record: %v", srcPath, rErr)
			} else if !destConfirmed {
				// Source restored: drop the unfinished destination too, since a
				// marked record reads as absent but still refuses its name to
				// later creates. When the restore fails the marked record is
				// the only catalog identity left — keep it for recovery. The
				// compare keeps a record that moved on since the failed publish.
				keys := make([]string, 0, len(written))
				for key := range written {
					keys = append(keys, key)
				}
				if backErr := h.removeExtendedAttributesIf(commitCtx, client, destPath, written, keys...); backErr != nil && !errors.Is(backErr, filer_pb.ErrNotFound) {
					glog.Warningf("rename %s: remove unfinished destination: %v", destPath, backErr)
				} else {
					h.removeLeftoverDirectory(commitCtx, client, destPath)
				}
			}
			return err
		}
		// The record is published: lift the source claim so the cleared source
		// directory stops refusing its namespace's delete and the old name
		// frees for reuse. Nothing else writes the cleared record, so a loss
		// here is a transient filer error; retry before letting a leftover
		// claim wedge the name and its namespace.
		var relErr error
		for attempt := 0; attempt < extendedAttributeUpdateAttempts; attempt++ {
			if relErr = h.removeExtendedAttributesIf(commitCtx, client, srcPath,
				map[string][]byte{ExtendedKeyDeleting: []byte("rename")}, ExtendedKeyDeleting); relErr == nil {
				break
			}
			if errors.Is(relErr, filer_pb.ErrNotFound) || errors.Is(relErr, ErrConcurrentUpdate) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if relErr != nil && !errors.Is(relErr, filer_pb.ErrNotFound) {
			glog.Warningf("rename %s: release source claim: %v", srcPath, relErr)
		}
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, kind.noun+" changed during rename, retry the request")
			return err
		}
		if errors.Is(err, ErrNameTaken) {
			h.writeError(w, http.StatusConflict, kind.existsCode, fmt.Sprintf("%s %s already exists", kind.noun, destName))
			return ErrTableAlreadyExists
		}
		if errors.Is(err, errSourceGone) {
			h.writeError(w, http.StatusNotFound, kind.notFoundCode, fmt.Sprintf("%s %s not found", kind.noun, srcName))
			return err
		}
		if h.writeNamespaceClosedError(w, err, destNamespace) {
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to rename "+kind.noun)
		return err
	}

	h.writeJSON(w, http.StatusOK, &RenameTableResponse{
		TableARN:         kind.resourceARN(h, metadata.OwnerAccountID, bucketName, destNamespace+"/"+destName),
		MetadataLocation: metadata.MetadataLocation,
	})
	return nil
}

// updateTableTarget is the table state an update was authorized against: the
// stored values the optimistic-concurrency check compares and the fields the
// response echoes back.
type updateTableTarget struct {
	bucketName     string
	namespaceName  string
	tableName      string
	tablePath      string
	tableARN       string
	metadata       tableMetadataInternal
	storedMetadata []byte
	storedPolicy   []byte
}

// authorizeUpdateTable validates the names an update names, loads the table
// and the policies governing it, and checks the caller may update it, writing
// the error response itself. Credential vending asks the same question without
// updating anything, so it runs this gate through Manager.AuthorizeUpdateTable.
func (h *S3TablesHandler) authorizeUpdateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient, req *UpdateTableRequest) (*updateTableTarget, error) {
	if req.TableBucketARN == "" || len(req.Namespace) == 0 || req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN, namespace, and name are required")
		return nil, fmt.Errorf("missing required parameters")
	}

	namespaceName, err := validateNamespace(req.Namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	tableName, err := validateTableName(req.Name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	if err := ValidateMetadataLocation(req.MetadataLocation, bucketName, namespaceName); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	tablePath := GetTablePath(bucketName, namespaceName, tableName)

	// Load existing metadata and policies for authorization
	var metadata tableMetadataInternal
	var storedMetadata []byte
	var storedPolicy []byte
	var tablePolicy string
	var bucketPolicy string
	var bucketTags map[string]string
	var tableTags map[string]string
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// 1. Get Table Metadata
		data, err := h.readTableMetadata(r.Context(), client, tablePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal table metadata: %w", err)
		}
		storedMetadata = data

		// 2. Get Table Policy & Tags
		policyData, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyPolicy)
		if err == nil {
			tablePolicy = string(policyData)
			storedPolicy = policyData
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch table policy: %w", err)
		}
		tableTags, err = h.readTags(r.Context(), client, tablePath)
		if err != nil {
			return err
		}

		// 3. Get Bucket Metadata, Policy & Tags
		bucketPath := GetTableBucketPath(bucketName)
		data, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %w", err)
		}
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %w", err)
		}
		bucketTags, err = h.readTags(r.Context(), client, bucketPath)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, "table not found")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, err.Error())
		}
		return nil, err
	}

	// Authorization Check
	tableARN := h.generateTableARN(metadata.OwnerAccountID, bucketName, namespaceName+"/"+tableName)
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)

	allowed := CheckTableOrBucketPermission("UpdateTable", principal,
		metadata.OwnerAccountID, tablePolicy, tableARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			Namespace:        namespaceName,
			TableName:        tableName,
			TableBucketTags:  bucketTags,
			ResourceTags:     tableTags,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		})

	if !allowed {
		if h.canReadEntry("GetTable", principal, metadata.OwnerAccountID, bucketMetadata.OwnerAccountID, tablePolicy, bucketPolicy, tableARN, bucketARN, bucketName, namespaceName, tableName, bucketTags, tableTags, identityActions, r) {
			h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to update table")
		} else {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, "table not found")
		}
		return nil, NewAuthError("UpdateTable", principal, "not authorized to update table")
	}

	return &updateTableTarget{
		bucketName:     bucketName,
		namespaceName:  namespaceName,
		tableName:      tableName,
		tablePath:      tablePath,
		tableARN:       tableARN,
		metadata:       metadata,
		storedMetadata: storedMetadata,
		storedPolicy:   storedPolicy,
	}, nil
}

// handleUpdateTable updates table metadata
func (h *S3TablesHandler) handleUpdateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req UpdateTableRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	target, err := h.authorizeUpdateTable(w, r, filerClient, &req)
	if err != nil {
		return err
	}
	metadata := target.metadata

	// Check version token if provided
	if req.VersionToken != "" && req.VersionToken != metadata.VersionToken {
		h.writeError(w, http.StatusConflict, ErrCodeConflict, "Version token mismatch")
		return ErrVersionTokenMismatch
	}

	// Update metadata
	if req.Metadata != nil {
		if metadata.Metadata == nil {
			metadata.Metadata = &TableMetadata{}
		}
		if req.Metadata.Iceberg != nil {
			if metadata.Metadata.Iceberg == nil {
				metadata.Metadata.Iceberg = &IcebergMetadata{}
			}
			if req.Metadata.Iceberg.TableUUID != "" {
				metadata.Metadata.Iceberg.TableUUID = req.Metadata.Iceberg.TableUUID
			}
		}
		if len(req.Metadata.FullMetadata) > 0 {
			metadata.Metadata.FullMetadata = req.Metadata.FullMetadata
		}
	}
	if req.ClearFullMetadata && metadata.Metadata != nil {
		metadata.Metadata.FullMetadata = nil
	}
	if req.MetadataLocation != "" {
		metadata.MetadataLocation = req.MetadataLocation
	}
	if req.MetadataVersion > 0 {
		metadata.MetadataVersion = req.MetadataVersion
	} else if metadata.MetadataVersion == 0 {
		metadata.MetadataVersion = 1
	}
	metadata.ModifiedAt = time.Now()
	metadata.VersionToken = generateVersionToken()

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal metadata")
		return err
	}

	// Conditional on the metadata this request read and authorized against:
	// two commits that both passed the version-token check would otherwise each
	// write their own metadata and the later one would drop the earlier
	// snapshot. mutateEntryExtended retries on a changed entry, so the check
	// lives in the mutation, where it sees the value that is current now.
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.mutateCatalogRecord(r.Context(), client, target.tablePath, func(extended map[string][]byte) error {
			if !bytes.Equal(extended[ExtendedKeyMetadata], target.storedMetadata) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyMetadata)
			}
			// The policy this request was authorized against must still be the
			// one in force: an administrator restricting it mid-commit should
			// send the caller back through authorization, not have its decision
			// applied afterwards.
			if !bytes.Equal(extended[ExtendedKeyPolicy], target.storedPolicy) {
				return fmt.Errorf("%w: %s", ErrConcurrentUpdate, ExtendedKeyPolicy)
			}
			extended[ExtendedKeyMetadata] = metadataBytes
			return nil
		})
	})

	if err != nil {
		if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table was updated concurrently")
			return ErrVersionTokenMismatch
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", target.tableName))
			return err
		}
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to update metadata")
		return err
	}

	h.writeJSON(w, http.StatusOK, &UpdateTableResponse{
		TableARN:         target.tableARN,
		MetadataLocation: metadata.MetadataLocation,
		VersionToken:     metadata.VersionToken,
	})
	return nil
}

// createTableTarget is the namespace a create or register resolved to, once the
// caller has been authorized to put a table there.
type createTableTarget struct {
	bucketName     string
	namespaceName  string
	tableName      string
	bucketFormat   string
	ownerAccountID string
}

// authorizeCreateTable validates the names a create names and checks the caller
// may create a table in that namespace, writing the error response itself.
// Deferred creates (Iceberg stage-create) write into the table bucket before any
// table is registered, so they run this same gate first through
// Manager.AuthorizeCreateTable.
func (h *S3TablesHandler) authorizeCreateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient, tableBucketARN string, namespace []string, name string, requestTags map[string]string, allowMissingNamespace bool) (*createTableTarget, error) {
	if tableBucketARN == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN is required")
		return nil, fmt.Errorf("tableBucketARN is required")
	}

	namespaceName, err := validateNamespace(namespace)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	if name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "name is required")
		return nil, fmt.Errorf("name is required")
	}

	bucketName, err := parseBucketNameFromARN(tableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	tableName, err := validateTableName(name)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return nil, err
	}

	// Check if namespace exists
	namespacePath := GetNamespacePath(bucketName, namespaceName)
	nsMetadata, err := h.loadNamespaceMetadata(r.Context(), filerClient, bucketName, namespaceName)
	namespaceExists := err == nil
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) && allowMissingNamespace {
			// An implicit namespace can be authorized before it is declared:
			// evaluate the grant against the bucket alone.
			err = nil
		} else {
			if errors.Is(err, filer_pb.ErrNotFound) {
				h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
			} else {
				h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
			}
			return nil, err
		}
	}

	// Authorize table creation using policy framework (namespace + bucket policies)
	accountID := h.getAccountID(r)
	bucketPath := GetTableBucketPath(bucketName)
	namespacePolicy := ""
	bucketPolicy := ""
	bucketTags := map[string]string{}
	var bucketMetadata tableBucketMetadata

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Fetch bucket metadata to use correct owner for bucket policy evaluation
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err == nil {
			if err := json.Unmarshal(data, &bucketMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal bucket metadata: %w", err)
			}
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket metadata: %v", err)
		}

		// Fetch namespace policy if it exists
		if namespaceExists {
			policyData, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyPolicy)
			if err == nil {
				namespacePolicy = string(policyData)
			} else if !errors.Is(err, ErrAttributeNotFound) {
				return fmt.Errorf("failed to fetch namespace policy: %v", err)
			}
		}
		var policyData []byte

		// Fetch bucket policy if it exists
		policyData, err = h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err == nil {
			bucketPolicy = string(policyData)
		} else if !errors.Is(err, ErrAttributeNotFound) {
			return fmt.Errorf("failed to fetch bucket policy: %v", err)
		}
		if tags, err := h.readTags(r.Context(), client, bucketPath); err != nil {
			return err
		} else if tags != nil {
			bucketTags = tags
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to fetch policies: %v", err))
		return nil, err
	}

	if !namespaceExists {
		// The implicit namespace has no record yet: authorize it as the
		// bucket owner's instead of dereferencing the absent metadata.
		nsMetadata = &namespaceMetadata{OwnerAccountID: bucketMetadata.OwnerAccountID}
	}
	bucketARN := h.generateTableBucketARN(bucketMetadata.OwnerAccountID, bucketName)
	identityActions := getIdentityActions(r)
	policyContext := &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		Namespace:        namespaceName,
		TableName:        tableName,
		RequestTags:      requestTags,
		TagKeys:          mapKeys(requestTags),
		TableBucketTags:  bucketTags,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}
	if !CheckTableOrBucketPermission("CreateTable", accountID,
		nsMetadata.OwnerAccountID, namespacePolicy, bucketARN,
		bucketMetadata.OwnerAccountID, bucketPolicy, bucketARN, policyContext) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to create table in this namespace")
		return nil, ErrAccessDenied
	}

	if namespaceExists {
		if err := h.refuseClaimedNamespace(w, r, filerClient, namespacePath, namespaceName); err != nil {
			return nil, err
		}
	}

	return &createTableTarget{
		bucketName:     bucketName,
		namespaceName:  namespaceName,
		tableName:      tableName,
		bucketFormat:   bucketMetadata.Format,
		ownerAccountID: nsMetadata.OwnerAccountID,
	}, nil
}

// writeNamespaceClosedError answers a create refused because its namespace is
// being created, being deleted, or was deleted, and reports whether err was one
// of those.
func (h *S3TablesHandler) writeNamespaceClosedError(w http.ResponseWriter, err error, namespaceName string) bool {
	switch {
	case errors.Is(err, errParentCreating):
		h.writeError(w, http.StatusConflict, ErrCodeConflict, fmt.Sprintf("namespace %s is still being created, retry the request", namespaceName))
	case errors.Is(err, errParentDeleting):
		h.writeError(w, http.StatusConflict, ErrCodeConflict, fmt.Sprintf("namespace %s is being deleted", namespaceName))
	case errors.Is(err, errParentGone), errors.Is(err, filer_pb.ErrNotFound):
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
	case errors.Is(err, errBackOutFailed):
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("namespace %s was deleted during the request and the new entry may remain", namespaceName))
	default:
		return false
	}
	return true
}

// refuseClaimedNamespace answers 409 when a delete has claimed the namespace or
// its create is not confirmed yet. Call it after the authorization check, so
// denied callers never see the delete.
func (h *S3TablesHandler) refuseClaimedNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient, namespacePath, namespaceName string) error {
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.checkParentOpen(r.Context(), client, namespacePath, isNamespaceEntry)
	})
	if errors.Is(err, errParentDeleting) || errors.Is(err, errParentCreating) {
		h.writeNamespaceClosedError(w, err, namespaceName)
		return err
	}
	return nil
}
