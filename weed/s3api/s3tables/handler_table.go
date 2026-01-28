package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateTable creates a new table in a namespace
func (h *S3TablesHandler) handleCreateTable(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req CreateTableRequest
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

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "name is required")
		return fmt.Errorf("name is required")
	}

	if req.Format == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "format is required")
		return fmt.Errorf("format is required")
	}

	// Validate format
	if req.Format != "ICEBERG" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "only ICEBERG format is supported")
		return fmt.Errorf("invalid format")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	// Validate table name
	if len(req.Name) < 1 || len(req.Name) > 255 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "table name must be between 1 and 255 characters")
		return fmt.Errorf("invalid table name length")
	}
	if req.Name == "." || req.Name == ".." || strings.Contains(req.Name, "/") {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "invalid table name: cannot be '.', '..' or contain '/'")
		return fmt.Errorf("invalid table name")
	}
	for _, ch := range req.Name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "invalid table name: only 'a-z', '0-9', and '_' are allowed")
		return fmt.Errorf("invalid table name")
	}

	// Check if namespace exists
	namespacePath := getNamespacePath(bucketName, namespaceName)
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, namespacePath, ExtendedKeyMetadata)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", namespaceName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check namespace: %v", err))
		}
		return err
	}

	tablePath := getTablePath(bucketName, namespaceName, req.Name)

	// Check if table already exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		return err
	})

	if err == nil {
		h.writeError(w, http.StatusConflict, ErrCodeTableAlreadyExists, fmt.Sprintf("table %s already exists", req.Name))
		return fmt.Errorf("table already exists")
	} else if !errors.Is(err, filer_pb.ErrNotFound) {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		return err
	}

	// Create the table
	now := time.Now()
	versionToken := generateVersionToken()

	metadata := &tableMetadataInternal{
		Name:         req.Name,
		Namespace:    namespaceName,
		Format:       req.Format,
		CreatedAt:    now,
		ModifiedAt:   now,
		OwnerID:      h.accountID,
		VersionToken: versionToken,
		Schema:       req.Metadata,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal table metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Create table directory
		if err := h.createDirectory(r.Context(), client, tablePath); err != nil {
			return err
		}

		// Create data subdirectory for Iceberg files
		dataPath := tablePath + "/data"
		if err := h.createDirectory(r.Context(), client, dataPath); err != nil {
			return err
		}

		// Set metadata as extended attribute
		if err := h.setExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata, metadataBytes); err != nil {
			return err
		}

		// Set tags if provided
		if len(req.Tags) > 0 {
			tagsBytes, err := json.Marshal(req.Tags)
			if err != nil {
				return fmt.Errorf("failed to marshal tags: %w", err)
			}
			if err := h.setExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyTags, tagsBytes); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create table")
		return err
	}

	tableARN := h.generateTableARN(bucketName, namespaceName+"/"+req.Name)

	resp := &CreateTableResponse{
		TableARN:     tableARN,
		VersionToken: versionToken,
	}

	h.writeJSON(w, http.StatusOK, resp)
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
		tableName = req.Name
	} else {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "either tableARN or (tableBucketARN, namespace, name) is required")
		return fmt.Errorf("missing required parameters")
	}

	tablePath := getTablePath(bucketName, namespace, tableName)

	var metadata tableMetadataInternal
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &metadata)
	})

	if err != nil {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", tableName))
		return err
	}

	tableARN := h.generateTableARN(bucketName, namespace+"/"+tableName)

	resp := &GetTableResponse{
		Name:             metadata.Name,
		TableARN:         tableARN,
		Namespace:        []string{metadata.Namespace},
		Format:           metadata.Format,
		CreatedAt:        metadata.CreatedAt,
		ModifiedAt:       metadata.ModifiedAt,
		OwnerAccountID:   metadata.OwnerID,
		MetadataLocation: metadata.MetadataLocation,
		VersionToken:     metadata.VersionToken,
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

	var tables []TableSummary

	// If namespace is specified, list tables in that namespace only
	if len(req.Namespace) > 0 {
		namespaceName, err := validateNamespace(req.Namespace)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
			return err
		}
		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return h.listTablesInNamespaceWithClient(r.Context(), client, bucketName, namespaceName, req.Prefix, maxTables, &tables)
		})
	} else {
		// List tables in all namespaces
		err = h.listTablesInAllNamespaces(r.Context(), filerClient, bucketName, req.Prefix, maxTables, &tables)
	}

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list tables: %v", err))
		return err
	}

	resp := &ListTablesResponse{
		Tables: tables,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

func (h *S3TablesHandler) listTablesInNamespaceWithClient(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, namespace, prefix string, maxTables int, tables *[]TableSummary) error {
	namespacePath := getNamespacePath(bucketName, namespace)

	var lastFileName string
	for len(*tables) < maxTables {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          namespacePath,
			Limit:              uint32(maxTables * 2),
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "",
		})
		if err != nil {
			return err
		}

		hasMore := false
		for {
			entry, respErr := resp.Recv()
			if respErr != nil {
				break
			}
			if entry.Entry == nil {
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

			// Read table metadata from extended attribute
			data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
			if !ok {
				continue
			}

			var metadata tableMetadataInternal
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}

			tableARN := h.generateTableARN(bucketName, namespace+"/"+entry.Entry.Name)

			*tables = append(*tables, TableSummary{
				Name:       metadata.Name,
				TableARN:   tableARN,
				Namespace:  []string{namespace},
				CreatedAt:  metadata.CreatedAt,
				ModifiedAt: metadata.ModifiedAt,
			})

			if len(*tables) >= maxTables {
				return nil
			}
		}

		if !hasMore {
			break
		}
	}

	return nil
}

func (h *S3TablesHandler) listTablesInAllNamespaces(ctx context.Context, filerClient FilerClient, bucketName, prefix string, maxTables int, tables *[]TableSummary) error {
	bucketPath := getTableBucketPath(bucketName)

	var lastFileName string
	return filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for {
			// List namespaces in batches
			resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
				Limit:              100,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "",
			})
			if err != nil {
				return err
			}

			hasMore := false
			for {
				entry, respErr := resp.Recv()
				if respErr != nil {
					break
				}
				if entry.Entry == nil {
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

				namespace := entry.Entry.Name

				// List tables in this namespace
				if err := h.listTablesInNamespaceWithClient(ctx, client, bucketName, namespace, prefix, maxTables-len(*tables), tables); err != nil {
					continue
				}

				if len(*tables) >= maxTables {
					return nil
				}
			}

			if !hasMore {
				break
			}
		}

		return nil
	})
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

	tablePath := getTablePath(bucketName, namespaceName, req.Name)

	// Check if table exists
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(r.Context(), client, tablePath, ExtendedKeyMetadata)
		return err
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchTable, fmt.Sprintf("table %s not found", req.Name))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to check table: %v", err))
		}
		return err
	}

	// Delete the table
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, tablePath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
