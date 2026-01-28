package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleCreateNamespace creates a new namespace in a table bucket
func (h *S3TablesHandler) handleCreateNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
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

	// For simplicity, use the first namespace element as the directory name
	namespaceName := req.Namespace[0]

	// Validate namespace name
	if len(namespaceName) < 1 || len(namespaceName) > 255 {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "namespace name must be between 1 and 255 characters")
		return fmt.Errorf("invalid namespace name length")
	}

	// Check if table bucket exists
	bucketPath := getTableBucketPath(bucketName)
	var bucketExists bool
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(client, bucketPath, ExtendedKeyMetadata)
		bucketExists = err == nil
		return nil
	})

	if !bucketExists {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return fmt.Errorf("bucket not found")
	}

	namespacePath := getNamespacePath(bucketName, namespaceName)

	// Check if namespace already exists
	exists := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := h.getExtendedAttribute(client, namespacePath, ExtendedKeyMetadata)
		exists = err == nil
		return nil
	})

	if exists {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceAlreadyExists, fmt.Sprintf("namespace %s already exists", namespaceName))
		return fmt.Errorf("namespace already exists")
	}

	// Create the namespace
	now := time.Now()
	metadata := &namespaceMetadata{
		Namespace: req.Namespace,
		CreatedAt: now,
		OwnerID:   h.accountID,
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to marshal namespace metadata")
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Create namespace directory
		if err := h.createDirectory(client, namespacePath); err != nil {
			return err
		}

		// Set metadata as extended attribute
		if err := h.setExtendedAttribute(client, namespacePath, ExtendedKeyMetadata, metadataBytes); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to create namespace")
		return err
	}

	resp := &CreateNamespaceResponse{
		Namespace:      req.Namespace,
		TableBucketARN: req.TableBucketARN,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleGetNamespace gets details of a namespace
func (h *S3TablesHandler) handleGetNamespace(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req GetNamespaceRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	if req.TableBucketARN == "" || req.Namespace == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN and namespace are required")
		return fmt.Errorf("tableBucketARN and namespace are required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := getNamespacePath(bucketName, req.Namespace)

	var metadata namespaceMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(client, namespacePath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &metadata)
	})

	if err != nil {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchNamespace, fmt.Sprintf("namespace %s not found", req.Namespace))
		return err
	}

	resp := &GetNamespaceResponse{
		Namespace:      metadata.Namespace,
		CreatedAt:      metadata.CreatedAt,
		OwnerAccountID: metadata.OwnerID,
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

	bucketPath := getTableBucketPath(bucketName)
	var namespaces []NamespaceSummary

	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: bucketPath,
			Limit:     uint32(maxNamespaces),
		})
		if err != nil {
			return err
		}

		for {
			entry, err := resp.Recv()
			if err != nil {
				break
			}

			if entry.Entry == nil || !entry.Entry.IsDirectory {
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

			// Read metadata from extended attribute
			data, ok := entry.Entry.Extended[ExtendedKeyMetadata]
			if !ok {
				continue
			}

			var metadata namespaceMetadata
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}

			namespaces = append(namespaces, NamespaceSummary{
				Namespace: metadata.Namespace,
				CreatedAt: metadata.CreatedAt,
			})
		}

		return nil
	})

	if err != nil {
		namespaces = []NamespaceSummary{}
	}

	resp := &ListNamespacesResponse{
		Namespaces: namespaces,
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

	if req.TableBucketARN == "" || req.Namespace == "" {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "tableBucketARN and namespace are required")
		return fmt.Errorf("tableBucketARN and namespace are required")
	}

	bucketName, err := parseBucketNameFromARN(req.TableBucketARN)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	namespacePath := getNamespacePath(bucketName, req.Namespace)

	// Check if namespace exists and is empty
	hasChildren := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: namespacePath,
			Limit:     10,
		})
		if err != nil {
			return err
		}

		for {
			entry, err := resp.Recv()
			if err != nil {
				break
			}
			if entry.Entry != nil && !strings.HasPrefix(entry.Entry.Name, ".") {
				hasChildren = true
				break
			}
		}

		return nil
	})

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeNamespaceNotEmpty, "namespace is not empty")
		return fmt.Errorf("namespace not empty")
	}

	// Delete the namespace
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(client, namespacePath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete namespace")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
