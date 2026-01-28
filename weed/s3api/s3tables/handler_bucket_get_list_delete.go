package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleGetTableBucket gets details of a table bucket
func (h *S3TablesHandler) handleGetTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanGetTableBucket(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to get table bucket details")
		return NewAuthError("GetTableBucket", principal, "not authorized to get table bucket details")
	}

	var req GetTableBucketRequest
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

	bucketPath := getTableBucketPath(bucketName)

	var metadata tableBucketMetadata
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &metadata)
	})

	if err != nil {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return err
	}

	resp := &GetTableBucketResponse{
		ARN:            h.generateTableBucketARN(bucketName),
		Name:           metadata.Name,
		OwnerAccountID: metadata.OwnerID,
		CreatedAt:      metadata.CreatedAt,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleListTableBuckets lists all table buckets
func (h *S3TablesHandler) handleListTableBuckets(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	var req ListTableBucketsRequest
	if err := h.readRequestBody(r, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, err.Error())
		return err
	}

	maxBuckets := req.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 100
	}

	var buckets []TableBucketSummary

	var lastFileName string
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(buckets) < maxBuckets {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          TablesPath,
				Limit:              uint32(maxBuckets * 2), // Fetch more than needed to account for filtering
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
				hasMore = true
				lastFileName = entry.Entry.Name

				if !entry.Entry.IsDirectory {
					continue
				}

				// Skip entries starting with "."
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

				var metadata tableBucketMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					continue
				}

				buckets = append(buckets, TableBucketSummary{
					ARN:       h.generateTableBucketARN(entry.Entry.Name),
					Name:      entry.Entry.Name,
					CreatedAt: metadata.CreatedAt,
				})

				if len(buckets) >= maxBuckets {
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
		// Check if it's a "not found" error - return empty list in that case
		if errors.Is(err, ErrNotFound) {
			buckets = []TableBucketSummary{}
		} else {
			// For other errors, return error response
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list table buckets: %v", err))
			return err
		}
	}

	resp := &ListTableBucketsResponse{
		TableBuckets: buckets,
	}

	h.writeJSON(w, http.StatusOK, resp)
	return nil
}

// handleDeleteTableBucket deletes a table bucket
func (h *S3TablesHandler) handleDeleteTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission
	principal := h.getPrincipalFromRequest(r)
	if !CanDeleteTableBucket(principal, h.accountID) {
		h.writeError(w, http.StatusForbidden, ErrCodeAccessDenied, "not authorized to delete table buckets")
		return NewAuthError("DeleteTableBucket", principal, "not authorized to delete table buckets")
	}

	var req DeleteTableBucketRequest
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

	bucketPath := getTableBucketPath(bucketName)

	// Check if bucket exists and is empty
	hasChildren := false
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
			Directory: bucketPath,
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

	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list bucket entries: %v", err))
			return err
		}
	}

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, "table bucket is not empty")
		return fmt.Errorf("bucket not empty")
	}

	// Delete the bucket
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(r.Context(), client, bucketPath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
