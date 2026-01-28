package s3tables

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleGetTableBucket gets details of a table bucket
func (h *S3TablesHandler) handleGetTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
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
		data, err := h.getExtendedAttribute(client, bucketPath, ExtendedKeyMetadata)
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

	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: TablesPath,
			Limit:     uint32(maxBuckets),
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
		}

		return nil
	})

	if err != nil {
		// Check if it's a "not found" error - return empty list in that case
		if strings.Contains(err.Error(), "no entry is found") || strings.Contains(err.Error(), "not found") {
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
		resp, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
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

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, "table bucket is not empty")
		return fmt.Errorf("bucket not empty")
	}

	// Delete the bucket
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return h.deleteDirectory(client, bucketPath)
	})

	if err != nil {
		h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket")
		return err
	}

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
