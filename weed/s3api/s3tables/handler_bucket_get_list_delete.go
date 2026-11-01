package s3tables

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// handleGetTableBucket gets details of a table bucket
func (h *S3TablesHandler) handleGetTableBucket(w http.ResponseWriter, r *http.Request, filerClient FilerClient) error {
	// Check permission

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

	bucketPath := GetTableBucketPath(bucketName)

	var metadata tableBucketMetadata
	var bucketPolicy string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
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

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || errors.Is(err, ErrAttributeNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to get table bucket: %v", err))
		}
		return err
	}

	bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
	principal := h.getAccountID(r)
	identityActions := getIdentityActions(r)
	if !CheckPermissionWithContext("GetTableBucket", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
		TrustedIssuers:   h.trustedIssuers(r),
		IsLocalAccountID: h.localAccountIDs,
		TableBucketName:  bucketName,
		IdentityActions:  identityActions,
		DefaultAllow:     h.defaultAllowFor(r),
	}) {
		h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		return ErrAccessDenied
	}

	resp := &GetTableBucketResponse{
		ARN:                     h.generateTableBucketARN(metadata.OwnerAccountID, bucketName),
		Name:                    metadata.Name,
		OwnerAccountID:          metadata.OwnerAccountID,
		CreatedAt:               metadata.CreatedAt,
		Format:                  metadata.Format,
		EncryptionConfiguration: metadata.EncryptionConfiguration,
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

	// No account-level gate: visibility is enforced per bucket below, so an
	// owner can always list its own buckets and others are filtered out.
	accountID := h.getAccountID(r)

	maxBuckets := req.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 100
	}
	// Cap to prevent uint32 overflow when used in uint32(maxBuckets*2)
	const maxBucketsLimit = 1000
	if maxBuckets > maxBucketsLimit {
		h.writeError(w, http.StatusBadRequest, ErrCodeInvalidRequest, "MaxBuckets exceeds maximum allowed value")
		return fmt.Errorf("invalid maxBuckets value: %d", maxBuckets)
	}

	var buckets []TableBucketSummary

	lastFileName := req.ContinuationToken
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for len(buckets) < maxBuckets {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          TablesPath,
				Limit:              uint32(maxBuckets * 2), // Fetch more than needed to account for filtering
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
				if len(buckets) == 0 && req.ContinuationToken != "" && entry.Entry.Name == req.ContinuationToken {
					continue
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

				if !IsTableBucketEntry(entry.Entry) {
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

				bucketPath := GetTableBucketPath(entry.Entry.Name)
				bucketPolicy := ""
				policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
				if err != nil {
					if !errors.Is(err, ErrAttributeNotFound) {
						continue
					}
				} else {
					bucketPolicy = string(policyData)
				}

				bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, entry.Entry.Name)
				identityActions := getIdentityActions(r)
				if !CheckPermissionWithContext("GetTableBucket", accountID, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
					TrustedIssuers:   h.trustedIssuers(r),
					IsLocalAccountID: h.localAccountIDs,
					TableBucketName:  entry.Entry.Name,
					IdentityActions:  identityActions,
					DefaultAllow:     h.defaultAllowFor(r),
				}) {
					continue
				}

				buckets = append(buckets, TableBucketSummary{
					ARN:       bucketARN,
					Name:      entry.Entry.Name,
					CreatedAt: metadata.CreatedAt,
					Format:    metadata.Format,
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
		if errors.Is(err, filer_pb.ErrNotFound) {
			buckets = []TableBucketSummary{}
		} else {
			// For other errors, return error response
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to list table buckets: %v", err))
			return err
		}
	}

	paginationToken := ""
	if len(buckets) >= maxBuckets {
		paginationToken = lastFileName
	}

	resp := &ListTableBucketsResponse{
		TableBuckets:      buckets,
		ContinuationToken: paginationToken,
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

	bucketPath := GetTableBucketPath(bucketName)

	// Check if bucket exists and perform ownership + emptiness check in one block
	var metadata tableBucketMetadata
	var bucketPolicy string
	hasChildren := false
	heldDir := ""
	// What the scan found besides namespaces: the bare directories a deleted
	// namespace left, as they were seen, and the names of the hidden entries.
	var leftovers []*filer_pb.Entry
	var hidden []string
	// The claim this delete holds on the bucket. It goes with the bucket when it
	// is deleted; any other way out lifts it, or the bucket would refuse every
	// create. bucketClaimed is the bucket's record as the claim left it, which the
	// removal has to find.
	bucketClaim := ""
	var bucketClaimed map[string][]byte
	defer func() {
		if bucketClaim == "" {
			return
		}
		// A claim left behind refuses every create in the bucket until the next
		// delete replaces it, so a failed release is retried.
		if err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			h.releaseDeleteClaimRetrying(r.Context(), client, bucketPath, bucketClaim)
			return nil
		}); err != nil {
			glog.Warningf("release delete claim on %s: %v", bucketPath, err)
		}
	}()
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// 1. Get metadata for ownership check
		data, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyMetadata)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		// Fetch bucket policy if it exists
		policyData, err := h.getExtendedAttribute(r.Context(), client, bucketPath, ExtendedKeyPolicy)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				// No bucket policy set; proceed with empty bucketPolicy
			} else {
				return fmt.Errorf("failed to fetch bucket policy: %w", err)
			}
		} else {
			bucketPolicy = string(policyData)
		}

		bucketARN := h.generateTableBucketARN(metadata.OwnerAccountID, bucketName)
		principal := h.getAccountID(r)
		identityActions := getIdentityActions(r)
		if !CheckPermissionWithContext("DeleteTableBucket", principal, metadata.OwnerAccountID, bucketPolicy, bucketARN, &PolicyContext{
			TrustedIssuers:   h.trustedIssuers(r),
			IsLocalAccountID: h.localAccountIDs,
			TableBucketName:  bucketName,
			IdentityActions:  identityActions,
			DefaultAllow:     h.defaultAllowFor(r),
		}) {
			return NewAuthError("DeleteTableBucket", principal, fmt.Sprintf("not authorized to delete bucket %s", bucketName))
		}

		// 2. Claim the bucket before scanning it. CreateNamespace refuses a
		// claimed bucket and checks it again after its write, so a namespace is
		// either seen by the scan below or backed out. The bucket is removed only
		// while it still carries this claim: once another delete has replaced or
		// lifted it, a create may have passed that check, and this delete answers
		// 409 instead. A claim an earlier delete left behind is replaced.
		bucketClaim, bucketClaimed, err = h.claimForDelete(r.Context(), client, bucketPath, IsTableBucketEntry)
		if err != nil {
			return err
		}

		// 3. Check if bucket is empty. A file or a namespace is a child. A bare
		// directory is what a deleted namespace leaves behind to keep the data
		// under it, so it is only a child while it still holds a file. The scan
		// pages through the whole bucket: the first page may be nothing but bare
		// directories sorted ahead of the namespaces.
		lastFileName := ""
		for !hasChildren {
			resp, err := client.ListEntries(r.Context(), &filer_pb.ListEntriesRequest{
				Directory:          bucketPath,
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
				if strings.HasPrefix(entry.Entry.Name, ".") {
					hidden = append(hidden, entry.Entry.Name)
					continue
				}
				if _, hasMetadata := entry.Entry.Extended[ExtendedKeyMetadata]; hasMetadata || !entry.Entry.IsDirectory {
					hasChildren = true
					break
				}
				leftovers = append(leftovers, entry.Entry)
			}
			if !hasMore {
				break
			}
		}

		// Looked at after the scan, not during it, so a bucket that still has a
		// namespace answers at once instead of walking the leftovers first. The
		// first directory holding a file settles it.
		if hasChildren {
			return nil
		}
		for _, leftover := range leftovers {
			holdsFiles, err := h.dirHoldsFiles(r.Context(), client, path.Join(bucketPath, leftover.Name))
			if err != nil {
				return err
			}
			if holdsFiles {
				heldDir = leftover.Name
				break
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else if isAuthError(err) {
			h.writeError(w, http.StatusNotFound, ErrCodeNoSuchBucket, fmt.Sprintf("table bucket %s not found", bucketName))
		} else if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table bucket changed during delete, retry the request")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, fmt.Sprintf("failed to delete table bucket: %v", err))
		}
		return err
	}

	if hasChildren {
		h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, "table bucket is not empty")
		return fmt.Errorf("bucket not empty")
	}

	// Nothing in the bucket is a namespace, but data sits under a directory a
	// deleted namespace left. Deleting the bucket would take it, so it stays.
	if heldDir != "" {
		h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, fmt.Sprintf("table bucket is not empty: directory %s still holds files", heldDir))
		return fmt.Errorf("bucket not empty: directory %s still holds files", heldDir)
	}

	// Delete the bucket
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Drop the leaf entry first, so it does not outlive the directory.
		if tableObjErr := h.deleteEntryIfExists(r.Context(), client, GetTableObjectBucketPath(bucketName)); tableObjErr != nil {
			glog.V(1).Infof("failed to delete table object for %s: %v", bucketName, tableObjErr)
		}
		// Only the hidden entries go by a recursive delete. A file written since
		// the scan anywhere else makes the filer refuse the delete that would have
		// taken it: each leftover goes only if it is still the bare directory the
		// scan saw and holds no file or catalog record, and the bucket only if
		// nothing is left in it.
		for _, leftover := range leftovers {
			if err := h.removeEmptyTree(r.Context(), client, path.Join(bucketPath, leftover.Name), leftover); err != nil {
				return err
			}
		}
		for _, name := range hidden {
			if err := h.deleteDirectory(r.Context(), client, path.Join(bucketPath, name)); err != nil {
				return err
			}
		}
		// The bucket is the directory, so a refused delete leaves it in place
		// and the caller must hear about it.
		return h.deleteEmptyDirectoryIfMatches(r.Context(), client, bucketPath, &filer_pb.Entry{Extended: bucketClaimed})
	})

	if err != nil {
		if errors.Is(err, errDirNotEmpty) {
			h.writeError(w, http.StatusConflict, ErrCodeBucketNotEmpty, "table bucket is not empty")
		} else if errors.Is(err, ErrConcurrentUpdate) {
			h.writeError(w, http.StatusConflict, ErrCodeConflict, "table bucket changed during delete, retry the request")
		} else {
			h.writeError(w, http.StatusInternalServerError, ErrCodeInternalError, "failed to delete table bucket")
		}
		return err
	}
	bucketClaim = ""

	h.writeJSON(w, http.StatusOK, nil)
	return nil
}
