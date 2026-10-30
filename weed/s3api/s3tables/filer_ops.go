package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrAttributeNotFound = errors.New("attribute not found")
	ErrConcurrentUpdate  = errors.New("attribute changed concurrently")
	ErrNameTaken         = errors.New("catalog name is taken")
)

// extendedAttributeUpdateAttempts bounds the read-modify-write retry loop in
// updateExtendedAttribute.
const extendedAttributeUpdateAttempts = 5

// Filer operations - Common functions for interacting with the filer

// createDirectory creates a new directory at the specified path
func (h *S3TablesHandler) createDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	return h.createDirectoryEntry(ctx, client, path, false, nil)
}

// createDirectoryExclusive creates a directory entry together with its
// extended attributes in one CreateEntry that fails with
// filer_pb.ErrEntryAlreadyExists when an entry is already there: nothing is
// replaced and no reader sees the directory without its attributes.
func (h *S3TablesHandler) createDirectoryExclusive(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, extended map[string][]byte) error {
	return h.createDirectoryEntry(ctx, client, path, true, extended)
}

func (h *S3TablesHandler) createDirectoryEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, exclusive bool, extended map[string][]byte) error {
	dir, name := splitPath(path)
	now := time.Now().Unix()
	return filer_pb.CreateEntry(ctx, client, &filer_pb.CreateEntryRequest{
		Directory: dir,
		OExcl:     exclusive,
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    now,
				Crtime:   now,
				FileMode: uint32(0755 | os.ModeDir),
			},
			Extended: extended,
		},
	})
}

// ensureDirectory ensures a directory exists at the specified path
func (h *S3TablesHandler) ensureDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, filer_pb.ErrNotFound) {
		return h.createDirectory(ctx, client, path)
	}
	return err
}

// deleteEntryIfExists removes an entry if it exists, ignoring missing errors
func (h *S3TablesHandler) deleteEntryIfExists(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	return filer_pb.DoRemove(ctx, client, dir, name, true, false, true, false, nil)
}

// mutateEntryExtended applies mutate to an entry's extended attributes and
// writes the entry back, asserting that nothing on it changed in between.
// UpdateEntry rewrites the whole entry from the snapshot read here, so an
// unconditional write would delete an attribute a concurrent writer created.
func (h *S3TablesHandler) mutateEntryExtended(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	mutate func(extended map[string][]byte) error,
) error {
	dir, name := splitPath(path)

	for attempt := 0; attempt < extendedAttributeUpdateAttempts; attempt++ {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}

		entry := resp.Entry
		expected := snapshotExtended(entry.Extended)
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		if err := mutate(entry.Extended); err != nil {
			return err
		}

		_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory:        dir,
			Entry:            entry,
			ExpectedExtended: expected,
		})
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.FailedPrecondition {
			return err
		}
	}

	return fmt.Errorf("%w: %s", ErrConcurrentUpdate, path)
}

// setExtendedAttribute sets an extended attribute on an existing entry
func (h *S3TablesHandler) setExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string, data []byte) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		extended[key] = data
		return nil
	})
}

// setExtendedAttributes sets multiple extended attributes on an existing entry
// in a single UpdateEntry, so callers don't leave the entry partially tagged.
func (h *S3TablesHandler) setExtendedAttributes(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, attrs map[string][]byte) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for key, data := range attrs {
			extended[key] = data
		}
		return nil
	})
}

// removeExtendedAttributes deletes the given extended attributes from an entry,
// leaving the directory and its children intact.
func (h *S3TablesHandler) removeExtendedAttributes(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, keys ...string) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for _, key := range keys {
			delete(extended, key)
		}
		return nil
	})
}

// removeExtendedAttributesIf drops the given attributes only while they still
// hold the values the caller captured. A rename copies attributes to the
// destination and then clears the source; without this, a write that landed in
// between is deleted here and never reaches the new name.
func (h *S3TablesHandler) removeExtendedAttributesIf(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	expected map[string][]byte,
	keys ...string,
) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for key, want := range expected {
			if !bytes.Equal(extended[key], want) {
				return fmt.Errorf("%w: %s changed on %s", ErrConcurrentUpdate, key, path)
			}
		}
		for _, key := range keys {
			delete(extended, key)
		}
		return nil
	})
}

// claimCatalogEntry writes a new catalog entry's marker attributes in one
// compare-checked update. Every catalog marker must still be absent: a
// delete's restore or a competing create that landed since the caller's check
// is a name conflict, not a record this write may overwrite — an overwrite
// would leave the loser's tags, policy, and maintenance settings on the new
// record.
func (h *S3TablesHandler) claimCatalogEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, attrs map[string][]byte) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for _, key := range s3tablesExtendedKeys {
			if _, ok := extended[key]; ok {
				return fmt.Errorf("%w: %s set on %s", ErrNameTaken, key, path)
			}
		}
		for key, data := range attrs {
			extended[key] = data
		}
		return nil
	})
}

// claimCatalogRecord reserves a catalog record for an in-flight delete or
// rename under one compare-checked write: the entry must still hold exactly
// expected. A record claimed by another kind of operation refuses the claim,
// so the loser backs out before any data or destination entry is written;
// a retry of the same kind may adopt its own earlier claim.
func (h *S3TablesHandler) claimCatalogRecord(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	expected map[string][]byte,
	claim string,
) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for key, want := range expected {
			if key == ExtendedKeyDeleting {
				continue
			}
			if !bytes.Equal(extended[key], want) {
				return fmt.Errorf("%w: %s changed on %s", ErrConcurrentUpdate, key, path)
			}
		}
		if cur, ok := extended[ExtendedKeyDeleting]; ok && string(cur) != claim {
			return fmt.Errorf("%w: %s held on %s", ErrConcurrentUpdate, ExtendedKeyDeleting, path)
		}
		extended[ExtendedKeyDeleting] = []byte(claim)
		return nil
	})
}

// releaseCatalogRecord lifts a claimCatalogRecord reservation while the entry
// still holds expected — which includes the claim itself — restoring the
// record so the failed operation can be retried.
func (h *S3TablesHandler) releaseCatalogRecord(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	expected map[string][]byte,
) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		for key, want := range expected {
			if !bytes.Equal(extended[key], want) {
				return fmt.Errorf("%w: %s changed on %s", ErrConcurrentUpdate, key, path)
			}
		}
		delete(extended, ExtendedKeyDeleting)
		return nil
	})
}

// updateExtendedAttribute applies mutate to the current value of key and writes
// the result back. mutate runs again on each retry, so it always sees the
// current value.
func (h *S3TablesHandler) updateExtendedAttribute(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path, key string,
	mutate func(current []byte) ([]byte, error),
) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		updated, err := mutate(extended[key])
		if err != nil {
			return err
		}
		extended[key] = updated
		return nil
	})
}

// s3tablesExtendedKeys is every attribute this package stores on a bucket or
// table entry. A whole-entry write has to assert the absent ones too: a key
// missing from the precondition is a key a concurrent writer can create and
// this write will then delete.
var s3tablesExtendedKeys = []string{
	ExtendedKeyTableBucket,
	ExtendedKeyMetadata,
	ExtendedKeyMetadataVersion,
	ExtendedKeyPolicy,
	ExtendedKeyTags,
	ExtendedKeyMaintenance,
	ExtendedKeyMaintenanceStatus,
	ExtendedKeyEntryType,
	ExtendedKeyDeleting,
	ExtendedKeyVectorPolicy,
}

// SnapshotExtended captures an entry's attributes as an UpdateEntry
// precondition, so a whole-entry write cannot silently revert or delete an
// attribute a concurrent writer touched. Attributes absent at read time are
// asserted absent, which fails the precondition if someone creates one first.
func SnapshotExtended(extended map[string][]byte, keys ...string) map[string][]byte {
	return snapshotExtended(extended, keys...)
}

func snapshotExtended(extended map[string][]byte, keys ...string) map[string][]byte {
	expected := make(map[string][]byte, len(extended)+len(s3tablesExtendedKeys))
	for k, v := range extended {
		expected[k] = v
	}
	for _, k := range s3tablesExtendedKeys {
		if _, ok := expected[k]; !ok {
			expected[k] = nil
		}
	}
	for _, k := range keys {
		if _, ok := expected[k]; !ok {
			expected[k] = nil
		}
	}
	return expected
}

// getExtendedAttribute gets an extended attribute from an entry
func (h *S3TablesHandler) getExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string) ([]byte, error) {
	dir, name := splitPath(path)
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}

	if resp.Entry.Extended == nil {
		return nil, fmt.Errorf("%w: %s", ErrAttributeNotFound, key)
	}

	data, ok := resp.Entry.Extended[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAttributeNotFound, key)
	}

	return data, nil
}

// loadNamespaceMetadata resolves a namespace to its metadata. A directory that
// carries no namespace metadata is not a namespace, so a missing attribute
// reports the same absence as a missing entry and every caller tests one
// condition instead of forgetting the second.
func (h *S3TablesHandler) loadNamespaceMetadata(ctx context.Context, filerClient FilerClient, bucketName, namespaceName string) (*namespaceMetadata, error) {
	var metadata namespaceMetadata
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.getExtendedAttribute(ctx, client, GetNamespacePath(bucketName, namespaceName), ExtendedKeyMetadata)
		if err != nil {
			if errors.Is(err, ErrAttributeNotFound) {
				return filer_pb.ErrNotFound
			}
			return err
		}
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("failed to unmarshal namespace metadata: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &metadata, nil
}

// lookupEntry returns the filer entry at the given path.
func (h *S3TablesHandler) lookupEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) (*filer_pb.Entry, error) {
	dir, name := splitPath(path)
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}
	return resp.Entry, nil
}

// deleteExtendedAttribute deletes an extended attribute from an entry
func (h *S3TablesHandler) deleteExtendedAttribute(ctx context.Context, client filer_pb.SeaweedFilerClient, path, key string) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		delete(extended, key)
		return nil
	})
}

// deleteDirectory deletes a directory and all its contents
func (h *S3TablesHandler) deleteDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) error {
	dir, name := splitPath(path)
	return filer_pb.DoRemove(ctx, client, dir, name, true, true, true, false, nil)
}

// deleteDirectoryIfMatches deletes a directory and all its contents only
// while the entry still carries the extended attributes the caller captured:
// the filer compares and removes in one operation under the path lock, so an
// attribute write that keeps Mtime or a delete+recreate in between refuses the
// delete with ErrConcurrentUpdate instead of losing the replacement.
func (h *S3TablesHandler) deleteDirectoryIfMatches(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, expected *filer_pb.Entry) error {
	dir, name := splitPath(path)
	_, err := filer_pb.DoRemoveRequest(ctx, client, &filer_pb.DeleteEntryRequest{
		Directory:            dir,
		Name:                 name,
		IsDeleteData:         true,
		IsRecursive:          true,
		IgnoreRecursiveError: true,
		ExpectedExtended:     snapshotExtended(expected.Extended),
	})
	if status.Code(err) == codes.FailedPrecondition {
		return fmt.Errorf("%w: %s", ErrConcurrentUpdate, path)
	}
	return err
}

var (
	errParentDeleting = errors.New("parent is being deleted")
	errParentGone     = errors.New("parent was deleted")
	errNoDeleteClaim  = errors.New("claim not held")
	errBackOutFailed  = errors.New("backing out the create failed")
	errSourceGone     = errors.New("rename source was deleted")
)

// claimForDelete writes a fresh token into ExtendedKeyDeleting, replacing any
// earlier claim, and returns it with the attributes the delete must still find
// when it removes the entry. The token makes a stale delete's removal fail.
func (h *S3TablesHandler) claimForDelete(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, isParent func(*filer_pb.Entry) bool) (string, map[string][]byte, error) {
	claim := generateVersionToken()
	var claimed map[string][]byte
	err := h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		if !isParent(&filer_pb.Entry{Extended: extended}) {
			return filer_pb.ErrNotFound
		}
		extended[ExtendedKeyDeleting] = []byte(claim)
		claimed = snapshotExtended(extended)
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return claim, claimed, nil
}

// releaseDeleteClaim removes claim from path while path still holds it, even
// after the request was cancelled.
func (h *S3TablesHandler) releaseDeleteClaim(ctx context.Context, client filer_pb.SeaweedFilerClient, path, claim string) {
	err := h.mutateEntryExtended(context.WithoutCancel(ctx), client, path, func(extended map[string][]byte) error {
		if string(extended[ExtendedKeyDeleting]) != claim {
			return errNoDeleteClaim
		}
		delete(extended, ExtendedKeyDeleting)
		return nil
	})
	if err != nil && !errors.Is(err, errNoDeleteClaim) && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.Warningf("release delete claim on %s: %v", path, err)
	}
}

// isNamespaceEntry reports whether entry carries a namespace's catalog record.
func isNamespaceEntry(entry *filer_pb.Entry) bool {
	_, ok := entry.Extended[ExtendedKeyMetadata]
	return ok
}

// checkParentOpen returns errParentGone if parentPath is missing or not a
// catalog entry, and errParentDeleting if a delete has claimed it.
func (h *S3TablesHandler) checkParentOpen(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool) error {
	entry, err := h.lookupEntry(ctx, client, parentPath)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return errParentGone
	}
	if err != nil {
		return err
	}
	if !isParent(entry) {
		return errParentGone
	}
	if _, claimed := entry.Extended[ExtendedKeyDeleting]; claimed {
		return errParentDeleting
	}
	return nil
}

// createClaim is the ExtendedKeyDeleting value of a catalog record whose
// create has written it but not yet confirmed its parent.
const createClaim = "create"

// isUnfinishedCreate reports whether extended belongs to a record still
// marked by the create that wrote it.
func isUnfinishedCreate(extended map[string][]byte) bool {
	return string(extended[ExtendedKeyDeleting]) == createClaim
}

// mutateCatalogRecord applies mutate to a catalog record, refusing a record
// an in-flight operation claimed: a create's unfinished record is absent to
// every reader, and a record claimed by a delete or rename must not take a
// write the operation's compare then trips over.
func (h *S3TablesHandler) mutateCatalogRecord(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	mutate func(extended map[string][]byte) error,
) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		if _, claimed := extended[ExtendedKeyDeleting]; !claimed {
			return mutate(extended)
		}
		if isUnfinishedCreate(extended) {
			return filer_pb.ErrNotFound
		}
		return fmt.Errorf("%w: %s claimed on %s", ErrConcurrentUpdate, ExtendedKeyDeleting, path)
	})
}

// confirmParent re-checks the parent after a create wrote written onto
// childPath. If the parent is open, it clears the record's unfinished mark.
// If the parent is gone or claimed, it removes what the create wrote and
// returns why, or errBackOutFailed if the record stays.
func (h *S3TablesHandler) confirmParent(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool, childPath string, written map[string][]byte, emptyDirs ...string) error {
	err := h.checkParentOpen(ctx, client, parentPath, isParent)
	if err == nil {
		// The mark comes off only while the record still holds what this
		// create wrote; a failed write backs the record out like a closed parent.
		if err = h.removeExtendedAttributesIf(ctx, client, childPath, written, ExtendedKeyDeleting); err == nil {
			return nil
		}
	}
	ctx = context.WithoutCancel(ctx)
	keys := make([]string, 0, len(written))
	for key := range written {
		keys = append(keys, key)
	}
	if backOutErr := h.removeExtendedAttributesIf(ctx, client, childPath, written, keys...); backOutErr != nil && !errors.Is(backOutErr, filer_pb.ErrNotFound) {
		glog.Warningf("back out %s: %v", childPath, backOutErr)
		return fmt.Errorf("%w: %s: %v (%v)", errBackOutFailed, childPath, backOutErr, err)
	}
	for _, dir := range emptyDirs {
		h.removeLeftoverDirectory(ctx, client, dir)
	}
	if errors.Is(err, errParentGone) {
		h.removeLeftoverDirectory(ctx, client, parentPath)
	}
	return err
}

// removeLeftoverDirectory deletes path if it is an empty directory without
// attributes; the filer refuses the non-recursive delete otherwise.
func (h *S3TablesHandler) removeLeftoverDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) {
	entry, err := h.lookupEntry(ctx, client, path)
	if err != nil || !entry.IsDirectory || len(entry.Extended) > 0 {
		return
	}
	dir, name := splitPath(path)
	if _, err := filer_pb.DoRemoveRequest(ctx, client, &filer_pb.DeleteEntryRequest{
		Directory:        dir,
		Name:             name,
		IsDeleteData:     true,
		ExpectedExtended: snapshotExtended(entry.Extended),
	}); err != nil {
		glog.V(1).Infof("keep %s: %v", path, err)
	}
}

// entryExists checks if an entry exists at the given path
func (h *S3TablesHandler) entryExists(ctx context.Context, client filer_pb.SeaweedFilerClient, path string) bool {
	dir, name := splitPath(path)
	_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	return err == nil
}
