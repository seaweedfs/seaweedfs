package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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
	return h.mutateEntry(ctx, client, path, func(entry *filer_pb.Entry) error {
		return mutate(entry.Extended)
	})
}

// mutateEntry is mutateEntryExtended for a mutation that also needs the entry
// itself, such as its type. The entry mutate inspects is the one written back,
// and the filer refuses to write a directory entry over a file, so a check on
// the entry's type holds at the write too.
func (h *S3TablesHandler) mutateEntry(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	mutate func(entry *filer_pb.Entry) error,
) error {
	return h.mutateEntryAsserting(ctx, client, path, nil, mutate)
}

// mutateEntryAsserting is mutateEntry with the keys in absent asserted at the
// write too, as this package's own attributes are: one absent when the entry was
// read and written by someone else since fails the compare, instead of being
// written over with the entry as it was read.
func (h *S3TablesHandler) mutateEntryAsserting(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	path string,
	absent []string,
	mutate func(entry *filer_pb.Entry) error,
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
		expected := snapshotExtended(entry.Extended, absent...)
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		if err := mutate(entry); err != nil {
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
		// The filer answers an entry removed since the lookup with
		// codes.NotFound; report it as the lookup's own miss.
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("%w: %s", filer_pb.ErrNotFound, path)
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
// record. A record still marked by an unfinished create is the exception:
// invisible to every reader, it is adopted rather than refused. A rename
// destination is not adoptable — its writer still owes the record a publish
// or a keep, and adopting it strands the renamed record with neither name.
func (h *S3TablesHandler) claimCatalogEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, attrs map[string][]byte) error {
	return h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		return claimExtended(extended, path, attrs)
	})
}

// claimExtended is claimCatalogEntry's change to the attributes it read.
func claimExtended(extended map[string][]byte, path string, attrs map[string][]byte) error {
	if string(extended[ExtendedKeyDeleting]) == createClaim {
		// A marked record is absent to every reader, so it holds nothing a
		// caller could have seen: the create that left it confirmed or lost
		// the name already. Take the record over, dropping whatever the
		// earlier write left, or an abandoned mark wedges the name forever.
		for _, key := range s3tablesExtendedKeys {
			delete(extended, key)
		}
	} else {
		for _, key := range s3tablesExtendedKeys {
			if _, ok := extended[key]; ok {
				return fmt.Errorf("%w: %s set on %s", ErrNameTaken, key, path)
			}
		}
	}
	for key, data := range attrs {
		extended[key] = data
	}
	return nil
}

// retakeDirectory writes a create's attributes onto a directory that is already
// at path, the way an exclusive create would have: a leftover that holds no
// catalog attribute is taken over, and anything else — a file, or a directory
// that carries a record — is a taken name. The type is checked on the entry the
// write carries, so a file that replaces the directory cannot be tagged: the
// filer refuses to write the directory entry over it. It reports that refusal
// only as an opaque failure, so a failed write is followed by a look at what
// holds the name now: a file there is a taken name, not a server error.
func (h *S3TablesHandler) retakeDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, attrs map[string][]byte) error {
	err := h.mutateEntry(ctx, client, path, func(entry *filer_pb.Entry) error {
		if !entry.IsDirectory {
			return fmt.Errorf("%w: %s is not a directory", ErrNameTaken, path)
		}
		return claimExtended(entry.Extended, path, attrs)
	})
	if err == nil || errors.Is(err, ErrNameTaken) {
		return err
	}
	if current, lookupErr := h.lookupEntry(ctx, client, path); lookupErr == nil && !current.IsDirectory {
		return fmt.Errorf("%w: %s is not a directory", ErrNameTaken, path)
	}
	return err
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

// readNamespaceMetadataAttr returns the namespace-metadata xattr at
// namespacePath. A directory without it is not a namespace — a delete that kept
// the data under it leaves one — so its absence reads as a missing entry. So
// does a namespace still marked by its create: like a table's, it is absent to
// every reader until the create confirms it.
func (h *S3TablesHandler) readNamespaceMetadataAttr(ctx context.Context, client filer_pb.SeaweedFilerClient, namespacePath string) ([]byte, error) {
	entry, err := h.lookupEntry(ctx, client, namespacePath)
	if err != nil {
		return nil, err
	}
	data, ok := entry.Extended[ExtendedKeyMetadata]
	if !ok || isUnfinishedCreate(entry.Extended) {
		return nil, fmt.Errorf("%w: %s", filer_pb.ErrNotFound, namespacePath)
	}
	return data, nil
}

// isFinishedNamespaceEntry is isNamespaceEntry for a namespace its create has
// confirmed, the only kind a reader or a delete may act on.
func isFinishedNamespaceEntry(entry *filer_pb.Entry) bool {
	return isNamespaceEntry(entry) && !isUnfinishedCreate(entry.Extended)
}

// loadNamespaceMetadata resolves a namespace to its metadata. A directory that
// carries no namespace metadata is not a namespace, so a missing attribute
// reports the same absence as a missing entry and every caller tests one
// condition instead of forgetting the second.
func (h *S3TablesHandler) loadNamespaceMetadata(ctx context.Context, filerClient FilerClient, bucketName, namespaceName string) (*namespaceMetadata, error) {
	var metadata namespaceMetadata
	err := filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := h.readNamespaceMetadataAttr(ctx, client, GetNamespacePath(bucketName, namespaceName))
		if err != nil {
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

// readTableMetadataAttr returns the table-metadata xattr at tablePath. A
// directory without it is not a table, so its absence reads as a missing entry.
func (h *S3TablesHandler) readTableMetadataAttr(ctx context.Context, client filer_pb.SeaweedFilerClient, tablePath string) ([]byte, error) {
	data, err := h.getExtendedAttribute(ctx, client, tablePath, ExtendedKeyMetadata)
	if errors.Is(err, ErrAttributeNotFound) {
		return nil, fmt.Errorf("%w: %s", filer_pb.ErrNotFound, tablePath)
	}
	return data, err
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
// delete with ErrConcurrentUpdate instead of losing the replacement. The raw
// RPC, not DoRemoveRequest: the filer answers a precondition delete on an
// entry that is already gone with FailedPrecondition carrying the not-found
// text, which that helper reads as a completed delete.
func (h *S3TablesHandler) deleteDirectoryIfMatches(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, expected *filer_pb.Entry) error {
	dir, name := splitPath(path)
	resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
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
	if err != nil {
		return err
	}
	if resp != nil && resp.Error != "" {
		return errors.New(resp.Error)
	}
	return nil
}

// errDirNotEmpty is a directory that holds something the caller meant to leave
// alone: a file in a leftover tree, or an entry the filer found in a directory
// it was asked to delete without its contents.
var errDirNotEmpty = errors.New("directory is not empty")

// deleteEmptyDirectoryIfMatches deletes a directory without its contents, only
// while it still carries the extended attributes the caller captured. The
// filer checks both under the path lock, in the one delete: a changed
// attribute is ErrConcurrentUpdate, and an entry that appeared in the
// directory is errDirNotEmpty, so neither a directory taken back since the
// caller looked at it nor a file written into it since is lost. The mark of an
// empty S3 object stored on the directory is asserted absent too, so one
// written since is kept the same way.
func (h *S3TablesHandler) deleteEmptyDirectoryIfMatches(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, expected *filer_pb.Entry) error {
	dir, name := splitPath(path)
	resp, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:        dir,
		Name:             name,
		IsDeleteData:     true,
		ExpectedExtended: snapshotExtended(expected.Extended, s3_constants.SeaweedFSPrefixObject),
	})
	if status.Code(err) == codes.FailedPrecondition {
		return fmt.Errorf("%w: %s", ErrConcurrentUpdate, path)
	}
	if err != nil {
		return err
	}
	if resp != nil && resp.Error != "" {
		if err := filer.DeleteEntryError(resp.Error); !errors.Is(err, filer.ErrNonEmptyFolder) {
			return err
		}
		return fmt.Errorf("%w: %s", errDirNotEmpty, path)
	}
	return nil
}

var (
	errParentDeleting = errors.New("parent is being deleted")
	errParentGone     = errors.New("parent was deleted")
	errParentCreating = errors.New("parent is still being created")
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

// tryReleaseDeleteClaim removes claim from path once, while path still holds
// it. A claim another delete replaced or lifted, or an entry that is gone,
// leaves nothing to release and is not an error.
func (h *S3TablesHandler) tryReleaseDeleteClaim(ctx context.Context, client filer_pb.SeaweedFilerClient, path, claim string) error {
	err := h.mutateEntryExtended(ctx, client, path, func(extended map[string][]byte) error {
		if string(extended[ExtendedKeyDeleting]) != claim {
			return errNoDeleteClaim
		}
		delete(extended, ExtendedKeyDeleting)
		return nil
	})
	if errors.Is(err, errNoDeleteClaim) || errors.Is(err, filer_pb.ErrNotFound) {
		return nil
	}
	return err
}

// releaseDeleteClaim removes claim from path while path still holds it, even
// after the request was cancelled.
func (h *S3TablesHandler) releaseDeleteClaim(ctx context.Context, client filer_pb.SeaweedFilerClient, path, claim string) {
	if err := h.tryReleaseDeleteClaim(context.WithoutCancel(ctx), client, path, claim); err != nil {
		glog.Warningf("release delete claim on %s: %v", path, err)
	}
}

// releaseDeleteClaimRetrying is releaseDeleteClaim for a claim that does harm
// while it stays: a bucket's refuses every create in it, a leftover directory's
// makes a create read its name as taken, and one on a directory under it counts
// as a child of the namespace it may become. It retries a failed release
// the way a rename retries lifting its source claim. Unlike there, a release
// that lost to a write landing in between is retried too: the bucket entry
// takes other writes, such as PutTableBucketPolicy, where nothing else writes a
// renamed source.
func (h *S3TablesHandler) releaseDeleteClaimRetrying(ctx context.Context, client filer_pb.SeaweedFilerClient, path, claim string) {
	ctx = context.WithoutCancel(ctx)
	var err error
	for attempt := 0; attempt < extendedAttributeUpdateAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(100 * time.Millisecond)
		}
		if err = h.tryReleaseDeleteClaim(ctx, client, path, claim); err == nil {
			return
		}
	}
	glog.Warningf("release delete claim on %s: %v", path, err)
}

// isTableBucketCreatedAt accepts only the table bucket created at createdAt. A
// bucket deleted and created again under the same name has a CreatedAt of its
// own, so a create authorized against the first reads the second as gone.
func isTableBucketCreatedAt(createdAt time.Time) func(*filer_pb.Entry) bool {
	return func(entry *filer_pb.Entry) bool {
		if !IsTableBucketEntry(entry) {
			return false
		}
		var metadata tableBucketMetadata
		if json.Unmarshal(entry.Extended[ExtendedKeyMetadata], &metadata) != nil {
			return false
		}
		return metadata.CreatedAt.Equal(createdAt)
	}
}

// isNamespaceEntry reports whether entry carries a namespace's catalog record.
func isNamespaceEntry(entry *filer_pb.Entry) bool {
	_, ok := entry.Extended[ExtendedKeyMetadata]
	return ok
}

// checkParentOpen returns errParentGone if parentPath is missing or not a
// catalog entry, errParentCreating if the create that wrote it has not
// confirmed it yet, and errParentDeleting if a delete has claimed it.
func (h *S3TablesHandler) checkParentOpen(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool) error {
	_, err := h.openParent(ctx, client, parentPath, isParent)
	return err
}

// openParent is checkParentOpen returning the entry it accepted, so a caller
// can later ask whether the parent is still that same record.
func (h *S3TablesHandler) openParent(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool) (*filer_pb.Entry, error) {
	entry, err := h.lookupEntry(ctx, client, parentPath)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, errParentGone
	}
	if err != nil {
		return nil, err
	}
	if !isParent(entry) {
		return nil, errParentGone
	}
	if isUnfinishedCreate(entry.Extended) {
		return nil, errParentCreating
	}
	if _, claimed := entry.Extended[ExtendedKeyDeleting]; claimed {
		return nil, errParentDeleting
	}
	return entry, nil
}

// parentStillOpen reports whether parentPath still holds the record parent
// captured. A delete claims, removes, or replaces the parent between the two
// reads; createdAt identifies the record since an update preserves it while a
// delete+recreate mints a new one, so an editable property changing in
// between does not read as a removal.
func (h *S3TablesHandler) parentStillOpen(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool, parent *filer_pb.Entry) error {
	current, err := h.openParent(ctx, client, parentPath, isParent)
	if err != nil {
		return err
	}
	var before, now namespaceMetadata
	if json.Unmarshal(parent.Extended[ExtendedKeyMetadata], &before) != nil ||
		json.Unmarshal(current.Extended[ExtendedKeyMetadata], &now) != nil ||
		!now.CreatedAt.Equal(before.CreatedAt) {
		return errParentGone
	}
	return nil
}

// createClaim is the ExtendedKeyDeleting value of a catalog record whose
// create has written it but not yet confirmed its parent. renameDestClaim is
// the same not-yet-published mark on a rename's destination, with a value of
// its own so a create refuses it instead of adopting the record.
const (
	createClaim     = "create"
	renameDestClaim = "rename-dest"
)

// isUnfinishedCreate reports whether extended belongs to a record still
// marked by the operation that wrote it — invisible to every reader.
func isUnfinishedCreate(extended map[string][]byte) bool {
	switch string(extended[ExtendedKeyDeleting]) {
	case createClaim, renameDestClaim:
		return true
	}
	return false
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
			if _, ok := extended[ExtendedKeyMetadata]; !ok {
				return filer_pb.ErrNotFound
			}
			return mutate(extended)
		}
		if isUnfinishedCreate(extended) {
			return filer_pb.ErrNotFound
		}
		return fmt.Errorf("%w: %s claimed on %s", ErrConcurrentUpdate, ExtendedKeyDeleting, path)
	})
}

// confirmParent re-checks the parent after a create wrote written onto
// childPath. If the parent is open, it clears the record's unfinished mark —
// then checks the parent once more, since a delete claiming it in between
// scans the still-marked record, skips it, and removes it under a create that
// already answered success. If the parent is gone or claimed at either check,
// it removes what the create wrote and returns why, or errBackOutFailed if
// the record stays.
func (h *S3TablesHandler) confirmParent(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool, childPath string, written map[string][]byte, emptyDirs ...string) error {
	return h.confirmChild(ctx, client, parentPath, isParent, true, childPath, written, emptyDirs)
}

// confirmParentOnce is confirmParent for a record that other creates build on,
// a namespace. Checking the parent again after the mark comes off would leave a
// window in which a child is created in the record and the check then removes
// the record under it, so the parent is checked once, before the mark comes
// off. That is enough while a delete of the parent counts the marked record: one
// claiming the parent after the check scans the record and is refused.
func (h *S3TablesHandler) confirmParentOnce(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool, childPath string, written map[string][]byte, emptyDirs ...string) error {
	return h.confirmChild(ctx, client, parentPath, isParent, false, childPath, written, emptyDirs)
}

func (h *S3TablesHandler) confirmChild(ctx context.Context, client filer_pb.SeaweedFilerClient, parentPath string, isParent func(*filer_pb.Entry) bool, recheck bool, childPath string, written map[string][]byte, emptyDirs []string) error {
	parent, err := h.openParent(ctx, client, parentPath, isParent)
	confirmed := false
	if err == nil {
		// The mark comes off only while the record still holds what this
		// create wrote; a failed write backs the record out like a closed parent.
		if err = h.removeExtendedAttributesIf(ctx, client, childPath, written, ExtendedKeyDeleting); err == nil {
			confirmed = true
			if recheck {
				err = h.parentStillOpen(ctx, client, parentPath, isParent, parent)
			}
		}
	}
	if err == nil {
		return nil
	}
	ctx = context.WithoutCancel(ctx)
	expected := written
	keys := make([]string, 0, len(written))
	if confirmed {
		// The confirmed record no longer carries the mark written expected.
		expected = make(map[string][]byte, len(written))
		for key, data := range written {
			if key == ExtendedKeyDeleting {
				continue
			}
			expected[key] = data
			keys = append(keys, key)
		}
	} else {
		for key := range written {
			keys = append(keys, key)
		}
	}
	if backOutErr := h.removeExtendedAttributesIf(ctx, client, childPath, expected, keys...); backOutErr != nil && !errors.Is(backOutErr, filer_pb.ErrNotFound) {
		glog.Warningf("back out %s: %v", childPath, backOutErr)
		if errors.Is(backOutErr, ErrConcurrentUpdate) {
			// The record moved past this create — adopted or rewritten — so
			// there is nothing of its own left to remove.
			return fmt.Errorf("%w: %s changed during create", ErrConcurrentUpdate, childPath)
		}
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

// dirListPageSize is how many entries dirHoldsFiles reads per ListEntries call.
const dirListPageSize = 1024

// dirHoldsFiles reports whether any file sits under dirPath, at any depth. It
// walks breadth-first, pages through each directory, and stops at the first
// file it sees. Returning at that file abandons the page being read, so the
// listings run on a context of their own that ends with the call: otherwise
// the filer keeps producing that page until the whole request is over.
func (h *S3TablesHandler) dirHoldsFiles(ctx context.Context, client filer_pb.SeaweedFilerClient, dirPath string) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pending := []string{dirPath}
	for len(pending) > 0 {
		dir := pending[0]
		pending = pending[1:]

		lastFileName := ""
		for {
			resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
				Directory:          dir,
				Limit:              dirListPageSize,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "",
			})
			if err != nil {
				if status.Code(err) == codes.NotFound {
					// the directory is gone, so there is nothing under it to keep
					break
				}
				return false, err
			}

			received := false
			for {
				entry, err := resp.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.NotFound {
						break
					}
					return false, err
				}
				if entry.Entry == nil {
					continue
				}
				received = true
				lastFileName = entry.Entry.Name
				if !entry.Entry.IsDirectory {
					return true, nil
				}
				pending = append(pending, path.Join(dir, entry.Entry.Name))
			}
			if !received {
				break
			}
		}
	}
	return false, nil
}

// claimTreeDirectory writes a fresh delete claim onto the directory at dirPath
// while it still carries expected, and returns the claim with the attributes it
// left. A claim seen in expected, one a delete that died left, is replaced like
// any other attribute; one written since fails the compare.
//
// The claim is what ties the later delete to this directory: the delete
// requires it, and only this directory carries it. A file or an S3 object at
// dirPath is refused with errDirNotEmpty, whether it was there when the entry
// was read or took the directory's place before the write, which the filer
// refuses since it never writes a directory entry over a file. One that takes
// the place after the write lacks the claim, so the delete is refused instead.
// An S3 object landing on the directory between the read and the write carries
// the prefix-object mark, which the write asserts absent, so the write fails
// instead of putting the bare directory back over the object.
func (h *S3TablesHandler) claimTreeDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, dirPath string, expected map[string][]byte) (string, map[string][]byte, error) {
	claim := generateVersionToken()
	var claimed map[string][]byte
	err := h.mutateEntryAsserting(ctx, client, dirPath, []string{s3_constants.SeaweedFSPrefixObject}, func(entry *filer_pb.Entry) error {
		if !entry.IsDirectory || entry.IsDirectoryKeyObject() {
			return fmt.Errorf("%w: %s is an object", errDirNotEmpty, dirPath)
		}
		for key, want := range expected {
			if !bytes.Equal(entry.Extended[key], want) {
				return fmt.Errorf("%w: %s changed on %s", ErrConcurrentUpdate, key, dirPath)
			}
		}
		entry.Extended[ExtendedKeyDeleting] = []byte(claim)
		claimed = snapshotExtended(entry.Extended)
		return nil
	})
	if errors.Is(err, filer_pb.ErrNotFound) {
		return "", nil, fmt.Errorf("%w: %s", ErrConcurrentUpdate, dirPath)
	}
	if err != nil && !errors.Is(err, errDirNotEmpty) && !errors.Is(err, ErrConcurrentUpdate) {
		// The filer reports the refused write over a file only as an opaque
		// failure, so look at what holds the path now.
		if current, lookupErr := h.lookupEntry(ctx, client, dirPath); lookupErr == nil && !current.IsDirectory {
			return "", nil, fmt.Errorf("%w: %s is an object", errDirNotEmpty, dirPath)
		}
	}
	if err != nil {
		return "", nil, err
	}
	return claim, claimed, nil
}

// removeTreeDirectory removes a directory the walk listed as listed, under a
// claim of its own, and lifts the claim if the delete is refused.
func (h *S3TablesHandler) removeTreeDirectory(ctx context.Context, client filer_pb.SeaweedFilerClient, dirPath string, listed *filer_pb.Entry) error {
	claim, claimed, err := h.claimTreeDirectory(ctx, client, dirPath, snapshotExtended(listed.Extended))
	if err != nil {
		return err
	}
	if err := h.deleteEmptyDirectoryIfMatches(ctx, client, dirPath, &filer_pb.Entry{Extended: claimed}); err != nil {
		h.releaseDeleteClaimRetrying(ctx, client, dirPath, claim)
		return err
	}
	return nil
}

// removeEmptyTree removes dirPath and every directory under it, provided no
// file, no S3 object stored on a directory entry, and no catalog record sits
// anywhere in the tree, dirPath included. dirEntry is dirPath as the caller saw
// it.
//
// dirPath is claimed first, in a write that requires the attributes the caller
// saw: a directory taken back as a namespace since then fails it, before
// anything under it is touched, and the claim keeps it from being taken back
// while the tree goes. With dirPath no namespace, nothing under it can become a
// catalog record either, so a record the walk meets was there before and is
// refused, not removed.
//
// The walk lists the tree a page at a time and stops at the first file, object
// or record with errDirNotEmpty, before anything is removed. Directories then go
// deepest first, each claimed with the attributes it had when listed and
// deleted under that claim, which refuses a directory that holds an entry by
// now or was replaced by a file. dirPath goes last, still under its claim; any
// other way out lifts the claim.
func (h *S3TablesHandler) removeEmptyTree(ctx context.Context, client filer_pb.SeaweedFilerClient, dirPath string, dirEntry *filer_pb.Entry) (err error) {
	claim, claimed, err := h.claimTreeDirectory(ctx, client, dirPath, snapshotExtended(dirEntry.Extended))
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			h.releaseDeleteClaimRetrying(ctx, client, dirPath, claim)
		}
	}()

	type listedDir struct {
		path  string
		entry *filer_pb.Entry
	}
	listed := []listedDir{{dirPath, &filer_pb.Entry{Extended: claimed}}}

	walkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// A directory is listed after its parent, so removing in reverse order
	// removes every directory after the ones under it.
	for next := 0; next < len(listed); next++ {
		dir := listed[next].path
		lastFileName := ""
		for {
			resp, err := client.ListEntries(walkCtx, &filer_pb.ListEntriesRequest{
				Directory:          dir,
				Limit:              dirListPageSize,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: lastFileName == "",
			})
			if err != nil {
				if status.Code(err) == codes.NotFound {
					break // gone, so its delete below reports the change
				}
				return err
			}

			received := false
			for {
				entry, err := resp.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.NotFound {
						break
					}
					return err
				}
				if entry.Entry == nil {
					continue
				}
				received = true
				lastFileName = entry.Entry.Name
				child := path.Join(dir, entry.Entry.Name)
				if !entry.Entry.IsDirectory || entry.Entry.IsDirectoryKeyObject() {
					return fmt.Errorf("%w: %s is an object", errDirNotEmpty, child)
				}
				if _, isRecord := entry.Entry.Extended[ExtendedKeyMetadata]; isRecord {
					return fmt.Errorf("%w: %s is a catalog record", errDirNotEmpty, child)
				}
				listed = append(listed, listedDir{child, entry.Entry})
			}
			if !received {
				break
			}
		}
	}

	for i := len(listed) - 1; i > 0; i-- {
		if err := h.removeTreeDirectory(ctx, client, listed[i].path, listed[i].entry); err != nil {
			return err
		}
	}
	return h.deleteEmptyDirectoryIfMatches(ctx, client, dirPath, &filer_pb.Entry{Extended: claimed})
}

// keepAsLeftover clears a namespace's catalog attributes and the delete's
// claim in one write, while the entry still holds claimed, keeping the
// directory and the data under it. The write compares against claimed so the
// delete's token still fails an ABA, and it drops the claim itself: a kept
// directory that carried it could never be claimed again. A directory removed
// since the scan is a concurrent change like any other the compare would
// catch, so its not-found answers ErrConcurrentUpdate rather than a filer
// failure.
func (h *S3TablesHandler) keepAsLeftover(ctx context.Context, client filer_pb.SeaweedFilerClient, path string, claimed map[string][]byte) error {
	keys := make([]string, 0, len(s3tablesExtendedKeys))
	for _, key := range s3tablesExtendedKeys {
		if _, ok := claimed[key]; ok {
			keys = append(keys, key)
		}
	}
	err := h.removeExtendedAttributesIf(ctx, client, path, claimed, keys...)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrConcurrentUpdate, path)
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
