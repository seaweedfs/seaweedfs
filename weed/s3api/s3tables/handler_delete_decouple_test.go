package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runDeleteTable(t *testing.T, m *Manager, fs *s3tablestest.MemFiler, namespace, name string) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.Client), "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{namespace},
		Name:           name,
	}, nil, "")
}

// A write that lands between the delete's read and its conditional commit —
// an xattr update keeps mtime — must turn a token delete into a conflict, not
// get erased by it.
func TestDeleteTableTokenDeleteLosesToConcurrentUpdate(t *testing.T) {
	fs, m := startRenameManager(t)

	token := "tok-ct"
	meta, _ := json.Marshal(tableMetadataInternal{
		Name:           "ct",
		Namespace:      "ns",
		Format:         "ICEBERG",
		OwnerAccountID: DefaultAccountID,
		VersionToken:   token,
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "ct", map[string][]byte{
		ExtendedKeyMetadata: meta,
	})
	tablePath := GetTablePath(renameTestBucket, "ns", "ct")
	fs.Put(tablePath, "data", nil)

	fs.BeforeDelete = func() {
		fs.Put(GetNamespacePath(renameTestBucket, "ns"), "ct", map[string][]byte{
			ExtendedKeyMetadata: meta,
			ExtendedKeyTags:     []byte(`{"k":"v"}`),
		})
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "ct",
		VersionToken:   token,
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeConflict, resp.Type)

	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "ct"), "the updated record must survive")
	assert.NotNil(t, fs.Get(tablePath, "data"), "the table's data must survive")
}

// A create that checked the name while it was free must not write over a
// delete's restore: the claim loses instead of layering new metadata over the
// old tags, policy, and maintenance settings.
func TestCreateTableLosesToRestoredMarkers(t *testing.T) {
	fs, m := startRenameManager(t)

	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.Put(nsPath, "newt", nil)

	oldMeta, _ := json.Marshal(tableMetadataInternal{
		Name:           "newt",
		Namespace:      "ns",
		Format:         "ICEBERG",
		OwnerAccountID: DefaultAccountID,
	})
	restored := map[string][]byte{
		ExtendedKeyMetadata:    oldMeta,
		ExtendedKeyEntryType:   []byte(EntryTypeTable),
		ExtendedKeyTags:        []byte(`{"old":"tag"}`),
		ExtendedKeyMaintenance: []byte(`{"old":"maintenance"}`),
	}
	fs.BeforeUpdate = func() {
		fs.Put(nsPath, "newt", restored)
	}

	status, resp := runHandler(t, m, fs.Client, "CreateTable", &CreateTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "newt",
		Format:         FormatIceberg,
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeTableAlreadyExists, resp.Type)

	got := fs.Get(nsPath, "newt")
	require.NotNil(t, got)
	assert.Equal(t, restored[ExtendedKeyTags], got.Extended[ExtendedKeyTags], "restored markers must not be overwritten")
	assert.Equal(t, restored[ExtendedKeyMetadata], got.Extended[ExtendedKeyMetadata])
}

// A table whose data was decoupled from its name (created over a leftover from a
// rename): catalog marker at ns/newt, data at ns/newt-x, and ns/newt still holds
// another table's leftover data. Dropping it must purge only its own data and
// clear the marker, never the data under the reused name path.
func TestDeleteTableDecoupledKeepsReusedNamePath(t *testing.T) {
	fs, m := startRenameManager(t)

	newtMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "newt",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/newt-x/metadata/v1.metadata.json",
	})
	markerKeys := []string{ExtendedKeyMetadata, ExtendedKeyMetadataVersion, ExtendedKeyPolicy, ExtendedKeyTags, ExtendedKeyEntryType}
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "newt", map[string][]byte{
		ExtendedKeyMetadata:        newtMeta,
		ExtendedKeyMetadataVersion: []byte("v1"),
		ExtendedKeyPolicy:          []byte(`{"Version":"2012-10-17"}`),
		ExtendedKeyTags:            []byte(`{"k":"v"}`),
		ExtendedKeyEntryType:       []byte(EntryTypeTable),
	})
	fs.Put(GetTablePath(renameTestBucket, "ns", "newt"), "leftover", nil) // another table's data under the name path
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "newt-x", nil)       // this table's own (decoupled) data
	fs.Put(GetTablePath(renameTestBucket, "ns", "newt-x"), "metadata", nil)

	require.NoError(t, runDeleteTable(t, m, fs, "ns", "newt"))

	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "newt-x"),
		"the table's own data location must be purged")
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "newt"), "leftover"),
		"data under the reused name path must survive")
	marker := fs.Get(GetNamespacePath(renameTestBucket, "ns"), "newt")
	require.NotNil(t, marker)
	for _, key := range markerKeys {
		_, present := marker.Extended[key]
		assert.Falsef(t, present, "catalog attribute %s must be cleared", key)
	}
}

// A table whose MetadataLocation resolves to an ancestor of its own name path
// (here the namespace root, e.g. from corrupt metadata) must not be deleted: a
// recursive purge of that data path would wipe sibling tables. The delete is
// refused and the namespace's other tables survive.
func TestDeleteTableRefusesAncestorDataPath(t *testing.T) {
	fs, m := startRenameManager(t)

	badMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "badt",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/metadata/v1.metadata.json",
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "badt", map[string][]byte{ExtendedKeyMetadata: badMeta})

	require.Error(t, runDeleteTable(t, m, fs, "ns", "badt"))

	// The sibling table seeded by startRenameManager and its data must survive.
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"),
		"sibling table marker must survive a refused delete")
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "t"), "data"),
		"sibling table data must survive a refused delete")
}

// A normal colocated table (data under its own name path) is removed wholesale.
func TestDeleteTableColocatedRemovesData(t *testing.T) {
	fs, m := startRenameManager(t)

	require.NoError(t, runDeleteTable(t, m, fs, "ns", "t"))

	assert.Nil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"), "colocated table entry must be deleted")
	assert.Nil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "t"), "metadata"), "colocated table data must be deleted")
}

// A table whose MetadataLocation points at a sibling that is still a live
// catalog entry must not be deleted: only the named table was authorized, so a
// recursive purge of that location would destroy another tenant's table.
func TestDeleteTableRefusesLiveSiblingDataPath(t *testing.T) {
	fs, m := startRenameManager(t)

	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"attackerns"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "attackerns", map[string][]byte{ExtendedKeyMetadata: nsMeta})

	decoyMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "decoy",
		Namespace:        "attackerns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v3.metadata.json",
	})
	fs.Put(GetNamespacePath(renameTestBucket, "attackerns"), "decoy", map[string][]byte{ExtendedKeyMetadata: decoyMeta})

	require.Error(t, runDeleteTable(t, m, fs, "attackerns", "decoy"))

	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "ns"), "t"),
		"the targeted table's catalog entry must survive")
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "t"), "data"),
		"the targeted table's data must survive")
}

// A location inside another table's directory carries no catalog marker of its
// own — the marker lives on the name path. Dropping a table whose
// MetadataLocation resolves into nsa/vtab/data must be refused by the ancestor
// check, or the victim's marker-less data subdirectory is purged wholesale.
func TestDeleteTableRefusesNestedSiblingDataPath(t *testing.T) {
	fs, m := startRenameManager(t)

	nsaMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"nsa"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "nsa", map[string][]byte{ExtendedKeyMetadata: nsaMeta})

	victimMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "vtab",
		Namespace:        "nsa",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/nsa/vtab/metadata/v1.metadata.json",
	})
	victimTablePath := GetTablePath(renameTestBucket, "nsa", "vtab")
	fs.Put(GetNamespacePath(renameTestBucket, "nsa"), "vtab", map[string][]byte{
		ExtendedKeyMetadata:  victimMeta,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})
	fs.Put(victimTablePath, "data", nil)
	fs.Put(path.Join(victimTablePath, "data"), "important-0001.parquet", nil)

	evilMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "evil",
		Namespace:        "attackerns2",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataLocation: "s3://" + renameTestBucket + "/nsa/vtab/data/metadata/v1.metadata.json",
	})
	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"attackerns2"}, OwnerAccountID: DefaultAccountID})
	fs.Put(GetTableBucketPath(renameTestBucket), "attackerns2", map[string][]byte{ExtendedKeyMetadata: nsMeta})
	fs.Put(GetNamespacePath(renameTestBucket, "attackerns2"), "evil", map[string][]byte{
		ExtendedKeyMetadata:  evilMeta,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})

	require.Error(t, runDeleteTable(t, m, fs, "attackerns2", "evil"))

	assert.NotNil(t, fs.Get(path.Join(victimTablePath, "data"), "important-0001.parquet"),
		"data inside another table's directory must survive")
	assert.NotNil(t, fs.Get(GetNamespacePath(renameTestBucket, "nsa"), "vtab"),
		"the victim table's catalog entry must survive")
}

// A rename that completes in the token delete's check-to-claim window moves
// the record to another name: the stale delete must lose on the claim instead
// of purging the data the moved record still points at.
func TestDeleteTableTokenDeleteLosesToCompletedRename(t *testing.T) {
	fs, m := startRenameManager(t)

	token := "tok-rn"
	meta, _ := json.Marshal(tableMetadataInternal{
		Name:             "rn",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		VersionToken:     token,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/old-x/metadata/v1.metadata.json",
	})
	fs.Put(GetNamespacePath(renameTestBucket, "ns"), "rn", map[string][]byte{
		ExtendedKeyMetadata:  meta,
		ExtendedKeyEntryType: []byte(EntryTypeTable),
	})
	dataPath := GetTablePath(renameTestBucket, "ns", "old-x")
	fs.Put(dataPath, "data", nil)

	fs.BeforeUpdate = func() {
		// The rename's source clear has already run by the time the delete's
		// claim write lands.
		fs.Put(GetNamespacePath(renameTestBucket, "ns"), "rn", nil)
	}

	status, resp := runHandler(t, m, fs.Client, "DeleteTable", &DeleteTableRequest{
		TableBucketARN: mustBucketARN(t),
		Namespace:      []string{"ns"},
		Name:           "rn",
		VersionToken:   token,
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeConflict, resp.Type)

	assert.NotNil(t, fs.Get(dataPath, "data"), "the moved table's data must survive the stale delete")
}

// A delete's claim on the source must turn a rename into a conflict before
// any destination entry exists: the claim is what keeps a losing rename from
// leaving a destination marker pointing at data the delete went on to purge.
func TestRenameTableLosesToDeleteClaim(t *testing.T) {
	fs, m := startRenameManager(t)

	nsPath := GetNamespacePath(renameTestBucket, "ns")
	fs.BeforeUpdate = func() {
		e := fs.Get(nsPath, "t")
		claimed := map[string][]byte{}
		for k, v := range e.Extended {
			claimed[k] = v
		}
		claimed[ExtendedKeyDeleting] = []byte("delete")
		fs.Put(nsPath, "t", claimed)
	}

	status, resp := runHandler(t, m, fs.Client, "RenameTable", &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, ErrCodeConflict, resp.Type)

	assert.Nil(t, fs.Get(nsPath, "t2"), "a losing rename must not publish the destination")
	src := fs.Get(nsPath, "t")
	require.NotNil(t, src)
	assert.NotNil(t, src.Extended[ExtendedKeyMetadata], "the claimed source record must survive")
	assert.NotNil(t, fs.Get(GetTablePath(renameTestBucket, "ns", "t"), "data"))
}
