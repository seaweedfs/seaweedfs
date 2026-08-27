package s3tables

import (
	"context"
	"encoding/json"
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
